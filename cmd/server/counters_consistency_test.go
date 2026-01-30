package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// TestCountersConsistency tests that job counters are consistent after async send
func TestCountersConsistency(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "um-ingest-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test CSV file with known number of rows
	// Total rows: 3951 (to test with batchSize=2000: 2 full batches + 1 partial)
	totalRows := 3951
	batchSize := 2000
	csvFile := filepath.Join(tmpDir, "test.csv")
	csvContent := "Name,Date,Amount\n"
	for i := 1; i <= totalRows; i++ {
		csvContent += fmt.Sprintf("item%d,01.01.2026,%d\n", i, i*10)
	}
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Create httptest servers for data and errors endpoints
	var dataBatchesReceived int
	var errorBatchesReceived int
	dataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch ingest.Batch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("Failed to decode data batch: %v", err)
		}
		dataBatchesReceived++
		w.WriteHeader(http.StatusOK)
	}))
	defer dataServer.Close()

	errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var errorBatch ingest.ErrorBatch
		if err := json.NewDecoder(r.Body).Decode(&errorBatch); err != nil {
			t.Errorf("Failed to decode error batch: %v", err)
		}
		errorBatchesReceived++
		w.WriteHeader(http.StatusOK)
	}))
	defer errorServer.Close()

	// Create job
	j := &job.Job{
		PackageID: "test-pkg-consistency",
		InputPath: csvFile,
		CSV: job.CSVConfig{
			Encoding:  "utf-8",
			Delimiter: ",",
			HasHeader: true,
			MapBy:     "header",
		},
		Schema: job.SchemaConfig{
			Register:     "TestRegister",
			IncludeRowNo: true,
			RowNoField:   "НомерСтрокиФайла",
			Fields: []job.FieldSpec{
				{Out: "Name", Type: "string", Source: job.SourceSpec{By: "header", Name: "Name"}},
				{Out: "Date", Type: "date", Source: job.SourceSpec{By: "header", Name: "Date"}, DateFormat: "DD.MM.YYYY"},
				{Out: "Amount", Type: "number", Source: job.SourceSpec{By: "header", Name: "Amount"}},
			},
		},
		Delivery: job.DeliveryConfig{
			Endpoint:       dataServer.URL,
			ErrorsEndpoint: errorServer.URL,
			Gzip:           false,
			BatchSize:      batchSize,
			TimeoutSeconds: 5,
			MaxRetries:     0,
			BackoffMs:      10,
			BackoffMaxMs:   100,
		},
		ProgressEvery: 1000,
	}

	// Create store and job
	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	// Process job
	processJob(context.Background(), j, store, tmpDir, "", "", 1, 8, 1, 8)

	// Wait for async operations to complete (with timeout)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		finalJob, _ := store.Get(jobID)
		if finalJob.Status == job.StatusSucceeded || finalJob.Status == job.StatusFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Get final job status
	finalJob, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	// Verify consistency
	expectedBatches := (totalRows + batchSize - 1) / batchSize // Ceiling division
	if finalJob.BatchesSent != int64(expectedBatches) {
		t.Errorf("BatchesSent mismatch: expected %d, got %d", expectedBatches, finalJob.BatchesSent)
	}

	if finalJob.RowsSent != int64(totalRows) {
		t.Errorf("RowsSent mismatch: expected %d, got %d", totalRows, finalJob.RowsSent)
	}

	if finalJob.BatchesSent != int64(dataBatchesReceived) {
		t.Errorf("BatchesSent (%d) != dataBatchesReceived (%d)", finalJob.BatchesSent, dataBatchesReceived)
	}

	// Verify that rowsSent == rowsRead - rowsSkipped (assuming no skipped rows)
	if finalJob.RowsSent != finalJob.RowsRead-finalJob.RowsSkipped {
		t.Errorf("RowsSent (%d) != RowsRead (%d) - RowsSkipped (%d)", finalJob.RowsSent, finalJob.RowsRead, finalJob.RowsSkipped)
	}

	// Verify currentBatchNo matches batchesSent (for full batches)
	if finalJob.CurrentBatchNo != finalJob.BatchesSent {
		t.Errorf("CurrentBatchNo (%d) != BatchesSent (%d)", finalJob.CurrentBatchNo, finalJob.BatchesSent)
	}

	t.Logf("Job completed: rowsRead=%d, rowsSent=%d, rowsSkipped=%d, batchesSent=%d, currentBatchNo=%d",
		finalJob.RowsRead, finalJob.RowsSent, finalJob.RowsSkipped, finalJob.BatchesSent, finalJob.CurrentBatchNo)
}

// TestCountersConsistencyPartialBatch tests consistency with partial last batch
func TestCountersConsistencyPartialBatch(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "um-ingest-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test CSV with non-multiple-of-batchSize rows
	// batchSize=10, totalRows=23 -> 3 batches (10, 10, 3)
	batchSize := 10
	totalRows := 23
	csvFile := filepath.Join(tmpDir, "test.csv")
	csvContent := "Name,Date,Amount\n"
	for i := 1; i <= totalRows; i++ {
		csvContent += fmt.Sprintf("item%d,01.01.2026,%d\n", i, i*10)
	}
	if err := os.WriteFile(csvFile, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Create httptest server for data endpoint
	var dataBatchesReceived int
	dataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch ingest.Batch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("Failed to decode data batch: %v", err)
		}
		dataBatchesReceived++
		w.WriteHeader(http.StatusOK)
	}))
	defer dataServer.Close()

	// Create job
	j := &job.Job{
		PackageID: "test-pkg-partial",
		InputPath: csvFile,
		CSV: job.CSVConfig{
			Encoding:  "utf-8",
			Delimiter: ",",
			HasHeader: true,
			MapBy:     "header",
		},
		Schema: job.SchemaConfig{
			Register:     "TestRegister",
			IncludeRowNo: true,
			RowNoField:   "НомерСтрокиФайла",
			Fields: []job.FieldSpec{
				{Out: "Name", Type: "string", Source: job.SourceSpec{By: "header", Name: "Name"}},
				{Out: "Date", Type: "date", Source: job.SourceSpec{By: "header", Name: "Date"}, DateFormat: "DD.MM.YYYY"},
				{Out: "Amount", Type: "number", Source: job.SourceSpec{By: "header", Name: "Amount"}},
			},
		},
		Delivery: job.DeliveryConfig{
			Endpoint:       dataServer.URL,
			Gzip:           false,
			BatchSize:      batchSize,
			TimeoutSeconds: 5,
			MaxRetries:     0,
			BackoffMs:      10,
			BackoffMaxMs:   100,
		},
		ProgressEvery: 1000,
	}

	// Create store and job
	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	// Process job
	processJob(context.Background(), j, store, tmpDir, "", "", 1, 8, 1, 8)

	// Wait for async operations to complete (with timeout)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		finalJob, _ := store.Get(jobID)
		if finalJob.Status == job.StatusSucceeded || finalJob.Status == job.StatusFailed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Get final job status
	finalJob, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	// Verify consistency with partial batch
	expectedBatches := 3 // 10, 10, 3
	if finalJob.BatchesSent != int64(expectedBatches) {
		t.Errorf("BatchesSent mismatch: expected %d, got %d", expectedBatches, finalJob.BatchesSent)
	}

	if finalJob.RowsSent != int64(totalRows) {
		t.Errorf("RowsSent mismatch: expected %d, got %d", totalRows, finalJob.RowsSent)
	}

	if finalJob.BatchesSent != int64(dataBatchesReceived) {
		t.Errorf("BatchesSent (%d) != dataBatchesReceived (%d)", finalJob.BatchesSent, dataBatchesReceived)
	}

	// Verify rowsSent == rowsRead - rowsSkipped
	if finalJob.RowsSent != finalJob.RowsRead-finalJob.RowsSkipped {
		t.Errorf("RowsSent (%d) != RowsRead (%d) - RowsSkipped (%d)", finalJob.RowsSent, finalJob.RowsRead, finalJob.RowsSkipped)
	}

	t.Logf("Job completed: rowsRead=%d, rowsSent=%d, rowsSkipped=%d, batchesSent=%d, currentBatchNo=%d",
		finalJob.RowsRead, finalJob.RowsSent, finalJob.RowsSkipped, finalJob.BatchesSent, finalJob.CurrentBatchNo)
}
