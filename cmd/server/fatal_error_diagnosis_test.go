package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// TestFatalErrorWithGzip tests that fatal HTTP error (415) is logged and persisted
// even when job is canceled due to the error
func TestFatalErrorWithGzip(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "um-ingest-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test CSV file
	totalRows := 10
	csvFile := filepath.Join(tmpDir, "test.csv")
	var csvBuilder strings.Builder
	csvBuilder.WriteString("Name,Date,Amount\n")
	for i := 1; i <= totalRows; i++ {
		csvBuilder.WriteString(fmt.Sprintf("item%d,01.01.2026,%d\n", i, i*10))
	}
	if err := os.WriteFile(csvFile, []byte(csvBuilder.String()), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Create httptest server that returns 415 with error message
	var receivedRequest bool
	dataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRequest = true
		w.WriteHeader(http.StatusUnsupportedMediaType) // 415
		w.Write([]byte("gzip not supported"))
	}))
	defer dataServer.Close()

	// Create job with gzip enabled
	j := &job.Job{
		PackageID: "test-pkg-gzip-error",
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
			Gzip:           true, // Enable gzip
			BatchSize:      5,
			TimeoutSeconds: 5,
			MaxRetries:     0, // No retries to fail fast
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

	// Wait for async operations to complete (short timeout with small sleep)
	deadline := time.Now().Add(500 * time.Millisecond)
	var finalJob *job.Job
	for time.Now().Before(deadline) {
		var err error
		finalJob, err = store.Get(jobID)
		if err != nil {
			t.Fatalf("Failed to get job: %v", err)
		}
		if finalJob.Status == job.StatusSucceeded || finalJob.Status == job.StatusFailed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if finalJob == nil {
		finalJob, _ = store.Get(jobID)
	}

	// Verify that request was received
	if !receivedRequest {
		t.Error("Expected HTTP request to be received, but it wasn't")
	}

	// Verify job status is failed or canceled (both are acceptable)
	if finalJob.Status != job.StatusFailed && finalJob.Status != job.StatusCanceled {
		t.Errorf("Expected job status to be failed or canceled, got %s", finalJob.Status)
	}

	// Verify that error message contains 415 and "gzip"
	if finalJob.LastError == "" {
		t.Error("Expected LastError to be set, but it's empty")
	} else {
		if !strings.Contains(finalJob.LastError, "415") {
			t.Errorf("Expected LastError to contain '415', got: %s", finalJob.LastError)
		}
		if !strings.Contains(finalJob.LastError, "gzip") {
			t.Errorf("Expected LastError to contain 'gzip', got: %s", finalJob.LastError)
		}
	}

	t.Logf("Job status: %s, LastError: %s", finalJob.Status, finalJob.LastError)
}

// TestFatalError406 tests that fatal HTTP error (406) on first batch
// results in job failed with lastError containing "406", rowsSent=0, batchesSent=0
func TestFatalError406(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "um-ingest-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test CSV file
	totalRows := 10
	csvFile := filepath.Join(tmpDir, "test.csv")
	var csvBuilder strings.Builder
	csvBuilder.WriteString("Name,Date,Amount\n")
	for i := 1; i <= totalRows; i++ {
		csvBuilder.WriteString(fmt.Sprintf("item%d,01.01.2026,%d\n", i, i*10))
	}
	if err := os.WriteFile(csvFile, []byte(csvBuilder.String()), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Create httptest server that returns 406 on first batch with delay
	var requestCount int
	dataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			// First batch returns 406 (Not Acceptable) after delay
			// This delay ensures that onFatalError/store.UpdateError completes before processJob exits
			time.Sleep(75 * time.Millisecond) // 50-100ms delay
			w.WriteHeader(http.StatusNotAcceptable) // 406
			w.Write([]byte("Not Acceptable"))
		} else {
			// Should not reach here
			t.Errorf("Unexpected request after first batch failure")
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer dataServer.Close()

	// Create job
	j := &job.Job{
		PackageID: "test-pkg-406",
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
			BatchSize:      5,
			TimeoutSeconds: 5,
			MaxRetries:     0, // No retries to fail fast
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

	// Wait for async operations to complete
	deadline := time.Now().Add(500 * time.Millisecond)
	var finalJob *job.Job
	for time.Now().Before(deadline) {
		var err error
		finalJob, err = store.Get(jobID)
		if err != nil {
			t.Fatalf("Failed to get job: %v", err)
		}
		if finalJob.Status == job.StatusSucceeded || finalJob.Status == job.StatusFailed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if finalJob == nil {
		finalJob, _ = store.Get(jobID)
	}

	// Verify that only one request was made
	if requestCount != 1 {
		t.Errorf("Expected 1 request, got %d", requestCount)
	}

	// Verify job status is failed (not canceled, because it has lastError)
	if finalJob.Status != job.StatusFailed {
		t.Errorf("Expected job status to be failed, got %s", finalJob.Status)
	}

	// Verify that error message contains "406"
	if finalJob.LastError == "" {
		t.Error("Expected LastError to be set, but it's empty")
	} else {
		if !strings.Contains(finalJob.LastError, "406") {
			t.Errorf("Expected LastError to contain '406', got: %s", finalJob.LastError)
		}
	}

	// Verify counters: rowsSent=0, batchesSent=0
	if finalJob.RowsSent != 0 {
		t.Errorf("Expected rowsSent=0, got %d", finalJob.RowsSent)
	}
	if finalJob.BatchesSent != 0 {
		t.Errorf("Expected batchesSent=0, got %d", finalJob.BatchesSent)
	}

	t.Logf("Job status: %s, LastError: %s, rowsSent=%d, batchesSent=%d", finalJob.Status, finalJob.LastError, finalJob.RowsSent, finalJob.BatchesSent)
}

// TestFatalErrorFromErrorsEndpoint tests that fatal HTTP error (406) from errors endpoint
// results in job failed with lastError containing "406" (not "context canceled")
func TestFatalErrorFromErrorsEndpoint(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "um-ingest-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test CSV file with one row that will cause an error
	csvFile := filepath.Join(tmpDir, "test.csv")
	var csvBuilder strings.Builder
	csvBuilder.WriteString("Name,Date,Amount\n")
	// Create a row with invalid date to trigger an error
	csvBuilder.WriteString("item1,invalid-date,100\n")
	if err := os.WriteFile(csvFile, []byte(csvBuilder.String()), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Create httptest server for data endpoint (returns 200 OK)
	var dataRequestCount int
	dataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dataRequestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer dataServer.Close()

	// Create httptest server for errors endpoint (returns 406)
	var errorsRequestCount int
	errorsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		errorsRequestCount++
		// Return 406 (Not Acceptable) with delay to ensure onFatalError completes
		time.Sleep(75 * time.Millisecond)
		w.WriteHeader(http.StatusNotAcceptable) // 406
		w.Write([]byte("Not Acceptable"))
	}))
	defer errorsServer.Close()

	// Create job with errorsEndpoint configured
	j := &job.Job{
		PackageID: "test-pkg-errors-406",
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
			ErrorsEndpoint: errorsServer.URL, // Configure errors endpoint
			Gzip:           false,
			BatchSize:      5,
			TimeoutSeconds: 5,
			MaxRetries:     0, // No retries to fail fast
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

	// Wait for async operations to complete
	deadline := time.Now().Add(500 * time.Millisecond)
	var finalJob *job.Job
	for time.Now().Before(deadline) {
		var err error
		finalJob, err = store.Get(jobID)
		if err != nil {
			t.Fatalf("Failed to get job: %v", err)
		}
		if finalJob.Status == job.StatusSucceeded || finalJob.Status == job.StatusFailed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if finalJob == nil {
		finalJob, _ = store.Get(jobID)
	}

	// Verify that errors endpoint was called
	if errorsRequestCount == 0 {
		t.Error("Expected errors endpoint to be called, but it wasn't")
	}

	// Verify job status is failed
	if finalJob.Status != job.StatusFailed {
		t.Errorf("Expected job status to be failed, got %s", finalJob.Status)
	}

	// Verify that error message contains "406" and NOT "context canceled"
	if finalJob.LastError == "" {
		t.Error("Expected LastError to be set, but it's empty")
	} else {
		if !strings.Contains(finalJob.LastError, "406") {
			t.Errorf("Expected LastError to contain '406', got: %s", finalJob.LastError)
		}
		if strings.Contains(finalJob.LastError, "context canceled") {
			t.Errorf("Expected LastError to NOT contain 'context canceled', got: %s", finalJob.LastError)
		}
	}

	t.Logf("Job status: %s, LastError: %s", finalJob.Status, finalJob.LastError)
}

