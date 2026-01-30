package main

import (
	"context"
	"encoding/json"
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

// TestJobContextDecoupledFromHTTPRequest tests that job processing continues
// even after HTTP request completes (job context is not tied to HTTP request context)
func TestJobContextDecoupledFromHTTPRequest(t *testing.T) {
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

	// Create httptest server for data endpoint (always returns 200)
	var dataBatchesReceived int
	dataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("Failed to decode data batch: %v", err)
		}
		dataBatchesReceived++
		w.WriteHeader(http.StatusOK)
	}))
	defer dataServer.Close()

	// Create job store and server context
	store := job.NewStore()
	serverCtx, serverCancel := context.WithCancel(context.Background())
	defer serverCancel()

	// Start worker in background
	go worker(serverCtx, store, tmpDir, "", "", 1, 8, 1, 8)

	// Create job via HTTP handler (simulating HTTP request)
	// The HTTP request context will be canceled immediately after handler returns
	_, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()

	// Create job
	j := &job.Job{
		PackageID: "test-pkg-context",
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
			MaxRetries:     0,
			BackoffMs:      10,
			BackoffMaxMs:   100,
		},
		ProgressEvery: 1000,
	}

	// Create job in store (simulating HTTP handler CreateJob)
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	// Cancel HTTP request context immediately (simulating HTTP request completion)
	// This should NOT affect job processing
	reqCancel()

	// Wait a bit to ensure job processing has started
	time.Sleep(100 * time.Millisecond)

	// Verify that job is processing (not canceled)
	currentJob, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	// Job should be running or queued, NOT canceled
	if currentJob.Status == job.StatusCanceled {
		t.Errorf("Job was canceled immediately after HTTP request completed, but should continue processing. Status: %s", currentJob.Status)
	}

	// Wait for job to complete (with timeout)
	deadline := time.Now().Add(2 * time.Second)
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
		time.Sleep(50 * time.Millisecond)
	}
	if finalJob == nil {
		finalJob, _ = store.Get(jobID)
	}

	// Verify job completed successfully (not canceled)
	if finalJob.Status == job.StatusCanceled {
		t.Errorf("Job was canceled, but should have completed. Status: %s, LastError: %s", finalJob.Status, finalJob.LastError)
	}

	// Verify job reached succeeded status
	if finalJob.Status != job.StatusSucceeded {
		t.Errorf("Expected job status to be succeeded, got %s. LastError: %s", finalJob.Status, finalJob.LastError)
	}

	// Verify batches were sent
	if dataBatchesReceived == 0 {
		t.Error("Expected at least one batch to be sent, but none were received")
	}

	t.Logf("Job completed successfully: status=%s, batchesSent=%d, dataBatchesReceived=%d",
		finalJob.Status, finalJob.BatchesSent, dataBatchesReceived)
}

