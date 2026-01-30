package ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestNoErrorsNoCallsToErrorsEndpoint(t *testing.T) {
	// Test that when errorsEndpoint is configured but no errors occur,
	// no HTTP calls are made to errorsEndpoint

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Age\nTest,25\n"
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-no-errors",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:      "Age",
					Type:     "int",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Age"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			ErrorsEndpoint: server.URL, // Configured but no errors should occur
			BatchSize:      10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()
	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	// Wait for processing to complete
	<-done

	// Should not have made any HTTP calls to errorsEndpoint
	if attempts != 0 {
		t.Errorf("Expected 0 calls to errorsEndpoint when no errors occur, got %d", attempts)
	}

	// Check that no error batches were sent
	select {
	case errorBatch := <-processor.GetErrorChan():
		if errorBatch != nil && len(errorBatch.Errors) > 0 {
			t.Errorf("Expected no error batches when no errors occur, got batch with %d errors", len(errorBatch.Errors))
		}
	default:
		// Good, no error batches
	}
}

func TestFirstErrorTriggersSend(t *testing.T) {
	// Test that when first error occurs, it creates error batch and sends to channel

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Age\n ,25\n" // Empty Name will trigger required field error
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-first-error",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: true, // Required field
					Source:   job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:      "Age",
					Type:     "int",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Age"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			ErrorsEndpoint: server.URL,
			BatchSize:      10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()
	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	// Wait for error batch (should be sent when error occurs)
	var errorBatch *ErrorBatch
	select {
	case errorBatch = <-processor.GetErrorChan():
		if errorBatch == nil {
			t.Fatal("Error batch is nil")
		}
		if len(errorBatch.Errors) == 0 {
			t.Fatal("Error batch should not be empty")
		}
		// Verify batch structure
		if errorBatch.PackageID != "test-first-error" {
			t.Errorf("Expected PackageID='test-first-error', got '%s'", errorBatch.PackageID)
		}
		if errorBatch.JobID != jobID {
			t.Errorf("Expected JobID='%s', got '%s'", jobID, errorBatch.JobID)
		}
		if errorBatch.BatchNo != 1 {
			t.Errorf("Expected BatchNo=1, got %d", errorBatch.BatchNo)
		}
	case <-done:
		t.Fatal("Processing completed without error batch")
	}

	// Wait for processing to complete
	<-done

	// Note: This test only verifies that error batch is created and sent to channel.
	// Actual HTTP call would be made by processJob in main.go, which is tested separately.
	// The key point is that error batch is created only when errors occur.
}

func TestDoNotSendEmptyChunkOnFlush(t *testing.T) {
	// Test that flush does not send empty error batch

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Age\nTest,25\n" // Valid data, no errors
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-no-empty-flush",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:      "Age",
					Type:     "int",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Age"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			ErrorsEndpoint: server.URL,
			BatchSize:      10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()
	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	// Wait for processing to complete
	<-done

	// Should not have made any HTTP calls (no errors, so no flush should send)
	if attempts != 0 {
		t.Errorf("Expected 0 calls to errorsEndpoint on flush with empty buffer, got %d", attempts)
	}

	// Check that no error batches were sent
	select {
	case errorBatch := <-processor.GetErrorChan():
		if errorBatch != nil && len(errorBatch.Errors) > 0 {
			t.Errorf("Expected no error batches on flush with empty buffer, got batch with %d errors", len(errorBatch.Errors))
		}
	default:
		// Good, no error batches
	}
}

