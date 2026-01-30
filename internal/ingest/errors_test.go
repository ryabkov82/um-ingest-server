package ingest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestErrorBatchFormat(t *testing.T) {
	// Create error batch
	errorBatch := &ErrorBatch{
		PackageID: "test-pkg",
		Errors: []ErrorItem{
			{
				RowNo:    123,
				Class:    "Техническая",
				Severity: "Ошибка",
				Code:     "НеПреобразуетсяВДату",
				Field:    "Date",
				Value:    "31-31-2026",
				Message:  "ожидаемый формат DD.MM.YYYY",
				TS:       "2026-01-26T22:10:00Z",
			},
		},
	}

	// Verify structure
	if errorBatch.PackageID != "test-pkg" {
		t.Errorf("Expected packageId 'test-pkg', got '%s'", errorBatch.PackageID)
	}

	if len(errorBatch.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(errorBatch.Errors))
	}

	errorItem := errorBatch.Errors[0]
	if errorItem.RowNo != 123 {
		t.Errorf("Expected rowNo 123, got %d", errorItem.RowNo)
	}
	if errorItem.Code != "НеПреобразуетсяВДату" {
		t.Errorf("Expected code 'НеПреобразуетсяВДату', got '%s'", errorItem.Code)
	}
	if errorItem.Field != "Date" {
		t.Errorf("Expected field 'Date', got '%s'", errorItem.Field)
	}
}

func TestErrorCounting(t *testing.T) {
	// Create a job with errorsEndpoint
	j := &job.Job{
		PackageID: "test-errors",
		CSV: job.CSVConfig{
			HasHeader: false,
			MapBy:     "order",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Fields: []job.FieldSpec{
				{
					Out:    "Date",
					Type:   "date",
					Source: job.SourceSpec{By: "order", Index: 0},
					DateFormat: "DD.MM.YYYY",
				},
			},
		},
		Delivery: job.DeliveryConfig{
			ErrorsEndpoint: "http://localhost/errors",
			BatchSize:      10,
		},
		InputPath: "/tmp/test.csv",
	}

	// Create a temporary CSV file with invalid date
	tmpFile := "/tmp/test_errors.csv"
	content := "invalid-date\n31.01.2026"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	j.InputPath = tmpFile

	store := job.NewStore()
	// Create job in store
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Create job error: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, "/tmp", nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	// Process in background
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	processErrChan := make(chan error, 1)
	go func() {
		processErrChan <- processor.Process(ctx)
	}()

	// Read error batches
	errorChan := processor.GetErrorChan()
	errorCount := 0
	
	// Read errors with timeout
	done := false
	for !done {
		select {
		case errorBatch, ok := <-errorChan:
			if !ok {
				done = true
				break
			}
			errorCount += len(errorBatch.Errors)
		case <-ctx.Done():
			done = true
		case err := <-processErrChan:
			if err != nil && err != context.Canceled {
				t.Logf("Processing error: %v", err)
			}
			done = true
		}
	}

	// Check error count
	total := processor.GetErrorsTotal()
	if total != 1 {
		t.Errorf("Expected 1 error total, got %d", total)
	}

	if errorCount != 1 {
		t.Errorf("Expected 1 error in batches, got %d", errorCount)
	}
}

