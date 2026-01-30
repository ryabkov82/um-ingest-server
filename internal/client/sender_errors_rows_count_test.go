package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
)

// TestErrorBatchRowsCountHeader tests that X-UM-RowsCount header is set correctly for error batches
func TestErrorBatchRowsCountHeader(t *testing.T) {
	// Create a test server that captures headers
	var receivedRowsCount string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRowsCount = r.Header.Get("X-UM-RowsCount")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create sender for errors endpoint
	sender := NewSender(
		server.URL,
		false, // no gzip
		1,     // timeoutSeconds
		0,     // maxRetries
		10,    // backoffMs
		100,   // backoffMaxMs
		nil,   // auth
		"",    // envUser
		"",    // envPass
		nil,   // timings
		true,  // isErrors = true for errors endpoint
	)

	// Create error batch with N elements
	N := 5
	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg",
		JobID:     "test-job",
		BatchNo:   1,
		Errors: []ingest.ErrorItem{
			{RowNo: 1, Code: "ERR1", Message: "Error 1"},
			{RowNo: 2, Code: "ERR2", Message: "Error 2"},
			{RowNo: 3, Code: "ERR3", Message: "Error 3"},
			{RowNo: 4, Code: "ERR4", Message: "Error 4"},
			{RowNo: 5, Code: "ERR5", Message: "Error 5"},
		},
	}

	// Send error batch
	ctx := context.Background()
	err := sender.SendErrorBatch(ctx, errorBatch)
	if err != nil {
		t.Fatalf("SendErrorBatch failed: %v", err)
	}

	// Check that X-UM-RowsCount header is present and correct
	if receivedRowsCount == "" {
		t.Fatal("X-UM-RowsCount header is missing")
	}
	if receivedRowsCount != strconv.Itoa(N) {
		t.Errorf("Expected X-UM-RowsCount='%d', got '%s'", N, receivedRowsCount)
	}
}

// TestErrorBatchRowsCountHeaderEmpty tests that X-UM-RowsCount is set to 0 for empty error batches
func TestErrorBatchRowsCountHeaderEmpty(t *testing.T) {
	// Create a test server that captures headers
	var receivedRowsCount string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRowsCount = r.Header.Get("X-UM-RowsCount")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create sender for errors endpoint
	sender := NewSender(
		server.URL,
		false,
		1,
		0,
		10,
		100,
		nil,
		"",
		"",
		nil,
		true,
	)

	// Create empty error batch
	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg",
		BatchNo:   1,
		Errors:    []ingest.ErrorItem{},
	}

	// Send error batch (should return nil immediately for empty batch)
	ctx := context.Background()
	err := sender.SendErrorBatch(ctx, errorBatch)
	if err != nil {
		t.Fatalf("SendErrorBatch failed: %v", err)
	}

	// Empty batch should not trigger HTTP request
	// So receivedRowsCount should remain empty
	if receivedRowsCount != "" {
		t.Errorf("Expected no HTTP request for empty batch, but got X-UM-RowsCount='%s'", receivedRowsCount)
	}
}

// TestErrorBatchRowsCountHeaderWithRetry tests that X-UM-RowsCount is set correctly even with retries
func TestErrorBatchRowsCountHeaderWithRetry(t *testing.T) {
	// Create a test server that fails first time, then succeeds
	attempts := 0
	var receivedRowsCount string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		receivedRowsCount = r.Header.Get("X-UM-RowsCount")
		if attempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503 - retryable
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	// Create sender with retries
	sender := NewSender(
		server.URL,
		false,
		1,
		1, // maxRetries = 1 (allows 1 retry)
		10,
		100,
		nil,
		"",
		"",
		nil,
		true,
	)

	// Create error batch with 3 elements
	N := 3
	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg",
		BatchNo:   1,
		Errors: []ingest.ErrorItem{
			{RowNo: 1, Code: "ERR1", Message: "Error 1"},
			{RowNo: 2, Code: "ERR2", Message: "Error 2"},
			{RowNo: 3, Code: "ERR3", Message: "Error 3"},
		},
	}

	// Send error batch
	ctx := context.Background()
	err := sender.SendErrorBatch(ctx, errorBatch)
	if err != nil {
		t.Fatalf("SendErrorBatch failed: %v", err)
	}

	// Check that X-UM-RowsCount header is present and correct on both attempts
	if receivedRowsCount != strconv.Itoa(N) {
		t.Errorf("Expected X-UM-RowsCount='%d', got '%s'", N, receivedRowsCount)
	}
	if attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", attempts)
	}
}

