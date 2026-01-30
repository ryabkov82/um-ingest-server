package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
)

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestErrorBatchHeaders(t *testing.T) {
	var receivedHeaders http.Header
	var receivedBody ingest.ErrorBatch

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		json.NewDecoder(r.Body).Decode(&receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg-123",
		JobID:     "test-job-456",
		BatchNo:   1,
		Errors: []ingest.ErrorItem{
			{
				RowNo:    1,
				Class:    "Техническая",
				Severity: "Ошибка",
				Code:     "ПустоеОбязательноеПоле",
				Field:    "Name",
				Message:  "required field is empty",
			},
		},
	}

	sender := NewSender(server.URL, false, 5, 3, 100, 1000, nil, "", "", nil, false)
	ctx := context.Background()

	err := sender.SendErrorBatch(ctx, errorBatch)
	if err != nil {
		t.Fatalf("SendErrorBatch() error = %v", err)
	}

	// Check headers
	if receivedHeaders.Get("X-UM-PackageId") != "test-pkg-123" {
		t.Errorf("Expected X-UM-PackageId='test-pkg-123', got '%s'", receivedHeaders.Get("X-UM-PackageId"))
	}
	if receivedHeaders.Get("X-UM-JobId") != "test-job-456" {
		t.Errorf("Expected X-UM-JobId='test-job-456', got '%s'", receivedHeaders.Get("X-UM-JobId"))
	}
	if receivedHeaders.Get("X-UM-BatchNo") != "1" {
		t.Errorf("Expected X-UM-BatchNo='1', got '%s'", receivedHeaders.Get("X-UM-BatchNo"))
	}
	if receivedHeaders.Get("X-UM-RowsCount") != "1" {
		t.Errorf("Expected X-UM-RowsCount='1', got '%s'", receivedHeaders.Get("X-UM-RowsCount"))
	}

	// Check body
	if receivedBody.PackageID != "test-pkg-123" {
		t.Errorf("Expected PackageID='test-pkg-123', got '%s'", receivedBody.PackageID)
	}
}

func TestErrorBatch401NoRetry(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("Unauthorized"))
	}))
	defer server.Close()

	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg",
		Errors:    []ingest.ErrorItem{{RowNo: 1, Code: "TestError"}},
	}

	sender := NewSender(server.URL, false, 1, 3, 10, 100, nil, "", "", nil, false)
	ctx := context.Background()

	err := sender.SendErrorBatch(ctx, errorBatch)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Should not retry on 401
	if attempts != 1 {
		t.Errorf("Expected 1 attempt (no retry), got %d", attempts)
	}

	httpErr, ok := GetHTTPError(err)
	if !ok {
		t.Fatalf("Expected HTTPError, got %T", err)
	}
	if httpErr.StatusCode != 401 {
		t.Errorf("Expected status 401, got %d", httpErr.StatusCode)
	}
}

func TestErrorBatch503RetryThenSuccess(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg",
		Errors:    []ingest.ErrorItem{{RowNo: 1, Code: "TestError"}},
	}

	sender := NewSender(server.URL, false, 1, 5, 10, 100, nil, "", "", nil, false)
	ctx := context.Background()

	err := sender.SendErrorBatch(ctx, errorBatch)
	if err != nil {
		t.Fatalf("SendErrorBatch() error = %v", err)
	}

	// Should retry and succeed
	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestErrorBatch503AlwaysFail(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg",
		Errors:    []ingest.ErrorItem{{RowNo: 1, Code: "TestError"}},
	}

	maxRetries := 3
	sender := NewSender(server.URL, false, 1, maxRetries, 10, 100, nil, "", "", nil, false)
	ctx := context.Background()

	err := sender.SendErrorBatch(ctx, errorBatch)
	if err == nil {
		t.Fatal("Expected error after max retries, got nil")
	}

	// Should retry maxRetries+1 times (initial + retries)
	expectedAttempts := maxRetries + 1
	if attempts != expectedAttempts {
		t.Errorf("Expected %d attempts, got %d", expectedAttempts, attempts)
	}

	httpErr, ok := GetHTTPError(err)
	if !ok {
		// Error might be wrapped
		if !contains(err.Error(), "max retries exceeded") {
			t.Errorf("Expected 'max retries exceeded' error, got: %v", err)
		}
	} else if httpErr.StatusCode != 503 {
		t.Errorf("Expected status 503, got %d", httpErr.StatusCode)
	}
}

func TestErrorBatch409Success(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusConflict) // 409
	}))
	defer server.Close()

	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg",
		Errors:    []ingest.ErrorItem{{RowNo: 1, Code: "TestError"}},
	}

	sender := NewSender(server.URL, false, 1, 3, 10, 100, nil, "", "", nil, false)
	ctx := context.Background()

	err := sender.SendErrorBatch(ctx, errorBatch)
	if err != nil {
		t.Fatalf("SendErrorBatch() should succeed on 409, got error: %v", err)
	}

	// Should succeed on first attempt (409 is success)
	if attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", attempts)
	}
}

func TestNoEmptyErrorBatchSent(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Empty error batch
	errorBatch := &ingest.ErrorBatch{
		PackageID: "test-pkg",
		Errors:    []ingest.ErrorItem{}, // Empty
	}

	sender := NewSender(server.URL, false, 1, 3, 10, 100, nil, "", "", nil, false)
	ctx := context.Background()

	err := sender.SendErrorBatch(ctx, errorBatch)
	if err != nil {
		t.Fatalf("SendErrorBatch() should return nil for empty batch, got error: %v", err)
	}

	// Should not make any HTTP calls
	if attempts != 0 {
		t.Errorf("Expected 0 HTTP calls for empty batch, got %d", attempts)
	}
}

func TestNoEmptyErrorBatchSentNil(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := NewSender(server.URL, false, 1, 3, 10, 100, nil, "", "", nil, false)
	ctx := context.Background()

	err := sender.SendErrorBatch(ctx, nil)
	if err != nil {
		t.Fatalf("SendErrorBatch() should return nil for nil batch, got error: %v", err)
	}

	// Should not make any HTTP calls
	if attempts != 0 {
		t.Errorf("Expected 0 HTTP calls for nil batch, got %d", attempts)
	}
}

