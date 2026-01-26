package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
)

func TestRetryAndBackoff(t *testing.T) {
	attempts := 0
	maxAttempts := 3

	// Create a test server that fails first 2 times, then succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < maxAttempts {
			w.WriteHeader(http.StatusServiceUnavailable) // 503
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := NewSender(server.URL, false, 5, 3, 100, 1000)

	batch := &ingest.Batch{
		PackageID: "test",
		BatchNo:   1,
		Register:  "test",
		Cols:      []string{"col1"},
		Rows:      [][]interface{}{{1}},
	}

	ctx := context.Background()
	start := time.Now()
	err := sender.SendBatch(ctx, batch)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("SendBatch() error = %v, want nil", err)
	}

	if attempts != maxAttempts {
		t.Errorf("Expected %d attempts, got %d", maxAttempts, attempts)
	}

	// Check that backoff was applied (should take at least some time)
	if duration < 100*time.Millisecond {
		t.Errorf("Expected backoff delay, but duration was only %v", duration)
	}
}

func TestRetryAfterHeader(t *testing.T) {
	attempts := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests) // 429
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := NewSender(server.URL, false, 5, 3, 10, 1000)

	batch := &ingest.Batch{
		PackageID: "test",
		BatchNo:   1,
		Register:  "test",
		Cols:      []string{"col1"},
		Rows:      [][]interface{}{{1}},
	}

	ctx := context.Background()
	start := time.Now()
	err := sender.SendBatch(ctx, batch)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("SendBatch() error = %v, want nil", err)
	}

	// Should respect Retry-After (at least 1 second, with some tolerance)
	if duration < 900*time.Millisecond {
		t.Errorf("Expected Retry-After delay (~1s), but duration was only %v", duration)
	}
}

func TestFatalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // 400 - fatal
		json.NewEncoder(w).Encode(map[string]string{"error": "bad request"})
	}))
	defer server.Close()

	sender := NewSender(server.URL, false, 5, 3, 100, 1000)

	batch := &ingest.Batch{
		PackageID: "test",
		BatchNo:   1,
		Register:  "test",
		Cols:      []string{"col1"},
		Rows:      [][]interface{}{{1}},
	}

	ctx := context.Background()
	err := sender.SendBatch(ctx, batch)

	if err == nil {
		t.Error("SendBatch() expected error for 400, got nil")
	}

	httpErr, ok := GetHTTPError(err)
	if !ok {
		t.Error("Expected HTTPError, got different error type")
		return
	}

	if httpErr.StatusCode != 400 {
		t.Errorf("Expected status 400, got %d", httpErr.StatusCode)
	}

	// Should not retry on 4xx (except 429)
	if !sender.isRetryable(err) {
		// This is correct - 4xx should not be retried
	} else {
		t.Error("4xx error should not be retryable")
	}
}

func TestGzip(t *testing.T) {
	var receivedGzip bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") == "gzip" {
			receivedGzip = true
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := NewSender(server.URL, true, 5, 0, 100, 1000)

	batch := &ingest.Batch{
		PackageID: "test",
		BatchNo:   1,
		Register:  "test",
		Cols:      []string{"col1"},
		Rows:      [][]interface{}{{1}},
	}

	ctx := context.Background()
	err := sender.SendBatch(ctx, batch)

	if err != nil {
		t.Errorf("SendBatch() error = %v", err)
	}

	if !receivedGzip {
		t.Error("Expected gzip encoding, but it was not received")
	}
}

