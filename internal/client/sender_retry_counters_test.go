package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
)

// TestSenderRetryCounters tests that retry counters are incremented correctly
func TestSenderRetryCounters(t *testing.T) {
	// Create a test server that fails first time, then succeeds
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			// First attempt: 500 (retryable)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			// Second attempt: 200 (success)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	// Create timings to track counters
	timings := ingest.NewTimings()

	// Create sender with timings
	sender := NewSender(
		server.URL,
		false, // no gzip
		1,     // timeoutSeconds
		1,     // maxRetries (allows 1 retry)
		10,    // backoffMs
		100,   // backoffMaxMs
		nil,   // auth
		"",    // envUser
		"",    // envPass
		timings,
		false, // isErrors = false for data sender
	)

	// Create a test batch
	batch := &ingest.Batch{
		PackageID: "test-pkg",
		BatchNo:   1,
		Register:  "test-register",
		Rows: []map[string]interface{}{
			{"id": 1},
		},
	}

	// Send batch
	ctx := context.Background()
	err := sender.SendBatch(ctx, batch)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	// Check that attempts and retries were incremented
	// attempts should be 2 (first attempt + retry)
	// retries should be 1 (one retry)
	if timings.DataAttempts != 2 {
		t.Errorf("Expected DataAttempts=2, got %d", timings.DataAttempts)
	}
	if timings.DataRetries != 1 {
		t.Errorf("Expected DataRetries=1, got %d", timings.DataRetries)
	}
}

// TestSenderRetryCountersNoRetry tests that counters work correctly when no retry is needed
func TestSenderRetryCountersNoRetry(t *testing.T) {
	// Create a test server that always succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create timings to track counters
	timings := ingest.NewTimings()

	// Create sender with timings
	sender := NewSender(
		server.URL,
		false,
		1,
		1,
		10,
		100,
		nil,
		"",
		"",
		timings,
		false,
	)

	// Create a test batch
	batch := &ingest.Batch{
		PackageID: "test-pkg",
		BatchNo:   1,
		Register:  "test-register",
		Rows:      []map[string]interface{}{{"id": 1}},
	}

	// Send batch
	ctx := context.Background()
	err := sender.SendBatch(ctx, batch)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	// Check that attempts=1, retries=0
	if timings.DataAttempts != 1 {
		t.Errorf("Expected DataAttempts=1, got %d", timings.DataAttempts)
	}
	if timings.DataRetries != 0 {
		t.Errorf("Expected DataRetries=0, got %d", timings.DataRetries)
	}
}

