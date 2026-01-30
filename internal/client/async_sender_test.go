package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
)

// TestAsyncSenderFatalError tests that fatal error stops processing and cancels context
func TestAsyncSenderFatalError(t *testing.T) {
	// Create a test server that returns 500 (retryable) then 400 (fatal)
	attempt := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt == 1 {
			// First attempt: 500 (retryable)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			// Second attempt: 400 (fatal)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	// Create base sender with short timeout and 1 retry
	baseSender := NewSender(
		server.URL,
		false, // no gzip
		1,     // timeoutSeconds
		1,     // maxRetries
		10,    // backoffMs
		100,   // backoffMaxMs
		nil,   // auth
		"",    // envUser
		"",    // envPass
		nil,   // timings
		false, // isErrors
	)

	// Create context and cancel function
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create async sender
	asyncSender := NewAsyncSender(baseSender, 8, cancel, nil)
	asyncSender.Start(ctx, 1)

	// Create a test batch
	batch := &ingest.Batch{
		PackageID: "test-pkg",
		BatchNo:   1,
		Register:  "test-register",
		Rows: []map[string]interface{}{
			{"id": 1},
		},
	}

	// Enqueue batch
	err := asyncSender.Enqueue(ctx, batch)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Wait a bit for processing
	time.Sleep(200 * time.Millisecond)

	// Close and wait - should return error
	done := make(chan error, 1)
	go func() {
		done <- asyncSender.CloseAndWait()
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Expected error from CloseAndWait, got nil")
		}
		// Check that context was canceled
		select {
		case <-ctx.Done():
			// Good, context was canceled
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Context should have been canceled after fatal error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CloseAndWait timed out")
	}
}

// TestAsyncSenderContextCancel tests that context cancellation stops enqueue
func TestAsyncSenderContextCancel(t *testing.T) {
	// Create a test server that always succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	baseSender := NewSender(
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
		false,
	)

	ctx, cancel := context.WithCancel(context.Background())
	asyncSender := NewAsyncSender(baseSender, 8, cancel, nil)
	asyncSender.Start(ctx, 1)

	// Cancel context
	cancel()

	// Try to enqueue - should fail with context error
	batch := &ingest.Batch{
		PackageID: "test-pkg",
		BatchNo:   1,
		Register:  "test-register",
		Rows:      []map[string]interface{}{{"id": 1}},
	}

	err := asyncSender.Enqueue(ctx, batch)
	if err == nil {
		t.Fatal("Expected error when enqueueing after context cancel, got nil")
	}
	if err != context.Canceled {
		t.Fatalf("Expected context.Canceled, got %v", err)
	}

	// Close and wait should succeed (no fatal error)
	err = asyncSender.CloseAndWait()
	if err != nil {
		t.Fatalf("CloseAndWait should return nil after context cancel, got %v", err)
	}
}

// TestAsyncSenderMultipleBatches tests that multiple batches are processed successfully
func TestAsyncSenderMultipleBatches(t *testing.T) {
	// Create a test server that counts requests
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	baseSender := NewSender(
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
		false,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	asyncSender := NewAsyncSender(baseSender, 8, cancel, nil)
	asyncSender.Start(ctx, 1)

	// Enqueue multiple batches
	for i := 1; i <= 5; i++ {
		batch := &ingest.Batch{
			PackageID: "pkg",
			BatchNo:   int64(i),
			Register:  "reg",
			Rows:      []map[string]interface{}{{"id": i}},
		}
		if err := asyncSender.Enqueue(ctx, batch); err != nil {
			t.Fatalf("Enqueue batch %d failed: %v", i, err)
		}
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Close and wait
	err := asyncSender.CloseAndWait()
	if err != nil {
		t.Fatalf("CloseAndWait failed: %v", err)
	}

	// Check that all batches were sent
	if requestCount != 5 {
		t.Fatalf("Expected 5 requests, got %d", requestCount)
	}
}
