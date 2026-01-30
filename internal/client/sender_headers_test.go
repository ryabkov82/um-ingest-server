package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
)

func TestBatchHeaders(t *testing.T) {
	var receivedHeaders map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = make(map[string]string)
		receivedHeaders["X-UM-PackageId"] = r.Header.Get("X-UM-PackageId")
		receivedHeaders["X-UM-BatchNo"] = r.Header.Get("X-UM-BatchNo")
		receivedHeaders["X-UM-RowsCount"] = r.Header.Get("X-UM-RowsCount")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := NewSender(server.URL, false, 5, 0, 100, 1000, nil, "", "", nil, false)

	batch := &ingest.Batch{
		PackageID: "test-package-123",
		BatchNo:   42,
		Register:  "TestRegister",
		Rows: []map[string]interface{}{
			{"field1": "value1"},
			{"field1": "value2"},
			{"field1": "value3"},
		},
	}

	ctx := context.Background()
	err := sender.SendBatch(ctx, batch)

	if err != nil {
		t.Errorf("SendBatch() error = %v", err)
	}

	if receivedHeaders["X-UM-PackageId"] != "test-package-123" {
		t.Errorf("Expected X-UM-PackageId 'test-package-123', got '%s'", receivedHeaders["X-UM-PackageId"])
	}

	if receivedHeaders["X-UM-BatchNo"] != "42" {
		t.Errorf("Expected X-UM-BatchNo '42', got '%s'", receivedHeaders["X-UM-BatchNo"])
	}

	if receivedHeaders["X-UM-RowsCount"] != "3" {
		t.Errorf("Expected X-UM-RowsCount '3', got '%s'", receivedHeaders["X-UM-RowsCount"])
	}
}

func TestBatchHeadersOnRetry(t *testing.T) {
	attempts := 0
	var headersOnAttempts []map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		headers := make(map[string]string)
		headers["X-UM-PackageId"] = r.Header.Get("X-UM-PackageId")
		headers["X-UM-BatchNo"] = r.Header.Get("X-UM-BatchNo")
		headers["X-UM-RowsCount"] = r.Header.Get("X-UM-RowsCount")
		headersOnAttempts = append(headersOnAttempts, headers)

		if attempts < 2 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	sender := NewSender(server.URL, false, 5, 3, 10, 1000, nil, "", "", nil, false)

	batch := &ingest.Batch{
		PackageID: "retry-package",
		BatchNo:   1,
		Register:  "TestRegister",
		Rows: []map[string]interface{}{
			{"field1": "value1"},
		},
	}

	ctx := context.Background()
	err := sender.SendBatch(ctx, batch)

	if err != nil {
		t.Errorf("SendBatch() error = %v", err)
	}

	if attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", attempts)
	}

	// Check headers on all attempts
	for i, headers := range headersOnAttempts {
		if headers["X-UM-PackageId"] != "retry-package" {
			t.Errorf("Attempt %d: Expected X-UM-PackageId 'retry-package', got '%s'", i+1, headers["X-UM-PackageId"])
		}
		if headers["X-UM-BatchNo"] != "1" {
			t.Errorf("Attempt %d: Expected X-UM-BatchNo '1', got '%s'", i+1, headers["X-UM-BatchNo"])
		}
		if headers["X-UM-RowsCount"] != "1" {
			t.Errorf("Attempt %d: Expected X-UM-RowsCount '1', got '%s'", i+1, headers["X-UM-RowsCount"])
		}
	}
}

