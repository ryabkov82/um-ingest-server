package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestGetJobByPackageReturnsActive(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "test_user", "test_pass", context.Background())

	// Create a job
	req := map[string]interface{}{
		"packageId": "test-pkg-active",
		"inputPath": testFile,
		"csv": map[string]interface{}{
			"encoding":  "utf-8",
			"delimiter": ",",
			"hasHeader": false,
			"mapBy":     "order",
		},
		"schema": map[string]interface{}{
			"register": "TestRegister",
			"fields":   []interface{}{},
		},
		"delivery": map[string]interface{}{
			"endpoint":  "http://localhost/test",
			"batchSize": 100,
		},
	}

	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.CreateJob(w, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create job: %d, body: %s", w.Code, w.Body.String())
	}

	var createResponse map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&createResponse); err != nil {
		t.Fatalf("Failed to decode create response: %v", err)
	}
	jobID := createResponse["jobId"].(string)

	// Get job by packageId (should return active job)
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/packages/test-pkg-active/job", nil)
	handler.GetJobByPackage(w2, r2)

	if w2.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d, body: %s", w2.Code, w2.Body.String())
	}

	var statusResponse map[string]interface{}
	if err := json.NewDecoder(w2.Body).Decode(&statusResponse); err != nil {
		t.Fatalf("Failed to decode status response: %v", err)
	}

	if statusResponse["jobId"] != jobID {
		t.Errorf("Expected jobId %s, got %v", jobID, statusResponse["jobId"])
	}
	if statusResponse["status"] != "queued" {
		t.Errorf("Expected status 'queued', got %v", statusResponse["status"])
	}
}

func TestGetJobByPackageReturnsLastAfterFinish(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "test_user", "test_pass", context.Background())

	// Create a job
	req := map[string]interface{}{
		"packageId": "test-pkg-last",
		"inputPath": testFile,
		"csv": map[string]interface{}{
			"encoding":  "utf-8",
			"delimiter": ",",
			"hasHeader": false,
			"mapBy":     "order",
		},
		"schema": map[string]interface{}{
			"register": "TestRegister",
			"fields":   []interface{}{},
		},
		"delivery": map[string]interface{}{
			"endpoint":  "http://localhost/test",
			"batchSize": 100,
		},
	}

	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.CreateJob(w, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create job: %d, body: %s", w.Code, w.Body.String())
	}

	var createResponse map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&createResponse); err != nil {
		t.Fatalf("Failed to decode create response: %v", err)
	}
	jobID := createResponse["jobId"].(string)

	// Manually update job status to succeeded
	err := store.UpdateStatus(jobID, job.StatusSucceeded)
	if err != nil {
		t.Fatalf("Failed to update job status: %v", err)
	}

	// Wait a bit to ensure status is updated
	time.Sleep(10 * time.Millisecond)

	// Get job by packageId (should return last job, not 404)
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/packages/test-pkg-last/job", nil)
	handler.GetJobByPackage(w2, r2)

	if w2.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d, body: %s", w2.Code, w2.Body.String())
	}

	var statusResponse map[string]interface{}
	if err := json.NewDecoder(w2.Body).Decode(&statusResponse); err != nil {
		t.Fatalf("Failed to decode status response: %v", err)
	}

	if statusResponse["jobId"] != jobID {
		t.Errorf("Expected jobId %s, got %v", jobID, statusResponse["jobId"])
	}
	if statusResponse["status"] != "succeeded" {
		t.Errorf("Expected status 'succeeded', got %v", statusResponse["status"])
	}
	if statusResponse["finishedAt"] == nil {
		t.Error("Expected finishedAt to be set")
	}
}

func TestGetJobByPackage404WhenNeverSeen(t *testing.T) {
	tmpDir := t.TempDir()
	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "test_user", "test_pass", context.Background())

	// Try to get job for packageId that never existed
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/packages/never-seen-package/job", nil)
	handler.GetJobByPackage(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d, body: %s", w.Code, w.Body.String())
	}
}

func TestGetJobByPackageReturnsLastFailed(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "test_user", "test_pass", context.Background())

	// Create a job
	req := map[string]interface{}{
		"packageId": "test-pkg-failed",
		"inputPath": testFile,
		"csv": map[string]interface{}{
			"encoding":  "utf-8",
			"delimiter": ",",
			"hasHeader": false,
			"mapBy":     "order",
		},
		"schema": map[string]interface{}{
			"register": "TestRegister",
			"fields":   []interface{}{},
		},
		"delivery": map[string]interface{}{
			"endpoint":  "http://localhost/test",
			"batchSize": 100,
		},
	}

	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.CreateJob(w, r)

	var createResponse map[string]interface{}
	json.NewDecoder(w.Body).Decode(&createResponse)
	jobID := createResponse["jobId"].(string)

	// Manually update job status to failed with error
	err := store.UpdateStatus(jobID, job.StatusFailed)
	if err != nil {
		t.Fatalf("Failed to update job status: %v", err)
	}
	store.UpdateError(jobID, fmt.Errorf("test error message"))

	// Get job by packageId (should return last failed job)
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/packages/test-pkg-failed/job", nil)
	handler.GetJobByPackage(w2, r2)

	if w2.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d, body: %s", w2.Code, w2.Body.String())
	}

	var statusResponse map[string]interface{}
	if err := json.NewDecoder(w2.Body).Decode(&statusResponse); err != nil {
		t.Fatalf("Failed to decode status response: %v", err)
	}

	if statusResponse["status"] != "failed" {
		t.Errorf("Expected status 'failed', got %v", statusResponse["status"])
	}
	if statusResponse["lastError"] == nil || statusResponse["lastError"] == "" {
		t.Error("Expected lastError to be set")
	}
	if statusResponse["finishedAt"] == nil {
		t.Error("Expected finishedAt to be set")
	}
}

