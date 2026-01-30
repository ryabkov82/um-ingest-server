package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestCreateJobDuplicatePackageId(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "test_user", "test_pass", context.Background())

	// Create first job
	req1 := map[string]interface{}{
		"packageId": "test-pkg-1",
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

	body1, _ := json.Marshal(req1)
	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body1))
	r1.Header.Set("Content-Type", "application/json")
	handler.CreateJob(w1, r1)

	if w1.Code != http.StatusCreated {
		t.Fatalf("Expected 201, got %d, body: %s", w1.Code, w1.Body.String())
	}

	// Try to create second job with same packageId
	body2, _ := json.Marshal(req1)
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body2))
	r2.Header.Set("Content-Type", "application/json")
	handler.CreateJob(w2, r2)

	if w2.Code != http.StatusConflict {
		t.Errorf("Expected 409 Conflict, got %d", w2.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(w2.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["error"] != "job_already_running" {
		t.Errorf("Expected error 'job_already_running', got %v", response["error"])
	}
	if response["packageId"] != "test-pkg-1" {
		t.Errorf("Expected packageId 'test-pkg-1', got %v", response["packageId"])
	}
}

func TestGetJobByPackage(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "test_user", "test_pass", context.Background())

	// Create a job
	req := map[string]interface{}{
		"packageId": "test-pkg-2",
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
	jobID, ok := createResponse["jobId"].(string)
	if !ok {
		t.Fatalf("jobId is not a string: %v", createResponse["jobId"])
	}

	// Get job by packageId
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/packages/test-pkg-2/job", nil)
	handler.GetJobByPackage(w2, r2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", w2.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(w2.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["jobId"] != jobID {
		t.Errorf("Expected jobId %s, got %v", jobID, response["jobId"])
	}
	if response["status"] != "queued" {
		t.Errorf("Expected status 'queued', got %v", response["status"])
	}

	// Try to get non-existent package
	w3 := httptest.NewRecorder()
	r3 := httptest.NewRequest("GET", "/packages/non-existent/job", nil)
	handler.GetJobByPackage(w3, r3)

	if w3.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w3.Code)
	}
}

func TestCancelJobByPackage(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "test_user", "test_pass", context.Background())

	// Create a job
	req := map[string]interface{}{
		"packageId": "test-pkg-3",
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
	jobID, ok := createResponse["jobId"].(string)
	if !ok {
		t.Fatalf("jobId is not a string: %v", createResponse["jobId"])
	}

	// Cancel job by packageId
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("POST", "/packages/test-pkg-3/cancel", nil)
	handler.CancelJobByPackage(w2, r2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", w2.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(w2.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "canceled" {
		t.Errorf("Expected status 'canceled', got %v", response["status"])
	}
	if response["jobId"] != jobID {
		t.Errorf("Expected jobId %s, got %v", jobID, response["jobId"])
	}

	// Verify job is canceled in store
	finalJob, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}
	if finalJob.Status != job.StatusCanceled {
		t.Errorf("Expected job status Canceled, got %s", finalJob.Status)
	}
	if finalJob.FinishedAt == nil {
		t.Error("Expected FinishedAt to be set")
	}

	// Verify cancel was logged (log should contain caller information from CancelJobByPackage handler)
	// The log should contain "Job {jobID} canceled" with caller pointing to handlers.go:386
	t.Logf("Job %s canceled via HTTP endpoint, status=%s (check logs for caller info: handlers.go:386)", jobID, finalJob.Status)

	// Try to cancel non-existent package
	w3 := httptest.NewRecorder()
	r3 := httptest.NewRequest("POST", "/packages/non-existent/cancel", nil)
	handler.CancelJobByPackage(w3, r3)

	if w3.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w3.Code)
	}
}

func TestCreateJobWithoutPackageId(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "test_user", "test_pass", context.Background())

	req := map[string]interface{}{
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
	handler.CreateJob(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

