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

func TestCreateJobWithoutDeliveryAuthWithEnv(t *testing.T) {
	// Set environment variables
	os.Setenv("UM_1C_BASIC_USER", "env_user")
	os.Setenv("UM_1C_BASIC_PASS", "env_pass")
	defer os.Unsetenv("UM_1C_BASIC_USER")
	defer os.Unsetenv("UM_1C_BASIC_PASS")

	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "env_user", "env_pass", context.Background())

	req := map[string]interface{}{
		"packageId": "test-pkg-env",
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
		t.Errorf("Expected 201, got %d, body: %s", w.Code, w.Body.String())
	}
}

func TestCreateJobWithoutDeliveryAuthWithoutEnv(t *testing.T) {
	// Ensure env vars are not set
	os.Unsetenv("UM_1C_BASIC_USER")
	os.Unsetenv("UM_1C_BASIC_PASS")

	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "", "", context.Background())

	req := map[string]interface{}{
		"packageId": "test-pkg-no-auth",
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

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d, body: %s", w.Code, w.Body.String())
	}

	if !bytes.Contains(w.Body.Bytes(), []byte("UM_1C_BASIC_USER/PASS")) {
		t.Errorf("Expected error message about UM_1C_BASIC_USER/PASS, got: %s", w.Body.String())
	}
}

func TestAuthSelectionPrefersPayloadOverEnv(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	// Handler has env credentials
	handler := NewHandler(store, tmpDir, "env_user", "env_pass", context.Background())

	req := map[string]interface{}{
		"packageId": "test-pkg-payload",
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
			"auth": map[string]interface{}{
				"type": "basic",
				"user": "payload_user",
				"pass": "payload_pass",
			},
		},
	}

	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.CreateJob(w, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected 201, got %d, body: %s", w.Code, w.Body.String())
	}

	// Get job to verify auth is stored
	var createResponse map[string]interface{}
	json.NewDecoder(w.Body).Decode(&createResponse)
	jobID := createResponse["jobId"].(string)

	j, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	if j.Delivery.Auth == nil {
		t.Fatal("Expected delivery.auth to be set")
	}
	if j.Delivery.Auth.User != "payload_user" {
		t.Errorf("Expected user 'payload_user', got '%s'", j.Delivery.Auth.User)
	}
	if j.Delivery.Auth.Pass != "payload_pass" {
		t.Errorf("Expected pass 'payload_pass', got '%s'", j.Delivery.Auth.Pass)
	}
}

func TestNoPasswordLeakInStatus(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "env_user", "env_pass", context.Background())

	req := map[string]interface{}{
		"packageId": "test-pkg-leak",
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
			"auth": map[string]interface{}{
				"type": "basic",
				"user": "test_user",
				"pass": "secret_password",
			},
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

	// Test GET /jobs/{jobId}
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/jobs/"+jobID, nil)
	handler.GetJobStatus(w2, r2)

	var statusResponse map[string]interface{}
	json.NewDecoder(w2.Body).Decode(&statusResponse)

	// Check that password is not in response
	responseBytes, _ := json.Marshal(statusResponse)
	if bytes.Contains(responseBytes, []byte("secret_password")) {
		t.Error("Password found in GET /jobs/{jobId} response")
	}

	// Check delivery.auth structure if present
	if delivery, ok := statusResponse["delivery"].(map[string]interface{}); ok {
		if auth, ok := delivery["auth"].(map[string]interface{}); ok {
			if _, hasPass := auth["pass"]; hasPass {
				t.Error("Password field found in delivery.auth in response")
			}
			if user, ok := auth["user"].(string); !ok || user != "test_user" {
				t.Errorf("Expected user 'test_user', got %v", auth["user"])
			}
		}
	}

	// Test GET /packages/{packageId}/job
	w3 := httptest.NewRecorder()
	r3 := httptest.NewRequest("GET", "/packages/test-pkg-leak/job", nil)
	handler.GetJobByPackage(w3, r3)

	var packageResponse map[string]interface{}
	json.NewDecoder(w3.Body).Decode(&packageResponse)

	// Check that password is not in response
	packageBytes, _ := json.Marshal(packageResponse)
	if bytes.Contains(packageBytes, []byte("secret_password")) {
		t.Error("Password found in GET /packages/{packageId}/job response")
	}
}

func TestEnvAuthUsedWhenNoPayload(t *testing.T) {
	// This test verifies that env credentials are used when no payload auth is provided
	// We test this indirectly by checking that job can be created without delivery.auth
	// when env is set, and that the job will use env credentials during processing

	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(testFile, []byte("test"), 0644)

	store := job.NewStore()
	handler := NewHandler(store, tmpDir, "env_user", "env_pass", context.Background())

	req := map[string]interface{}{
		"packageId": "test-pkg-env-only",
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
			// No auth field
		},
	}

	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.CreateJob(w, r)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected 201, got %d, body: %s", w.Code, w.Body.String())
	}

	// Verify job was created without delivery.auth
	var createResponse map[string]interface{}
	json.NewDecoder(w.Body).Decode(&createResponse)
	jobID := createResponse["jobId"].(string)

	j, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	// Job should not have delivery.auth when env is used
	if j.Delivery.Auth != nil {
		t.Error("Expected delivery.auth to be nil when using env credentials")
	}
}

