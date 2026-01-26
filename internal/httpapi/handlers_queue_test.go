package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestCreateJobQueueFull(t *testing.T) {
	// Create temp directory for test
	tmpDir := t.TempDir()
	allowedBase := tmpDir + "/incoming"
	
	store := job.NewStore()
	handler := NewHandler(store, allowedBase)

	// Create test file
	testFile := allowedBase + "/test.csv"
	os.MkdirAll(allowedBase, 0755)
	os.WriteFile(testFile, []byte("test"), 0644)

	// Fill the queue
	for i := 0; i < 1000; i++ {
		j := &job.Job{
			PackageID: "test",
			InputPath:  testFile,
		}
		_, _ = store.Create(j)
	}

	// Create request
	reqBody := map[string]interface{}{
		"packageId": "test",
		"inputPath": testFile,
		"csv": map[string]interface{}{
			"encoding":  "utf-8",
			"delimiter": ",",
			"hasHeader": false,
			"mapBy":     "order",
		},
		"schema": map[string]interface{}{
			"register": "Test",
			"fields":   []interface{}{},
		},
		"delivery": map[string]interface{}{
			"endpoint":       "http://localhost/test",
			"batchSize":      1000,
			"timeoutSeconds": 30,
			"maxRetries":     3,
			"backoffMs":      100,
			"backoffMaxMs":   1000,
		},
	}

	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.CreateJob(w, req)

	if w.Code != http.StatusTooManyRequests {
		t.Errorf("Expected status 429, got %d", w.Code)
	}

	if w.Body.String() == "" {
		t.Error("Expected error message in response")
	}
}

