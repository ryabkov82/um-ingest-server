package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestGetVersion(t *testing.T) {
	store := job.NewStore()
	handler := NewHandler(store, "/tmp", "", "")

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/version", nil)
	handler.GetVersion(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'", w.Header().Get("Content-Type"))
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// Check required fields
	requiredFields := []string{"name", "version", "gitCommit", "buildTime", "goVersion"}
	for _, field := range requiredFields {
		if _, ok := response[field]; !ok {
			t.Errorf("Response missing required field: %s", field)
		}
	}

	// Check name
	if response["name"] != "um-ingest-server" {
		t.Errorf("Expected name 'um-ingest-server', got '%s'", response["name"])
	}

	// Check that version is set
	if response["version"] == "" {
		t.Error("Version should not be empty")
	}
}

func TestGetVersionMethodNotAllowed(t *testing.T) {
	store := job.NewStore()
	handler := NewHandler(store, "/tmp", "", "")

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/version", nil)
	handler.GetVersion(w, r)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

