package version

import (
	"encoding/json"
	"testing"
)

func TestString(t *testing.T) {
	// Test that String() returns a non-empty string
	str := String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	if len(str) < 10 {
		t.Errorf("String() seems too short: %s", str)
	}
}

func TestInfo(t *testing.T) {
	info := Info()

	// Check required fields
	requiredFields := []string{"name", "version", "gitCommit", "buildTime", "goVersion"}
	for _, field := range requiredFields {
		if _, ok := info[field]; !ok {
			t.Errorf("Info() missing required field: %s", field)
		}
	}

	// Check name
	if info["name"] != "um-ingest-server" {
		t.Errorf("Expected name 'um-ingest-server', got '%s'", info["name"])
	}

	// Check that version is set (even if "dev")
	if info["version"] == "" {
		t.Error("Version should not be empty")
	}

	// Check that goVersion is set
	if info["goVersion"] == "" {
		t.Error("GoVersion should not be empty")
	}
}

func TestInfoJSON(t *testing.T) {
	info := Info()

	// Test that info can be marshaled to JSON
	jsonData, err := json.Marshal(info)
	if err != nil {
		t.Errorf("Failed to marshal Info() to JSON: %v", err)
	}

	if len(jsonData) == 0 {
		t.Error("JSON data is empty")
	}

	// Unmarshal back to verify structure
	var unmarshaled map[string]string
	if err := json.Unmarshal(jsonData, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal JSON: %v", err)
	}

	if unmarshaled["name"] != "um-ingest-server" {
		t.Errorf("Expected name 'um-ingest-server', got '%s'", unmarshaled["name"])
	}
}

