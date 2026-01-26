package ingest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidatePath(t *testing.T) {
	// Create temporary directory structure
	tmpDir, err := os.MkdirTemp("", "um_ingest_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	allowedBase := filepath.Join(tmpDir, "incoming")
	if err := os.MkdirAll(allowedBase, 0755); err != nil {
		t.Fatalf("Failed to create allowed base: %v", err)
	}

	// Create a test file
	testFile := filepath.Join(allowedBase, "test.csv")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	tests := []struct {
		name        string
		inputPath   string
		allowedBase string
		wantErr     bool
		description string
	}{
		{
			name:        "valid path",
			inputPath:   testFile,
			allowedBase: allowedBase,
			wantErr:     false,
			description: "file inside allowed directory",
		},
		{
			name:        "path traversal with ..",
			inputPath:   filepath.Join(allowedBase, "..", "test.csv"),
			allowedBase: allowedBase,
			wantErr:     true,
			description: "should reject path with ..",
		},
		{
			name:        "path traversal with incoming_evil",
			inputPath:   filepath.Join(tmpDir, "incoming_evil", "test.csv"),
			allowedBase: allowedBase,
			wantErr:     true,
			description: "should reject incoming_evil directory",
		},
		{
			name:        "absolute path outside",
			inputPath:   "/etc/passwd",
			allowedBase: allowedBase,
			wantErr:     true,
			description: "should reject absolute path outside allowed base",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidatePath(tt.inputPath, tt.allowedBase)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidatePath() expected error for %s, got nil", tt.description)
				}
			} else {
				if err != nil {
					t.Errorf("ValidatePath() unexpected error for %s: %v", tt.description, err)
				}
			}
		})
	}
}

func TestValidatePathSymlink(t *testing.T) {
	// Create temporary directory structure
	tmpDir, err := os.MkdirTemp("", "um_ingest_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	allowedBase := filepath.Join(tmpDir, "incoming")
	if err := os.MkdirAll(allowedBase, 0755); err != nil {
		t.Fatalf("Failed to create allowed base: %v", err)
	}

	// Create a directory outside allowed base
	outsideDir := filepath.Join(tmpDir, "outside")
	if err := os.MkdirAll(outsideDir, 0755); err != nil {
		t.Fatalf("Failed to create outside dir: %v", err)
	}

	// Create a file outside
	outsideFile := filepath.Join(outsideDir, "secret.csv")
	if err := os.WriteFile(outsideFile, []byte("secret"), 0644); err != nil {
		t.Fatalf("Failed to create outside file: %v", err)
	}

	// Create symlink inside allowed base pointing to outside file
	symlinkPath := filepath.Join(allowedBase, "link.csv")
	if err := os.Symlink(outsideFile, symlinkPath); err != nil {
		// Symlinks might not work on all systems, skip test
		t.Skipf("Symlinks not supported: %v", err)
	}

	// Should reject symlink that escapes
	_, err = ValidatePath(symlinkPath, allowedBase)
	if err == nil {
		t.Error("ValidatePath() should reject symlink that escapes allowed base")
	}
}

