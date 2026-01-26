package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidatePath checks if inputPath is within allowedBaseDir
// Uses filepath.Abs and filepath.EvalSymlinks for security
func ValidatePath(inputPath, allowedBaseDir string) (string, error) {
	// Resolve absolute paths
	absInput, err := filepath.Abs(inputPath)
	if err != nil {
		return "", fmt.Errorf("invalid input path: %w", err)
	}

	absBase, err := filepath.Abs(allowedBaseDir)
	if err != nil {
		return "", fmt.Errorf("invalid base directory: %w", err)
	}

	// Resolve symlinks
	resolvedInput, err := filepath.EvalSymlinks(absInput)
	if err != nil {
		// File must exist for validation (ValidatePathExists will be called anyway)
		return "", fmt.Errorf("cannot resolve input path: %w", err)
	}

	resolvedBase, err := filepath.EvalSymlinks(absBase)
	if err != nil {
		return "", fmt.Errorf("cannot resolve base directory: %w", err)
	}

	// Check if resolvedInput is within resolvedBase
	rel, err := filepath.Rel(resolvedBase, resolvedInput)
	if err != nil {
		return "", fmt.Errorf("cannot compute relative path: %w", err)
	}

	// Check for path traversal (.. or .. + separator)
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal detected: %s", rel)
	}

	return resolvedInput, nil
}

// ValidatePathExists checks if the path exists and is a file
func ValidatePathExists(resolvedPath string) error {
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return fmt.Errorf("file does not exist: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("path is a directory, not a file: %s", resolvedPath)
	}
	return nil
}

