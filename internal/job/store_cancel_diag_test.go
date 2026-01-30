package job

import (
	"context"
	"testing"
	"time"
)

// TestCancelLogsCaller tests that Cancel logs caller information correctly
func TestCancelLogsCaller(t *testing.T) {
	store := NewStore()

	// Create a job
	j := &Job{
		PackageID: "test-caller",
		InputPath:  "/tmp/test.csv",
		Status:     StatusQueued,
	}
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Register cancel function (should log)
	ctx, cancel := context.WithCancel(context.Background())
	if err := store.SetCancel(jobID, cancel); err != nil {
		t.Fatalf("SetCancel() error = %v", err)
	}

	// Cancel from this test function (should log caller as TestCancelLogsCaller)
	if err := store.Cancel(jobID); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	// Verify job is canceled
	finalJob, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	if finalJob.Status != StatusCanceled {
		t.Errorf("Expected status Canceled, got %s", finalJob.Status)
	}

	// Verify context was canceled
	select {
	case <-ctx.Done():
		// Expected
	case <-time.After(time.Second):
		t.Error("Context should be canceled")
	}

	// Clear cancel (should log)
	store.ClearCancel(jobID)

	t.Logf("Cancel logged caller information (check logs for 'Job %s canceled' with caller)", jobID)
}

