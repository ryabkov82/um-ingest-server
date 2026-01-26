package job

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStoreCancel(t *testing.T) {
	store := NewStore()

	// Create a job
	j := &Job{
		PackageID: "test",
		InputPath:  "/tmp/test.csv",
		Status:     StatusQueued,
	}
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Create a context and cancel function
	ctx, cancel := context.WithCancel(context.Background())

	// Register cancel
	if err := store.SetCancel(jobID, cancel); err != nil {
		t.Fatalf("SetCancel() error = %v", err)
	}

	// Test cancel
	if err := store.Cancel(jobID); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	// Check that context was canceled
	select {
	case <-ctx.Done():
		// Expected
	case <-time.After(time.Second):
		t.Error("Context should be canceled")
	}

	// Check job status
	job, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if job.Status != StatusCanceled {
		t.Errorf("Expected status Canceled, got %s", job.Status)
	}

	// Clear cancel
	store.ClearCancel(jobID)

	// Try to cancel again (should fail - already finished)
	if err := store.Cancel(jobID); err == nil {
		t.Error("Cancel() should fail for already finished job")
	}
}

func TestStoreCancelNoDataRace(t *testing.T) {
	store := NewStore()

	// Create multiple jobs
	jobIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		j := &Job{
			PackageID: "test",
			InputPath: "/tmp/test.csv",
			Status:    StatusQueued,
		}
		jobID, err := store.Create(j)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		jobIDs[i] = jobID
	}

	// Concurrently set cancel functions
	var wg sync.WaitGroup
	for _, jobID := range jobIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			store.SetCancel(id, cancel)
			time.Sleep(10 * time.Millisecond)
			store.Cancel(id)
			store.ClearCancel(id)
			<-ctx.Done() // Wait for cancel
		}(jobID)
	}

	wg.Wait()

	// All jobs should be canceled
	for _, jobID := range jobIDs {
		j, err := store.Get(jobID)
		if err != nil {
			t.Errorf("Get() error = %v", err)
			continue
		}
		if j.Status != StatusCanceled {
			t.Errorf("Job %s: expected status Canceled, got %s", jobID, j.Status)
		}
	}
}

func TestStoreCancelQueuedJob(t *testing.T) {
	store := NewStore()

	// Create a job
	j := &Job{
		PackageID: "test",
		InputPath: "/tmp/test.csv",
		Status:    StatusQueued,
	}
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Cancel without setting cancel function (job is queued)
	if err := store.Cancel(jobID); err != nil {
		t.Fatalf("Cancel() should work even without cancel function: %v", err)
	}

	// Check job status
	job, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if job.Status != StatusCanceled {
		t.Errorf("Expected status Canceled, got %s", job.Status)
	}
}

