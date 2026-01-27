package job

import (
	"errors"
	"fmt"
	"testing"
)

func TestStoreCreateQueueFull(t *testing.T) {
	store := NewStore()

	// Fill queue to capacity (without consumer, queue will fill up)
	// Use unique packageId for each job to avoid ErrJobAlreadyRunning
	for i := 0; i < 1000; i++ {
		j := &Job{
			PackageID: fmt.Sprintf("test-%d", i),
			InputPath: "/tmp/test.csv",
		}
		_, err := store.Create(j)
		if err != nil {
			t.Fatalf("Create() should succeed when queue has space: %v", err)
		}
	}

	// Now queue should be full - next Create should fail
	j := &Job{
		PackageID: "test-1000",
		InputPath: "/tmp/test.csv",
	}
	_, err := store.Create(j)
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("Create() should return ErrQueueFull when queue is full, got: %v", err)
	}

	// Job should not be created (only 1000 jobs in store)
	store.mu.RLock()
	jobCount := len(store.jobs)
	store.mu.RUnlock()
	if jobCount != 1000 {
		t.Errorf("Expected 1000 jobs, got %d", jobCount)
	}
}

