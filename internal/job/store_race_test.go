package job

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestStoreCreateRace tests that job is registered in store before worker can access it
// This prevents race condition where worker calls UpdateStatus/SetCancel before job is registered
func TestStoreCreateRace(t *testing.T) {
	// Create store with very small queue to increase chance of race
	store := &Store{
		jobs:              make(map[string]*Job),
		activeJobByPackage: make(map[string]string),
		lastJobByPackage:   make(map[string]string),
		queue:             make(chan *Job, 1), // Very small queue
		cancels:           make(map[string]context.CancelFunc),
	}

	// Channel to synchronize worker start
	workerStarted := make(chan struct{})
	workerReady := make(chan struct{})

	// Start worker goroutine that immediately reads from queue
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(workerReady)
		<-workerStarted

		// Worker immediately tries to get job and update status
		j, err := store.NextJob(ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}
			t.Errorf("NextJob() error = %v", err)
			return
		}

		// Immediately try to update status (this should work because job is already registered)
		if err := store.UpdateStatus(j.ID, StatusRunning); err != nil {
			t.Errorf("UpdateStatus() error = %v (job should be registered)", err)
		}

		// Verify job is in Running status
		job, err := store.Get(j.ID)
		if err != nil {
			t.Errorf("Get() error = %v", err)
			return
		}
		if job.Status != StatusRunning {
			t.Errorf("Expected status Running, got %s", job.Status)
		}
	}()

	// Wait for worker to be ready
	<-workerReady

	// Create job (this should register it in store before enqueueing)
	j := &Job{
		PackageID: "test-race",
		InputPath:  "/tmp/test.csv",
		Status:     StatusQueued,
	}

	// Signal worker to start (race condition: worker will try to read from queue)
	close(workerStarted)

	// Create job (register + enqueue)
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Wait for worker to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Worker finished successfully
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not finish within timeout")
	}

	// Verify job exists and is in correct state
	finalJob, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	// Job should be in Running status (updated by worker)
	if finalJob.Status != StatusRunning {
		t.Errorf("Expected status Running, got %s", finalJob.Status)
	}

	// Cleanup
	cancel()
}

// TestStoreCreateQueueFullRollback tests that job registration is rolled back if queue is full
func TestStoreCreateQueueFullRollback(t *testing.T) {
	// Create store with very small queue (size = 1)
	store := &Store{
		jobs:              make(map[string]*Job),
		activeJobByPackage: make(map[string]string),
		lastJobByPackage:   make(map[string]string),
		queue:             make(chan *Job, 1), // Queue size = 1
		cancels:           make(map[string]context.CancelFunc),
	}

	// Fill the queue with first job
	j1 := &Job{
		PackageID: "test-1",
		InputPath:  "/tmp/test1.csv",
	}
	jobID1, err := store.Create(j1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Verify j1 is in store
	_, err = store.Get(jobID1)
	if err != nil {
		t.Fatalf("Job j1 should be in store, got error: %v", err)
	}

	// Try to create second job (should fail with ErrQueueFull because queue is full)
	j2 := &Job{
		PackageID: "test-2",
		InputPath:  "/tmp/test2.csv",
	}
	jobID2, err := store.Create(j2)
	if err != ErrQueueFull {
		t.Errorf("Expected ErrQueueFull, got %v (jobID2=%s)", err, jobID2)
	}

	// Verify j2 is NOT in store (rollback worked)
	// Note: j2.ID is generated in Create(), so we need to check if it exists
	if jobID2 != "" {
		_, err = store.Get(jobID2)
		if err == nil {
			t.Error("Job j2 should not be in store after ErrQueueFull")
		}
	}

	// Verify j2 is NOT in activeJobByPackage
	activeJobID, _ := store.GetActiveJobByPackage("test-2")
	if activeJobID != "" {
		t.Errorf("Package test-2 should not have active job, got %s", activeJobID)
	}

	// Verify j1 is still in store
	_, err = store.Get(jobID1)
	if err != nil {
		t.Errorf("Job j1 should still be in store, got error: %v", err)
	}
}

