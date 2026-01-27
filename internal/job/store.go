package job

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrQueueFull is returned when the job queue is full
var ErrQueueFull = errors.New("queue is full")

// Store manages jobs in memory
type Store struct {
	mu      sync.RWMutex
	jobs    map[string]*Job
	queue   chan *Job
	cancels map[string]context.CancelFunc
}

// NewStore creates a new job store
func NewStore() *Store {
	return &Store{
		jobs:    make(map[string]*Job),
		queue:   make(chan *Job, 1000),
		cancels: make(map[string]context.CancelFunc),
	}
}

// Create creates a new job and returns its ID
// Returns ErrQueueFull if the queue is full (job is not created)
func (s *Store) Create(j *Job) (string, error) {
	j.ID = uuid.New().String()
	j.Status = StatusQueued

	// Try to send to queue (non-blocking)
	select {
	case s.queue <- j:
		// Successfully queued, now create the job
		s.mu.Lock()
		s.jobs[j.ID] = j
		s.mu.Unlock()
		return j.ID, nil
	default:
		// Queue full, don't create job
		return "", ErrQueueFull
	}
}

// Get retrieves a job by ID
func (s *Store) Get(id string) (*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, ok := s.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job not found: %s", id)
	}
	return j, nil
}

// UpdateStatus updates job status and related fields
func (s *Store) UpdateStatus(id string, status JobStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job not found: %s", id)
	}

	j.Status = status
	now := time.Now()

	switch status {
	case StatusRunning:
		if j.StartedAt == nil {
			j.StartedAt = &now
		}
	case StatusSucceeded, StatusFailed, StatusCanceled:
		if j.FinishedAt == nil {
			j.FinishedAt = &now
		}
	}

	return nil
}

// UpdateProgress updates job progress counters (deprecated, use UpdateParseProgress/UpdateSendProgress)
func (s *Store) UpdateProgress(id string, rowsRead, rowsSent, rowsSkipped, batchesSent, currentBatchNo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return
	}

	j.RowsRead = rowsRead
	j.RowsSent = rowsSent
	j.RowsSkipped = rowsSkipped
	j.BatchesSent = batchesSent
	j.CurrentBatchNo = currentBatchNo
}

// UpdateParseProgress updates parsing progress (rowsRead, rowsSkipped, currentBatchNo)
// Does not modify rowsSent or batchesSent
func (s *Store) UpdateParseProgress(id string, rowsRead, rowsSkipped, currentBatchNo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return
	}

	j.RowsRead = rowsRead
	j.RowsSkipped = rowsSkipped
	j.CurrentBatchNo = currentBatchNo
}

// UpdateSendProgress updates sending progress (rowsSent, batchesSent)
// Does not modify rowsRead, rowsSkipped, or currentBatchNo
func (s *Store) UpdateSendProgress(id string, rowsSent, batchesSent int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return
	}

	j.RowsSent = rowsSent
	j.BatchesSent = batchesSent
}

// UpdateErrors updates error counters
func (s *Store) UpdateErrors(id string, errorsTotal, errorsSent int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return
	}

	j.ErrorsTotal = errorsTotal
	j.ErrorsSent = errorsSent
}

// UpdateError updates job error message
func (s *Store) UpdateError(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return
	}

	if err != nil {
		j.LastError = err.Error()
	} else {
		j.LastError = ""
	}
}

// SetCancel registers a cancel function for a job
func (s *Store) SetCancel(jobID string, cf context.CancelFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.jobs[jobID]
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}

	s.cancels[jobID] = cf
	return nil
}

// ClearCancel removes cancel function for a job
func (s *Store) ClearCancel(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.cancels, jobID)
}

// Cancel cancels a job
func (s *Store) Cancel(id string) error {
	var cf context.CancelFunc

	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("job not found: %s", id)
	}

	if j.Status == StatusSucceeded || j.Status == StatusFailed || j.Status == StatusCanceled {
		s.mu.Unlock()
		return fmt.Errorf("job already finished: %s", j.Status)
	}

	// Get cancel function (if exists) and update status under lock
	if cancelFunc, exists := s.cancels[id]; exists {
		cf = cancelFunc
	}

	j.Status = StatusCanceled
	now := time.Now()
	j.FinishedAt = &now
	s.mu.Unlock()

	// Call cancel function outside of lock
	if cf != nil {
		cf()
	}

	return nil
}

// NextJob returns the next job from the queue (blocking)
func (s *Store) NextJob(ctx context.Context) (*Job, error) {
	select {
	case j := <-s.queue:
		return j, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

