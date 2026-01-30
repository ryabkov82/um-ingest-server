package client

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
)

// OnBatchSent is called after a batch is successfully sent
// batch: the batch that was sent
type OnBatchSent func(batch *ingest.Batch)

// OnFatalError is called when a fatal error occurs (before cancel)
// jobID: job identifier
// batchNo: batch number (0 for error batches)
// endpointType: "data" or "errors"
// err: the fatal error
type OnFatalError func(jobID string, batchNo int64, endpointType string, err error)

// AsyncSender wraps a Sender to provide asynchronous batch sending with a bounded queue
type AsyncSender struct {
	base         *Sender
	q            chan *ingest.Batch
	wg           sync.WaitGroup
	cancel       context.CancelFunc
	errOnce      sync.Once
	err          atomic.Value // stores error
	ctx          context.Context
	onBatchSent  OnBatchSent  // callback after successful send
	onFatalError OnFatalError // callback on fatal error (before cancel)
	jobID        string       // job identifier for logging
}

// NewAsyncSender creates a new AsyncSender
// base: underlying sender to use for actual HTTP requests
// queueSize: size of the bounded queue (must be > 0)
// cancel: cancel function to call on fatal error
// onBatchSent: optional callback called after successful batch send
// onFatalError: optional callback called on fatal error (before cancel)
// jobID: job identifier for logging
func NewAsyncSender(base *Sender, queueSize int, cancel context.CancelFunc, onBatchSent OnBatchSent, onFatalError OnFatalError, jobID string) *AsyncSender {
	if queueSize < 1 {
		queueSize = 1
	}
	return &AsyncSender{
		base:         base,
		q:            make(chan *ingest.Batch, queueSize),
		cancel:       cancel,
		onBatchSent:  onBatchSent,
		onFatalError: onFatalError,
		jobID:        jobID,
	}
}

// Start starts worker goroutines to process batches from the queue
// ctx: context for workers (will be used for base.SendBatch calls)
func (a *AsyncSender) Start(ctx context.Context, workers int) {
	a.ctx = ctx
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		a.wg.Add(1)
		go a.worker(ctx)
	}
}

// worker processes batches from the queue
func (a *AsyncSender) worker(ctx context.Context) {
	defer a.wg.Done()

	for batch := range a.q {
		// Check if context is already done
		select {
		case <-ctx.Done():
			// Store context error if no other error was stored
			a.errOnce.Do(func() {
				a.err.Store(ctx.Err())
			})
			return
		default:
		}

		// Send batch using base sender
		err := a.base.SendBatch(ctx, batch)
		if err != nil {
			// Check if error is fatal (4xx except 429) for data batches
			isFatal := false
			if httpErr, ok := GetHTTPError(err); ok {
				if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 && httpErr.StatusCode != 429 {
					isFatal = true
				}
			} else {
				// Network errors are retryable, not fatal
				isFatal = false
			}

			if isFatal {
				// Fatal error - log, store it, save to store, then cancel context
				a.errOnce.Do(func() {
					a.err.Store(err)
					// Log fatal error with details
					a.logFatalError(batch.BatchNo, err)
					// Call callback to save error in store (before cancel)
					if a.onFatalError != nil {
						a.onFatalError(a.jobID, batch.BatchNo, "data", err)
					}
					if a.cancel != nil {
						a.cancel()
					}
				})
				// Continue processing remaining batches in queue (they will see ctx.Done())
			}
			// Retryable errors are not stored - they will be retried by base.SendBatch
		} else {
			// Successfully sent - call callback if provided
			if a.onBatchSent != nil {
				a.onBatchSent(batch)
			}
		}
	}
}

// Enqueue adds a batch to the queue for asynchronous sending
// Returns error if context is done or if a fatal error was already encountered
func (a *AsyncSender) Enqueue(ctx context.Context, batch *ingest.Batch) error {
	// Check if context is already done
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Check if fatal error was already encountered
	if storedErr := a.err.Load(); storedErr != nil {
		if err, ok := storedErr.(error); ok && err != nil {
			return err
		}
	}

	// Try to enqueue batch
	select {
	case a.q <- batch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseAndWait closes the queue and waits for all workers to finish
// Returns the first fatal error encountered (if any), otherwise nil
func (a *AsyncSender) CloseAndWait() error {
	// Close queue (safe to call multiple times, but we'll use sync.Once pattern)
	close(a.q)

	// Wait for all workers to finish
	a.wg.Wait()

	// Return first fatal error (if any)
	if storedErr := a.err.Load(); storedErr != nil {
		if err, ok := storedErr.(error); ok && err != nil {
			return err
		}
	}
	return nil
}

// logFatalError logs fatal error with details
func (a *AsyncSender) logFatalError(batchNo int64, err error) {
	var statusCode int
	var retryAfter time.Duration
	var body string

	if httpErr, ok := GetHTTPError(err); ok {
		statusCode = httpErr.StatusCode
		retryAfter = httpErr.RetryAfter
		body = httpErr.Body
		// Truncate body to 2KB
		if len(body) > 2048 {
			body = body[:2048] + "..."
		}
	}

	log.Printf("Job %s: Fatal error sending data batch %d: statusCode=%d retryAfter=%v body=%q",
		a.jobID, batchNo, statusCode, retryAfter, body)
}
