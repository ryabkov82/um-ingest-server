package client

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
)

// OnErrorBatchSent is called after an error batch is successfully sent
// errorBatch: the error batch that was sent
type OnErrorBatchSent func(errorBatch *ingest.ErrorBatch)

// AsyncErrorSender wraps a Sender to provide asynchronous error batch sending with a bounded queue
// Any error during sending will fail the job (errorsEndpoint delivery is required)
type AsyncErrorSender struct {
	base             *Sender
	q                chan *ingest.ErrorBatch
	wg               sync.WaitGroup
	cancel           context.CancelFunc
	errOnce          sync.Once
	err              atomic.Value // stores error
	ctx              context.Context
	onErrorBatchSent OnErrorBatchSent // callback after successful send
	onFatalError     OnFatalError     // callback on fatal error (before cancel)
	jobID            string           // job identifier for logging
}

// NewAsyncErrorSender creates a new AsyncErrorSender for error batches
// base: underlying sender to use for actual HTTP requests (must have isErrors=true)
// queueSize: size of the bounded queue (must be > 0)
// cancel: cancel function to call on fatal error
// onErrorBatchSent: optional callback called after successful error batch send
// onFatalError: optional callback called on fatal error (before cancel)
// jobID: job identifier for logging
func NewAsyncErrorSender(base *Sender, queueSize int, cancel context.CancelFunc, onErrorBatchSent OnErrorBatchSent, onFatalError OnFatalError, jobID string) *AsyncErrorSender {
	if queueSize < 1 {
		queueSize = 1
	}
	return &AsyncErrorSender{
		base:             base,
		q:                make(chan *ingest.ErrorBatch, queueSize),
		cancel:           cancel,
		onErrorBatchSent: onErrorBatchSent,
		onFatalError:     onFatalError,
		jobID:            jobID,
	}
}

// Start starts worker goroutines to process error batches from the queue
// ctx: context for workers (will be used for base.SendErrorBatch calls)
func (a *AsyncErrorSender) Start(ctx context.Context, workers int) {
	a.ctx = ctx
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		a.wg.Add(1)
		go a.worker(ctx)
	}
}

// worker processes error batches from the queue
func (a *AsyncErrorSender) worker(ctx context.Context) {
	defer a.wg.Done()

	for errorBatch := range a.q {
		// Skip nil batches (can happen if channel was closed)
		if errorBatch == nil {
			continue
		}

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

		// Send error batch using base sender
		// Any error is fatal for error batch delivery
		err := a.base.SendErrorBatch(ctx, errorBatch)
		if err != nil {
			// Fatal error - store it and cancel context
			a.errOnce.Do(func() {
				a.err.Store(err)
				if a.cancel != nil {
					a.cancel()
				}
			})
			// Continue processing remaining batches in queue (they will see ctx.Done)
		} else {
			// Successfully sent - call callback if provided
			if a.onErrorBatchSent != nil {
				a.onErrorBatchSent(errorBatch)
			}
		}
	}
}

// Enqueue adds an error batch to the queue for asynchronous sending
// Returns error if context is done or if a fatal error was already encountered
func (a *AsyncErrorSender) Enqueue(ctx context.Context, errorBatch *ingest.ErrorBatch) error {
	// Never enqueue empty error batches
	if errorBatch == nil || len(errorBatch.Errors) == 0 {
		return nil
	}

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

	// Try to enqueue error batch
	select {
	case a.q <- errorBatch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseAndWait closes the queue and waits for all workers to finish
// Returns the first fatal error encountered (if any), otherwise nil
func (a *AsyncErrorSender) CloseAndWait() error {
	// Close queue
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
func (a *AsyncErrorSender) logFatalError(batchNo int64, err error) {
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

	log.Printf("Job %s: Fatal error sending error batch %d: statusCode=%d retryAfter=%v body=%q",
		a.jobID, batchNo, statusCode, retryAfter, body)
}

