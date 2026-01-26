package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/client"
	"github.com/ryabkov82/um-ingest-server/internal/httpapi"
	"github.com/ryabkov82/um-ingest-server/internal/ingest"
	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func main() {
	// Get configuration from environment
	allowedBaseDir := os.Getenv("ALLOWED_BASE_DIR")
	if allowedBaseDir == "" {
		allowedBaseDir = "/data/incoming"
		log.Printf("Using default ALLOWED_BASE_DIR: %s", allowedBaseDir)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Create job store
	store := job.NewStore()

	// Start worker goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker(ctx, store, allowedBaseDir)

	// Setup HTTP server
	handler := httpapi.NewHandler(store, allowedBaseDir)
	router := httpapi.SetupRouter(handler)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Server starting on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-sigChan
	log.Println("Shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	cancel() // Cancel worker context

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Server stopped")
}

// worker processes jobs from the queue (synchronously, one at a time)
func worker(ctx context.Context, store *job.Store, allowedBaseDir string) {
	for {
		// Get next job (blocking)
		j, err := store.NextJob(ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}
			log.Printf("Error getting next job: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Process job synchronously (no goroutine)
		processJob(ctx, j, store, allowedBaseDir)
	}
}

// processJob processes a single job
func processJob(ctx context.Context, j *job.Job, store *job.Store, allowedBaseDir string) {
	// Create cancel context for this job from parent context
	jobCtx, jobCancel := context.WithCancel(ctx)
	defer jobCancel()

	// Register cancel function in store
	if err := store.SetCancel(j.ID, jobCancel); err != nil {
		log.Printf("Job %s: Failed to register cancel: %v", j.ID, err)
	}
	defer store.ClearCancel(j.ID)

	// Create processor
	processor, err := ingest.NewProcessor(j, store, allowedBaseDir)
	if err != nil {
		log.Printf("Job %s: Failed to create processor: %v", j.ID, err)
		store.UpdateError(j.ID, err)
		store.UpdateStatus(j.ID, job.StatusFailed)
		return
	}

	// Create sender
	sender := client.NewSender(
		j.Delivery.Endpoint,
		j.Delivery.Gzip,
		j.Delivery.TimeoutSeconds,
		j.Delivery.MaxRetries,
		j.Delivery.BackoffMs,
		j.Delivery.BackoffMaxMs,
	)

	// Start processing (parser goroutine)
	processErrChan := make(chan error, 1)
	go func() {
		processErrChan <- processor.Process(jobCtx)
	}()

	// Send batches and track progress
	batchChan := processor.GetBatchChan()
	var sendErr error
	var rowsSent, batchesSent int64

	for {
		select {
		case <-jobCtx.Done():
			// Job was canceled
			store.UpdateStatus(j.ID, job.StatusCanceled)
			return
		case batch, ok := <-batchChan:
			if !ok {
				// Channel closed, processing done
				goto done
			}

			// Send batch
			if err := sender.SendBatch(jobCtx, batch); err != nil {
				log.Printf("Job %s: Batch %d send error: %v", j.ID, batch.BatchNo, err)
				sendErr = err
				// Check if error is fatal (4xx except 429)
				if httpErr, ok := client.GetHTTPError(err); ok {
					if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 && httpErr.StatusCode != 429 {
						// Fatal error, cancel processing
						jobCancel()
						store.UpdateError(j.ID, err)
						store.UpdateStatus(j.ID, job.StatusFailed)
						return
					}
				}
				// Retryable error, continue (don't count as sent)
			} else {
				// Successfully sent - update sending progress
				rowsSent += int64(len(batch.Rows))
				batchesSent++
				store.UpdateSendProgress(j.ID, rowsSent, batchesSent)
				log.Printf("Job %s: Batch %d sent successfully (%d rows)", j.ID, batch.BatchNo, len(batch.Rows))
			}

		case err := <-processErrChan:
			if err != nil && err != context.Canceled {
				log.Printf("Job %s: Processing error: %v", j.ID, err)
				store.UpdateError(j.ID, err)
				store.UpdateStatus(j.ID, job.StatusFailed)
				return
			}
			goto done
		}
	}

done:
	// Wait for any remaining batches
	for batch := range batchChan {
		if err := sender.SendBatch(jobCtx, batch); err != nil {
			log.Printf("Job %s: Final batch %d send error: %v", j.ID, batch.BatchNo, err)
			sendErr = err
		} else {
			// Successfully sent
			rowsSent += int64(len(batch.Rows))
			batchesSent++
			store.UpdateSendProgress(j.ID, rowsSent, batchesSent)
		}
	}

	// Check final status
	if sendErr != nil {
		store.UpdateError(j.ID, sendErr)
		store.UpdateStatus(j.ID, job.StatusFailed)
	} else if jobCtx.Err() == context.Canceled {
		store.UpdateStatus(j.ID, job.StatusCanceled)
	} else {
		log.Printf("Job %s: Completed successfully", j.ID)
		store.UpdateStatus(j.ID, job.StatusSucceeded)
	}
}
