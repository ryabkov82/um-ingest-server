package main

import (
	"context"
	"flag"
	"fmt"
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
	"github.com/ryabkov82/um-ingest-server/internal/version"
)

func main() {
	// Handle --version flag
	showVersion := flag.Bool("version", false, "Show version information")
	showBuildInfo := flag.Bool("build-info", false, "Show build information in JSON format")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		os.Exit(0)
	}

	if *showBuildInfo {
		info := version.Info()
		fmt.Printf("{\n")
		fmt.Printf("  \"name\": \"%s\",\n", info["name"])
		fmt.Printf("  \"version\": \"%s\",\n", info["version"])
		fmt.Printf("  \"gitCommit\": \"%s\",\n", info["gitCommit"])
		fmt.Printf("  \"buildTime\": \"%s\",\n", info["buildTime"])
		fmt.Printf("  \"goVersion\": \"%s\"\n", info["goVersion"])
		fmt.Printf("}\n")
		os.Exit(0)
	}

	// Log version at startup
	log.Printf("Starting %s", version.String())

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

	// Get 1C Basic auth credentials from environment
	env1CUser := os.Getenv("UM_1C_BASIC_USER")
	env1CPass := os.Getenv("UM_1C_BASIC_PASS")

	// Create job store
	store := job.NewStore()

	// Start worker goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker(ctx, store, allowedBaseDir, env1CUser, env1CPass)

	// Setup HTTP server
	handler := httpapi.NewHandler(store, allowedBaseDir, env1CUser, env1CPass)
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
func worker(ctx context.Context, store *job.Store, allowedBaseDir, env1CUser, env1CPass string) {
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
		processJob(ctx, j, store, allowedBaseDir, env1CUser, env1CPass)
	}
}

// processJob processes a single job
func processJob(ctx context.Context, j *job.Job, store *job.Store, allowedBaseDir, env1CUser, env1CPass string) {
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

	// Create sender for data batches
	sender := client.NewSender(
		j.Delivery.Endpoint,
		j.Delivery.Gzip,
		j.Delivery.TimeoutSeconds,
		j.Delivery.MaxRetries,
		j.Delivery.BackoffMs,
		j.Delivery.BackoffMaxMs,
		j.Delivery.Auth,
		env1CUser,
		env1CPass,
	)

	// Create error sender if errorsEndpoint is configured
	var errorSender *client.Sender
	if j.Delivery.ErrorsEndpoint != "" {
		errorSender = client.NewSender(
			j.Delivery.ErrorsEndpoint,
			j.Delivery.Gzip,
			j.Delivery.TimeoutSeconds,
			j.Delivery.MaxRetries,
			j.Delivery.BackoffMs,
			j.Delivery.BackoffMaxMs,
			j.Delivery.Auth,
			env1CUser,
			env1CPass,
		)
	}

	// Start processing (parser goroutine)
	processErrChan := make(chan error, 1)
	go func() {
		processErrChan <- processor.Process(jobCtx)
	}()

	// Send batches and track progress
	batchChan := processor.GetBatchChan()
	errorChan := processor.GetErrorChan()
	var sendErr error
	var rowsSent, batchesSent, errorsSent int64
	batchChanClosed := false
	errorChanClosed := false

	for {
		select {
		case <-jobCtx.Done():
			// Job was canceled
			store.UpdateStatus(j.ID, job.StatusCanceled)
			return
		case batch, ok := <-batchChan:
			if !ok {
				batchChanClosed = true
				if errorChanClosed || errorSender == nil {
					goto done
				}
				continue
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

		case errorBatch, ok := <-errorChan:
			if !ok {
				errorChanClosed = true
				if batchChanClosed {
					goto done
				}
				continue
			}

			// Send error batch
			if errorSender != nil {
				if err := errorSender.SendErrorBatch(jobCtx, errorBatch); err != nil {
					log.Printf("Job %s: Error batch send error: %v", j.ID, err)
					// Don't fail job on error batch send failure
				} else {
					errorsSent += int64(len(errorBatch.Errors))
					store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
					log.Printf("Job %s: Error batch sent successfully (%d errors)", j.ID, len(errorBatch.Errors))
				}
			}

		case err := <-processErrChan:
			if err != nil && err != context.Canceled {
				log.Printf("Job %s: Processing error: %v", j.ID, err)
				store.UpdateError(j.ID, err)
				store.UpdateStatus(j.ID, job.StatusFailed)
				return
			}
			// Processing done, continue to process remaining batches and errors
		}

		// Check if both channels are closed
		if batchChanClosed && (errorChanClosed || errorSender == nil) {
			goto done
		}
	}

done:
	// Wait for any remaining batches
	if !batchChanClosed {
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
	}

	// Wait for any remaining error batches
	if errorSender != nil && !errorChanClosed {
		for errorBatch := range errorChan {
			if err := errorSender.SendErrorBatch(jobCtx, errorBatch); err != nil {
				log.Printf("Job %s: Final error batch send error: %v", j.ID, err)
			} else {
				errorsSent += int64(len(errorBatch.Errors))
				store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
			}
		}
	}

	// Final error count update
	if errorSender != nil {
		store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
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
