package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers
	"os"
	"os/signal"
	"runtime/pprof"
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

	// Start CPU profiling if requested
	cpuProfilePath := os.Getenv("UM_CPU_PROFILE")
	if cpuProfilePath != "" {
		f, err := os.Create(cpuProfilePath)
		if err != nil {
			log.Fatalf("Failed to create CPU profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Failed to start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
		log.Printf("CPU profiling enabled, writing to %s", cpuProfilePath)
	}

	// Start pprof HTTP server if requested
	pprofAddr := os.Getenv("UM_PPROF_ADDR")
	if pprofAddr != "" {
		go func() {
			log.Printf("Starting pprof HTTP server on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof HTTP server error: %v", err)
			}
		}()
	}

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
	debugErrors := os.Getenv("UM_DEBUG_ERRORS") == "1"

	// Create cancel context for this job from parent context
	jobCtx, jobCancel := context.WithCancel(ctx)
	defer jobCancel()

	// Register cancel function in store
	if err := store.SetCancel(j.ID, jobCancel); err != nil {
		log.Printf("Job %s: Failed to register cancel: %v", j.ID, err)
	}
	defer store.ClearCancel(j.ID)

	// Create timings for metrics collection
	timings := ingest.NewTimings()

	// Create processor
	processor, err := ingest.NewProcessor(j, store, allowedBaseDir, timings)
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
		timings,
		false, // isErrors = false for data endpoint
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
			timings,
			true, // isErrors = true for errors endpoint
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

			// Send error batch (required if errorsEndpoint is configured)
			if errorSender != nil {

				if errorBatch == nil || len(errorBatch.Errors) == 0 {
					// optional log
					continue
				}

				if debugErrors {
					first := errorBatch.Errors[0]
					log.Printf("[debug] job=%s errorBatch: batchNo=%d errors=%d first={rowNo=%d code=%q msg=%q} processorErrorsTotal=%d storeRowsRead=%d storeErrorsTotal=%d",
						j.ID,
						errorBatch.BatchNo,
						len(errorBatch.Errors),
						first.RowNo,
						first.Code,
						first.Message,
						processor.GetErrorsTotal(),
						j.RowsRead,
						j.ErrorsTotal,
					)
				}

				if err := errorSender.SendErrorBatch(jobCtx, errorBatch); err != nil {
					log.Printf("Job %s: Error batch %d send error: %v", j.ID, errorBatch.BatchNo, err)
					// Fail job if error batch delivery fails (errorsEndpoint is configured)
					// Format error message with details
					errorMsg := fmt.Sprintf("failed to deliver error batch %d to %s: %v", errorBatch.BatchNo, j.Delivery.ErrorsEndpoint, err)
					if httpErr, ok := client.GetHTTPError(err); ok {
						bodySnippet := httpErr.Body
						if len(bodySnippet) > 200 {
							bodySnippet = bodySnippet[:200] + "..."
						}
						errorMsg = fmt.Sprintf("failed to deliver error batch %d to %s: HTTP %d: %s", errorBatch.BatchNo, j.Delivery.ErrorsEndpoint, httpErr.StatusCode, bodySnippet)
					}
					// Best-effort: persist current error counters before failing
					store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
					store.UpdateError(j.ID, fmt.Errorf(errorMsg))
					store.UpdateStatus(j.ID, job.StatusFailed)
					return
				} else {
					errorsSent += int64(len(errorBatch.Errors))
					store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
					log.Printf("Job %s: Error batch %d sent successfully (%d errors)", j.ID, errorBatch.BatchNo, len(errorBatch.Errors))
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

	// Wait for any remaining error batches (required if errorsEndpoint is configured)
	if errorSender != nil && !errorChanClosed {
		for errorBatch := range errorChan {
			// Skip empty error batches
			if len(errorBatch.Errors) == 0 {
				log.Printf("Job %s: Skipping empty final error batch %d", j.ID, errorBatch.BatchNo)
				continue
			}

			if err := errorSender.SendErrorBatch(jobCtx, errorBatch); err != nil {
				log.Printf("Job %s: Final error batch %d send error: %v", j.ID, errorBatch.BatchNo, err)
				// Fail job if error batch delivery fails (errorsEndpoint is configured)
				errorMsg := fmt.Sprintf("failed to deliver error batch %d to %s: %v", errorBatch.BatchNo, j.Delivery.ErrorsEndpoint, err)
				if httpErr, ok := client.GetHTTPError(err); ok {
					bodySnippet := httpErr.Body
					if len(bodySnippet) > 200 {
						bodySnippet = bodySnippet[:200] + "..."
					}
					errorMsg = fmt.Sprintf("failed to deliver error batch %d to %s: HTTP %d: %s", errorBatch.BatchNo, j.Delivery.ErrorsEndpoint, httpErr.StatusCode, bodySnippet)
				}
				store.UpdateError(j.ID, fmt.Errorf(errorMsg))
				store.UpdateStatus(j.ID, job.StatusFailed)
				return
			} else {
				errorsSent += int64(len(errorBatch.Errors))
				store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
			}
		}
	}

	// Final check: flush any remaining errors in buffer (only if errorsEndpoint is configured)
	// This will only send if there are actual errors in the buffer
	if errorSender != nil {
		// Wait a bit to ensure processor has flushed its buffer
		// The processor will flush remaining errors in parseAndBatch before closing errorChan
		// So by the time we get here, all errors should have been sent via errorChan
		// But we don't need to do anything here - errors are sent via errorChan
	}

	// Final error count update
	if errorSender != nil {
		store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
	}

	// Log timings summary
	log.Printf("Job %s: Timings summary: %s", j.ID, timings.String())

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
