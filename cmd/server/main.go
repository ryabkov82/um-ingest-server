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

	// Create timings for metrics collection (only if UM_TIMINGS=1)
	var timings *ingest.Timings
	if os.Getenv("UM_TIMINGS") == "1" {
		timings = ingest.NewTimings()
	}

	// Create processor
	processor, err := ingest.NewProcessor(j, store, allowedBaseDir, timings)
	if err != nil {
		log.Printf("Job %s: Failed to create processor: %v", j.ID, err)
		store.UpdateError(j.ID, err)
		store.UpdateStatus(j.ID, job.StatusFailed)
		return
	}

	// Create base sender for data batches
	dataBaseSender := client.NewSender(
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

	// Create async sender for data batches with callback to track progress
	var rowsSent, batchesSent, errorsSent int64
	asyncDataSender := client.NewAsyncSender(
		dataBaseSender,
		8, // queueSize
		jobCancel,
		func(batch *ingest.Batch) {
			// Callback after successful batch send
			rowsSent += int64(len(batch.Rows))
			batchesSent++
			store.UpdateSendProgress(j.ID, rowsSent, batchesSent)
			log.Printf("Job %s: Batch %d sent successfully (%d rows)", j.ID, batch.BatchNo, len(batch.Rows))
		},
	)
	asyncDataSender.Start(jobCtx, 1) // workers=1 to maintain order

	// Create async error sender if errorsEndpoint is configured
	var asyncErrorSender *client.AsyncErrorSender
	if j.Delivery.ErrorsEndpoint != "" {
		errorBaseSender := client.NewSender(
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
		asyncErrorSender = client.NewAsyncErrorSender(
			errorBaseSender,
			8, // queueSize
			jobCancel,
			func(errorBatch *ingest.ErrorBatch) {
				// Callback after successful error batch send
				errorsSent += int64(len(errorBatch.Errors))
				store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
				log.Printf("Job %s: Error batch %d sent successfully (%d errors)", j.ID, errorBatch.BatchNo, len(errorBatch.Errors))
			},
		)
		asyncErrorSender.Start(jobCtx, 1) // workers=1 to maintain order
	}

	// Start processing (parser goroutine)
	processErrChan := make(chan error, 1)
	go func() {
		processErrChan <- processor.Process(jobCtx)
	}()

	// Send batches and track progress using async senders
	batchChan := processor.GetBatchChan()
	errorChan := processor.GetErrorChan()
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
				if errorChanClosed || asyncErrorSender == nil {
					goto done
				}
				continue
			}

			// Enqueue batch for async sending
			if err := asyncDataSender.Enqueue(jobCtx, batch); err != nil {
				log.Printf("Job %s: Batch %d enqueue error: %v", j.ID, batch.BatchNo, err)
				// If enqueue failed due to fatal error, job will be canceled by async sender
				if err != context.Canceled {
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
				}
				// Context canceled or retryable error, continue
			}

		case errorBatch, ok := <-errorChan:
			if !ok {
				errorChanClosed = true
				if batchChanClosed {
					goto done
				}
				continue
			}

			// Enqueue error batch for async sending (required if errorsEndpoint is configured)
			if asyncErrorSender != nil {
				if errorBatch == nil || len(errorBatch.Errors) == 0 {
					// Skip empty error batches
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

				if err := asyncErrorSender.Enqueue(jobCtx, errorBatch); err != nil {
					log.Printf("Job %s: Error batch %d enqueue error: %v", j.ID, errorBatch.BatchNo, err)
					// If enqueue failed, error batch delivery will fail job (handled in CloseAndWait)
					if err != context.Canceled {
						// Format error message with details
						errorMsg := fmt.Sprintf("failed to enqueue error batch %d: %v", errorBatch.BatchNo, err)
						store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
						store.UpdateError(j.ID, fmt.Errorf(errorMsg))
						store.UpdateStatus(j.ID, job.StatusFailed)
						return
					}
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
	}

done:
	// Wait for all async senders to finish and check for errors
	var sendErr error
	if err := asyncDataSender.CloseAndWait(); err != nil {
		log.Printf("Job %s: Data sender error: %v", j.ID, err)
		sendErr = err
	}

	var errorSendErr error
	if asyncErrorSender != nil {
		if err := asyncErrorSender.CloseAndWait(); err != nil {
			log.Printf("Job %s: Error sender error: %v", j.ID, err)
			errorSendErr = err
			// Format error message with details
			errorMsg := fmt.Sprintf("failed to deliver error batch to %s: %v", j.Delivery.ErrorsEndpoint, err)
			if httpErr, ok := client.GetHTTPError(err); ok {
				bodySnippet := httpErr.Body
				if len(bodySnippet) > 200 {
					bodySnippet = bodySnippet[:200] + "..."
				}
				errorMsg = fmt.Sprintf("failed to deliver error batch to %s: HTTP %d: %s", j.Delivery.ErrorsEndpoint, httpErr.StatusCode, bodySnippet)
			}
			store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
			store.UpdateError(j.ID, fmt.Errorf(errorMsg))
			store.UpdateStatus(j.ID, job.StatusFailed)
			return
		}
	}

	// Final error count update
	if asyncErrorSender != nil {
		store.UpdateErrors(j.ID, processor.GetErrorsTotal(), errorsSent)
	}

	// Log timings summary (only if timings are enabled)
	if timings != nil {
		log.Printf("Job %s: Timings summary: %s", j.ID, timings.String())
	}

	// Check final status
	if sendErr != nil {
		store.UpdateError(j.ID, sendErr)
		store.UpdateStatus(j.ID, job.StatusFailed)
	} else if errorSendErr != nil {
		// Error batch delivery error already handled above
	} else if jobCtx.Err() == context.Canceled {
		store.UpdateStatus(j.ID, job.StatusCanceled)
	} else {
		log.Printf("Job %s: Completed successfully", j.ID)
		store.UpdateStatus(j.ID, job.StatusSucceeded)
	}
}
