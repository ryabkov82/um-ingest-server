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
	"strconv"
	"sync"
	"sync/atomic"
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

	// Helper function to parse int env with validation
	parseIntEnv := func(key string, defaultValue, min, max int) int {
		valStr := os.Getenv(key)
		if valStr == "" {
			return defaultValue
		}
		val, err := strconv.Atoi(valStr)
		if err != nil {
			log.Printf("WARNING: Invalid %s value '%s', using default %d", key, valStr, defaultValue)
			return defaultValue
		}
		if val < min || val > max {
			log.Printf("WARNING: %s value %d out of range [%d, %d], using default %d", key, val, min, max, defaultValue)
			return defaultValue
		}
		return val
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

	// Parse async sender configuration
	sendWorkers := parseIntEnv("UM_SEND_WORKERS", 1, 1, 16)
	sendQueue := parseIntEnv("UM_SEND_QUEUE", 8, 1, 1024)
	errSendWorkers := parseIntEnv("UM_ERR_SEND_WORKERS", sendWorkers, 1, 16)
	errSendQueue := parseIntEnv("UM_ERR_SEND_QUEUE", sendQueue, 1, 1024)

	// Create job store
	store := job.NewStore()

	// Start worker goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker(ctx, store, allowedBaseDir, env1CUser, env1CPass, sendWorkers, sendQueue, errSendWorkers, errSendQueue)

	// Setup HTTP server
	// Pass server context to handler (jobs will use this context, not HTTP request context)
	handler := httpapi.NewHandler(store, allowedBaseDir, env1CUser, env1CPass, ctx)
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
func worker(ctx context.Context, store *job.Store, allowedBaseDir, env1CUser, env1CPass string, sendWorkers, sendQueue, errSendWorkers, errSendQueue int) {
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
		processJob(ctx, j, store, allowedBaseDir, env1CUser, env1CPass, sendWorkers, sendQueue, errSendWorkers, errSendQueue)
	}
}

// processJob processes a single job
func processJob(ctx context.Context, j *job.Job, store *job.Store, allowedBaseDir, env1CUser, env1CPass string, sendWorkers, sendQueue, errSendWorkers, errSendQueue int) {
	debugErrors := os.Getenv("UM_DEBUG_ERRORS") == "1"

	// Track final error and context error for logging
	var finalErr error
	var ctxErr error

	// Defer log at exit (will execute on any return)
	defer func() {
		// Get final job state from store
		finalJob, _ := store.Get(j.ID)
		if finalJob == nil {
			log.Printf("Job %s: exit (job not found in store)", j.ID)
			return
		}

		// Determine exit reason
		exitReason := ""
		if ctxErr == context.Canceled && finalErr == nil {
			exitReason = "job canceled: ctx canceled without error"
		} else if finalErr != nil {
			exitReason = fmt.Sprintf("job failed: err=%v", finalErr)
		} else if finalJob.Status == job.StatusCanceled {
			exitReason = "job canceled"
		} else if finalJob.Status == job.StatusFailed {
			exitReason = fmt.Sprintf("job failed: lastError=%q", finalJob.LastError)
		} else if finalJob.Status == job.StatusSucceeded {
			exitReason = "job succeeded"
		}

		// Log exit with all details
		log.Printf("Job %s: exit status=%s err=%v ctxErr=%v lastError=%q rowsRead=%d rowsSent=%d currentBatchNo=%d reason=%s",
			j.ID,
			finalJob.Status,
			finalErr,
			ctxErr,
			finalJob.LastError,
			finalJob.RowsRead,
			finalJob.RowsSent,
			finalJob.CurrentBatchNo,
			exitReason,
		)
	}()

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

	// Atomic counters for async send progress (updated in callbacks, synced to store periodically)
	var sentRows atomic.Int64
	var sentBatches atomic.Int64
	var sentErrors atomic.Int64
	var sentErrorBatches atomic.Int64

	// Create async sender for data batches with callback to track progress
	asyncDataSender := client.NewAsyncSender(
		dataBaseSender,
		sendQueue,
		jobCancel,
		func(batch *ingest.Batch) {
			// Callback after successful batch send - only update atomic counters
			sentRows.Add(int64(len(batch.Rows)))
			sentBatches.Add(1)
			log.Printf("Job %s: Batch %d sent successfully (%d rows)", j.ID, batch.BatchNo, len(batch.Rows))
		},
		func(jobID string, batchNo int64, endpointType string, err error) {
			// Callback on fatal error - save to store BEFORE cancel
			store.UpdateError(j.ID, err)
		},
		j.ID,
	)
	asyncDataSender.Start(jobCtx, sendWorkers)

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
			errSendQueue,
			jobCancel,
			func(errorBatch *ingest.ErrorBatch) {
				// Callback after successful error batch send - only update atomic counters
				sentErrors.Add(int64(len(errorBatch.Errors)))
				sentErrorBatches.Add(1)
				log.Printf("Job %s: Error batch %d sent successfully (%d errors)", j.ID, errorBatch.BatchNo, len(errorBatch.Errors))
			},
			func(jobID string, batchNo int64, endpointType string, err error) {
				// Callback on fatal error - save to store BEFORE cancel
				store.UpdateError(j.ID, err)
			},
			j.ID,
		)
		asyncErrorSender.Start(jobCtx, errSendWorkers)
	}

	// Start periodic sync of counters to store (every 1 second)
	syncTicker := time.NewTicker(1 * time.Second)
	stopSync := make(chan struct{})
	syncDone := make(chan struct{})
	var stopOnce sync.Once
	stopSyncFunc := func() {
		close(stopSync)
	}
	go func() {
		defer close(syncDone)
		for {
			select {
			case <-syncTicker.C:
				// Periodic sync
				store.UpdateSendProgress(j.ID, sentRows.Load(), sentBatches.Load())
				if asyncErrorSender != nil {
					store.UpdateErrors(j.ID, processor.GetErrorsTotal(), sentErrors.Load())
				}
			case <-jobCtx.Done():
				return
			case <-stopSync:
				return
			}
		}
	}()

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
	shouldFinalize := false // Flag to exit loop and finalize

	for !shouldFinalize {
		select {
		case <-jobCtx.Done():
			// Job was canceled - mark for finalization
			ctxErr = jobCtx.Err()
			shouldFinalize = true
		case batch, ok := <-batchChan:
			if !ok {
				batchChanClosed = true
				if errorChanClosed || asyncErrorSender == nil {
					shouldFinalize = true
				} else {
					continue
				}
			}

			// Enqueue batch for async sending
			if err := asyncDataSender.Enqueue(jobCtx, batch); err != nil {
				// If enqueue failed due to fatal error, error was already saved by onFatalError callback
				if err == context.Canceled {
					// Check if there's already an error in store (from fatal error that caused cancel)
					// Don't overwrite it with context.Canceled
					currentJob, _ := store.Get(j.ID)
					if currentJob != nil && currentJob.LastError != "" {
						// Error was already saved by onFatalError callback, mark for finalization
						ctxErr = jobCtx.Err()
						shouldFinalize = true
					}
					// No stored error, this is a normal cancel - continue to handle it in jobCtx.Done() case
				} else {
					log.Printf("Job %s: Batch %d enqueue error: %v", j.ID, batch.BatchNo, err)
					// Check if error is fatal (4xx except 429)
					if httpErr, ok := client.GetHTTPError(err); ok {
						if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 && httpErr.StatusCode != 429 {
							// Fatal error, cancel processing
							finalErr = err
							jobCancel()
							ctxErr = jobCtx.Err()
							shouldFinalize = true
						}
					}
				}
				// Context canceled (with no stored error) or retryable error, continue
			}

		case errorBatch, ok := <-errorChan:
			if !ok {
				errorChanClosed = true
				if batchChanClosed {
					shouldFinalize = true
				} else {
					continue
				}
			}

			// Enqueue error batch for async sending (required if errorsEndpoint is configured)
			if asyncErrorSender != nil {
				if len(errorBatch.Errors) == 0 {
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
					// If enqueue failed due to fatal error, error was already saved by onFatalError callback
					if err == context.Canceled {
						// Check if there's already an error in store (from fatal error that caused cancel)
						// Don't overwrite it with context.Canceled
						currentJob, _ := store.Get(j.ID)
						if currentJob != nil && currentJob.LastError != "" {
							// Error was already saved by onFatalError callback, mark for finalization
							ctxErr = jobCtx.Err()
							shouldFinalize = true
						}
						// No stored error, this is a normal cancel - continue to handle it in jobCtx.Done() case
					} else {
						log.Printf("Job %s: Error batch %d enqueue error: %v", j.ID, errorBatch.BatchNo, err)
						// Format error message with details
						errorMsg := fmt.Sprintf("failed to enqueue error batch %d: %v", errorBatch.BatchNo, err)
						finalErr = fmt.Errorf(errorMsg)
						ctxErr = jobCtx.Err()
						shouldFinalize = true
					}
				}
			}

		case err := <-processErrChan:
			if err != nil && err != context.Canceled {
				log.Printf("Job %s: Processing error: %v", j.ID, err)
				finalErr = err
				ctxErr = jobCtx.Err()
				shouldFinalize = true
			}
			// Processing done, continue to process remaining batches and errors
		}
	}

	// Common finalization block: always stop sync, wait for senders, then finalize status
	// Stop periodic sync ticker
	stopOnce.Do(stopSyncFunc)
	syncTicker.Stop()
	<-syncDone

	// Wait for all async senders to finish and check for errors
	// This ensures onFatalError callbacks have completed and errors are saved to store
	var sendErr error
	if err := asyncDataSender.CloseAndWait(); err != nil {
		log.Printf("Job %s: Data sender error: %v", j.ID, err)
		sendErr = err
	}

	if asyncErrorSender != nil {
		if err := asyncErrorSender.CloseAndWait(); err != nil {
			log.Printf("Job %s: Error sender error: %v", j.ID, err)
			// Format error message with details
			errorMsg := fmt.Sprintf("failed to deliver error batch to %s: %v", j.Delivery.ErrorsEndpoint, err)
			if httpErr, ok := client.GetHTTPError(err); ok {
				bodySnippet := httpErr.Body
				if len(bodySnippet) > 200 {
					bodySnippet = bodySnippet[:200] + "..."
				}
				errorMsg = fmt.Sprintf("failed to deliver error batch to %s: HTTP %d: %s", j.Delivery.ErrorsEndpoint, httpErr.StatusCode, bodySnippet)
			}
			if sendErr == nil {
				sendErr = fmt.Errorf(errorMsg)
			}
		}
	}

	// Final sync of all counters to store (after all senders finished)
	store.UpdateSendProgress(j.ID, sentRows.Load(), sentBatches.Load())
	if asyncErrorSender != nil {
		store.UpdateErrors(j.ID, processor.GetErrorsTotal(), sentErrors.Load())
	}

	// Now finalize status based on errors and context
	ctxErr = jobCtx.Err()
	if sendErr != nil {
		finalErr = sendErr
		store.UpdateError(j.ID, sendErr)
		store.UpdateStatus(j.ID, job.StatusFailed)
	} else if finalErr != nil {
		// Processing error or enqueue error that wasn't fatal HTTP
		store.UpdateError(j.ID, finalErr)
		store.UpdateStatus(j.ID, job.StatusFailed)
	} else if jobCtx.Err() == context.Canceled {
		// If canceled but has lastError, treat as failed (fatal sender error)
		finalJob, _ := store.Get(j.ID)
		if finalJob != nil && finalJob.LastError != "" {
			store.UpdateStatus(j.ID, job.StatusFailed)
		} else {
			store.UpdateStatus(j.ID, job.StatusCanceled)
		}
	} else {
		// Normal completion
		log.Printf("Job %s: Completed successfully", j.ID)
		store.UpdateStatus(j.ID, job.StatusSucceeded)
	}

	// Log timings summary (only if timings are enabled)
	if timings != nil {
		log.Printf("Job %s: Timings summary: %s", j.ID, timings.String())
	}
}
