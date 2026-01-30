package client

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// Sender sends batches to 1C endpoint with retry and backoff
type Sender struct {
	client       *http.Client
	endpoint     string
	gzip         bool
	maxRetries   int
	backoffMs    int
	backoffMaxMs int
	authHeader   string          // "Basic base64(user:pass)" or empty
	timings      *ingest.Timings // Optional timings for metrics
	isErrors     bool            // true if this sender is for errors endpoint
}

// ResolveDeliveryAuth resolves authentication credentials for 1C delivery
// Priority: job.Delivery.Auth > env credentials
// Returns (user, pass, fromPayload) where fromPayload indicates if credentials came from job payload
func ResolveDeliveryAuth(jobAuth *job.AuthConfig, envUser, envPass string) (string, string, bool) {
	// Priority 1: job payload auth (deprecated but supported)
	if jobAuth != nil && jobAuth.Type == "basic" && jobAuth.User != "" {
		return jobAuth.User, jobAuth.Pass, true
	}

	// Priority 2: environment credentials
	if envUser != "" && envPass != "" {
		return envUser, envPass, false
	}

	// No valid credentials
	return "", "", false
}

// NewSender creates a new sender
// If timings is nil, metrics collection is disabled
// isErrors indicates if this sender is for errors endpoint (affects which timings are recorded)
func NewSender(endpoint string, gzip bool, timeoutSeconds, maxRetries, backoffMs, backoffMaxMs int, auth *job.AuthConfig, envUser, envPass string, timings *ingest.Timings, isErrors bool) *Sender {
	user, pass, fromPayload := ResolveDeliveryAuth(auth, envUser, envPass)

	authHeader := ""
	if user != "" && pass != "" {
		credentials := user + ":" + pass
		authHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte(credentials))

		// Log deprecation warning if using payload auth
		if fromPayload {
			log.Printf("WARNING: delivery.auth is deprecated, use UM_1C_BASIC_USER/PASS environment variables instead")
		}
	}

	return &Sender{
		client: &http.Client{
			Timeout: time.Duration(timeoutSeconds) * time.Second,
		},
		endpoint:     endpoint,
		gzip:         gzip,
		maxRetries:   maxRetries,
		backoffMs:    backoffMs,
		backoffMaxMs: backoffMaxMs,
		authHeader:   authHeader,
		timings:      timings,
		isErrors:     isErrors,
	}
}

// SendBatch sends a batch with retry logic
func (s *Sender) SendBatch(ctx context.Context, batch *ingest.Batch) error {
	var lastErr error

	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		// Increment attempts counter
		if s.timings != nil {
			if s.isErrors {
				s.timings.IncErrorsAttempt()
			} else {
				s.timings.IncDataAttempt()
			}
		}

		if attempt > 0 {
			// Increment retries counter
			if s.timings != nil {
				if s.isErrors {
					s.timings.IncErrorsRetry()
				} else {
					s.timings.IncDataRetry()
				}
			}
			// Calculate backoff
			backoff := time.Duration(s.backoffMs) * time.Duration(1<<uint(attempt-1)) * time.Millisecond
			if backoff > time.Duration(s.backoffMaxMs)*time.Millisecond {
				backoff = time.Duration(s.backoffMaxMs) * time.Millisecond
			}

			// Check if last error has Retry-After header
			if lastErr != nil {
				if httpErr, ok := lastErr.(*HTTPError); ok && httpErr.RetryAfter > 0 {
					backoff = httpErr.RetryAfter
				}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		err := s.sendBatchOnce(ctx, batch)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !s.isRetryable(err) {
			return err
		}
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// sendBatchOnce sends a batch once
func (s *Sender) sendBatchOnce(ctx context.Context, batch *ingest.Batch) error {
	// Marshal JSON
	marshalStart := time.Now()
	jsonData, err := json.Marshal(batch)
	if s.timings != nil {
		if s.isErrors {
			s.timings.ObserveErrorsMarshal(time.Since(marshalStart))
		} else {
			s.timings.ObserveDataMarshal(time.Since(marshalStart))
		}
	}
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	// Compress if needed
	var body io.Reader = bytes.NewReader(jsonData)
	contentEncoding := ""
	if s.gzip {
		gzipStart := time.Now()
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(jsonData); err != nil {
			return fmt.Errorf("gzip error: %w", err)
		}
		if err := gz.Close(); err != nil {
			return fmt.Errorf("gzip close error: %w", err)
		}
		if s.timings != nil {
			if s.isErrors {
				s.timings.ObserveErrorsGzip(time.Since(gzipStart))
			} else {
				s.timings.ObserveDataGzip(time.Since(gzipStart))
			}
		}
		body = &buf
		contentEncoding = "gzip"
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, "POST", s.endpoint, body)
	if err != nil {
		return fmt.Errorf("create request error: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	if s.authHeader != "" {
		req.Header.Set("Authorization", s.authHeader)
	}

	// Add batch headers for 1C
	req.Header.Set("X-UM-PackageId", batch.PackageID)
	req.Header.Set("X-UM-BatchNo", fmt.Sprintf("%d", batch.BatchNo))
	req.Header.Set("X-UM-RowsCount", fmt.Sprintf("%d", len(batch.Rows)))

	// Send request
	httpStart := time.Now()
	resp, err := s.client.Do(req)
	if s.timings != nil {
		if s.isErrors {
			s.timings.ObserveErrorsHTTP(time.Since(httpStart))
		} else {
			s.timings.ObserveDataHTTP(time.Since(httpStart))
		}
	}
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	// Check response (200, 201, 409 are success)
	if resp.StatusCode == 200 || resp.StatusCode == 201 || resp.StatusCode == 409 {
		return nil
	}

	// Read error body for logging
	bodyBytes, _ := io.ReadAll(resp.Body)
	return &HTTPError{
		StatusCode: resp.StatusCode,
		Body:       string(bodyBytes),
		RetryAfter: s.parseRetryAfter(resp.Header.Get("Retry-After")),
	}
}

// isRetryable checks if error is retryable
func (s *Sender) isRetryable(err error) bool {
	httpErr, ok := err.(*HTTPError)
	if !ok {
		// Network errors are retryable
		return true
	}

	// 429 and 503 are retryable
	if httpErr.StatusCode == 429 || httpErr.StatusCode == 503 {
		return true
	}

	// 5xx are retryable
	if httpErr.StatusCode >= 500 {
		return true
	}

	// 4xx (except 429) are not retryable
	return false
}

// parseRetryAfter parses Retry-After header
func (s *Sender) parseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}

	// Try as seconds
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}

	// Try as HTTP date (simplified - just return 0 if can't parse)
	return 0
}

// HTTPError represents an HTTP error
type HTTPError struct {
	StatusCode int
	Body       string
	RetryAfter time.Duration
}

// GetHTTPError extracts HTTPError from error if possible
func GetHTTPError(err error) (*HTTPError, bool) {
	httpErr, ok := err.(*HTTPError)
	return httpErr, ok
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}

// SendErrorBatch sends an error batch with retry logic
// Returns nil immediately if batch is empty (no HTTP call)
func (s *Sender) SendErrorBatch(ctx context.Context, errorBatch *ingest.ErrorBatch) error {
	// Never send empty error batches
	if errorBatch == nil || len(errorBatch.Errors) == 0 {
		return nil
	}

	var lastErr error

	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		// Increment attempts counter
		if s.timings != nil {
			s.timings.IncErrorsAttempt()
		}

		if attempt > 0 {
			// Increment retries counter
			if s.timings != nil {
				s.timings.IncErrorsRetry()
			}

			// Calculate backoff
			backoff := time.Duration(s.backoffMs) * time.Duration(1<<uint(attempt-1)) * time.Millisecond
			if backoff > time.Duration(s.backoffMaxMs)*time.Millisecond {
				backoff = time.Duration(s.backoffMaxMs) * time.Millisecond
			}

			// Check if last error has Retry-After header
			if lastErr != nil {
				if httpErr, ok := lastErr.(*HTTPError); ok && httpErr.RetryAfter > 0 {
					backoff = httpErr.RetryAfter
				}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		err := s.sendErrorBatchOnce(ctx, errorBatch)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !s.isRetryable(err) {
			return err
		}
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// sendErrorBatchOnce sends an error batch once
func (s *Sender) sendErrorBatchOnce(ctx context.Context, errorBatch *ingest.ErrorBatch) error {
	// Marshal JSON
	marshalStart := time.Now()
	jsonData, err := json.Marshal(errorBatch)
	if s.timings != nil {
		s.timings.ObserveErrorsMarshal(time.Since(marshalStart))
	}
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	// Compress if needed
	var body io.Reader = bytes.NewReader(jsonData)
	contentEncoding := ""
	if s.gzip {
		gzipStart := time.Now()
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(jsonData); err != nil {
			return fmt.Errorf("gzip error: %w", err)
		}
		if err := gz.Close(); err != nil {
			return fmt.Errorf("gzip close error: %w", err)
		}
		if s.timings != nil {
			s.timings.ObserveErrorsGzip(time.Since(gzipStart))
		}
		body = &buf
		contentEncoding = "gzip"
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, "POST", s.endpoint, body)
	if err != nil {
		return fmt.Errorf("create request error: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	if s.authHeader != "" {
		req.Header.Set("Authorization", s.authHeader)
	}

	// Add error batch headers for idempotency
	req.Header.Set("X-UM-PackageId", errorBatch.PackageID)
	if errorBatch.JobID != "" {
		req.Header.Set("X-UM-JobId", errorBatch.JobID)
	}
	if errorBatch.BatchNo > 0 {
		req.Header.Set("X-UM-BatchNo", fmt.Sprintf("%d", errorBatch.BatchNo))
	}
	// Set X-UM-RowsCount to the number of error items in the batch
	req.Header.Set("X-UM-RowsCount", fmt.Sprintf("%d", len(errorBatch.Errors)))

	// Send request
	httpStart := time.Now()
	resp, err := s.client.Do(req)
	if s.timings != nil {
		s.timings.ObserveErrorsHTTP(time.Since(httpStart))
	}
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	// Check response (200, 201, 409 are success)
	if resp.StatusCode == 200 || resp.StatusCode == 201 || resp.StatusCode == 409 {
		return nil
	}

	// Read error body for logging
	bodyBytes, _ := io.ReadAll(resp.Body)
	return &HTTPError{
		StatusCode: resp.StatusCode,
		Body:       string(bodyBytes),
		RetryAfter: s.parseRetryAfter(resp.Header.Get("Retry-After")),
	}
}
