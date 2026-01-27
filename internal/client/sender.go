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
	authHeader   string // "Basic base64(user:pass)" or empty
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
func NewSender(endpoint string, gzip bool, timeoutSeconds, maxRetries, backoffMs, backoffMaxMs int, auth *job.AuthConfig, envUser, envPass string) *Sender {
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
	}
}

// SendBatch sends a batch with retry logic
func (s *Sender) SendBatch(ctx context.Context, batch *ingest.Batch) error {
	var lastErr error

	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
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
	jsonData, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	// Compress if needed
	var body io.Reader = bytes.NewReader(jsonData)
	contentEncoding := ""
	if s.gzip {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(jsonData); err != nil {
			return fmt.Errorf("gzip error: %w", err)
		}
		if err := gz.Close(); err != nil {
			return fmt.Errorf("gzip close error: %w", err)
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
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
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
func (s *Sender) SendErrorBatch(ctx context.Context, errorBatch *ingest.ErrorBatch) error {
	var lastErr error

	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
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
	jsonData, err := json.Marshal(errorBatch)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	// Compress if needed
	var body io.Reader = bytes.NewReader(jsonData)
	contentEncoding := ""
	if s.gzip {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(jsonData); err != nil {
			return fmt.Errorf("gzip error: %w", err)
		}
		if err := gz.Close(); err != nil {
			return fmt.Errorf("gzip close error: %w", err)
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

	// Send request
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
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
