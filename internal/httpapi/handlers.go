package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
	"github.com/ryabkov82/um-ingest-server/internal/job"
	"github.com/ryabkov82/um-ingest-server/internal/version"
)

// Handler handles HTTP requests
type Handler struct {
	store          *job.Store
	allowedBaseDir string
	env1CUser      string
	env1CPass      string
	serverCtx      context.Context // Long-lived server context (not tied to HTTP request)
}

// NewHandler creates a new handler
// serverCtx: long-lived server context (used for job processing, not tied to HTTP request)
func NewHandler(store *job.Store, allowedBaseDir, env1CUser, env1CPass string, serverCtx context.Context) *Handler {
	// Ensure allowedBaseDir is absolute
	absDir, err := filepath.Abs(allowedBaseDir)
	if err != nil {
		log.Fatalf("Invalid ALLOWED_BASE_DIR: %v", err)
	}

	// Use context.Background() if serverCtx is nil (for backward compatibility in tests)
	// In production, serverCtx should always be provided
	if serverCtx == nil {
		serverCtx = context.Background()
		log.Printf("WARNING: Handler created without serverCtx, using context.Background()")
	}

	return &Handler{
		store:          store,
		allowedBaseDir: absDir,
		env1CUser:      env1CUser,
		env1CPass:      env1CPass,
		serverCtx:      serverCtx,
	}
}

// CreateJob handles POST /jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		PackageID     string             `json:"packageId"`
		InputPath     string             `json:"inputPath"`
		CSV           job.CSVConfig      `json:"csv"`
		Schema        job.SchemaConfig   `json:"schema"`
		Delivery      job.DeliveryConfig `json:"delivery"`
		ErrorsJsonl   string             `json:"errorsJsonl"`
		ProgressEvery int                `json:"progressEvery"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.PackageID == "" {
		http.Error(w, "packageId is required", http.StatusBadRequest)
		return
	}
	if req.InputPath == "" {
		http.Error(w, "inputPath is required", http.StatusBadRequest)
		return
	}
	if req.Delivery.Endpoint == "" {
		http.Error(w, "delivery.endpoint is required", http.StatusBadRequest)
		return
	}
	if req.Delivery.BatchSize <= 0 {
		http.Error(w, "delivery.batchSize must be > 0", http.StatusBadRequest)
		return
	}
	if req.ProgressEvery <= 0 {
		req.ProgressEvery = 50000 // default
	}

	// Validate delivery auth
	if req.Delivery.Auth != nil {
		// If delivery.auth is provided, validate it
		if req.Delivery.Auth.Type != "basic" {
			http.Error(w, "delivery.auth.type must be 'basic'", http.StatusBadRequest)
			return
		}
		if req.Delivery.Auth.User == "" || req.Delivery.Auth.Pass == "" {
			http.Error(w, "delivery.auth.user and delivery.auth.pass are required when delivery.auth is specified", http.StatusBadRequest)
			return
		}
	} else {
		// If delivery.auth is not provided, check if env credentials are available
		if h.env1CUser == "" || h.env1CPass == "" {
			http.Error(w, "delivery.auth is required unless UM_1C_BASIC_USER/PASS are set", http.StatusBadRequest)
			return
		}
	}

	// Validate CSV parameters
	if req.CSV.Delimiter != ";" && req.CSV.Delimiter != "," {
		http.Error(w, "csv.delimiter must be ';' or ','", http.StatusBadRequest)
		return
	}
	if req.CSV.MapBy != "order" && req.CSV.MapBy != "header" {
		http.Error(w, "csv.mapBy must be 'order' or 'header'", http.StatusBadRequest)
		return
	}
	if req.CSV.MapBy == "header" && !req.CSV.HasHeader {
		http.Error(w, "csv.hasHeader must be true when csv.mapBy is 'header'", http.StatusBadRequest)
		return
	}
	if req.CSV.Encoding != "" && req.CSV.Encoding != "utf-8" && req.CSV.Encoding != "windows-1251" {
		http.Error(w, "csv.encoding must be 'utf-8' or 'windows-1251' (or empty for utf-8)", http.StatusBadRequest)
		return
	}
	if req.CSV.Encoding == "" {
		req.CSV.Encoding = "utf-8" // default
	}

	// Validate input path (security check)
	if _, err := ingest.ValidatePath(req.InputPath, h.allowedBaseDir); err != nil {
		http.Error(w, fmt.Sprintf("Invalid input path: %v", err), http.StatusBadRequest)
		return
	}

	// Detect file type
	fileType := ""
	if ext := strings.ToLower(filepath.Ext(req.InputPath)); ext != "" {
		fileType = ext[1:] // remove dot
	}

	j := &job.Job{
		PackageID:     req.PackageID,
		InputPath:     req.InputPath,
		CSV:           req.CSV,
		Schema:        req.Schema,
		Delivery:      req.Delivery,
		ErrorsJsonl:   req.ErrorsJsonl,
		ProgressEvery: req.ProgressEvery,
		FileType:      fileType,
	}

	jobID, err := h.store.Create(j)
	if err != nil {
		if err == job.ErrQueueFull {
			http.Error(w, "Queue is full, please try again later", http.StatusTooManyRequests)
			return
		}
		if err == job.ErrJobAlreadyRunning {
			// Get the existing active job
			existingJobID, _ := h.store.GetActiveJobByPackage(req.PackageID)
			var existingJob *job.Job
			if existingJobID != "" {
				existingJob, _ = h.store.Get(existingJobID)
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			response := map[string]interface{}{
				"error":     "job_already_running",
				"packageId": req.PackageID,
			}
			if existingJob != nil {
				response["jobId"] = existingJob.ID
				response["status"] = string(existingJob.Status)
			}
			json.NewEncoder(w).Encode(response)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to create job: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Job created: %s, packageId: %s, inputPath: %s", jobID, req.PackageID, req.InputPath)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"jobId":  jobID,
		"status": "queued",
	})
}

// GetJobStatus handles GET /jobs/{jobId}
func (h *Handler) GetJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if jobID == "" {
		http.Error(w, "jobId is required", http.StatusBadRequest)
		return
	}

	j, err := h.store.Get(jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	response := map[string]interface{}{
		"status":         j.Status,
		"rowsRead":       j.RowsRead,
		"rowsSent":       j.RowsSent,
		"rowsSkipped":    j.RowsSkipped,
		"batchesSent":    j.BatchesSent,
		"currentBatchNo": j.CurrentBatchNo,
		"errorsTotal":    j.ErrorsTotal,
		"errorsSent":     j.ErrorsSent,
		"inputPath":      j.InputPath,
	}

	if j.StartedAt != nil {
		response["startedAt"] = j.StartedAt.Format(time.RFC3339)
	}
	if j.FinishedAt != nil {
		response["finishedAt"] = j.FinishedAt.Format(time.RFC3339)
	}
	if j.LastError != "" {
		response["lastError"] = j.LastError
	}
	if j.FileType != "" {
		response["fileType"] = j.FileType
	}

	// Include delivery config but sanitize auth (never return password)
	if j.Delivery.Auth != nil {
		response["delivery"] = map[string]interface{}{
			"endpoint":       j.Delivery.Endpoint,
			"errorsEndpoint": j.Delivery.ErrorsEndpoint,
			"gzip":           j.Delivery.Gzip,
			"batchSize":      j.Delivery.BatchSize,
			"timeoutSeconds": j.Delivery.TimeoutSeconds,
			"maxRetries":     j.Delivery.MaxRetries,
			"backoffMs":      j.Delivery.BackoffMs,
			"backoffMaxMs":   j.Delivery.BackoffMaxMs,
			"auth": map[string]interface{}{
				"type": j.Delivery.Auth.Type,
				"user": j.Delivery.Auth.User,
				// pass is intentionally omitted for security
			},
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// GetJobByPackage handles GET /packages/{packageId}/job
func (h *Handler) GetJobByPackage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract packageId from path like /packages/{packageId}/job
	path := strings.TrimPrefix(r.URL.Path, "/packages/")
	path = strings.TrimSuffix(path, "/job")
	packageID := path
	if packageID == "" {
		http.Error(w, "packageId is required", http.StatusBadRequest)
		return
	}

	// First, try to get active job
	jobID, err := h.store.GetActiveJobByPackage(packageID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Internal error: %v", err), http.StatusInternalServerError)
		return
	}

	// If no active job, try to get last job
	if jobID == "" {
		jobID, err = h.store.GetLastJobByPackage(packageID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal error: %v", err), http.StatusInternalServerError)
			return
		}
		if jobID == "" {
			http.Error(w, "Job not found", http.StatusNotFound)
			return
		}
	}

	j, err := h.store.Get(jobID)
	if err != nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	// Return same format as GET /jobs/{jobId} but with jobId included
	response := map[string]interface{}{
		"jobId":          j.ID,
		"status":         j.Status,
		"rowsRead":       j.RowsRead,
		"rowsSent":       j.RowsSent,
		"rowsSkipped":    j.RowsSkipped,
		"batchesSent":    j.BatchesSent,
		"currentBatchNo": j.CurrentBatchNo,
		"errorsTotal":    j.ErrorsTotal,
		"errorsSent":     j.ErrorsSent,
		"inputPath":      j.InputPath,
	}

	if j.StartedAt != nil {
		response["startedAt"] = j.StartedAt.Format(time.RFC3339)
	}
	if j.FinishedAt != nil {
		response["finishedAt"] = j.FinishedAt.Format(time.RFC3339)
	}
	if j.LastError != "" {
		response["lastError"] = j.LastError
	}
	if j.FileType != "" {
		response["fileType"] = j.FileType
	}

	// Include delivery config but sanitize auth (never return password)
	if j.Delivery.Auth != nil {
		response["delivery"] = map[string]interface{}{
			"endpoint":       j.Delivery.Endpoint,
			"errorsEndpoint": j.Delivery.ErrorsEndpoint,
			"gzip":           j.Delivery.Gzip,
			"batchSize":      j.Delivery.BatchSize,
			"timeoutSeconds": j.Delivery.TimeoutSeconds,
			"maxRetries":     j.Delivery.MaxRetries,
			"backoffMs":      j.Delivery.BackoffMs,
			"backoffMaxMs":   j.Delivery.BackoffMaxMs,
			"auth": map[string]interface{}{
				"type": j.Delivery.Auth.Type,
				"user": j.Delivery.Auth.User,
				// pass is intentionally omitted for security
			},
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// CancelJobByPackage handles POST /packages/{packageId}/cancel
func (h *Handler) CancelJobByPackage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract packageId from path like /packages/{packageId}/cancel
	path := strings.TrimPrefix(r.URL.Path, "/packages/")
	path = strings.TrimSuffix(path, "/cancel")
	packageID := path
	if packageID == "" {
		http.Error(w, "packageId is required", http.StatusBadRequest)
		return
	}

	jobID, err := h.store.GetActiveJobByPackage(packageID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Internal error: %v", err), http.StatusInternalServerError)
		return
	}

	if jobID == "" {
		http.Error(w, "No active job found for this packageId", http.StatusNotFound)
		return
	}

	// Cancel the job
	err = h.store.Cancel(jobID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to cancel job: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "canceled",
		"jobId":  jobID,
	})
}

// CancelJob handles POST /jobs/{jobId}/cancel
func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")
	jobID = strings.TrimSuffix(jobID, "/cancel")
	if jobID == "" {
		http.Error(w, "jobId is required", http.StatusBadRequest)
		return
	}

	if err := h.store.Cancel(jobID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Job canceled: %s", jobID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "canceled",
	})
}

// GetVersion handles GET /version
func (h *Handler) GetVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	info := version.Info()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}
