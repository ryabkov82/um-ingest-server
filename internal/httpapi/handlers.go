package httpapi

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// Handler handles HTTP requests
type Handler struct {
	store          *job.Store
	allowedBaseDir string
}

// NewHandler creates a new handler
func NewHandler(store *job.Store, allowedBaseDir string) *Handler {
	// Ensure allowedBaseDir is absolute
	absDir, err := filepath.Abs(allowedBaseDir)
	if err != nil {
		log.Fatalf("Invalid ALLOWED_BASE_DIR: %v", err)
	}

	return &Handler{
		store:          store,
		allowedBaseDir: absDir,
	}
}

// CreateJob handles POST /jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		PackageID    string                 `json:"packageId"`
		InputPath    string                 `json:"inputPath"`
		CSV          job.CSVConfig          `json:"csv"`
		Schema       job.SchemaConfig       `json:"schema"`
		Delivery     job.DeliveryConfig     `json:"delivery"`
		ErrorsJsonl  string                 `json:"errorsJsonl"`
		ProgressEvery int                   `json:"progressEvery"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Validate required fields
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
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

