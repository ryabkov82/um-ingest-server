package httpapi

import (
	"net/http"
	"strings"
)

// SetupRouter sets up HTTP routes
func SetupRouter(handler *Handler) http.Handler {
	mux := http.NewServeMux()

	// GET /version
	mux.HandleFunc("/version", handler.GetVersion)

	// POST /jobs
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			handler.CreateJob(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// GET /jobs/{jobId}
	// POST /jobs/{jobId}/cancel
	mux.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "/cancel") {
			if r.Method == http.MethodPost {
				handler.CancelJob(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else {
			if r.Method == http.MethodGet {
				handler.GetJobStatus(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		}
	})

	// GET /packages/{packageId}/job
	// POST /packages/{packageId}/cancel
	mux.HandleFunc("/packages/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "/job") {
			if r.Method == http.MethodGet {
				handler.GetJobByPackage(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else if strings.HasSuffix(path, "/cancel") {
			if r.Method == http.MethodPost {
				handler.CancelJobByPackage(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	})

	// Apply auth middleware, but exclude /version endpoint
	wrapped := AuthMiddleware(mux)
	
	// Wrap to exclude /version from auth
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/version" {
			mux.ServeHTTP(w, r)
			return
		}
		wrapped.ServeHTTP(w, r)
	})
}

