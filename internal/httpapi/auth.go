package httpapi

import (
	"net/http"
	"os"
)

// AuthMiddleware checks X-API-Key header
func AuthMiddleware(next http.Handler) http.Handler {
	apiKey := os.Getenv("UM_INGEST_API_KEY")

	// If no API key is set, skip auth (for testing)
	if apiKey == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providedKey := r.Header.Get("X-API-Key")
		if providedKey != apiKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

