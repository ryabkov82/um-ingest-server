package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// This reproduces the reported symptom:
// - job fails immediately on errors delivery (errorsEndpoint returns 404)
// - store shows rowsRead=0, errorsTotal=0
// - lastError says "failed to deliver error batch 1"
//
// Root cause is that rowsRead/errorsTotal are not necessarily persisted to store
// before the orchestration fails the job on error delivery.
func TestRepro_ErrorBatchCanFailJobWhileStoreStillShowsZeroCounters(t *testing.T) {
	t.Setenv("UM_DEBUG_ERRORS", "1")

	// Errors endpoint returns 404 and records request
	var calls int
	var gotHeaders http.Header
	var gotBody ingest.ErrorBatch
	errorsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		gotHeaders = r.Header.Clone()
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r.Body)
		_ = json.Unmarshal(buf.Bytes(), &gotBody)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer errorsSrv.Close()

	// Data endpoint (not used in this repro, but required by config)
	dataSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer dataSrv.Close()

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "in.csv")
	// First data row has empty Name -> required error
	if err := os.WriteFile(csvPath, []byte("Name,Age\n ,25\n"), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	j := &job.Job{
		PackageID: "pkg-repro",
		InputPath: csvPath,
		ProgressEvery: 50000, // so store RowsRead won't update via progress logging
		CSV: job.CSVConfig{
			Encoding:  "utf-8",
			Delimiter: ",",
			HasHeader: true,
			MapBy:     "header",
		},
		Schema: job.SchemaConfig{
			Register: "TestReg",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: true,
					Source:   job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:      "Age",
					Type:     "int",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Age"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			Endpoint:       dataSrv.URL,
			ErrorsEndpoint: errorsSrv.URL,
			Gzip:           false,
			TimeoutSeconds: 1,
			MaxRetries:     0,
			BackoffMs:      1,
			BackoffMaxMs:   1,
			BatchSize:      0, // IMPORTANT: makes errorBuffer flush on first error (len>=0)
		},
	}

	store := job.NewStore()
	id, err := store.Create(j)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	j.ID = id

	processJob(context.Background(), j, store, tmpDir, "", "")

	gotJob, _ := store.Get(id)

	// Prove we attempted to deliver an error batch
	if calls != 1 {
		t.Fatalf("expected 1 call to errorsEndpoint, got %d", calls)
	}
	if gotHeaders.Get("X-UM-PackageId") != "pkg-repro" {
		t.Fatalf("missing X-UM-PackageId header, got %q", gotHeaders.Get("X-UM-PackageId"))
	}
	if gotHeaders.Get("X-UM-JobId") != id {
		t.Fatalf("missing X-UM-JobId header, got %q", gotHeaders.Get("X-UM-JobId"))
	}
	if gotHeaders.Get("X-UM-BatchNo") != "1" {
		t.Fatalf("expected X-UM-BatchNo=1, got %q", gotHeaders.Get("X-UM-BatchNo"))
	}
	if len(gotBody.Errors) == 0 {
		t.Fatalf("expected non-empty error batch body, got 0 errors")
	}
	first := gotBody.Errors[0]
	if first.Code == "" || first.Message == "" {
		t.Fatalf("expected populated error item, got code=%q message=%q", first.Code, first.Message)
	}

	// Observe the symptom: store can still show zeros when job fails fast
	if gotJob.Status != job.StatusFailed {
		t.Fatalf("expected job failed, got %s", gotJob.Status)
	}
	// After the fix: even if job fails fast on error delivery, we persist counters.
	if gotJob.RowsRead == 0 {
		t.Fatalf("expected store RowsRead to be >0, got %d", gotJob.RowsRead)
	}
	if gotJob.ErrorsTotal == 0 {
		t.Fatalf("expected store ErrorsTotal to be >0, got %d", gotJob.ErrorsTotal)
	}
	if gotJob.LastError == "" {
		t.Fatalf("expected LastError to be set")
	}
}


