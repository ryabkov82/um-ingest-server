package ingest

import (
	"fmt"
	"sync"
	"time"
)

// Timings tracks timing metrics for different stages of the pipeline
type Timings struct {
	mu sync.Mutex

	// CSV reading
	CSVReadTotal time.Duration
	CSVReadCount int64

	// Transformation
	TransformTotal time.Duration
	TransformCount int64

	// Batch assembly
	BatchAssemblyTotal time.Duration
	BatchAssemblyCount int64

	// Sender metrics (data endpoint)
	DataMarshalTotal time.Duration
	DataMarshalCount int64
	DataGzipTotal    time.Duration
	DataGzipCount    int64
	DataHTTPTotal    time.Duration
	DataHTTPCount    int64

	// Sender metrics (errors endpoint)
	ErrorsMarshalTotal time.Duration
	ErrorsMarshalCount int64
	ErrorsGzipTotal    time.Duration
	ErrorsGzipCount    int64
	ErrorsHTTPTotal    time.Duration
	ErrorsHTTPCount    int64
}

// NewTimings creates a new Timings instance
func NewTimings() *Timings {
	return &Timings{}
}

// ObserveCSVRead records a CSV read operation duration
func (t *Timings) ObserveCSVRead(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.CSVReadTotal += duration
	t.CSVReadCount++
}

// ObserveTransform records a TransformRow operation duration
func (t *Timings) ObserveTransform(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.TransformTotal += duration
	t.TransformCount++
}

// ObserveBatchAssembly records a batch assembly operation duration
func (t *Timings) ObserveBatchAssembly(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.BatchAssemblyTotal += duration
	t.BatchAssemblyCount++
}

// ObserveDataMarshal records a JSON marshal operation duration for data batches
func (t *Timings) ObserveDataMarshal(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.DataMarshalTotal += duration
	t.DataMarshalCount++
}

// ObserveDataGzip records a gzip operation duration for data batches
func (t *Timings) ObserveDataGzip(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.DataGzipTotal += duration
	t.DataGzipCount++
}

// ObserveDataHTTP records an HTTP round-trip duration for data batches
func (t *Timings) ObserveDataHTTP(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.DataHTTPTotal += duration
	t.DataHTTPCount++
}

// ObserveErrorsMarshal records a JSON marshal operation duration for error batches
func (t *Timings) ObserveErrorsMarshal(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ErrorsMarshalTotal += duration
	t.ErrorsMarshalCount++
}

// ObserveErrorsGzip records a gzip operation duration for error batches
func (t *Timings) ObserveErrorsGzip(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ErrorsGzipTotal += duration
	t.ErrorsGzipCount++
}

// ObserveErrorsHTTP records an HTTP round-trip duration for error batches
func (t *Timings) ObserveErrorsHTTP(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ErrorsHTTPTotal += duration
	t.ErrorsHTTPCount++
}

// String returns a formatted summary of all timings
func (t *Timings) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	var result string

	// CSV reading
	if t.CSVReadCount > 0 {
		avg := t.CSVReadTotal / time.Duration(t.CSVReadCount)
		result += fmt.Sprintf("CSV read: total=%v count=%d avg=%v; ", t.CSVReadTotal, t.CSVReadCount, avg)
	}

	// Transformation
	if t.TransformCount > 0 {
		avg := t.TransformTotal / time.Duration(t.TransformCount)
		result += fmt.Sprintf("Transform: total=%v count=%d avg=%v; ", t.TransformTotal, t.TransformCount, avg)
	}

	// Batch assembly
	if t.BatchAssemblyCount > 0 {
		avg := t.BatchAssemblyTotal / time.Duration(t.BatchAssemblyCount)
		result += fmt.Sprintf("Batch assembly: total=%v count=%d avg=%v; ", t.BatchAssemblyTotal, t.BatchAssemblyCount, avg)
	}

	// Data endpoint
	if t.DataMarshalCount > 0 {
		avg := t.DataMarshalTotal / time.Duration(t.DataMarshalCount)
		result += fmt.Sprintf("Data marshal: total=%v count=%d avg=%v; ", t.DataMarshalTotal, t.DataMarshalCount, avg)
	}
	if t.DataGzipCount > 0 {
		avg := t.DataGzipTotal / time.Duration(t.DataGzipCount)
		result += fmt.Sprintf("Data gzip: total=%v count=%d avg=%v; ", t.DataGzipTotal, t.DataGzipCount, avg)
	}
	if t.DataHTTPCount > 0 {
		avg := t.DataHTTPTotal / time.Duration(t.DataHTTPCount)
		result += fmt.Sprintf("Data HTTP: total=%v count=%d avg=%v; ", t.DataHTTPTotal, t.DataHTTPCount, avg)
	}

	// Errors endpoint
	if t.ErrorsMarshalCount > 0 {
		avg := t.ErrorsMarshalTotal / time.Duration(t.ErrorsMarshalCount)
		result += fmt.Sprintf("Errors marshal: total=%v count=%d avg=%v; ", t.ErrorsMarshalTotal, t.ErrorsMarshalCount, avg)
	}
	if t.ErrorsGzipCount > 0 {
		avg := t.ErrorsGzipTotal / time.Duration(t.ErrorsGzipCount)
		result += fmt.Sprintf("Errors gzip: total=%v count=%d avg=%v; ", t.ErrorsGzipTotal, t.ErrorsGzipCount, avg)
	}
	if t.ErrorsHTTPCount > 0 {
		avg := t.ErrorsHTTPTotal / time.Duration(t.ErrorsHTTPCount)
		result += fmt.Sprintf("Errors HTTP: total=%v count=%d avg=%v; ", t.ErrorsHTTPTotal, t.ErrorsHTTPCount, avg)
	}

	if result == "" {
		return "No timings recorded"
	}

	// Remove trailing "; "
	return result[:len(result)-2]
}
