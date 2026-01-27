package ingest

// Batch represents a batch of rows to send
type Batch struct {
	PackageID string                   `json:"packageId"`
	BatchNo   int64                    `json:"batchNo"`
	Register  string                   `json:"register"`
	Rows      []map[string]interface{} `json:"rows"` // Array of objects, keys are field names
}

// ErrorBatch represents a batch of errors to send
type ErrorBatch struct {
	PackageID string      `json:"packageId"`
	Errors    []ErrorItem `json:"errors"`
}

// ErrorItem represents a single error
type ErrorItem struct {
	RowNo    int64  `json:"rowNo"`
	Class    string `json:"class"`
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Field    string `json:"field,omitempty"`
	Value    string `json:"value,omitempty"`
	Message  string `json:"message"`
	TS       string `json:"ts"`
}

// RowError represents a parsing error for a single row (internal)
type RowError struct {
	RowNo   int64
	Message string
	Row     []string
	Field   string
	Value   string
	Code    string
}

