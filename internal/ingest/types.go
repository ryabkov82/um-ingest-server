package ingest

// Batch represents a batch of rows to send
type Batch struct {
	PackageID string
	BatchNo   int64
	Register  string
	Cols      []string
	Rows      [][]interface{}
}

// RowError represents a parsing error for a single row
type RowError struct {
	RowNo   int64
	Message string
	Row     []string
}

