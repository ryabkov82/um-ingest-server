package job

import (
	"time"
)

// JobStatus represents the status of a job
type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusRunning   JobStatus = "running"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
	StatusCanceled  JobStatus = "canceled"
)

// Job represents a job request
type Job struct {
	ID             string
	PackageID      string
	InputPath      string
	CSV            CSVConfig
	Schema         SchemaConfig
	Delivery       DeliveryConfig
	ErrorsJsonl    string
	ProgressEvery  int
	Status         JobStatus
	StartedAt      *time.Time
	FinishedAt     *time.Time
	RowsRead       int64
	RowsSent       int64
	RowsSkipped    int64
	BatchesSent    int64
	LastError      string
	CurrentBatchNo int64
	FileType       string
}

// CSVConfig represents CSV parsing configuration
type CSVConfig struct {
	Encoding  string `json:"encoding"`  // "utf-8" or "windows-1251"
	Delimiter string `json:"delimiter"` // ";" or ","
	HasHeader bool   `json:"hasHeader"`
	MapBy     string `json:"mapBy"` // "order" or "header"
}

// SchemaConfig represents field mapping and transformation
type SchemaConfig struct {
	Register     string      `json:"register"`
	IncludeRowNo bool        `json:"includeRowNo"`
	RowNoField   string      `json:"rowNoField"`
	Fields       []FieldSpec `json:"fields"`
}

// FieldSpec defines how to extract and transform a field
type FieldSpec struct {
	Out              string                 `json:"out"`
	Type             string                 `json:"type"` // "string", "int", "number", "date"
	Source           SourceSpec             `json:"source"`
	DateFormat       string                 `json:"dateFormat,omitempty"`
	DateFallbacks    []string               `json:"dateFallbacks,omitempty"`
	DecimalSeparator string                 `json:"decimalSeparator,omitempty"`
	Constraints      map[string]interface{} `json:"constraints,omitempty"`
}

// SourceSpec defines how to extract field from CSV
type SourceSpec struct {
	By    string `json:"by"`    // "order" or "header"
	Index int    `json:"index"` // for "order"
	Name  string `json:"name"`  // for "header"
}

// DeliveryConfig represents delivery settings
type DeliveryConfig struct {
	Endpoint       string `json:"endpoint"`
	Gzip           bool   `json:"gzip"`
	BatchSize      int    `json:"batchSize"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
	MaxRetries     int    `json:"maxRetries"`
	BackoffMs      int    `json:"backoffMs"`
	BackoffMaxMs   int    `json:"backoffMaxMs"`
}
