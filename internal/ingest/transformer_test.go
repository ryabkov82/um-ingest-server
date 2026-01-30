package ingest

import (
	"os"
	"testing"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestParseDate(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		format   string
		fallbacks []string
		wantErr  bool
		check    func(time.Time) bool
	}{
		{
			name:      "DD.MM.YYYY format",
			value:     "31.01.2026",
			format:    "DD.MM.YYYY",
			fallbacks: []string{},
			wantErr:   false,
			check: func(t time.Time) bool {
				return t.Year() == 2026 && t.Month() == 1 && t.Day() == 31
			},
		},
		{
			name:      "DD.MM.YY fallback",
			value:     "31.01.26",
			format:    "DD.MM.YYYY",
			fallbacks: []string{"DD.MM.YY"},
			wantErr:   false,
			check: func(t time.Time) bool {
				return t.Year() == 2026 && t.Month() == 1 && t.Day() == 31
			},
		},
		{
			name:      "YYYY-MM-DD fallback",
			value:     "2026-01-31",
			format:    "DD.MM.YYYY",
			fallbacks: []string{"DD.MM.YY", "YYYY-MM-DD"},
			wantErr:   false,
			check: func(t time.Time) bool {
				return t.Year() == 2026 && t.Month() == 1 && t.Day() == 31
			},
		},
		{
			name:      "invalid date",
			value:     "invalid",
			format:    "DD.MM.YYYY",
			fallbacks: []string{},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date, err := parseDate(tt.value, tt.format, tt.fallbacks)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseDate() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("parseDate() error = %v", err)
				return
			}
			if !tt.check(date) {
				t.Errorf("parseDate() = %v, check failed", date)
			}
		})
	}
}

func TestHeaderMapping(t *testing.T) {
	// Create a mock job with header mapping
	j := &job.Job{
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Fields: []job.FieldSpec{
				{
					Out:    "Field1",
					Type:   "string",
					Source: job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:    "Field2",
					Type:   "int",
					Source: job.SourceSpec{By: "header", Name: "Age"},
				},
			},
		},
		InputPath: "/tmp/test.csv",
	}

	// Create a temporary CSV file
	tmpFile := "/tmp/test_header_mapping.csv"
	content := "Name,Age,City\nJohn,30,NYC\nJane,25,LA"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	j.InputPath = tmpFile

	parser, err := NewParser(j, "/tmp", nil)
	if err != nil {
		t.Fatalf("NewParser() error = %v", err)
	}
	defer parser.Close()

	// Check header map
	if parser.headerMap["Name"] != 0 {
		t.Errorf("headerMap['Name'] = %d, want 0", parser.headerMap["Name"])
	}
	if parser.headerMap["Age"] != 1 {
		t.Errorf("headerMap['Age'] = %d, want 1", parser.headerMap["Age"])
	}
	if parser.headerMap["City"] != 2 {
		t.Errorf("headerMap['City'] = %d, want 2", parser.headerMap["City"])
	}

	// Test GetFieldIndex
	idx, err := parser.GetFieldIndex(j.Schema.Fields[0])
	if err != nil {
		t.Errorf("GetFieldIndex() error = %v", err)
	}
	if idx != 0 {
		t.Errorf("GetFieldIndex() = %d, want 0", idx)
	}
}

