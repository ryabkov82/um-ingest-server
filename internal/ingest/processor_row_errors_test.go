package ingest

import (
	"context"
	"os"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// TestMalformedCSVRowStillFailsJob - проверяет, что malformed CSV row всё ещё приводит к job fail
func TestMalformedCSVRowStillFailsJob(t *testing.T) {
	// CSV с битой строкой (незакрытые кавычки)
	tmpFile := "/tmp/test_malformed.csv"
	content := "Name,Age\nJohn,30\n\"Jane,25\nBob,40"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	j := &job.Job{
		ID:        "test-job-malformed",
		PackageID: "pkg-malformed",
		InputPath: tmpFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Fields: []job.FieldSpec{
				{
					Out:    "Name",
					Type:   "string",
					Source: job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:    "Age",
					Type:   "int",
					Source: job.SourceSpec{By: "header", Name: "Age"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			Endpoint:  "http://localhost:8080/batch",
			BatchSize: 10,
		},
	}

	parser, err := NewParser(j, "/tmp")
	if err != nil {
		t.Fatalf("NewParser failed: %v", err)
	}
	defer parser.Close()

	transformer, err := NewTransformer(j, parser)
	if err != nil {
		t.Fatalf("NewTransformer failed: %v", err)
	}

	ctx := context.Background()
	
	// Читаем первую строку (должна быть OK)
	row1, err := parser.ReadRow(ctx)
	if err != nil {
		t.Fatalf("ReadRow(1) failed: %v", err)
	}
	
	// Трансформируем первую строку (должна быть OK)
	_, fieldErrors1 := transformer.TransformRow(parser, row1)
	if len(fieldErrors1) > 0 {
		t.Fatalf("First row should not have errors, got: %v", fieldErrors1)
	}
	
	// Читаем вторую строку (битая - должна вернуть ошибку)
	_, err = parser.ReadRow(ctx)
	if err == nil {
		t.Fatal("Expected error for malformed CSV row, but got nil")
	}
	
	// Проверяем, что ошибка содержит "csv read error"
	errMsg := err.Error()
	if errMsg != "csv read error: record on line 3: wrong number of fields" {
		t.Fatalf("Expected 'csv read error' in error message, got: %v", err)
	}
	
	t.Logf("Malformed CSV row correctly returned error: %v", err)
}

// TestHeaderMismatchStillFailsJob - проверяет, что missing header column всё ещё приводит к job fail
func TestHeaderMismatchStillFailsJob(t *testing.T) {
	tmpFile := "/tmp/test_header_mismatch.csv"
	content := "Name,Age\nJohn,30"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	j := &job.Job{
		ID:        "test-job-header",
		PackageID: "pkg-header",
		InputPath: tmpFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Fields: []job.FieldSpec{
				{
					Out:    "Name",
					Type:   "string",
					Source: job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:    "City",
					Type:   "string",
					Source: job.SourceSpec{By: "header", Name: "City"}, // Missing column
				},
			},
		},
		Delivery: job.DeliveryConfig{
			Endpoint:  "http://localhost:8080/batch",
			BatchSize: 10,
		},
	}

	parser, err := NewParser(j, "/tmp")
	if err != nil {
		t.Fatalf("NewParser failed: %v", err)
	}
	defer parser.Close()

	// NewTransformer должна упасть с ошибкой missing header column
	_, err = NewTransformer(j, parser)
	if err == nil {
		t.Fatal("Expected error for missing header column, but got nil")
	}
	
	// Проверяем, что ошибка содержит "header not found"
	errMsg := err.Error()
	if errMsg != "field City: header not found: City" {
		t.Fatalf("Expected 'header not found' in error message, got: %v", err)
	}
	
	t.Logf("Missing header column correctly returned error: %v", err)
}

