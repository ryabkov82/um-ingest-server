package ingest

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestBatchFormat(t *testing.T) {
	// Test that batch format is correct (rows as array of objects)
	batch := &Batch{
		PackageID: "test-pkg",
		BatchNo:   1,
		Register:  "TestRegister",
		Rows: []map[string]interface{}{
			{
				"НомерСтрокиФайла": 1,
				"Name":             "abc",
				"Date":             "2026-01-31",
				"Amount":           123.45,
			},
			{
				"НомерСтрокиФайла": 2,
				"Name":             "def",
				"Date":             "2026-02-01",
				"Amount":           10.0,
			},
		},
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// Unmarshal to verify structure
	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	// Check that rows is an array
	rowsVal, ok := result["rows"]
	if !ok {
		t.Fatal("rows field not found")
	}
	rows, ok := rowsVal.([]interface{})
	if !ok {
		t.Fatalf("rows should be an array, got %T", rowsVal)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	// Check first row is an object
	row1, ok := rows[0].(map[string]interface{})
	if !ok {
		t.Fatal("row should be an object")
	}

	// Check date format is ISO (YYYY-MM-DD)
	date, ok := row1["Date"].(string)
	if !ok {
		t.Fatal("Date should be a string")
	}
	if date != "2026-01-31" {
		t.Errorf("Expected date '2026-01-31', got '%s'", date)
	}

	// Check row number
	rowNo, ok := row1["НомерСтрокиФайла"].(float64) // JSON numbers are float64
	if !ok {
		t.Fatal("НомерСтрокиФайла should be a number")
	}
	if rowNo != 1 {
		t.Errorf("Expected rowNo 1, got %v", rowNo)
	}
}

func TestTransformerReturnsMap(t *testing.T) {
	// Create a job
	j := &job.Job{
		CSV: job.CSVConfig{
			HasHeader: false,
			MapBy:     "order",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			IncludeRowNo: true,
			RowNoField:   "НомерСтрокиФайла",
			Fields: []job.FieldSpec{
				{
					Out:    "Name",
					Type:   "string",
					Source: job.SourceSpec{By: "order", Index: 0},
				},
				{
					Out:    "Date",
					Type:   "date",
					Source: job.SourceSpec{By: "order", Index: 1},
					DateFormat: "DD.MM.YYYY",
				},
				{
					Out:    "Amount",
					Type:   "number",
					Source: job.SourceSpec{By: "order", Index: 2},
				},
			},
		},
		InputPath: "/tmp/test.csv",
	}

	// Create a temporary CSV file
	tmpFile := "/tmp/test_batch_format.csv"
	content := "abc,31.01.2026,123.45\ndef,01.02.2026,10.0"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	j.InputPath = tmpFile

	parser, err := NewParser(j, "/tmp")
	if err != nil {
		t.Fatalf("NewParser() error = %v", err)
	}
	defer parser.Close()

	transformer, err := NewTransformer(j, parser)
	if err != nil {
		t.Fatalf("NewTransformer() error = %v", err)
	}

	// Read and transform a row
	ctx := context.Background()
	row, err := parser.ReadRow(ctx)
	if err != nil {
		t.Fatalf("ReadRow() error = %v", err)
	}

	result, err := transformer.TransformRow(parser, row)
	if err != nil {
		t.Fatalf("TransformRow() error = %v", err)
	}

	// Verify result is a map
	if result == nil {
		t.Fatal("Result should not be nil")
	}

	// Check result is a map
	if result == nil {
		t.Fatal("Result should not be nil")
	}

	// Check row number field
	rowNo, ok := result["НомерСтрокиФайла"].(int)
	if !ok {
		t.Fatalf("НомерСтрокиФайла should be int, got %T", result["НомерСтрокиФайла"])
	}
	if rowNo != 1 {
		t.Errorf("Expected rowNo 1, got %d", rowNo)
	}

	// Check Name field
	name, ok := result["Name"].(string)
	if !ok {
		t.Fatalf("Name should be string, got %T", result["Name"])
	}
	if name != "abc" {
		t.Errorf("Expected name 'abc', got '%s'", name)
	}

	// Check Date field is ISO format
	date, ok := result["Date"].(string)
	if !ok {
		t.Fatalf("Date should be string (ISO), got %T", result["Date"])
	}
	if date != "2026-01-31" {
		t.Errorf("Expected date '2026-01-31', got '%s'", date)
	}

	// Check Amount field
	amount, ok := result["Amount"].(float64)
	if !ok {
		t.Fatalf("Amount should be float64, got %T", result["Amount"])
	}
	if amount != 123.45 {
		t.Errorf("Expected amount 123.45, got %v", amount)
	}
}

