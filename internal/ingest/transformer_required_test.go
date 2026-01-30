package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestRequiredFieldStringEmpty(t *testing.T) {
	// Create temp CSV file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Age\n ,25\n"
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-required",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
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
			ErrorsEndpoint: "http://localhost/errors",
			BatchSize:      10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()

	// Start processing in goroutine
	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	// Wait for error batch
	var errorBatch *ErrorBatch
	select {
	case errorBatch = <-processor.GetErrorChan():
		// Got error batch
	case <-done:
		t.Fatal("Processing completed without error batch")
	}

	if errorBatch == nil {
		t.Fatal("Error batch is nil")
	}
	if len(errorBatch.Errors) != 1 {
		t.Fatalf("Expected 1 error, got %d", len(errorBatch.Errors))
	}

	errItem := errorBatch.Errors[0]
	if errItem.Code != "ПустоеОбязательноеПоле" {
		t.Errorf("Expected error code 'ПустоеОбязательноеПоле', got '%s'", errItem.Code)
	}
	if errItem.Field != "Name" {
		t.Errorf("Expected field 'Name', got '%s'", errItem.Field)
	}
	if errItem.Message != "required field is empty" {
		t.Errorf("Expected message 'required field is empty', got '%s'", errItem.Message)
	}

	// Check that data batch was sent (row is NOT skipped anymore, even with required field error)
	select {
	case batch := <-processor.GetBatchChan():
		if batch == nil {
			t.Error("Expected data batch, but got nil")
		} else if len(batch.Rows) != 1 {
			t.Errorf("Expected batch with 1 row, but got batch with %d rows", len(batch.Rows))
		} else {
			// Verify the row is in the batch (even though it has a required field error)
			row := batch.Rows[0]
			if row == nil {
				t.Error("Expected row in batch, but got nil")
			}
		}
	default:
		t.Error("Expected data batch, but none was sent (row was skipped, but it should not be)")
	}

	// Wait for processing to complete
	<-done

	// Check errorsTotal
	errorsTotal := processor.GetErrorsTotal()
	if errorsTotal != 1 {
		t.Errorf("Expected errorsTotal=1, got %d", errorsTotal)
	}
}

func TestRequiredFieldNumberEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Amount\nTest, \n"
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-required-number",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:      "Amount",
					Type:     "number",
					Required: true,
					Source:   job.SourceSpec{By: "header", Name: "Amount"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			ErrorsEndpoint: "http://localhost/errors",
			BatchSize:      10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()

	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	var errorBatch *ErrorBatch
	select {
	case errorBatch = <-processor.GetErrorChan():
	case <-done:
		t.Fatal("Processing completed without error batch")
	}

	if len(errorBatch.Errors) != 1 {
		t.Fatalf("Expected 1 error, got %d", len(errorBatch.Errors))
	}

	errItem := errorBatch.Errors[0]
	if errItem.Code != "ПустоеОбязательноеПоле" {
		t.Errorf("Expected error code 'ПустоеОбязательноеПоле', got '%s'", errItem.Code)
	}
	if errItem.Field != "Amount" {
		t.Errorf("Expected field 'Amount', got '%s'", errItem.Field)
	}
}

func TestRequiredFieldDateEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Date\nTest, \n"
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-required-date",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:        "Date",
					Type:       "date",
					Required:   true,
					DateFormat: "DD.MM.YYYY",
					Source:     job.SourceSpec{By: "header", Name: "Date"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			ErrorsEndpoint: "http://localhost/errors",
			BatchSize:      10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()

	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	var errorBatch *ErrorBatch
	select {
	case errorBatch = <-processor.GetErrorChan():
	case <-done:
		t.Fatal("Processing completed without error batch")
	}

	if len(errorBatch.Errors) != 1 {
		t.Fatalf("Expected 1 error, got %d", len(errorBatch.Errors))
	}

	errItem := errorBatch.Errors[0]
	if errItem.Code != "ПустоеОбязательноеПоле" {
		t.Errorf("Expected error code 'ПустоеОбязательноеПоле', got '%s'", errItem.Code)
	}
	if errItem.Field != "Date" {
		t.Errorf("Expected field 'Date', got '%s'", errItem.Field)
	}
}

func TestRequiredFalseNumberEmpty(t *testing.T) {
	// Test that required=false allows empty number field
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Amount\nTest, \n"
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-required-false-number",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:      "Amount",
					Type:     "number",
					Required: false, // Not required
					Source:   job.SourceSpec{By: "header", Name: "Amount"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			BatchSize: 10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()

	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	// Should get a batch (no error)
	var batch *Batch
	select {
	case batch = <-processor.GetBatchChan():
	case <-done:
		t.Fatal("Processing completed without batch")
	}

	if len(batch.Rows) != 1 {
		t.Fatalf("Expected 1 row in batch, got %d", len(batch.Rows))
	}

	row := batch.Rows[0]
	if row["Name"] != "Test" {
		t.Errorf("Expected Name='Test', got %v", row["Name"])
	}

	// Amount should not be in the row (omitted for empty non-required fields)
	if _, ok := row["Amount"]; ok {
		t.Errorf("Expected Amount to be omitted, but it's present: %v", row["Amount"])
	}

	// No errors should be generated
	if processor.GetErrorsTotal() != 0 {
		t.Errorf("Expected 0 errors, got %d", processor.GetErrorsTotal())
	}
}

func TestRequiredFalseDateEmpty(t *testing.T) {
	// Test that required=false allows empty date field
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Date\nTest, \n"
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-required-false-date",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: false,
					Source:   job.SourceSpec{By: "header", Name: "Name"},
				},
				{
					Out:        "Date",
					Type:       "date",
					Required:   false, // Not required
					DateFormat: "DD.MM.YYYY",
					Source:     job.SourceSpec{By: "header", Name: "Date"},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			BatchSize: 10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()

	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	// Should get a batch (no error)
	var batch *Batch
	select {
	case batch = <-processor.GetBatchChan():
	case <-done:
		t.Fatal("Processing completed without batch")
	}

	if len(batch.Rows) != 1 {
		t.Fatalf("Expected 1 row in batch, got %d", len(batch.Rows))
	}

	row := batch.Rows[0]
	if row["Name"] != "Test" {
		t.Errorf("Expected Name='Test', got %v", row["Name"])
	}

	// Date should not be in the row (omitted for empty non-required fields)
	if _, ok := row["Date"]; ok {
		t.Errorf("Expected Date to be omitted, but it's present: %v", row["Date"])
	}

	// No errors should be generated
	if processor.GetErrorsTotal() != 0 {
		t.Errorf("Expected 0 errors, got %d", processor.GetErrorsTotal())
	}
}

func TestRequiredFieldWithOrderSource(t *testing.T) {
	// Test required field with source.by=order
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := " ,25\n"
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-required-order",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: false,
			MapBy:     "order",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
			Fields: []job.FieldSpec{
				{
					Out:      "Name",
					Type:     "string",
					Required: true,
					Source:   job.SourceSpec{By: "order", Index: 0},
				},
				{
					Out:      "Age",
					Type:     "int",
					Required: false,
					Source:   job.SourceSpec{By: "order", Index: 1},
				},
			},
		},
		Delivery: job.DeliveryConfig{
			ErrorsEndpoint: "http://localhost/errors",
			BatchSize:      10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()

	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	var errorBatch *ErrorBatch
	select {
	case errorBatch = <-processor.GetErrorChan():
	case <-done:
		t.Fatal("Processing completed without error batch")
	}

	if len(errorBatch.Errors) != 1 {
		t.Fatalf("Expected 1 error, got %d", len(errorBatch.Errors))
	}

	errItem := errorBatch.Errors[0]
	if errItem.Code != "ПустоеОбязательноеПоле" {
		t.Errorf("Expected error code 'ПустоеОбязательноеПоле', got '%s'", errItem.Code)
	}
	if errItem.Field != "Name" {
		t.Errorf("Expected field 'Name', got '%s'", errItem.Field)
	}
}

func TestRequiredFieldWithWhitespace(t *testing.T) {
	// Test that whitespace-only values are considered empty
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.csv")
	content := "Name,Age\n   \t  ,25\n"
	os.WriteFile(testFile, []byte(content), 0644)

	j := &job.Job{
		PackageID: "test-required-whitespace",
		InputPath: testFile,
		CSV: job.CSVConfig{
			HasHeader: true,
			MapBy:     "header",
			Delimiter: ",",
			Encoding:  "utf-8",
		},
		Schema: job.SchemaConfig{
			Register: "TestRegister",
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
			ErrorsEndpoint: "http://localhost/errors",
			BatchSize:      10,
		},
	}

	store := job.NewStore()
	jobID, err := store.Create(j)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}
	j.ID = jobID

	processor, err := NewProcessor(j, store, tmpDir, nil)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	ctx := context.Background()

	done := make(chan bool)
	go func() {
		processor.Process(ctx)
		done <- true
	}()

	var errorBatch *ErrorBatch
	select {
	case errorBatch = <-processor.GetErrorChan():
	case <-done:
		t.Fatal("Processing completed without error batch")
	}

	if len(errorBatch.Errors) != 1 {
		t.Fatalf("Expected 1 error, got %d", len(errorBatch.Errors))
	}

	errItem := errorBatch.Errors[0]
	if errItem.Code != "ПустоеОбязательноеПоле" {
		t.Errorf("Expected error code 'ПустоеОбязательноеПоле', got '%s'", errItem.Code)
	}
}
