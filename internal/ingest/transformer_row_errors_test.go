package ingest

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// TestRowLevelErrorDoesNotSkipRow - проверяет, что row-level ошибки не пропускают строку
// Строка попадает в batch, ошибки фиксируются через createErrorItem
func TestRowLevelErrorDoesNotSkipRow(t *testing.T) {
	// Создаём CSV с required полем, которое пустое
	tmpFile := "/tmp/test_required_empty.csv"
	content := "Name,Age\n,30\nJane,25"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	j := &job.Job{
		ID:        "test-job-required",
		PackageID: "pkg-required",
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
					Out:      "Name",
					Type:     "string",
					Required: true, // Required field
					Source:   job.SourceSpec{By: "header", Name: "Name"},
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
	
	// Читаем первую строку данных (после header)
	row, err := parser.ReadRow(ctx)
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}

	// TransformRow должен вернуть результат И ошибки
	result, fieldErrors := transformer.TransformRow(parser, row)
	
	// Проверяем, что результат НЕ nil (строка не пропущена)
	if result == nil {
		t.Fatal("TransformRow returned nil result - row was skipped, but it should not be")
	}
	
	// Проверяем, что есть ошибка required field
	if len(fieldErrors) == 0 {
		t.Fatal("Expected field error for required empty field, but got none")
	}
	
	// Проверяем, что ошибка содержит правильный текст (для createErrorItem)
	foundRequiredError := false
	for _, fieldErr := range fieldErrors {
		if fieldErr.Error() == "required field Name is empty" {
			foundRequiredError = true
			break
		}
	}
	if !foundRequiredError {
		t.Fatalf("Expected 'required field Name is empty' error, got: %v", fieldErrors)
	}
	
	// Проверяем, что результат содержит Age (поле без ошибки)
	if result["Age"] == nil {
		t.Fatal("Result should contain Age field")
	}
	
	// Проверяем, что Name отсутствует или пустое (required field с ошибкой)
	// Но строка всё равно в результате
	t.Logf("TransformRow result: %v, errors: %v", result, fieldErrors)
}

// TestRowLevelTypeErrorDoesNotSkipRow - проверяет, что ошибка типа не пропускает строку
func TestRowLevelTypeErrorDoesNotSkipRow(t *testing.T) {
	tmpFile := "/tmp/test_type_error.csv"
	content := "Name,Age\nJohn,abc\nJane,25"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	j := &job.Job{
		ID:        "test-job-type",
		PackageID: "pkg-type",
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
					Type:   "int", // Ожидается int, но будет "abc"
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
	row, err := parser.ReadRow(ctx)
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}

	result, fieldErrors := transformer.TransformRow(parser, row)
	
	// Строка не должна быть пропущена
	if result == nil {
		t.Fatal("TransformRow returned nil result - row was skipped, but it should not be")
	}
	
	// Должна быть ошибка типа
	if len(fieldErrors) == 0 {
		t.Fatal("Expected field error for invalid int, but got none")
	}
	
	// Проверяем, что ошибка содержит правильный текст
	foundTypeError := false
	for _, fieldErr := range fieldErrors {
		errMsg := fieldErr.Error()
		// Проверяем, что ошибка содержит "field Age: invalid int"
		if strings.HasPrefix(errMsg, "field Age: invalid int") {
			foundTypeError = true
			break
		}
	}
	if !foundTypeError {
		t.Fatalf("Expected 'field Age: invalid int' error, got: %v", fieldErrors)
	}
	
	// Проверяем, что Name всё равно в результате
	if result["Name"] != "John" {
		t.Fatalf("Expected Name='John', got: %v", result["Name"])
	}
	
	t.Logf("TransformRow result: %v, errors: %v", result, fieldErrors)
}

// TestIndexOutOfRangeDoesNotSkipRow - проверяет, что index out of range не пропускает строку
// Используем source.by=order с индексом, который выходит за границы строки
func TestIndexOutOfRangeDoesNotSkipRow(t *testing.T) {
	tmpFile := "/tmp/test_index_error.csv"
	content := "Name,Age\nJohn,30\nJane,25" // Строки имеют 2 колонки
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	j := &job.Job{
		ID:        "test-job-index",
		PackageID: "pkg-index",
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
					Source: job.SourceSpec{By: "order", Index: 5}, // Индекс 5, но строка имеет только 2 колонки (0,1)
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
	row, err := parser.ReadRow(ctx)
	if err != nil {
		t.Fatalf("ReadRow failed: %v", err)
	}

	result, fieldErrors := transformer.TransformRow(parser, row)
	
	// Строка не должна быть пропущена
	if result == nil {
		t.Fatal("TransformRow returned nil result - row was skipped, but it should not be")
	}
	
	// Должна быть ошибка index out of range
	if len(fieldErrors) == 0 {
		t.Fatal("Expected field error for index out of range, but got none")
	}
	
	// Проверяем, что ошибка содержит правильный текст
	foundIndexError := false
	for _, fieldErr := range fieldErrors {
		errMsg := fieldErr.Error()
		if errMsg == "field City: index 5 out of range (row has 2 columns)" {
			foundIndexError = true
			break
		}
	}
	if !foundIndexError {
		t.Fatalf("Expected 'field City: index 5 out of range (row has 2 columns)' error, got: %v", fieldErrors)
	}
	
	// Проверяем, что Name всё равно в результате
	if result["Name"] != "John" {
		t.Fatalf("Expected Name='John', got: %v", result["Name"])
	}
	
	t.Logf("TransformRow result: %v, errors: %v", result, fieldErrors)
}

