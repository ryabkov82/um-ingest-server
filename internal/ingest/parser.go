package ingest

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/text/encoding/charmap"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// Parser handles CSV parsing and field mapping
type Parser struct {
	job         *job.Job
	file        *os.File
	reader      *csv.Reader
	headerMap   map[string]int
	rowNo       int64
	errorsFile  *os.File
	errorsWriter *bufio.Writer
	errorsEncoder *json.Encoder
}

// NewParser creates a new parser for a job
func NewParser(j *job.Job, allowedBaseDir string) (*Parser, error) {
	// Validate and resolve input path (with symlink resolution)
	resolvedPath, err := ValidatePath(j.InputPath, allowedBaseDir)
	if err != nil {
		return nil, err
	}

	// Check that file exists
	if err := ValidatePathExists(resolvedPath); err != nil {
		return nil, err
	}

	// Open CSV file using resolved path
	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Setup encoding
	var reader io.Reader = file
	if j.CSV.Encoding == "windows-1251" {
		decoder := charmap.Windows1251.NewDecoder()
		reader = decoder.Reader(file)
	}

	// Setup CSV reader
	csvReader := csv.NewReader(reader)
	csvReader.Comma = rune(j.CSV.Delimiter[0])
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true

	p := &Parser{
		job:       j,
		file:      file,
		reader:    csvReader,
		headerMap: make(map[string]int),
		rowNo:     0,
	}

	// Open errors file if specified
	if j.ErrorsJsonl != "" {
		p.errorsFile, err = os.OpenFile(j.ErrorsJsonl, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to open errors file: %w", err)
		}
		p.errorsWriter = bufio.NewWriter(p.errorsFile)
		p.errorsEncoder = json.NewEncoder(p.errorsWriter)
	}

	// Read header if needed
	if j.CSV.HasHeader && j.CSV.MapBy == "header" {
		header, err := csvReader.Read()
		if err != nil {
			file.Close()
			if p.errorsFile != nil {
				p.errorsFile.Close()
			}
			return nil, fmt.Errorf("failed to read header: %w", err)
		}

		for i, name := range header {
			p.headerMap[strings.TrimSpace(name)] = i
		}
	}

	return p, nil
}

// Close closes the parser and its resources
func (p *Parser) Close() error {
	var errs []error
	if p.errorsWriter != nil {
		if err := p.errorsWriter.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("flush errors writer: %w", err))
		}
	}
	if p.errorsFile != nil {
		if err := p.errorsFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.file != nil {
		if err := p.file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing parser: %v", errs)
	}
	return nil
}

// GetFieldIndex returns the CSV column index for a field
func (p *Parser) GetFieldIndex(field job.FieldSpec) (int, error) {
	if field.Source.By == "order" {
		return field.Source.Index, nil
	} else if field.Source.By == "header" {
		idx, ok := p.headerMap[field.Source.Name]
		if !ok {
			return -1, fmt.Errorf("header not found: %s", field.Source.Name)
		}
		return idx, nil
	}
	return -1, fmt.Errorf("unknown source type: %s", field.Source.By)
}

// ReadRow reads the next row from CSV
func (p *Parser) ReadRow(ctx context.Context) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	record, err := p.reader.Read()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("csv read error: %w", err)
	}

	p.rowNo++
	return record, nil
}

// GetRowNo returns current row number
func (p *Parser) GetRowNo() int64 {
	return p.rowNo
}

// LogError logs a parsing error to JSONL file
func (p *Parser) LogError(rowNo int64, message string, row []string) {
	if p.errorsEncoder == nil {
		return
	}

	err := p.errorsEncoder.Encode(map[string]interface{}{
		"rowNo":   rowNo,
		"message": message,
		"row":     row,
	})
	if err != nil {
		// Log error but don't fail the job
		fmt.Printf("Failed to write error log: %v\n", err)
	}
}

