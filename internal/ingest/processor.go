package ingest

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// Processor orchestrates parsing, transformation, and batching
type Processor struct {
	job           *job.Job
	store         *job.Store
	parser        *Parser
	transformer   *Transformer
	batchChan     chan *Batch
	errorChan     chan *ErrorBatch
	errors        chan RowError
	errorBuffer   []ErrorItem
	errorsTotal   int64
	mu            sync.Mutex // Protects errorsTotal
}

// NewProcessor creates a new processor
func NewProcessor(j *job.Job, store *job.Store, allowedBaseDir string) (*Processor, error) {
	parser, err := NewParser(j, allowedBaseDir)
	if err != nil {
		return nil, err
	}

	transformer, err := NewTransformer(j, parser)
	if err != nil {
		parser.Close()
		return nil, err
	}

	// Bounded channel for backpressure (buffer = 2 batches)
	batchChan := make(chan *Batch, 2)

	errorChan := make(chan *ErrorBatch, 2)

	return &Processor{
		job:         j,
		store:       store,
		parser:      parser,
		transformer: transformer,
		batchChan:   batchChan,
		errorChan:   errorChan,
		errors:      make(chan RowError, 100),
		errorBuffer: make([]ErrorItem, 0, j.Delivery.BatchSize),
		errorsTotal: 0,
	}, nil
}

// Process runs the ingestion process
// Note: status updates (succeeded/failed/canceled) are handled by orchestration layer (processJob)
func (p *Processor) Process(ctx context.Context) error {
	defer p.parser.Close()

	// Update status to running
	if err := p.store.UpdateStatus(p.job.ID, job.StatusRunning); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var parseErr error

	// Start parser goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		parseErr = p.parseAndBatch(ctx)
		close(p.batchChan)
	}()

	// Wait for completion
	wg.Wait()

	// Return error if any (orchestration will handle status)
	if parseErr != nil && parseErr != context.Canceled {
		p.store.UpdateError(p.job.ID, parseErr)
		return parseErr
	}

	if ctx.Err() == context.Canceled {
		return context.Canceled
	}

	return nil
}

// GetBatchChan returns the channel for batches
func (p *Processor) GetBatchChan() <-chan *Batch {
	return p.batchChan
}

// GetErrorChan returns the channel for error batches
func (p *Processor) GetErrorChan() <-chan *ErrorBatch {
	return p.errorChan
}

// GetErrorsTotal returns total number of errors
func (p *Processor) GetErrorsTotal() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.errorsTotal
}

// parseAndBatch reads CSV, transforms, and creates batches
// Note: rowsSent and batchesSent are updated by orchestration layer after successful send
func (p *Processor) parseAndBatch(ctx context.Context) error {
	var currentBatch []map[string]interface{}
	var rowsRead, rowsSkipped int64
	var batchNo int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := p.parser.ReadRow(ctx)
		if err == context.Canceled {
			return err
		}
		if err != nil {
			// EOF is normal
			if err == io.EOF {
				break
			}
			return err
		}

		rowsRead++

		// Skip completely empty rows
		isEmpty := true
		for _, cell := range row {
			if strings.TrimSpace(cell) != "" {
				isEmpty = false
				break
			}
		}
		if isEmpty {
			continue
		}

		// Transform row
		transformed, err := p.transformer.TransformRow(p.parser, row)
		if err != nil {
			// Handle error
			rowsSkipped++
			p.mu.Lock()
			p.errorsTotal++
			p.mu.Unlock()
			rowNo := p.parser.GetRowNo()
			
			// Log to JSONL file if configured
			p.parser.LogError(rowNo, err.Error(), row)
			
			// Create error item and buffer for 1C if errorsEndpoint is configured
			if p.job.Delivery.ErrorsEndpoint != "" {
				errorItem := p.createErrorItem(rowNo, err, row)
				p.errorBuffer = append(p.errorBuffer, errorItem)
				
				// Send error batch when buffer is full
				if len(p.errorBuffer) >= p.job.Delivery.BatchSize {
					p.flushErrorBatch(ctx)
				}
			}
			continue
		}

		currentBatch = append(currentBatch, transformed)

		// Send batch when full
		if len(currentBatch) >= p.job.Delivery.BatchSize {
			batchNo++
			batch := &Batch{
				PackageID: p.job.PackageID,
				BatchNo:   batchNo,
				Register:  p.job.Schema.Register,
				Rows:      currentBatch,
			}

			// Send batch (blocking if channel full - backpressure)
			select {
			case p.batchChan <- batch:
				// Update parsing progress (does not touch rowsSent/batchesSent)
				p.store.UpdateParseProgress(p.job.ID, rowsRead, rowsSkipped, batchNo)
			case <-ctx.Done():
				return ctx.Err()
			}

			currentBatch = make([]map[string]interface{}, 0, p.job.Delivery.BatchSize)

			// Progress logging
			if rowsRead%int64(p.job.ProgressEvery) == 0 {
				p.store.UpdateParseProgress(p.job.ID, rowsRead, rowsSkipped, batchNo)
			}
		}
	}

	// Send remaining batch
	if len(currentBatch) > 0 {
		batchNo++
		batch := &Batch{
			PackageID: p.job.PackageID,
			BatchNo:   batchNo,
			Register:  p.job.Schema.Register,
			Rows:      currentBatch,
		}

		select {
		case p.batchChan <- batch:
			// Update parsing progress (does not touch rowsSent/batchesSent)
			p.store.UpdateParseProgress(p.job.ID, rowsRead, rowsSkipped, batchNo)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Flush remaining error batch
	if len(p.errorBuffer) > 0 {
		p.flushErrorBatch(ctx)
	}

	// Close error channel
	close(p.errorChan)

	// Final progress update
	p.store.UpdateParseProgress(p.job.ID, rowsRead, rowsSkipped, batchNo)
	return nil
}

// createErrorItem creates an ErrorItem from transformation error
func (p *Processor) createErrorItem(rowNo int64, err error, row []string) ErrorItem {
	errorCode := "ОшибкаРазбораCSV"
	field := ""
	value := ""
	message := err.Error()

	// Check if it's a RequiredFieldError (from transformer package)
	// RequiredFieldError.Error() returns "required field FieldName is empty"
	if strings.Contains(message, "required field") && strings.Contains(message, "is empty") {
		errorCode = "ПустоеОбязательноеПоле"
		// Extract field name from message: "required field FieldName is empty"
		parts := strings.Fields(message)
		if len(parts) >= 3 {
			field = parts[2] // "FieldName"
		}
		message = "required field is empty"
		// Try to get value from row
		if p.transformer != nil {
			for i, f := range p.job.Schema.Fields {
				if f.Out == field {
					if i < len(p.transformer.indexes) {
						idx := p.transformer.indexes[i]
						if idx >= 0 && idx < len(row) {
							value = row[idx]
						}
					}
					break
				}
			}
		}
	} else if strings.HasPrefix(message, "field ") {
		// Try to extract field name from error message (format: "field FieldName: error message")
		parts := strings.SplitN(message, ": ", 2)
		if len(parts) == 2 {
			fieldPart := strings.TrimPrefix(parts[0], "field ")
			field = strings.Fields(fieldPart)[0] // Get first word after "field "
			message = parts[1]
		}
	}

	// Determine error code based on message
	if strings.Contains(message, "invalid date") {
		errorCode = "НеПреобразуетсяВДату"
	} else if strings.Contains(message, "invalid int") {
		errorCode = "НеПреобразуетсяВЧисло"
	} else if strings.Contains(message, "invalid number") {
		errorCode = "НеПреобразуетсяВЧисло"
	} else if strings.Contains(message, "exceeds maxLen") {
		errorCode = "СлишкомДлинноеЗначение"
	} else if strings.Contains(message, "out of range") {
		errorCode = "НеверноеЧислоКолонок"
	}

	// Get value if field is known
	if field != "" && p.transformer != nil {
		for i, f := range p.job.Schema.Fields {
			if f.Out == field {
				if i < len(p.transformer.indexes) {
					idx := p.transformer.indexes[i]
					if idx >= 0 && idx < len(row) {
						value = row[idx]
					}
				}
				break
			}
		}
	}

	return ErrorItem{
		RowNo:    rowNo,
		Class:    "Техническая",
		Severity: "Ошибка",
		Code:     errorCode,
		Field:    field,
		Value:    value,
		Message:  message,
		TS:       time.Now().UTC().Format(time.RFC3339),
	}
}

// flushErrorBatch sends buffered errors to error channel
func (p *Processor) flushErrorBatch(ctx context.Context) {
	if len(p.errorBuffer) == 0 {
		return
	}

	errorBatch := &ErrorBatch{
		PackageID: p.job.PackageID,
		Errors:    make([]ErrorItem, len(p.errorBuffer)),
	}
	copy(errorBatch.Errors, p.errorBuffer)

	select {
	case p.errorChan <- errorBatch:
		p.errorBuffer = p.errorBuffer[:0]
	case <-ctx.Done():
		return
	}
}

