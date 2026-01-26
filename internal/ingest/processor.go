package ingest

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// Processor orchestrates parsing, transformation, and batching
type Processor struct {
	job      *job.Job
	store    *job.Store
	parser   *Parser
	transformer *Transformer
	batchChan chan *Batch
	errors    chan RowError
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

	return &Processor{
		job:         j,
		store:       store,
		parser:      parser,
		transformer: transformer,
		batchChan:   batchChan,
		errors:      make(chan RowError, 100),
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

// parseAndBatch reads CSV, transforms, and creates batches
// Note: rowsSent and batchesSent are updated by orchestration layer after successful send
func (p *Processor) parseAndBatch(ctx context.Context) error {
	var currentBatch []interface{}
	var cols []string
	var rowsRead, rowsSkipped int64
	var batchNo int64

	// Build column names
	if p.job.Schema.IncludeRowNo {
		cols = append(cols, p.job.Schema.RowNoField)
	}
	for _, field := range p.job.Schema.Fields {
		cols = append(cols, field.Out)
	}

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
			// Log error but continue
			rowsSkipped++
			p.parser.LogError(p.parser.GetRowNo(), err.Error(), row)
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
				Cols:      cols,
				Rows:      make([][]interface{}, len(currentBatch)),
			}
			for i, row := range currentBatch {
				// row is already []interface{} from TransformRow
				if rowSlice, ok := row.([]interface{}); ok {
					batch.Rows[i] = rowSlice
				} else {
					// Fallback (shouldn't happen)
					batch.Rows[i] = []interface{}{row}
				}
			}

			// Send batch (blocking if channel full - backpressure)
			select {
			case p.batchChan <- batch:
				// Update parsing progress (does not touch rowsSent/batchesSent)
				p.store.UpdateParseProgress(p.job.ID, rowsRead, rowsSkipped, batchNo)
			case <-ctx.Done():
				return ctx.Err()
			}

			currentBatch = currentBatch[:0]

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
			Cols:      cols,
			Rows:      make([][]interface{}, len(currentBatch)),
		}
		for i, row := range currentBatch {
			// row is already []interface{} from TransformRow
			if rowSlice, ok := row.([]interface{}); ok {
				batch.Rows[i] = rowSlice
			} else {
				// Fallback (shouldn't happen)
				batch.Rows[i] = []interface{}{row}
			}
		}

		select {
		case p.batchChan <- batch:
			// Update parsing progress (does not touch rowsSent/batchesSent)
			p.store.UpdateParseProgress(p.job.ID, rowsRead, rowsSkipped, batchNo)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Final progress update
	p.store.UpdateParseProgress(p.job.ID, rowsRead, rowsSkipped, batchNo)
	return nil
}

