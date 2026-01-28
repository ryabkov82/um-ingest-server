package ingest

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ryabkov82/um-ingest-server/internal/job"
)

// Transformer converts CSV rows to typed values
type Transformer struct {
	job     *job.Job
	indexes []int // Precomputed field indexes
}

// NewTransformer creates a new transformer with precomputed field indexes
func NewTransformer(j *job.Job, parser *Parser) (*Transformer, error) {
	indexes := make([]int, len(j.Schema.Fields))
	for i, field := range j.Schema.Fields {
		idx, err := parser.GetFieldIndex(field)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Out, err)
		}
		indexes[i] = idx
	}

	return &Transformer{
		job:     j,
		indexes: indexes,
	}, nil
}

// TransformRow transforms a CSV row to a map[string]interface{} according to schema
func (t *Transformer) TransformRow(parser *Parser, row []string) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(t.job.Schema.Fields)+1)

	// Add row number if needed
	if t.job.Schema.IncludeRowNo {
		result[t.job.Schema.RowNoField] = int(parser.GetRowNo())
	}

	// Transform each field using precomputed indexes
	for i, field := range t.job.Schema.Fields {
		idx := t.indexes[i]

		if idx < 0 || idx >= len(row) {
			return nil, fmt.Errorf("field %s: index %d out of range (row has %d columns)", field.Out, idx, len(row))
		}

		rawValue := row[idx]
		value := strings.TrimSpace(rawValue)

		// Check required constraint before type parsing
		if field.Required && value == "" {
			return nil, fmt.Errorf("required field %s is empty", field.Out)
		}

		// If not required and empty, skip type parsing (return nil to omit field)
		if !field.Required && value == "" {
			// For non-required empty fields, skip adding to result
			// This means the field won't appear in the output map
			continue
		}

		transformed, err := t.transformValue(field, value)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Out, err)
		}

		result[field.Out] = transformed
	}

	return result, nil
}

// transformValue converts a string value to the target type
// Note: required check and empty value handling is done in TransformRow before calling this
func (t *Transformer) transformValue(field job.FieldSpec, value string) (interface{}, error) {
	// Check maxLen constraint (only for non-empty values)
	if maxLen, ok := field.Constraints["maxLen"].(float64); ok {
		if int(maxLen) > 0 && len(value) > int(maxLen) {
			return nil, fmt.Errorf("value exceeds maxLen %d", int(maxLen))
		}
	}

	switch field.Type {
	case "string":
		return value, nil

	case "int":
		if value == "" {
			return nil, nil
		}
		val, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int: %w", err)
		}
		return val, nil

	case "number":
		if value == "" {
			return nil, nil
		}
		// Replace decimal separator if needed
		numStr := value
		if field.DecimalSeparator == "," {
			numStr = strings.Replace(numStr, ",", ".", 1)
		}
		val, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number: %w", err)
		}
		return val, nil

	case "date":
		if value == "" {
			return nil, nil
		}
		date, err := parseDate(value, field.DateFormat, field.DateFallbacks)
		if err != nil {
			return nil, fmt.Errorf("invalid date: %w", err)
		}
		// Return as ISO date string (YYYY-MM-DD)
		return date.Format("2006-01-02"), nil

	default:
		return nil, fmt.Errorf("unknown type: %s", field.Type)
	}
}

// parseDate parses a date string with format and fallbacks
func parseDate(value, format string, fallbacks []string) (time.Time, error) {
	// Fast path for DD.MM.YYYY (most common case)
	if format == "DD.MM.YYYY" && len(value) == 10 {
		if date, ok := parseDateFastDDMMYYYY(value); ok {
			return date, nil
		}
	}

	// Fast path for DD.MM.YY fallback
	for _, fb := range fallbacks {
		if fb == "DD.MM.YY" && len(value) == 8 {
			if date, ok := parseDateFastDDMMYY(value); ok {
				return date, nil
			}
		}
	}

	// General fallback using time.Parse
	formats := []string{format}
	formats = append(formats, fallbacks...)

	for _, fmt := range formats {
		parsed, err := parseDateWithFormat(value, fmt)
		if err == nil {
			return parsed, nil
		}
	}

	return time.Time{}, fmt.Errorf("could not parse date %q with formats %v", value, formats)
}

// parseDateFastDDMMYYYY parses DD.MM.YYYY format (10 chars: "31.01.2026")
func parseDateFastDDMMYYYY(value string) (time.Time, bool) {
	if len(value) != 10 || value[2] != '.' || value[5] != '.' {
		return time.Time{}, false
	}

	// Parse day
	day := int(value[0]-'0')*10 + int(value[1]-'0')
	if day < 1 || day > 31 {
		return time.Time{}, false
	}

	// Parse month
	month := int(value[3]-'0')*10 + int(value[4]-'0')
	if month < 1 || month > 12 {
		return time.Time{}, false
	}

	// Parse year
	year := int(value[6]-'0')*1000 + int(value[7]-'0')*100 + int(value[8]-'0')*10 + int(value[9]-'0')
	if year < 1900 || year > 2100 {
		return time.Time{}, false
	}

	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), true
}

// parseDateFastDDMMYY parses DD.MM.YY format (8 chars: "31.01.26")
// Year interpretation: <30 -> 2000+, >=30 -> 1900+
func parseDateFastDDMMYY(value string) (time.Time, bool) {
	if len(value) != 8 || value[2] != '.' || value[5] != '.' {
		return time.Time{}, false
	}

	// Parse day
	day := int(value[0]-'0')*10 + int(value[1]-'0')
	if day < 1 || day > 31 {
		return time.Time{}, false
	}

	// Parse month
	month := int(value[3]-'0')*10 + int(value[4]-'0')
	if month < 1 || month > 12 {
		return time.Time{}, false
	}

	// Parse year (2 digits)
	yy := int(value[6]-'0')*10 + int(value[7]-'0')
	var year int
	if yy < 30 {
		year = 2000 + yy
	} else {
		year = 1900 + yy
	}

	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), true
}

// parseDateWithFormat parses a date with a specific format
func parseDateWithFormat(value, format string) (time.Time, error) {
	// Replace format placeholders
	format = strings.Replace(format, "DD", "02", -1)
	format = strings.Replace(format, "MM", "01", -1)
	format = strings.Replace(format, "YYYY", "2006", -1)
	format = strings.Replace(format, "YY", "06", -1)

	return time.Parse(format, value)
}

