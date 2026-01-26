package ingest

import (
	"testing"
)

func TestParseDateFastDDMMYYYY(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		wantOk   bool
		wantYear int
		wantMonth int
		wantDay  int
	}{
		{
			name:      "valid date",
			value:     "31.01.2026",
			wantOk:    true,
			wantYear:  2026,
			wantMonth: 1,
			wantDay:   31,
		},
		{
			name:     "invalid format - wrong separator",
			value:    "31-01-2026",
			wantOk:   false,
		},
		{
			name:     "invalid format - wrong length",
			value:    "31.01.26",
			wantOk:   false,
		},
		{
			name:     "invalid day",
			value:    "32.01.2026",
			wantOk:   false,
		},
		{
			name:     "invalid month",
			value:    "31.13.2026",
			wantOk:   false,
		},
		{
			name:      "edge case - first day",
			value:     "01.01.2026",
			wantOk:    true,
			wantYear:  2026,
			wantMonth: 1,
			wantDay:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date, ok := parseDateFastDDMMYYYY(tt.value)
			if ok != tt.wantOk {
				t.Errorf("parseDateFastDDMMYYYY() ok = %v, want %v", ok, tt.wantOk)
				return
			}
			if !tt.wantOk {
				return
			}
			if date.Year() != tt.wantYear {
				t.Errorf("Year() = %d, want %d", date.Year(), tt.wantYear)
			}
			if int(date.Month()) != tt.wantMonth {
				t.Errorf("Month() = %d, want %d", date.Month(), tt.wantMonth)
			}
			if date.Day() != tt.wantDay {
				t.Errorf("Day() = %d, want %d", date.Day(), tt.wantDay)
			}
		})
	}
}

func TestParseDateFastDDMMYY(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		wantOk   bool
		wantYear int
		wantMonth int
		wantDay  int
	}{
		{
			name:      "year < 30 -> 2000+",
			value:     "31.01.26",
			wantOk:    true,
			wantYear:  2026,
			wantMonth: 1,
			wantDay:   31,
		},
		{
			name:      "year >= 30 -> 1900+",
			value:     "31.01.99",
			wantOk:    true,
			wantYear:  1999,
			wantMonth: 1,
			wantDay:   31,
		},
		{
			name:      "year 30 -> 1930",
			value:     "31.01.30",
			wantOk:    true,
			wantYear:  1930,
			wantMonth: 1,
			wantDay:   31,
		},
		{
			name:     "invalid format",
			value:    "31-01-26",
			wantOk:   false,
		},
		{
			name:     "invalid length",
			value:    "31.01.2026",
			wantOk:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date, ok := parseDateFastDDMMYY(tt.value)
			if ok != tt.wantOk {
				t.Errorf("parseDateFastDDMMYY() ok = %v, want %v", ok, tt.wantOk)
				return
			}
			if !tt.wantOk {
				return
			}
			if date.Year() != tt.wantYear {
				t.Errorf("Year() = %d, want %d", date.Year(), tt.wantYear)
			}
			if int(date.Month()) != tt.wantMonth {
				t.Errorf("Month() = %d, want %d", date.Month(), tt.wantMonth)
			}
			if date.Day() != tt.wantDay {
				t.Errorf("Day() = %d, want %d", date.Day(), tt.wantDay)
			}
		})
	}
}

func TestParseDateWithFastPath(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		format    string
		fallbacks []string
		wantYear  int
		wantMonth int
		wantDay   int
	}{
		{
			name:      "DD.MM.YYYY fast path",
			value:     "31.01.2026",
			format:    "DD.MM.YYYY",
			fallbacks: []string{},
			wantYear:  2026,
			wantMonth: 1,
			wantDay:   31,
		},
		{
			name:      "DD.MM.YY fallback fast path",
			value:     "31.01.26",
			format:    "DD.MM.YYYY",
			fallbacks: []string{"DD.MM.YY"},
			wantYear:  2026,
			wantMonth: 1,
			wantDay:   31,
		},
		{
			name:      "DD.MM.YY fallback with year >= 30",
			value:     "31.01.99",
			format:    "DD.MM.YYYY",
			fallbacks: []string{"DD.MM.YY"},
			wantYear:  1999,
			wantMonth: 1,
			wantDay:   31,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date, err := parseDate(tt.value, tt.format, tt.fallbacks)
			if err != nil {
				t.Errorf("parseDate() error = %v", err)
				return
			}
			if date.Year() != tt.wantYear {
				t.Errorf("Year() = %d, want %d", date.Year(), tt.wantYear)
			}
			if int(date.Month()) != tt.wantMonth {
				t.Errorf("Month() = %d, want %d", date.Month(), tt.wantMonth)
			}
			if date.Day() != tt.wantDay {
				t.Errorf("Day() = %d, want %d", date.Day(), tt.wantDay)
			}
		})
	}
}

