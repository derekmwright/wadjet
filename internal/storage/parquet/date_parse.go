package parquet

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// DateParseError is the classified failure of ParseDateDays: a DATE string
// that cannot be stored. FieldRange separates PostgreSQL's two SQLSTATEs —
// 22008 (datetime_field_overflow) for a well-formed but nonexistent or
// out-of-range calendar date, 22007 (invalid_datetime_format) for a string
// that is not a date at all — which every consumer (the filter kernel via
// kernel.IsDateSyntaxError, the writer, the ingest boundary) needs so the
// wire carries the code a PostgreSQL client branches on (#560).
type DateParseError struct {
	Text       string
	FieldRange bool
}

func (e *DateParseError) Error() string {
	if e.FieldRange {
		return fmt.Sprintf("date/time field value out of range: %q", e.Text)
	}
	return fmt.Sprintf("invalid input syntax for type date: %q", e.Text)
}

// IsDateSyntaxError reports whether err is a ParseDateDays failure of the
// malformed-literal kind (PostgreSQL 22007). A nonexistent/out-of-range
// calendar date (22008) and every non-date error return false.
func IsDateSyntaxError(err error) bool {
	var e *DateParseError
	return errors.As(err, &e) && !e.FieldRange
}

// IsDateParseError reports whether err is any ParseDateDays failure.
func IsDateParseError(err error) bool {
	var e *DateParseError
	return errors.As(err, &e)
}

// ParseDateDays converts a DATE string to days since 1970-01-01, or returns a
// classified DateParseError. It is the single string→date conversion for the
// engine: the filter kernel (kernel.parseDateToDays), the parquet writers
// (parseDateForWrite / the native writer's leaf), the ingest boundary
// (ingest.checkType via ValidateDateString) and the row→batch builder
// (batch.parseDateString) all route through it, so the accept-set and the
// error classification are decided in exactly one place.
//
// The accept-set is the UNAMBIGUOUS year-first spellings a client sends —
// those PostgreSQL's default DateStyle (ISO, MDY) parses one, deterministic
// way, so wadjet can match its value exactly: a four-digit (or wider) leading
// YEAR with a '-', '/' or '.' separator ("2026-01-02", "2026-1-2",
// "2026/01/02", "2026.1.1"), the compact 8-digit form ("20260102"),
// surrounding whitespace, and a trailing time-of-day (space- or T-separated,
// optional 'Z'/offset, truncated to the date, matching a timestamp text cast
// to date). It rejects — never silently reads as the epoch or a guessed year
// — a string that is not a date at all (22007) and a well-formed but
// nonexistent or out-of-range calendar date such as 2026-02-30, month 13 or
// day 32 (22008).
//
// Not accepted, and deliberately ERRORING rather than guessing (#639): any
// spelling whose field ORDER PostgreSQL decides from DateStyle rather than
// from the digits — a short leading field it reads as the MONTH ("5/6/7" is
// 2007-05-06, "01/02/2026" is 2026-01-02, "31/1/2" is month 31 → rejected),
// two-digit years, DMY, and month names ("Jan 2 2026"). The invariant is that
// each ERRORS: wadjet never accept-and-stores a DATE whose value would differ
// from PostgreSQL's, so no unsupported spelling can become 1970-01-01 or a
// wrong year.
func ParseDateDays(s string) (int32, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return 0, &DateParseError{Text: s}
	}

	// A year-first date, optionally followed by a time-of-day. Split the time
	// off first so the flexible-width date spellings below do not have to
	// carry it; the date is what a DATE column stores.
	datePart := trimmed
	if i := strings.IndexAny(trimmed, " T"); i >= 0 {
		datePart = trimmed[:i]
		if !isTimeOfDay(strings.TrimSpace(trimmed[i+1:])) {
			return 0, &DateParseError{Text: s}
		}
	}

	y, m, d, kind := splitDateFields(datePart)
	switch kind {
	case dateFieldsNone:
		return 0, &DateParseError{Text: s}
	case dateFieldsBad:
		return 0, &DateParseError{Text: s, FieldRange: true}
	}

	// Month and day ranges, then calendar existence via a UTC round-trip:
	// time.Date normalizes an impossible day (2026-02-30 → 2026-03-02), so a
	// mismatch after construction is a nonexistent date.
	if m < 1 || m > 12 || d < 1 || d > 31 {
		return 0, &DateParseError{Text: s, FieldRange: true}
	}
	t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
	if t.Year() != y || int(t.Month()) != m || t.Day() != d {
		return 0, &DateParseError{Text: s, FieldRange: true}
	}

	days := civilDaysSinceEpoch(t)
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, &DateParseError{Text: s, FieldRange: true}
	}
	return int32(days), nil
}

const dateSecondsPerDay = 86400

// civilDaysSinceEpoch floors a UTC instant to whole days since 1970-01-01,
// computed from Unix seconds rather than a time.Duration round trip (which
// saturates at ±math.MaxInt64 ns, ~292 years — the #451 clamp).
func civilDaysSinceEpoch(t time.Time) int64 {
	sec := t.Unix()
	days := sec / dateSecondsPerDay
	if sec%dateSecondsPerDay < 0 {
		days--
	}
	return days
}

type dateFieldsKind int

const (
	dateFieldsOK   dateFieldsKind = iota // y/m/d are set and numeric
	dateFieldsNone                       // not a recognizable numeric date shape (22007)
	dateFieldsBad                        // numeric shape but a field is out of range (22008)
)

// splitDateFields parses a year-first date with '-' or '/' separators
// (flexible field widths) or the compact 8-digit form. It only decides that
// the SHAPE is a numeric date and reads the integers; ParseDateDays owns the
// calendar validity check.
func splitDateFields(s string) (y, m, d int, kind dateFieldsKind) {
	// Compact YYYYMMDD.
	if len(s) == 8 && allDigits(s) {
		return atoiN(s[0:4]), atoiN(s[4:6]), atoiN(s[6:8]), dateFieldsOK
	}
	sep := byte(0)
	switch {
	case strings.IndexByte(s, '-') >= 0:
		sep = '-'
	case strings.IndexByte(s, '/') >= 0:
		sep = '/'
	case strings.IndexByte(s, '.') >= 0:
		sep = '.'
	default:
		return 0, 0, 0, dateFieldsNone
	}
	parts := strings.Split(s, string(sep))
	if len(parts) != 3 {
		return 0, 0, 0, dateFieldsNone
	}
	// Every field must be all digits; a non-digit anywhere is a malformed
	// literal, not an out-of-range field.
	for _, p := range parts {
		if p == "" || !allDigits(p) {
			return 0, 0, 0, dateFieldsNone
		}
	}
	// YEAR-FIRST ONLY, and only when the leading field is an UNAMBIGUOUS
	// year — four or more digits, which cannot be a month (1-12) or a day
	// (1-31). PostgreSQL's default DateStyle (ISO, MDY) reads a shorter
	// leading field as the MONTH, not the year: "5/6/7" is 2007-05-06 to
	// PostgreSQL, "01/02/2026" is 2026-01-02, and "31/1/2" is month 31 →
	// rejected. Guessing year-first for those would store a value that
	// DIFFERS from PostgreSQL's — the silent-divergence this whole change
	// exists to prevent — so a non-4-digit leading field is refused here and
	// the MDY/DMY/two-digit-year spellings are deferred to #639. Each ERRORS
	// (never becomes 1970-01-01, never a wrong year), which is the invariant:
	// if it cannot be parsed identically to PostgreSQL, it is an error.
	if len(parts[0]) < 4 {
		return 0, 0, 0, dateFieldsNone
	}
	return atoiN(parts[0]), atoiN(parts[1]), atoiN(parts[2]), dateFieldsOK
}

// isTimeOfDay reports whether s is a plausible HH:MM[:SS[.fraction]] time,
// so a trailing time-of-day on a DATE input is accepted and truncated rather
// than being read as garbage.
func isTimeOfDay(s string) bool {
	if s == "" {
		return false
	}
	// Strip a trailing timezone offset or 'Z'; the date is unaffected by it.
	if i := strings.IndexAny(s, "Zz+"); i > 0 {
		s = s[:i]
	}
	for _, layout := range []string{"15:04:05.999999999", "15:04:05", "15:04"} {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// atoiN reads an all-digits string (guaranteed by allDigits) as an int.
func atoiN(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		n = n*10 + int(s[i]-'0')
	}
	return n
}

// ValidateNestedDates walks val against col and returns the first invalid DATE
// string it finds, at any depth of a ROW/ARRAY/MAP. It is the container form
// of ValidateDateString: the ingest boundary uses it so a DATE nested inside a
// container is rejected up front, the same as a top-level one, rather than
// only failing later when the native writer's leaf hits it (#560).
func ValidateNestedDates(col Column, val any) error {
	if val == nil {
		return nil
	}
	switch col.Type {
	case TypeDate:
		if s, ok := val.(string); ok {
			if _, err := ParseDateDays(s); err != nil {
				return err
			}
		}
	case TypeArray:
		if col.ElementType == nil {
			return nil
		}
		if arr, ok := val.([]any); ok {
			for _, e := range arr {
				if err := ValidateNestedDates(*col.ElementType, e); err != nil {
					return err
				}
			}
		}
	case TypeRow:
		if m, ok := val.(map[string]any); ok {
			for _, f := range col.Fields {
				if err := ValidateNestedDates(f, m[f.Name]); err != nil {
					return err
				}
			}
		}
	case TypeMap:
		// A MAP is stored as ARRAY(ROW("key","value")); its element carries
		// the key/value column pair in ElementType.Fields, the same shape
		// decomposeMap reads.
		if col.ElementType == nil || len(col.ElementType.Fields) != 2 {
			return nil
		}
		keyCol, valCol := col.ElementType.Fields[0], col.ElementType.Fields[1]
		if m, ok := val.(map[string]any); ok {
			for k, v := range m {
				if keyCol.Type == TypeDate {
					if _, err := ParseDateDays(k); err != nil {
						return err
					}
				}
				if err := ValidateNestedDates(valCol, v); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
