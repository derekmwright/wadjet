package parquet

import (
	"errors"
	"fmt"
	"math"
	"strconv"
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

// SQLState makes the classification above reach a PostgreSQL client, through
// sqlerr.Coder rather than by importing sqlerr (which would be a cycle, since
// sqlerr is below this package). The two codes are the ones the doc comment
// names, and they are the ones psql/pgx branch on.
//
// It used to carry none: a DATE this package refused crossed the wire as the
// blanket 42000 even though the failure had already been classified here
// (#673). Every consumer of ParseDateDays gets the code for free.
func (e *DateParseError) SQLState() string {
	if e.FieldRange {
		return "22008"
	}
	return "22007"
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
// Two of those refusals exist because PostgreSQL refuses them and they used to
// be SUPERSET accepts here, on the WRITE path, so wadjet STORED a date no
// PostgreSQL client could have written (#641): YEAR ZERO in every spelling
// (22008 — PostgreSQL's calendar puts 1 BC immediately before 1 AD), and a
// MONTH field of exactly three digits (see threeDigitMonthKind, which is also
// why a FOUR-digit month and a three-digit DAY are still accepted).
//
// The accept-set is still narrower than PostgreSQL's in the other direction,
// and every one of those is a REFUSAL rather than a different value: the
// two-field day-of-year form ('2026-003' is 2026-01-03 there), the BC suffix,
// and the DateStyle-dependent spellings deferred to #639.
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

	// There is no year zero. PostgreSQL's calendar runs 4713 BC .. 5874897 AD
	// with 1 BC immediately before 1 AD, so every spelling of year 0000 is
	// 22008 there — '0000-01-01', '0000-1-1', '0000/01/01', '0000.01.01',
	// '0000-12-31', '00000101' and '00001231', all measured live on
	// postgres:17-alpine. Go's calendar is proleptic and HAS one, so this used
	// to store day -719528 for a string PostgreSQL refuses to parse: an
	// accepted value on the WRITE path that no PostgreSQL client could have
	// written (#641).
	if y == 0 {
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
	if kind := threeDigitMonthKind(parts[1]); kind != dateFieldsOK {
		return 0, 0, 0, kind
	}
	return atoiN(parts[0]), atoiN(parts[1]), atoiN(parts[2]), dateFieldsOK
}

// threeDigitMonthKind refuses a middle field of EXACTLY three digits, which is
// the one width PostgreSQL will not read as a month.
//
// The rule is PostgreSQL's DecodeNumber (datetime.c), and it is narrower than
// "wider than two digits": a three-digit field with only the YEAR decided so
// far, whose value is 1..366, is a DAY OF YEAR. So '2026-003' is 2026-01-03
// there — but in a THREE-field date the day-of-year leaves the third field
// nowhere to go and PostgreSQL answers 22007, while a three-digit value
// OUTSIDE 1..366 is not a day of year at all, falls through to the month slot
// and is 22008. Measured live on postgres:17-alpine:
//
//	'2026-003-12'   22007       '2026-012-12'   22007       '2026-366-12'  22007
//	'2026-000-12'   22008       '2026-367-12'   22008       '2026-999-12'  22008
//	'2026-0003-12'  2026-03-12  '2026-00003-12' 2026-03-12
//	'2026-01-003'   2026-01-03  '2026-01-0003'  2026-01-03
//
// FOUR or more digits is a year-shaped token PostgreSQL accepts as the month,
// and a three-digit DAY is accepted too — the year and the month are already
// decided by then, so the day-of-year branch cannot fire. Both keep working
// here unchanged, which is why this tests the width EXACTLY rather than
// bounding it: refusing len > 2 would have refused input PostgreSQL takes,
// which is the divergence ADR-0012 item 1 forbids, in exchange for closing one
// it permits.
//
// wadjet used to read every all-digit middle field as a month, so
// '2026-003-12' stored 2026-03-12 for a string PostgreSQL rejects (#641).
// dateFieldsOK here means "not this case", not "valid".
func threeDigitMonthKind(month string) dateFieldsKind {
	if len(month) != 3 {
		return dateFieldsOK
	}
	if v := atoiN(month); v >= 1 && v <= 366 {
		return dateFieldsNone // PostgreSQL's day-of-year, then a field too many
	}
	return dateFieldsBad // read as a month, and out of range
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

// ValidateNestedLeaves walks val against col and returns the first value that
// no leaf of that declaration can hold, at any depth of a ROW/ARRAY/MAP.
//
// It is the container form of the per-leaf checks the native writer applies in
// decomposeLeaf, and the ingest boundary uses it so a bad value nested inside a
// container is rejected UP FRONT — the same as a top-level one — rather than
// only failing at the flush that eventually writes it. The flush is per BUFFER,
// so a bad row failing there takes a batch of already-accepted rows with it and
// reports against a partition rather than against the statement that carried it.
//
// It covers every leaf type whose conversion can FAIL: DATE, where an
// unparseable or nonexistent calendar date used to be stored as the epoch
// (#560), and DECIMAL, where a value with no carrier at the leaf's (p, s) used
// to be stored as a wrapped int64 or a zero (#647). Adding a third such type
// means adding it to validateNestedLeaf and nowhere else — the reason this
// walks leaves rather than dates.
func ValidateNestedLeaves(col Column, val any) error {
	if val == nil {
		return nil
	}
	switch col.Type {
	case TypeArray:
		if col.ElementType == nil {
			return nil
		}
		if arr, ok := val.([]any); ok {
			for _, e := range arr {
				if err := ValidateNestedLeaves(*col.ElementType, e); err != nil {
					return err
				}
			}
		}
	case TypeRow:
		if m, ok := val.(map[string]any); ok {
			for _, f := range col.Fields {
				if err := ValidateNestedLeaves(f, m[f.Name]); err != nil {
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
		m, ok := val.(map[string]any)
		if !ok {
			// The MAP's STORAGE shape — the []any of {key,value} entry maps
			// batch.Vector.GetValue hands back, which every row that passed
			// through RowAt/ToRows carries (UPDATE's and MERGE's re-ingest of
			// a boxed row). decomposeMap accepts it, so this must too, or a
			// bad value in that shape is admitted here and kills the buffer at
			// the flush instead (#647 re-review). Mirroring the writer's
			// fallback is the point: the two must accept the same shapes.
			m, ok = mapFromStorageShapeEntries(val, keyCol.Name, valCol.Name)
		}
		if ok {
			for k, v := range m {
				if err := validateNestedLeaf(keyCol, k); err != nil {
					return err
				}
				if err := ValidateNestedLeaves(valCol, v); err != nil {
					return err
				}
			}
		}
	default:
		return validateNestedLeaf(col, val)
	}
	return nil
}

// validateNestedLeaf is the per-leaf half of ValidateNestedLeaves: one value
// against one primitive declaration, and nil for every type whose conversion
// cannot fail.
func validateNestedLeaf(col Column, val any) error {
	if val == nil {
		return nil
	}
	switch col.Type {
	case TypeDate, TypeTimestamp, TypeDuration:
		if _, _, err := normalizeTemporalBox(col.Type, val); err != nil {
			return err
		}
	case TypeDecimal:
		if _, err := DecimalValueFromBox(val, col.Precision, col.Scale); err != nil {
			return err
		}
	}
	return nil
}

// normalizeTemporalBox converts a box handed to a DATE, TIMESTAMP or DURATION
// column into the integer that column's leaf stores — days, milliseconds and
// nanoseconds respectively — and reports whether it converted anything.
//
// It exists because "which boxes are acceptable" was answered in two places
// that disagreed. ingest.checkType admits, per type:
//
//	DATE       time.Time, int32, int64, string
//	TIMESTAMP  time.Time, int64, string
//	DURATION   time.Duration, int64, string
//
// and of those the writer converted exactly one — a DATE string. Every other
// non-integer box fell through toInt32/toInt64's `default: return 0` and was
// stored as ZERO, silently: a time.Time DATE (the box the SQL literal path
// produces, #673), a string TIMESTAMP and a string or time.Duration DURATION
// (time.Duration is a NAMED type, so `case int64` in a Go type switch does not
// match it). An accepted box that stores a wrong value is worse than a
// rejected one, so this is the single conversion both boundaries use, and a
// box it cannot convert is an error naming the column and the row rather than
// a zero.
//
// A DATE takes the CALENDAR DATE as written in the time's own location, not
// its UTC instant — a DATE is a date, and this is also the rule
// ingest.formatPartitionValue already formats a DATE partition key by
// (t.Format("2006-01-02")), so a partition's directory name and its stored
// value cannot disagree.
func normalizeTemporalBox(t TypeID, val any) (any, bool, error) {
	switch t {
	case TypeDate:
		switch v := val.(type) {
		case string:
			d, err := ParseDateDays(v)
			if err != nil {
				return nil, false, err
			}
			return d, true, nil
		case time.Time:
			y, mo, d := v.Date()
			days := civilDaysSinceEpoch(time.Date(y, mo, d, 0, 0, 0, 0, time.UTC))
			if days < math.MinInt32 || days > math.MaxInt32 {
				return nil, false, &DateParseError{Text: v.Format("2006-01-02"), FieldRange: true}
			}
			return int32(days), true, nil
		}
	case TypeTimestamp:
		if s, ok := val.(string); ok {
			ms, err := ParseTimestampMillis(s)
			if err != nil {
				return nil, false, err
			}
			return ms, true, nil
		}
	case TypeDuration:
		switch v := val.(type) {
		case time.Duration:
			return int64(v), true, nil
		case string:
			n, err := ParseDurationNanos(v)
			if err != nil {
				return nil, false, err
			}
			return n, true, nil
		}
	}
	return nil, false, nil
}

// timestampLayouts is the accept-set for a TIMESTAMP text literal, in the
// order tried. It is the ONE list: the comparison kernel and the scan filter
// reach it through ParseTimestampMillisOrZero rather than keeping copies,
// which is what makes "a literal that STORES is a literal a predicate over
// the same column reads the same way" a fact rather than a comment. The two
// copies it used to describe had drifted (#692).
var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	// The SPACE-separated spellings with an offset, and the two-digit offset
	// form. PostgreSQL accepts all four — `'2020-01-01 12:00:00-08:00'` and
	// `'…-08'` are ordinary timestamp input there — and this list had none of
	// them, so the "a literal's offset is DISCARDED on every path" rule held
	// only for the T-separated spelling and the others were 22007 (review
	// P10). Refusing input PostgreSQL accepts is what ADR-0012 item 1 forbids.
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04:05-07",
	"2006-01-02T15:04:05.999999999-07",
	"2006-01-02T15:04:05-07",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02",
}

// ParseTimestampMillis converts a TIMESTAMP string to epoch milliseconds — the
// unit the parquet schema declares for the column (TimestampMillis) and the
// unit its reader hands back.
//
// The failure is 22007 rather than a zero. The kernel's copy of this list
// returns 0 for an unparseable string, which is a defensible answer for a
// COMPARISON (it cannot match) and an indefensible one for a WRITE, where 0 is
// 1970-01-01T00:00:00Z stored under the caller's timestamp.
func ParseTimestampMillis(s string) (int64, error) {
	trimmed := strings.TrimSpace(s)
	if t, ok := ParseTimestampWallClock(trimmed); ok {
		return t.UnixMilli(), nil
	}
	// The literal named no timestamp. Which KIND of failure it is decides the
	// SQLSTATE, exactly as it does for DATE.
	if timestampFieldsOutOfRange(trimmed) {
		return 0, &TimestampParseError{Text: s, FieldRange: true}
	}
	return 0, &TimestampParseError{Text: s}
}

// ParseTimestampWallClock is THE timestamp accept-set: every layout this
// engine takes, with the offset discarded, returning the instant whose UTC
// fields are the literal's wall clock.
//
// It is exported because there were FOUR copies of this decision and they
// disagreed after #692 fixed two of them: the writer and the two comparison
// kernels discarded the offset while `expr.parseTimestampToEpochMsOK` and
// `expr.castTemporal` still applied it, so a row inserted with
// `'2020-06-01T12:00:00+05:30'` could not be found by `WHERE t = ` that same
// literal — right→wrong on the one invariant the commit exists to establish
// (review B2). Every path now reads this function.
func ParseTimestampWallClock(s string) (time.Time, bool) {
	trimmed := normalizeTimestampOverflow(strings.TrimSpace(s))
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, trimmed); err == nil {
			return time.Date(t.Year(), t.Month(), t.Day(),
				t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC), true
		}
	}
	return time.Time{}, false
}

// normalizeTimestampOverflow rewrites the two clock spellings PostgreSQL
// ACCEPTS and Go's time.Parse rejects: hour 24 (the end of a day, which
// PostgreSQL reads as 00:00 of the next one) and second 60 (a leap second,
// which it reads as the next minute). `'2020-01-01 24:00:00'::timestamp` and
// `'2020-01-01 23:59:60'::timestamp` are both `2020-01-02 00:00:00` there —
// measured, not remembered. This engine refused both, which is the ADR-0012
// item 1 violation review P9 names.
//
// The rewrite is textual and conservative: it fires only on an exact `24:00`
// hour with zero minutes and seconds, or an exact `:60` second, and leaves
// everything else — hour 25, minute 60 — to the parser and the field-range
// classifier.
func normalizeTimestampOverflow(s string) string {
	sep := strings.IndexAny(s, "T ")
	if sep < 0 {
		return s
	}
	date, clock := s[:sep], s[sep+1:]
	zone := ""
	if i := strings.IndexAny(clock, "Zz+"); i > 0 {
		clock, zone = clock[:i], clock[i:]
	} else if i := strings.LastIndex(clock, "-"); i > 0 {
		clock, zone = clock[:i], clock[i:]
	}
	day, err := ParseDateDays(date)
	if err != nil {
		return s
	}
	switch {
	case clock == "24:00:00" || clock == "24:00":
		next := time.Unix(int64(day+1)*86400, 0).UTC()
		return next.Format("2006-01-02") + s[sep:sep+1] + "00:00:00" + zone
	case strings.HasSuffix(clock, ":60"):
		hm := strings.Split(strings.TrimSuffix(clock, ":60"), ":")
		if len(hm) != 2 {
			return s
		}
		h, herr := strconv.Atoi(hm[0])
		m, merr := strconv.Atoi(hm[1])
		if herr != nil || merr != nil || h > 23 || m > 59 {
			return s
		}
		bumped := time.Date(1970, 1, 1, h, m, 0, 0, time.UTC).Add(time.Minute)
		if bumped.Day() != 1 {
			next := time.Unix(int64(day+1)*86400, 0).UTC()
			return next.Format("2006-01-02") + s[sep:sep+1] + "00:00:00" + zone
		}
		return date + s[sep:sep+1] + bumped.Format("15:04:05") + zone
	}
	return s
}

// ParseTimestampMillisOrZero is the COMPARISON contract: the epoch
// millisecond value, or 0 for text that names no timestamp.
//
// It exists so the comparison kernels stop carrying their own copy of the
// layout list. They had two, and both had DRIFTED from the writer's — the
// space-separated millisecond form stored fine and no predicate could read it
// back — while the doc comment on timestampLayouts asserted the three were the
// same list. A literal that STORES has to be a literal a predicate over the
// same column reads the same way, and one function is the only way to keep
// that true (#692).
//
// Zero is a defensible answer for a comparison (it cannot match) and an
// indefensible one for a WRITE, which is why the writer's entry point above
// returns an error instead.
func ParseTimestampMillisOrZero(s string) int64 {
	ms, err := ParseTimestampMillis(s)
	if err != nil {
		return 0
	}
	return ms
}

// timestampWallClockMillis reads a parsed timestamp as PostgreSQL's
// `timestamp without time zone` does: the WALL-CLOCK FIELDS are the value and
// any offset the literal carried is DISCARDED.
//
// `time.Parse(RFC3339, "2020-01-01T05:30:00+05:30")` yields 05:30 in a fixed
// +05:30 zone, and UnixMilli then converts it to the UTC INSTANT — midnight —
// so wadjet stored a different timestamp than the literal spells. PostgreSQL
// stores 05:30:00, and `'…+05:30'::timestamp` = `2020-01-01 05:30:00` is
// verifiable on any server. This engine's TIMESTAMP is declared as
// `timestamp without time zone` on the wire, so it has to mean what that type
// means (ADR-0012: PostgreSQL decides). A literal spelling `Z` is unaffected —
// discarding a zero offset changes nothing.
func timestampWallClockMillis(t time.Time) int64 {
	return time.Date(t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC).UnixMilli()
}

var _ = timestampWallClockMillis

// timestampFieldsOutOfRange reports whether text SHAPED like a timestamp names
// field values no calendar or clock has — 2020-02-30, month 13, hour 25 —
// which PostgreSQL answers with 22008 (datetime_field_overflow) rather than
// 22007 (invalid_datetime_format). Text that is not a timestamp at all
// ("not-a-timestamp") is 22007 and returns false here.
//
// The DATE side has carried this classification since #560; TIMESTAMP simply
// never got it, so every bad literal was 22007 (#692).
func timestampFieldsOutOfRange(s string) bool {
	datePart, timePart := s, ""
	if i := strings.IndexAny(s, "T "); i >= 0 {
		datePart, timePart = s[:i], strings.TrimSpace(s[i+1:])
	}
	// The DATE parser owns the calendar rule, round trip included.
	if _, err := ParseDateDays(datePart); err != nil {
		var de *DateParseError
		if errors.As(err, &de) {
			return de.FieldRange
		}
		return false
	}
	if timePart == "" {
		return false
	}
	// Strip a zone suffix before reading the clock fields.
	if i := strings.IndexAny(timePart, "Zz+"); i > 0 {
		timePart = timePart[:i]
	} else if i := strings.LastIndex(timePart, "-"); i > 0 {
		timePart = timePart[:i]
	}
	fields := strings.Split(timePart, ":")
	if len(fields) < 2 || len(fields) > 3 {
		return false
	}
	limits := []int{23, 59, 60} // PostgreSQL accepts second 60 (leap second)
	for i, f := range fields {
		if i == 2 {
			f = strings.SplitN(f, ".", 2)[0]
		}
		n, err := strconv.Atoi(f)
		if err != nil {
			return false
		}
		if n < 0 || n > limits[i] {
			return true
		}
	}
	return false
}

// TimestampParseError is ParseTimestampMillis's failure. FieldRange separates
// PostgreSQL's two classes exactly as DateParseError's does: 22008 for a
// timestamp whose fields name no instant, 22007 for text that is not a
// timestamp at all.
type TimestampParseError struct {
	Text       string
	FieldRange bool
}

func (e *TimestampParseError) Error() string {
	if e.FieldRange {
		return fmt.Sprintf("date/time field value out of range: %q", e.Text)
	}
	return fmt.Sprintf("invalid input syntax for type timestamp: %q", e.Text)
}

func (e *TimestampParseError) SQLState() string {
	if e.FieldRange {
		return "22008"
	}
	return "22007"
}

// ParseDurationNanos converts a DURATION string to nanoseconds.
//
// A plain integer count of nanoseconds is the only accepted spelling, and
// deliberately so: schema.go defines the type as "nanoseconds, stored as
// int64", that is the unit Vector.GetValue reads back, and nothing in the
// system — parser, ingest, or a named-form registration — has ever defined
// another literal for it. Accepting Go's "1h30m" spelling here would invent a
// grammar the SQL literal path does not have, which is the divergence this
// function exists to close rather than widen.
func ParseDurationNanos(s string) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, &DurationParseError{Text: s}
	}
	return n, nil
}

// DurationParseError is ParseDurationNanos's failure. PostgreSQL has no
// DURATION type; 22007 is the code it uses for the interval literal this is
// closest to.
type DurationParseError struct{ Text string }

func (e *DurationParseError) Error() string {
	return fmt.Sprintf("invalid input syntax for type duration (expected an integer count of nanoseconds): %q", e.Text)
}

func (e *DurationParseError) SQLState() string { return "22007" }
