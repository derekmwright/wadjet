// SPDX-License-Identifier: MIT

package parquet

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
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
	// OutOfRange marks a real calendar date outside PostgreSQL's DATE range
	// (MinDateDay … MaxDateDay): still 22008, under PostgreSQL's own words.
	OutOfRange bool
	// ZoneRange marks a zone offset past PostgreSQL's ±15:59:59, or a minute
	// or second past 59 in one: 22009 (invalid_time_zone_displacement_value).
	ZoneRange bool
}

// PostgreSQL's DATE and TIMESTAMP ranges in the carriers' units — epoch days
// and epoch milliseconds — measured on 17.11: DATE 4714-11-24 BC …
// 5874897-12-31, TIMESTAMP 4714-11-24 00:00 BC … 294276-12-31 23:59:59.999999.
// The carriers (int32 days, int64 ms) hold wider values; nothing PostgreSQL
// refuses may be constructed or stored (arc VL round 4, #911's family). The
// expression layer's constructors (expr temporal_range.go) and this package's
// text readers take the bounds from here, so there is one range.
const (
	MinDateDay        int64 = -2440588
	MaxDateDay        int64 = 2145042905
	MinTimestampMilli int64 = -210866803200000
	EndTimestampMilli int64 = 9224318016000000 // exclusive
)

// The two infinite values of each temporal type are the carrier's extremes,
// as PostgreSQL stores them (DATEVAL_NOBEGIN / DATEVAL_NOEND are INT32_MIN /
// INT32_MAX, DT_NOBEGIN / DT_NOEND INT64_MIN / INT64_MAX): `-infinity` sorts
// below and `infinity` above every finite value by the integer order alone,
// so a comparison, a sort, a group, a join key, MIN / MAX and a row-group
// statistic need no case of their own. No finite value reaches them — the
// finite range (MinDateDay … MaxDateDay, MinTimestampMilli …
// EndTimestampMilli) ends millions of years short of either — so a stored
// extreme can only be one of the two words. Only the text grammar
// (parseTemporalText) produces them and only the two printers
// (FormatDateDays, batch.FormatTimestamp) print them; an operation that
// computes on a temporal value must answer them in the function that owns it
// (expr), never read them as the instant their integer would name.
const (
	DateNegInfinity      int32 = math.MinInt32
	DatePosInfinity      int32 = math.MaxInt32
	TimestampNegInfinity int64 = math.MinInt64
	TimestampPosInfinity int64 = math.MaxInt64
)

// IsInfiniteDate reports whether an epoch-day count (any integer width) is
// one of the DATE carrier's two infinite values.
func IsInfiniteDate(n int64) bool {
	return n == int64(DatePosInfinity) || n == int64(DateNegInfinity)
}

// IsInfiniteTimestamp reports whether an epoch-millisecond count is one of
// the TIMESTAMP carrier's two infinite values.
func IsInfiniteTimestamp(ms int64) bool {
	return ms == TimestampPosInfinity || ms == TimestampNegInfinity
}

// DateDaysFinite is the range question a CONSTRUCTION asks — arithmetic, a
// cast from a number, a clock: nil when n is a finite day PostgreSQL's DATE
// holds, else 22008. An infinite value is never computed: `DATE
// '1970-01-02' + 2147483646` lands on the carrier's maximum and is 22008, as
// PostgreSQL's date_pli refuses it (IS_VALID_DATE).
func DateDaysFinite(n int64) error {
	if n < MinDateDay || n > MaxDateDay {
		return sqlerr.New("22008", "date out of range")
	}
	return nil
}

// TimestampMillisFinite is DateDaysFinite for a TIMESTAMP's epoch
// milliseconds.
func TimestampMillisFinite(ms int64) error {
	if ms < MinTimestampMilli || ms >= EndTimestampMilli {
		return sqlerr.New("22008", "timestamp out of range")
	}
	return nil
}

// DateDaysInRange is THE range question for a STORED DATE's epoch-day count:
// nil when PostgreSQL's DATE holds it — a finite day in range or one of the
// two infinite values — else its 22008. Every construction of a
// DATE asks it — the expression layer's constructors (expr temporal_range.go
// reads it through expr.DateDaysInRange), the SQL write doors, and this
// writer's own box normalisation (normalizeTemporalBox), which the embedded
// ingester API reaches through CheckLeafBox: a typed int32 day count past the
// range was stored there and read back as year 5881580 (arc VL round-4
// review P2).
func DateDaysInRange(n int64) error {
	if IsInfiniteDate(n) {
		return nil
	}
	return DateDaysFinite(n)
}

// TimestampMillisInRange is DateDaysInRange for a TIMESTAMP's epoch
// milliseconds.
func TimestampMillisInRange(ms int64) error {
	if IsInfiniteTimestamp(ms) {
		return nil
	}
	return TimestampMillisFinite(ms)
}

// timestampInstantMillis is a time.Time as a TIMESTAMP box, range-checked on
// the INSTANT before UnixMilli (which wraps past ±292 million years) is taken.
func timestampInstantMillis(t time.Time) (int64, error) {
	if t.Before(time.UnixMilli(MinTimestampMilli)) || !t.Before(time.UnixMilli(EndTimestampMilli)) {
		return 0, sqlerr.New("22008", "timestamp out of range")
	}
	return t.UnixMilli(), nil
}

func (e *DateParseError) Error() string {
	if e.ZoneRange {
		return fmt.Sprintf("time zone displacement out of range: %q", e.Text)
	}
	if e.OutOfRange {
		return fmt.Sprintf("date out of range: %q", e.Text)
	}
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
	if e.ZoneRange {
		return "22009"
	}
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
	return errors.As(err, &e) && !e.FieldRange && !e.ZoneRange
}

// IsDateParseError reports whether err is any ParseDateDays failure.
func IsDateParseError(err error) bool {
	var e *DateParseError
	return errors.As(err, &e)
}

// ParseDateDays is the shared string-to-DATE value/error parser for readers,
// writers, ingest and batch construction. Accept unambiguous year-first
// (-, /, .; year >= four digits), compact YYYYMMDD, outer whitespace and
// validated trailing time truncated to the date. Never guess a date (#639).
// Invalid syntax is 22007; nonexistent/out-of-range dates and year zero
// are 22008. Exactly three-digit months use threeDigitMonthKind (#641).
// Four-digit months and three-digit days remain legal when values fit.
// Decline day-of-year, BC, DateStyle-dependent orders, two-digit years,
// DMY and month names; no unsupported spelling becomes epoch or wrong year.
// See docs/internals/parquet-date-text-accept-set.md for the design.
func ParseDateDays(s string) (int32, error) {
	tt, kind := parseTemporalText(s)
	switch kind {
	case dateFieldsNone:
		return 0, &DateParseError{Text: s}
	case dateFieldsBad:
		return 0, &DateParseError{Text: s, FieldRange: true}
	case dateFieldsZone:
		return 0, &DateParseError{Text: s, ZoneRange: true}
	}
	if tt.infinite > 0 {
		return DatePosInfinity, nil
	}
	if tt.infinite < 0 {
		return DateNegInfinity, nil
	}
	// The DATE is the date fields: a trailing time-of-day is validated and
	// dropped, `24:00:00` included (PostgreSQL 17.11: DATE '2024-03-04
	// 24:00:00' is 2024-03-04, the TIMESTAMP 2024-03-05 00:00:00).
	t := time.Date(tt.year, time.Month(tt.month), tt.day, 0, 0, 0, 0, time.UTC)
	days := civilDaysSinceEpoch(t)
	if days < MinDateDay || days > MaxDateDay {
		// PostgreSQL's DATE range, not the int32 carrier's: the ONE range
		// rule (expr's temporal_range.go reads these same bounds).
		return 0, &DateParseError{Text: s, FieldRange: true, OutOfRange: true}
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
	dateFieldsZone                       // a zone offset past ±15:59:59 (22009)
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
	// Wider than any field PostgreSQL's calendar holds (its last year,
	// 5874897, has seven digits), and wide enough to wrap atoiN's int: out
	// of range, never a wrapped value that happens to land in range.
	for _, p := range parts {
		if len(p) > 9 {
			return 0, 0, 0, dateFieldsBad
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

// threeDigitMonthKind checks EXACTLY three-digit middle fields (#641).
// With only the year decided, values 1..366 mean day-of-year in PostgreSQL;
// in a three-field date the extra field makes that 22007 (dateFieldsNone).
// Other three-digit values are invalid months, 22008 (dateFieldsBad).
// Do not reject four-or-more-digit months or three-digit days on width alone
// (ADR-0012 item 1). dateFieldsOK means only "not this case", not valid.
// See docs/internals/parquet-three-digit-month-classification.md for the design.
func threeDigitMonthKind(month string) dateFieldsKind {
	if len(month) != 3 {
		return dateFieldsOK
	}
	if v := atoiN(month); v >= 1 && v <= 366 {
		return dateFieldsNone // PostgreSQL's day-of-year, then a field too many
	}
	return dateFieldsBad // read as a month, and out of range
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
		// arrayElements, not a bare []any assertion: it normalises a typed
		// slice and REFUSES a box with no reading as an array, so this
		// boundary admits exactly what the writer stores. The assertion form
		// walked only the shapes that asserted, which is why a box the writer
		// turned into a NULL was one this function said nothing about (#889).
		arr, err := arrayElements(col, val)
		if err != nil {
			return err
		}
		for _, e := range arr {
			if err := ValidateNestedLeaves(*col.ElementType, e); err != nil {
				return err
			}
		}
	case TypeRow:
		m, err := rowFields(col, val, "a ROW")
		if err != nil {
			return err
		}
		for _, f := range col.Fields {
			if err := ValidateNestedLeaves(f, m[f.Name]); err != nil {
				return err
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
		if !ok {
			conv, err := rowFields(col, val, "a MAP")
			if err != nil {
				return err
			}
			m, ok = conv, true
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

// validateNestedLeaf applies CheckLeafBox to every non-NULL primitive leaf,
// identically to flat-column validation and decomposeLeaf.
// Include box, numeric range, temporal/DECIMAL and VECTOR width checks.
// Ingest must reject a bad nested value before buffering, rather than fail
// a later partition flush that also contains already-accepted good rows.
// See docs/internals/parquet-nested-leaf-validation.md for the design.
func validateNestedLeaf(col Column, val any) error {
	if val == nil {
		return nil
	}
	return CheckLeafBox(col, val)
}

// normalizeTemporalBox shares temporal conversion between ingest and writer:
// DATE stores epoch days, TIMESTAMP milliseconds and DURATION nanoseconds.
// Normalize DATE string/time.Time/typed day count, TIMESTAMP string/
// time.Time (#673)/int64 and DURATION string/time.Duration; every DATE and
// TIMESTAMP is held to PostgreSQL's range (DateDaysInRange,
// TimestampMillisInRange: 22008).
// A DATE time.Time uses its CALENDAR DATE in its own location, not its UTC
// instant, matching temporal partition-key formatting.
// Unconvertible boxes must fail the write with column/row context, never
// fall through an integer converter to zero.
// See docs/internals/parquet-temporal-box-normalization.md for the design.
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
			if days < MinDateDay || days > MaxDateDay {
				return nil, false, &DateParseError{Text: v.Format("2006-01-02"), FieldRange: true, OutOfRange: true}
			}
			return int32(days), true, nil
		case int, int32, int64:
			// A typed day count: the carrier holds ±5.8 million years,
			// PostgreSQL's DATE does not.
			n := reflectInt(v)
			if err := DateDaysInRange(n); err != nil {
				return nil, false, err
			}
			return int32(n), true, nil
		}
	case TypeTimestamp:
		switch v := val.(type) {
		case string:
			ms, err := ParseTimestampMillis(v)
			if err != nil {
				return nil, false, err
			}
			return ms, true, nil
		case time.Time:
			ms, err := timestampInstantMillis(v)
			if err != nil {
				return nil, false, err
			}
			return ms, true, nil
		case int64:
			if err := TimestampMillisInRange(v); err != nil {
				return nil, false, err
			}
			return v, true, nil
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

// reflectInt widens the typed integer day-count boxes a DATE accepts.
func reflectInt(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

// temporalText is a DATE or TIMESTAMP text as THE accept-set reads it: the
// date fields, the clock when one is spelled, and the zone offset when one is
// spelled (east of UTC, in seconds).
type temporalText struct {
	year, month, day         int
	hour, minute, second, ns int
	hasZone                  bool
	offsetSeconds            int
	// infinite is +1 for `infinity`, -1 for `-infinity` and 0 for a finite
	// value; the fields above are unset when it is not 0.
	infinite int
}

// parseTemporalText is THE date/time text grammar, one for DATE and
// TIMESTAMP alike (#1426's splice: when the TIMESTAMP reader was a list of
// Go layouts — two-digit fields, `-` separators, seconds required —
// `'2024/03/04'`, `'2024-3-4'` and `'2024-03-04 12:00'` were 22007 as a
// TIMESTAMP while the DATE reader took them, and a timestamp parameter bound
// over pgwire, spliced as `CAST('…' AS TIMESTAMP)`, was refused where
// PostgreSQL answers):
//
//	text   = ws* date [ sep clock [ ws* zone ] ] ws*
//	date   = splitDateFields' year-first shapes (a four-or-more-digit year,
//	         `-` `/` `.` separators, one or more digits a field; YYYYMMDD)
//	sep    = one or more ws | `T` | `t`
//	clock  = H[H] `:` M[M] [ `:` S[S] [ `.` digits* ] ]
//	zone   = `Z` | `z` | (`+`|`-`) ws* digits [ `:` [digits] [ `:` [digits] ] ]
//	ws     = temporalSpace: PostgreSQL's isspace (space, tab, \n, \r, \v, \f)
//	         (readZone: PostgreSQL's DecodeTimezone digit rule)
//
// The accepted forms are PostgreSQL 17.11's, measured per spelling (gated by
// pgwire.TestArcPWRound2MatchesPostgres and the zone table of
// coordinator.TestArcPWZoneSpellingsEveryArm), plus `epoch` and the two
// infinite values (infinityWord). PostgreSQL also reads month
// names, `now` / `today`, BC years, AM / PM, Julian
// days, zone NAMES — a POSIX zone spec among them: `…12:00:00Z+05` is the
// zone `Z+05`, five hours WEST of UTC (17:00 UTC as a timestamptz), not `Z`
// and then an offset — and a leading one-to-three-digit field as MDY; this
// grammar does not, and each is 22007 here
// (docs/adr/0012-divergences/temporal.md) — refused, never guessed.
//
// The calendar rule is ParseDateDays' (no year zero, month 1–12, the day
// existing in its month); the clock's is PostgreSQL's: hour 0–24 with `24`
// only as `24:00:00`, minute 0–59, second 0–60 (a leap second, no fraction).
// A field outside its range is 22008 (dateFieldsBad), a shape outside the
// grammar 22007 (dateFieldsNone).
func parseTemporalText(s string) (temporalText, dateFieldsKind) {
	var tt temporalText
	text := strings.TrimSpace(s)
	if text == "" {
		return tt, dateFieldsNone
	}
	// PostgreSQL's special input 'epoch' (any case, outer whitespace) is a
	// finite constant: 1970-01-01 00:00:00 for TIMESTAMP and DATE alike.
	if strings.EqualFold(text, "epoch") {
		tt.year, tt.month, tt.day = 1970, 1, 1
		return tt, dateFieldsOK
	}
	if sign := infinityWord(text); sign != 0 {
		tt.infinite = sign
		return tt, dateFieldsOK
	}
	datePart, rest := text, ""
	if i := strings.IndexAny(text, temporalSpace+"Tt"); i >= 0 {
		datePart, rest = text[:i], strings.TrimLeft(text[i+1:], temporalSpace)
		if rest == "" {
			return tt, dateFieldsNone
		}
	}
	y, m, d, kind := splitDateFields(datePart)
	if kind != dateFieldsOK {
		return tt, kind
	}
	// There is no year zero. PostgreSQL's calendar runs 4713 BC .. 5874897 AD
	// with 1 BC immediately before 1 AD, so every spelling of year 0000 is
	// 22008 there — '0000-01-01', '0000-1-1', '0000/01/01', '0000.01.01',
	// '0000-12-31', '00000101' and '00001231', all measured live on
	// postgres:17-alpine. Go's calendar is proleptic and HAS one, so this used
	// to store day -719528 for a string PostgreSQL refuses to parse (#641).
	if y == 0 {
		return tt, dateFieldsBad
	}
	// Month and day ranges, then calendar existence via a UTC round-trip:
	// time.Date normalizes an impossible day (2026-02-30 → 2026-03-02), so a
	// mismatch after construction is a nonexistent date.
	if m < 1 || m > 12 || d < 1 || d > 31 {
		return tt, dateFieldsBad
	}
	if t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC); t.Year() != y || int(t.Month()) != m || t.Day() != d {
		return tt, dateFieldsBad
	}
	tt.year, tt.month, tt.day = y, m, d
	if rest == "" {
		return tt, dateFieldsOK
	}
	return tt, tt.readClock(rest)
}

// infinityWord reads PostgreSQL's two infinite values: `infinity`, any case,
// with an optional sign that whitespace may separate from the word (`+
// infinity` and `-\tInfinity` read there; measured on 17.11). It answers +1,
// -1, or 0 for any other text — `inf`, `--infinity`, `+-infinity`,
// `infinity x`, `infinity 10:00`, `infinity+05` are 22007 as in PostgreSQL.
// PostgreSQL's date/time lexer also drops punctuation (`infinity,`,
// `"infinity"`) and an era or meridiem beside the word (`infinity BC`,
// `infinity AM`); this grammar reads neither (temporal r25), as for a finite
// value. text has its outer whitespace trimmed.
func infinityWord(text string) int {
	sign := 1
	if text != "" && (text[0] == '+' || text[0] == '-') {
		if text[0] == '-' {
			sign = -1
		}
		text = strings.TrimLeft(text[1:], temporalSpace)
	}
	if strings.EqualFold(text, "infinity") {
		return sign
	}
	return 0
}

// temporalSpace is the grammar's whitespace: PostgreSQL's ParseDateTime
// lexer skips isspace() — space, tab, \n, \r, \v, \f — between fields and
// after a zone's sign (`'2024-03-04\n12:00:00'`, `'…12:00:00+\n05'` read
// there, measured on PostgreSQL 17.11).
const temporalSpace = " \t\n\r\v\f"

// readClock reads `clock [ ws* zone ]` into tt.
func (tt *temporalText) readClock(s string) dateFieldsKind {
	i := 0
	field := func() (int, bool) {
		j := i
		for i < len(s) && i-j < 2 && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == j || (i < len(s) && s[i] >= '0' && s[i] <= '9') {
			return 0, false
		}
		return atoiN(s[j:i]), true
	}
	var ok bool
	if tt.hour, ok = field(); !ok || i >= len(s) || s[i] != ':' {
		return dateFieldsNone
	}
	i++
	if tt.minute, ok = field(); !ok {
		return dateFieldsNone
	}
	if i < len(s) && s[i] == ':' {
		i++
		if tt.second, ok = field(); !ok {
			return dateFieldsNone
		}
		if i < len(s) && s[i] == '.' {
			i++
			j := i
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
			frac := s[j:i]
			if len(frac) > 9 {
				frac = frac[:9]
			}
			ns := atoiN(frac)
			for k := len(frac); k < 9; k++ {
				ns *= 10
			}
			tt.ns = ns
		}
	}
	if kind := tt.readZone(strings.TrimLeft(s[i:], temporalSpace)); kind != dateFieldsOK {
		return kind
	}
	switch {
	case tt.hour > 24 || tt.minute > 59 || tt.second > 60:
		return dateFieldsBad
	case tt.hour == 24 && (tt.minute != 0 || tt.second != 0 || tt.ns != 0):
		return dateFieldsBad
	case tt.second == 60 && tt.ns != 0:
		return dateFieldsBad
	}
	return dateFieldsOK
}

// readZone reads the zone suffix ("" is none). A numeric offset is read as
// PostgreSQL 17.11's DecodeTimezone reads it: the digits after the sign are
// ONE integer, the hour; `:` then the minute, `:` then the second (each
// strtoint's reading: an optional `-`, any number of digits, none being 0);
// with no `:` and more than two digits, the last two digits are the minute
// and the rest the hour — there is no run-together seconds form. So `+5`,
// `+05`, `+530`, `+0530`, `+00130` and `+00000000130` read (the last three
// as 01:30), `+000130` is 01:30 and `+001500` 15:00, `+05:` is +05, while
// `+053000` (hour 530) and `+0530:00` (hour 530) are 22009. The range check
// (hour ≤ 15, minute and second 0–59; a field past int32 overflows) comes
// before the check for trailing text, PostgreSQL's order: `+16.5` is 22009,
// `+05.5` 22007.
func (tt *temporalText) readZone(z string) dateFieldsKind {
	switch {
	case z == "":
		return dateFieldsOK
	case z == "Z" || z == "z":
		tt.hasZone = true
		return dateFieldsOK
	case z[0] != '+' && z[0] != '-':
		return dateFieldsNone
	}
	// PostgreSQL's lexer soaks whitespace after a zone's sign: `+ 05` is +05.
	sign, body := 1, strings.TrimLeft(z[1:], temporalSpace)
	if z[0] == '-' {
		sign = -1
	}
	if body == "" || body[0] < '0' || body[0] > '9' {
		return dateFieldsNone
	}
	// num is strtoint(t, &rest, 10): an optional '-' then digits; with no
	// digit it reads 0 and consumes nothing; ok is false on int32 overflow.
	num := func(t string) (v int, rest string, ok bool) {
		j := 0
		if j < len(t) && t[j] == '-' {
			j++
		}
		k := j
		for k < len(t) && t[k] >= '0' && t[k] <= '9' {
			k++
		}
		if k == j {
			return 0, t, true
		}
		n, err := strconv.ParseInt(t[:k], 10, 32)
		return int(n), t[k:], err == nil
	}
	hr, rest, ok := num(body)
	if !ok {
		return dateFieldsZone
	}
	mn, sec := 0, 0
	switch {
	case strings.HasPrefix(rest, ":"):
		if mn, rest, ok = num(rest[1:]); !ok {
			return dateFieldsZone
		}
		if strings.HasPrefix(rest, ":") {
			if sec, rest, ok = num(rest[1:]); !ok {
				return dateFieldsZone
			}
		}
	case rest == "" && len(body) > 2:
		mn, hr = hr%100, hr/100
	}
	if hr > 15 || mn < 0 || mn > 59 || sec < 0 || sec > 59 {
		return dateFieldsZone
	}
	if rest != "" {
		// PostgreSQL's lexer ends a zone field at a `+`, or at whitespace,
		// and decodes what follows as a field of its own before refusing a
		// second zone: `+05+16` and `+05 -16` are 22009 there, `+05+05` 22007.
		if next := strings.TrimLeft(rest, temporalSpace); next != "" && (next[0] == '+' || (next[0] == '-' && len(next) < len(rest))) {
			var other temporalText
			if other.readZone(next) == dateFieldsZone {
				return dateFieldsZone
			}
		}
		return dateFieldsNone
	}
	tt.hasZone, tt.offsetSeconds = true, sign*(hr*3600+mn*60+sec)
	return dateFieldsOK
}

// wallClock is tt's wall-clock fields as a UTC instant. time.Date carries
// `24:00:00` and second 60 into the next day / minute, which is PostgreSQL's
// reading of both (`'2020-01-01 24:00:00'::timestamp` and `'2020-01-01
// 23:59:60'::timestamp` are both 2020-01-02 00:00:00 there).
func (tt temporalText) wallClock() time.Time {
	return time.Date(tt.year, time.Month(tt.month), tt.day, tt.hour, tt.minute, tt.second, tt.ns, time.UTC)
}

// ParseTimestampMillis converts a TIMESTAMP string to epoch milliseconds — the
// unit the parquet schema declares for the column (TimestampMillis) and the
// unit its reader hands back.
//
// The failure is 22007 rather than a zero: 0 is 1970-01-01T00:00:00Z stored
// under the caller's timestamp. A spelling the grammar reads whose fields name
// no instant, or one outside PostgreSQL's TIMESTAMP range, is 22008.
func ParseTimestampMillis(s string) (int64, error) {
	tt, kind := parseTemporalText(s)
	switch kind {
	case dateFieldsNone:
		return 0, &TimestampParseError{Text: s}
	case dateFieldsBad:
		return 0, &TimestampParseError{Text: s, FieldRange: true}
	case dateFieldsZone:
		return 0, &TimestampParseError{Text: s, ZoneRange: true}
	}
	if tt.infinite > 0 {
		return TimestampPosInfinity, nil
	}
	if tt.infinite < 0 {
		return TimestampNegInfinity, nil
	}
	ms, err := timestampInstantMillis(tt.wallClock())
	if err != nil {
		return 0, &TimestampParseError{Text: s, FieldRange: true}
	}
	return ms, nil
}

// ParseTimestampWallClock is THE timestamp accept-set (parseTemporalText),
// with the offset discarded, returning the instant whose UTC fields are the
// literal's wall clock — PostgreSQL's `timestamp without time zone` reading.
//
// It is exported because there were FOUR copies of this decision and they
// disagreed after #692 fixed two of them: the writer and the two comparison
// kernels discarded the offset while `expr.parseTimestampToEpochMsOK` and
// `expr.castTemporal` still applied it, so a row inserted with
// `'2020-06-01T12:00:00+05:30'` could not be found by `WHERE t = ` that same
// literal (review B2). Every path now reads this function.
func ParseTimestampWallClock(s string) (time.Time, bool) {
	t, _, _, ok := ParseTimestampZone(s)
	return t, ok
}

// ParseTimestampZone is ParseTimestampWallClock with the zone the text
// spelled: offsetSeconds east of UTC when hasZone. A `timestamp with time
// zone` reading of the text is the instant wall − offset (pgwire's
// timestamptz parameter, which this engine binds as the TIMESTAMP of its UTC
// instant). ok=false for text outside the grammar and for a wall clock
// outside PostgreSQL's TIMESTAMP range — and for `infinity` / `-infinity`,
// which name no instant: a caller that must store them reads
// ParseTimestampMillis, which answers the carrier's extremes.
func ParseTimestampZone(s string) (wall time.Time, offsetSeconds int, hasZone bool, ok bool) {
	tt, kind := parseTemporalText(s)
	if kind != dateFieldsOK || tt.infinite != 0 {
		return time.Time{}, 0, false, false
	}
	wall = tt.wallClock()
	if _, err := timestampInstantMillis(wall); err != nil {
		return time.Time{}, 0, false, false
	}
	return wall, tt.offsetSeconds, tt.hasZone, true
}

// WallClockMillis reads a parsed timestamp as PostgreSQL's
// `timestamp without time zone` does and returns it in the engine's ONE
// TIMESTAMP unit, epoch MILLISECONDS: the WALL-CLOCK FIELDS are the value and
// any offset the text carried is DISCARDED.
//
// `time.Parse(RFC3339, "2020-01-01T05:30:00+05:30")` yields 05:30 in a fixed
// +05:30 zone, and UnixMilli then converts it to the UTC INSTANT — midnight —
// so a producer that stops there stores a different timestamp than the text
// spells. PostgreSQL stores 05:30:00, and `'…+05:30'::timestamp` =
// `2020-01-01 05:30:00` is verifiable on any server. A text spelling `Z` is
// unaffected — discarding a zero offset changes nothing.
//
// It is the write the file readers (read_csv, read_json) make after matching
// one of their own layouts, so a field and the TIMESTAMP literal with the same
// text are the same stored value (#1266: they stored t.UnixMicro(), 1000x the
// carrier). Sub-millisecond digits are FLOORED — UnixMilli counts whole
// milliseconds toward the past, pre-1970 included — the same as
// ParseTimestampMillis.
func WallClockMillis(t time.Time) int64 {
	return time.Date(t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC).UnixMilli()
}

// TimestampParseError is ParseTimestampMillis's failure. FieldRange separates
// PostgreSQL's two classes exactly as DateParseError's does: 22008 for a
// timestamp whose fields name no instant, 22007 for text that is not a
// timestamp at all.
type TimestampParseError struct {
	Text       string
	FieldRange bool
	ZoneRange  bool // 22009, as DateParseError's
}

func (e *TimestampParseError) Error() string {
	if e.ZoneRange {
		return fmt.Sprintf("time zone displacement out of range: %q", e.Text)
	}
	if e.FieldRange {
		return fmt.Sprintf("date/time field value out of range: %q", e.Text)
	}
	return fmt.Sprintf("invalid input syntax for type timestamp: %q", e.Text)
}

func (e *TimestampParseError) SQLState() string {
	if e.ZoneRange {
		return "22009"
	}
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
