// SPDX-License-Identifier: MIT

package parquet

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestArcTITemporalGrammarReadsTheInfiniteValues: the one date/time grammar
// reads PostgreSQL 17.11's two infinite values into the carriers' extremes,
// by the spelling family measured there (ti_author/pg_grammar.txt): the word
// in any case, an optional sign that whitespace may separate from it, outer
// whitespace (PostgreSQL's isspace). Every other spelling is 22007 —
// including the ones PostgreSQL's lexer reads by dropping punctuation or an
// era / meridiem beside the word (`infinity,`, `"infinity"`, `infinity BC`,
// `infinity AM`), which this grammar reads for no value (temporal r25).
func TestArcTITemporalGrammarReadsTheInfiniteValues(t *testing.T) {
	for _, c := range []struct {
		text string
		sign int // +1 / -1 read; 0 refused 22007
	}{
		{"infinity", 1}, {"-infinity", -1}, {"+infinity", 1},
		{"Infinity", 1}, {"INFINITY", 1}, {"-INFinity", -1},
		{" infinity ", 1}, {"  -Infinity  ", -1}, {"\tinfinity\n", 1},
		{"- infinity", -1}, {"+ infinity", 1}, {"-  infinity", -1}, {"+\tinfinity", 1}, {"- Infinity ", -1},
		{"inf", 0}, {"-inf", 0}, {"+inf", 0}, {"infinit", 0}, {"infinityy", 0},
		{"--infinity", 0}, {"++infinity", 0}, {"+-infinity", 0}, {"-+infinity", 0}, {"- -infinity", 0}, {"+ -infinity", 0},
		{"infinity x", 0}, {"infinity 10:00", 0}, {"infinity+05", 0}, {"infinity Z", 0}, {"infinity UTC", 0},
		{"infinity infinity", 0}, {"infinity epoch", 0}, {"infinity today", 0}, {"2024-01-01 infinity", 0},
		{"infinity.", 0}, {"infinity-", 0}, {"-infinity-", 0}, {"Tinfinity", 0}, {"infinity T", 0},
		// PostgreSQL answers these; the grammar reads no punctuation and no era
		{"infinity BC", 0}, {"infinity AM", 0}, {"BC infinity", 0}, {"infinity,", 0}, {",infinity", 0},
		{"infinity:", 0}, {`"infinity"`, 0},
	} {
		days, derr := ParseDateDays(c.text)
		ms, terr := ParseTimestampMillis(c.text)
		_, _, _, zok := ParseTimestampZone(c.text)
		switch c.sign {
		case 0:
			if s := sqlerr.StateOf(derr); s != "22007" {
				t.Errorf("DATE %q: %d %v, want 22007", c.text, days, derr)
			}
			if s := sqlerr.StateOf(terr); s != "22007" {
				t.Errorf("TIMESTAMP %q: %d %v, want 22007", c.text, ms, terr)
			}
		default:
			wantD, wantT := DatePosInfinity, TimestampPosInfinity
			if c.sign < 0 {
				wantD, wantT = DateNegInfinity, TimestampNegInfinity
			}
			if derr != nil || days != wantD {
				t.Errorf("DATE %q: %d %v, want %d", c.text, days, derr, wantD)
			}
			if terr != nil || ms != wantT {
				t.Errorf("TIMESTAMP %q: %d %v, want %d", c.text, ms, terr, wantT)
			}
		}
		// The infinite values name no instant: the wall-clock reading
		// refuses them (a caller that stores them reads the millisecond one).
		if c.sign != 0 && zok {
			t.Errorf("ParseTimestampZone(%q) answered an instant", c.text)
		}
	}
	if got := FormatDateDays(DatePosInfinity) + "|" + FormatDateDays(DateNegInfinity); got != "infinity|-infinity" {
		t.Errorf("FormatDateDays of the extremes: %s", got)
	}
	if got := FormatDateDays(DatePosInfinity - 1); got == "infinity" {
		t.Errorf("FormatDateDays(MaxInt32-1) printed infinity")
	}
	// The finite range question refuses the extremes; the stored one admits
	// them and nothing beside them.
	for _, n := range []int64{int64(DatePosInfinity), int64(DateNegInfinity)} {
		if DateDaysFinite(n) == nil || DateDaysInRange(n) != nil {
			t.Errorf("day %d: finite %v, stored %v", n, DateDaysFinite(n), DateDaysInRange(n))
		}
	}
	for _, ms := range []int64{TimestampPosInfinity, TimestampNegInfinity} {
		if TimestampMillisFinite(ms) == nil || TimestampMillisInRange(ms) != nil {
			t.Errorf("ms %d: finite %v, stored %v", ms, TimestampMillisFinite(ms), TimestampMillisInRange(ms))
		}
	}
}

// TestArcTIInfiniteValuesRoundTripTheFile: the writer stores the carriers'
// extremes as the ordinary int64 / int32 they are — no writer change — and
// the reader returns them unchanged, with each row group's statistics
// bounded by them, so a min / max prune orders them below and above every
// finite value.
func TestArcTIInfiniteValuesRoundTripTheFile(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "ts", Type: TypeTimestamp, Nullable: true},
		{Name: "d", Type: TypeDate, Nullable: true},
	}}
	rows := []map[string]any{
		{"ts": TimestampPosInfinity, "d": DatePosInfinity},
		{"ts": int64(1705314600000), "d": int32(19737)},
		{"ts": TimestampNegInfinity, "d": "-infinity"},
		{"ts": "infinity", "d": nil},
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"9223372036854775807 2147483647",
		"1705314600000 19737",
		"-9223372036854775808 -2147483648",
		"9223372036854775807 <nil>",
	}
	for i, row := range got {
		// The row reader boxes a DATE as its text (FormatDateDays).
		d := row["d"]
		if s, ok := d.(string); ok {
			n, err := ParseDateDays(s)
			if err != nil {
				t.Fatalf("row %d: date text %q: %v", i, s, err)
			}
			d = n
		}
		if s := fmt.Sprintf("%v %v", row["ts"], d); s != want[i] {
			t.Errorf("row %d: %s, want %s", i, s, want[i])
		}
	}
	st := r.RowGroupStats(0)
	if c := st.Columns["ts"]; c.MinValue != TimestampNegInfinity || c.MaxValue != TimestampPosInfinity {
		t.Errorf("ts statistics %v … %v", c.MinValue, c.MaxValue)
	}
	// A DATE's statistics are widened to int64.
	if c := st.Columns["d"]; c.MinValue != int64(DateNegInfinity) || c.MaxValue != int64(DatePosInfinity) {
		t.Errorf("d statistics %v (%T) … %v", c.MinValue, c.MinValue, c.MaxValue)
	}
}
