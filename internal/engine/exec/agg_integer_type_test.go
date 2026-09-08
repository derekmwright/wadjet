package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestIntegerAccOutputTypeIsPostgresRule holds the ONE table every
// accumulating aggregate's declaration comes from, grouped and windowed
// alike. Every row was measured on live postgres:17.11 (`\gdesc` beside
// `pg_typeof`), and the types this engine does NOT put in the table are
// asserted too — a type silently joining it moves a declared OID for every
// query that sums that column.
func TestIntegerAccOutputTypeIsPostgresRule(t *testing.T) {
	const p = batch.MaxDecimalPrecision
	for _, tc := range []struct {
		name  string
		avg   bool
		in    parquet.TypeID
		out   parquet.TypeID
		prec  int
		scale int
		ok    bool
	}{
		// pg_typeof(sum(int4)) -> bigint
		{"sum int4", false, parquet.TypeInt32, parquet.TypeInt64, 0, 0, true},
		// pg_typeof(sum(int8)) -> numeric
		{"sum int8", false, parquet.TypeInt64, parquet.TypeDecimal, p, 0, true},
		// pg_typeof(avg(int4)) -> numeric ; pg_typeof(avg(int8)) -> numeric
		{"avg int4", true, parquet.TypeInt32, parquet.TypeDecimal, p, batch.AvgScale(0), true},
		{"avg int8", true, parquet.TypeInt64, parquet.TypeDecimal, p, batch.AvgScale(0), true},
		// PORT and PROTOCOL declare int4 on the wire (#834), so they follow
		// int4's rules in both spellings (#953).
		{"sum port", false, parquet.TypePort, parquet.TypeInt64, 0, 0, true},
		{"sum protocol", false, parquet.TypeProtocol, parquet.TypeInt64, 0, 0, true},
		{"avg port", true, parquet.TypePort, parquet.TypeDecimal, p, batch.AvgScale(0), true},
		{"avg protocol", true, parquet.TypeProtocol, parquet.TypeDecimal, p, batch.AvgScale(0), true},

		// NOT in the table. A DECIMAL input has its own rule
		// (WindowDecimalAggMeta / aggSpecOutputDecimal), and the float widths
		// keep their own accumulator; the int-backed wadjet types below have
		// no PostgreSQL `sum` to follow.
		{"decimal declines", false, parquet.TypeDecimal, 0, 0, 0, false},
		{"float64 declines", false, parquet.TypeFloat64, 0, 0, 0, false},
		{"float32 declines", false, parquet.TypeFloat32, 0, 0, 0, false},
		{"date declines", false, parquet.TypeDate, 0, 0, 0, false},
		{"timestamp declines", false, parquet.TypeTimestamp, 0, 0, 0, false},
		{"duration declines", false, parquet.TypeDuration, 0, 0, 0, false},
		{"string declines", false, parquet.TypeString, 0, 0, 0, false},
		{"avg float64 declines", true, parquet.TypeFloat64, 0, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, prec, scale, ok := IntegerAccOutputType(tc.avg, tc.in)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if out != tc.out || prec != tc.prec || scale != tc.scale {
				t.Fatalf("got (%v, %d, %d), want (%v, %d, %d)",
					out, prec, scale, tc.out, tc.prec, tc.scale)
			}
			if got := integerAccInput(tc.in); !got {
				t.Fatalf("integerAccInput(%v) = false while the table names it", tc.in)
			}
		})
	}
}

// TestTheFlatScatterCoversEveryTypeTheIntegerTableNames is the SoA half of the
// #953 blocker (round-1 B1).
//
// isFlatSumType is what tells the aggregate that the flat scatter has a SUM/AVG
// arm for a column type — and therefore that it increments the count for it.
// TypeProtocol was missing from it, and from the scatter's four SUM/AVG arms,
// which is one half of why the GROUPED SUM(c_proto) answered NULL. Reverting
// that entry ALONE changes no query answer, because HashAggregate falls back to
// a producer that is now also correct; a redundancy is not a gate, so the
// agreement is asserted here, where the entry is visible on its own.
//
// The two lists are not the same list, and the boundary types say so: DATE,
// TIMESTAMP and DURATION ARE flat-summable and are deliberately NOT in the
// integer table, because PostgreSQL has no `sum(date)` to follow.
func TestTheFlatScatterCoversEveryTypeTheIntegerTableNames(t *testing.T) {
	for _, typ := range []parquet.TypeID{
		parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol,
	} {
		if !integerAccInput(typ) {
			t.Errorf("%v is not in the integer result-type table", typ)
		}
		if !isFlatSumType(typ) {
			t.Errorf("%v is in the integer result-type table but the flat scatter has no "+
				"SUM/AVG arm for it — the declaration and the carrier disagree", typ)
		}
	}
	for _, typ := range []parquet.TypeID{
		parquet.TypeDate, parquet.TypeTimestamp, parquet.TypeDuration,
	} {
		if integerAccInput(typ) {
			t.Errorf("%v joined the integer result-type table; PostgreSQL has no "+
				"integer aggregate for it to follow", typ)
		}
		if !isFlatSumType(typ) {
			t.Errorf("%v stopped being flat-summable — that is a value change, not a typing one", typ)
		}
	}
}
