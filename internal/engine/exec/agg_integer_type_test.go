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
