package harness

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// Value signatures: a compact, order-insensitive, wobble-tolerant summary
// of a query result's numeric content — per-column sums formatted at fixed
// precision, compared with relative tolerance.
//
// Motivation (2026-07-26/27): two incidents produced WRONG VALUES under
// green row-count gates — catalog file-entry duplication multiplied every
// aggregate 3× (#278), and an eager fused-chain feed hijack inflated Q18
// through its LIMIT (§14.3 of eager-consumer-dispatch.md). Raw row
// checksums cannot serve as the gate: float summation order makes them
// wobble run-to-run (q01/q06/q07). Per-column sums in float64 put that
// wobble ~6+ orders of magnitude below any real corruption, so a relative
// tolerance separates the two cleanly: ulp noise passes, inflation fails.
//
// Non-numeric columns contribute nothing (both incident classes corrupt
// numerics; strings/dates are covered by the row-count gate).

// ValueSigRelTol is the relative tolerance CompareValueSigs applies per
// column. Summation-order noise on float64 column sums is ~1e-13 relative;
// the corruption classes this gate exists for are ≥ percents.
const ValueSigRelTol = 1e-6

// ValueSigAccum accumulates per-column numeric sums over result rows.
type ValueSigAccum struct {
	sums    []float64
	numeric []bool // column ever contributed a numeric value
}

func (a *ValueSigAccum) grow(n int) {
	for len(a.sums) < n {
		a.sums = append(a.sums, 0)
		a.numeric = append(a.numeric, false)
	}
}

// AddFloat folds one numeric value into column col.
func (a *ValueSigAccum) AddFloat(col int, v float64) {
	a.grow(col + 1)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	a.sums[col] += v
	a.numeric[col] = true
}

// AddVals folds one row of driver-level values (database/sql scan targets
// or engine row slices). Numeric types and numeric-parseable strings
// (pgwire renders decimals as text) contribute; everything else is skipped.
func (a *ValueSigAccum) AddVals(vals []any) {
	a.grow(len(vals))
	for i, v := range vals {
		switch x := v.(type) {
		case int64:
			a.AddFloat(i, float64(x))
		case int32:
			a.AddFloat(i, float64(x))
		case int:
			a.AddFloat(i, float64(x))
		case float64:
			a.AddFloat(i, x)
		case float32:
			a.AddFloat(i, float64(x))
		case []byte:
			if f, err := strconv.ParseFloat(string(x), 64); err == nil {
				a.AddFloat(i, f)
			}
		case string:
			if f, err := strconv.ParseFloat(x, 64); err == nil {
				a.AddFloat(i, f)
			}
		default:
			// pgx renders NUMERIC columns as pgtype.Numeric (a
			// Float64Valuer); anything else non-numeric is skipped.
			if fv, ok := v.(pgtype.Float64Valuer); ok {
				if f, err := fv.Float64Value(); err == nil && f.Valid {
					a.AddFloat(i, f.Float64)
				}
			}
		}
	}
}

// Signature renders the accumulated sums as "c<i>:<sum %.9e>" pairs joined
// by ",". Columns that never contributed a numeric value are omitted.
// Empty string when no column was numeric.
func (a *ValueSigAccum) Signature() string {
	var parts []string
	for i, ok := range a.numeric {
		if ok {
			parts = append(parts, fmt.Sprintf("c%d:%.9e", i, a.sums[i]))
		}
	}
	return strings.Join(parts, ",")
}

// CompareValueSigs compares two signatures column-wise with relative
// tolerance. Returns ok=true when every shared column agrees within relTol
// and both signatures cover the same columns. The detail string names the
// first divergence.
func CompareValueSigs(base, got string, relTol float64) (ok bool, detail string) {
	pb, err := parseValueSig(base)
	if err != nil {
		return false, fmt.Sprintf("baseline signature unparseable: %v", err)
	}
	pg, err := parseValueSig(got)
	if err != nil {
		return false, fmt.Sprintf("measured signature unparseable: %v", err)
	}
	if len(pb) != len(pg) {
		return false, fmt.Sprintf("column sets differ: baseline %d numeric columns, measured %d", len(pb), len(pg))
	}
	for col, bv := range pb {
		gv, present := pg[col]
		if !present {
			return false, fmt.Sprintf("column %s missing from measured signature", col)
		}
		scale := math.Max(1, math.Max(math.Abs(bv), math.Abs(gv)))
		if math.Abs(bv-gv)/scale > relTol {
			return false, fmt.Sprintf("column %s diverges: baseline %.9e vs measured %.9e (rel %.2e > %.0e)",
				col, bv, gv, math.Abs(bv-gv)/scale, relTol)
		}
	}
	return true, ""
}

func parseValueSig(s string) (map[string]float64, error) {
	out := map[string]float64{}
	if s == "" {
		return out, nil
	}
	for _, part := range strings.Split(s, ",") {
		col, val, found := strings.Cut(part, ":")
		if !found {
			return nil, fmt.Errorf("malformed part %q", part)
		}
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("part %q: %w", part, err)
		}
		out[col] = f
	}
	return out, nil
}
