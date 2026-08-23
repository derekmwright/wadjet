package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// promoteVec builds a one-row vector of typ holding the given value.
func promoteVec(t *testing.T, typ batch.TypeID, val any) *batch.Vector {
	t.Helper()
	var v *batch.Vector
	if typ == batch.TypeDecimal {
		v = batch.NewVectorWithScale(typ, 1, 4)
	} else {
		v = batch.NewVector(typ, 1)
	}
	v.SetValue(0, val)
	return v
}

// TestNumericPromotionCoversTheIntegerBackedTypes is the regression for the
// promotion table.
//
// Three places answered "read this cell as a float64" from three different
// type lists, so one query answered differently depending on which it reached:
//
//   - resolveFloat64Extractor returned nil for PORT, PROTOCOL and DURATION, and
//     updateGroup reads nil as "skip every row" — STDDEV/VARIANCE/MEDIAN/
//     PERCENTILE/MODE/CORR/COVAR over them answered NULL.
//   - vecFloat64 returned 0 for DECIMAL, PORT, PROTOCOL, DATE and DURATION and
//     marked the row VALID — a window SUM/AVG over them computed zero, which is
//     a wrong number rather than a missing one.
//   - kernel.ResolveRowSum already accepted most of them, so the grouped and
//     windowed forms of the same query disagreed.
func TestNumericPromotionCoversTheIntegerBackedTypes(t *testing.T) {
	cases := []struct {
		typ  batch.TypeID
		val  any
		want float64
	}{
		{batch.TypeInt32, int32(7), 7},
		{batch.TypeInt64, int64(7), 7},
		{batch.TypeFloat32, float32(1.5), 1.5},
		{batch.TypeFloat64, 1.5, 1.5},
		{batch.TypeDecimal, "1.5000", 1.5},
		{batch.TypePort, int32(443), 443},
		{batch.TypeProtocol, int32(6), 6},
		{batch.TypeDuration, int64(5000000), 5000000},
		{batch.TypeTimestamp, int64(1700000000000), 1700000000000},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.typ.String(), func(t *testing.T) {
			v := promoteVec(t, tc.typ, tc.val)

			ext := resolveFloat64Extractor(tc.typ)
			if ext == nil {
				t.Fatalf("no float64 extractor: the statistical aggregates answer NULL over %s", tc.typ)
			}
			if got := ext(v, 0); got != tc.want {
				t.Errorf("extractor = %v, want %v", got, tc.want)
			}
			if got := vecFloat64(v, 0); got != tc.want {
				t.Errorf("vecFloat64 = %v, want %v — a window SUM/AVG reads through this", got, tc.want)
			}
			// The grouped SUM's own list is the reference: a type this table
			// admits and ResolveRowSum refuses would put the windowed and
			// grouped forms of one query back into disagreement.
			if kernel.ResolveRowSum(tc.typ) == nil && tc.typ != batch.TypeProtocol {
				t.Errorf("kernel.ResolveRowSum has no arm for %s but the promotion table does", tc.typ)
			}
		})
	}

	// DATE is promotable for reading, and ResolveRowSum already accepts it.
	dv := promoteVec(t, batch.TypeDate, "1970-01-11")
	if got := vecFloat64(dv, 0); got != 10 {
		t.Errorf("DATE vecFloat64 = %v, want 10 (days since epoch)", got)
	}
}

// TestOrderKeyPromotionIsWiderThanNumeric: MIN_BY/MAX_BY's second argument
// only has to ORDER — it never becomes the answer — so it admits the types
// whose stored integer form orders like the value. Before this split,
// MIN_BY(x, ipv4_col) resolved a nil extractor and answered NULL.
func TestOrderKeyPromotionIsWiderThanNumeric(t *testing.T) {
	cases := []struct {
		typ  batch.TypeID
		val  any
		want float64
	}{
		{batch.TypeIPv4, "0.0.1.0", 256},
		{batch.TypeMAC, "00:00:00:00:01:00", 256},
		{batch.TypeBool, true, 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.typ.String(), func(t *testing.T) {
			if resolveFloat64Extractor(tc.typ) != nil {
				t.Errorf("%s is not a numeric VALUE — PostgreSQL has no sum/avg/stddev over it", tc.typ)
			}
			ext := resolveOrderKeyExtractor(tc.typ)
			if ext == nil {
				t.Fatalf("no ordering extractor: MIN_BY/MAX_BY ordered by %s answers NULL", tc.typ)
			}
			if got := ext(promoteVec(t, tc.typ, tc.val), 0); got != tc.want {
				t.Errorf("ordering extractor = %v, want %v", got, tc.want)
			}
		})
	}
	if resolveOrderKeyExtractor(batch.TypePort) == nil {
		t.Error("the ordering table must be a SUPERSET of the numeric one")
	}
}
