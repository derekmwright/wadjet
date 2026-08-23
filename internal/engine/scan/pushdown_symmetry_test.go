package scan

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// compareValuesOK decides whether the prune layer may act, and
// CanPruneRowGroup hands it its operands in TWO different orders: OpEq passes
// (literal, stat) and the five ordered operators pass (stat, literal). So an
// asymmetric answer means an operator's prune depends on which arm it took,
// which is how the DECIMAL half of #442 stayed invisible on `<`, `<=`, `>`
// and `>=` — they were saved by a refusal, not by being right.
func TestCompareValuesIsSymmetric(t *testing.T) {
	vals := []any{
		int64(0), int64(1), int64(-1), int64(25), int64(math.MaxInt64), int64(math.MinInt64),
		int32(7), 0.0, 0.25, -0.25, 1.5, 25.0, float32(1.5),
		float64(math.MaxInt64), 1e300, -1e300,
		"", "a", "b",
	}
	for _, a := range vals {
		for _, b := range vals {
			ab, okAB := compareValuesOK(a, b)
			ba, okBA := compareValuesOK(b, a)
			if okAB != okBA {
				t.Errorf("compareValuesOK(%#v, %#v) ok=%v but the reverse ok=%v", a, b, okAB, okBA)
				continue
			}
			if okAB && ab != -ba {
				t.Errorf("compareValuesOK(%#v, %#v) = %d but the reverse = %d", a, b, ab, ba)
			}
		}
	}
}

// The integer/float ordering has to be EXACT, not a float64 widening: past
// 2^53 a widened int64 loses its low bits and two different integers compare
// equal to the same float.
func TestCompareValuesIntFloatIsExact(t *testing.T) {
	for _, tc := range []struct {
		i    int64
		f    float64
		want int
	}{
		{0, 0, 0},
		{25, 0.25, 1},
		{0, 0.25, -1},
		{-1, -0.5, -1},
		{-1, -1.5, 1},
		{math.MaxInt64, 1e300, -1},
		{math.MinInt64, -1e300, 1},
		// 2^53+1 has no float64; the widening compare called it equal to 2^53.
		{1 << 53, float64(1 << 53), 0},
		{(1 << 53) + 1, float64(1 << 53), 1},
		// 2^63 as a float is exactly one past MaxInt64.
		{math.MaxInt64, float64(1<<62) * 2, -1},
	} {
		got, ok := compareValuesOK(tc.i, tc.f)
		if !ok {
			t.Errorf("compareValuesOK(%d, %v) refused the pair", tc.i, tc.f)
			continue
		}
		if got != tc.want {
			t.Errorf("compareValuesOK(%d, %v) = %d, want %d", tc.i, tc.f, got, tc.want)
		}
	}
	if _, ok := compareValuesOK(int64(1), math.NaN()); ok {
		t.Error("NaN was ordered against an integer")
	}
	if _, ok := compareValuesOK(math.NaN(), math.NaN()); ok {
		t.Error("NaN was ordered against NaN")
	}
}

// The whole prune contract in one property: a row group whose bounds CONTAIN
// the literal must survive every operator that could match a row in it.
func TestPruneNeverDropsARowGroupThatCanMatch(t *testing.T) {
	stats := pqt.RowGroupStats{
		NumRows: 40,
		Columns: map[string]pqt.ColumnStats{
			// A DECIMAL(9,2)'s bounds are the UNSCALED integers: 21.00 .. 60.00.
			"dec": {HasStats: true, MinValue: int64(2100), MaxValue: int64(6000)},
			// An IPV6's are the raw sixteen bytes.
			"v6": {HasStats: true,
				MinValue: "\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
				MaxValue: "\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\xff"},
		},
	}
	ops := []exec.CompareOp{exec.OpEq, exec.OpNe, exec.OpLt, exec.OpLe, exec.OpGt, exec.OpGe}
	for _, tc := range []struct {
		name string
		col  string
		val  any
	}{
		{"decimal_inside", "dec", int64(3100)},
		{"ipv6_inside", "v6", "\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\xdc"},
	} {
		for _, op := range ops {
			pred := StatsPredicate{Column: tc.col, Op: op, Value: tc.val}
			if CanPruneRowGroup(pred, stats) && op != exec.OpNe {
				t.Errorf("%s: op %v pruned a row group whose bounds contain the value", tc.name, op)
			}
		}
	}
	// A value ON a bound: equality can still match it, whatever the ordered
	// operators conclude.
	for _, val := range []any{int64(2100), int64(6000)} {
		if CanPruneRowGroup(StatsPredicate{Column: "dec", Op: exec.OpEq, Value: val}, stats) {
			t.Errorf("equality against the bound %v pruned the row group holding it", val)
		}
	}

	// And a value genuinely outside the bounds still prunes, so the fix did
	// not simply turn pruning off.
	for _, tc := range []struct {
		col string
		op  exec.CompareOp
		val any
	}{
		{"dec", exec.OpEq, int64(100)},
		{"dec", exec.OpEq, int64(9999)},
		{"dec", exec.OpLt, int64(2100)},
		{"dec", exec.OpGt, int64(6000)},
		{"v6", exec.OpEq, "\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff"},
	} {
		pred := StatsPredicate{Column: tc.col, Op: tc.op, Value: tc.val}
		if !CanPruneRowGroup(pred, stats) {
			t.Errorf("column %s op %v value %#v: the row group cannot match and was not pruned",
				tc.col, tc.op, tc.val)
		}
	}
}
