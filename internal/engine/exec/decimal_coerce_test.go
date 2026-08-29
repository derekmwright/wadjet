package exec

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// dcBatch builds a one-column batch of the given type. Decimal values are
// UNSCALED integers at scale, the way ADR-0018 §4 says the box works.
func dcBatch(t *testing.T, col parquet.Column, vals []int64, nulls ...int) *batch.RecordBatch {
	t.Helper()
	v := batch.NewVectorWithScale(col.Type, len(vals), col.Scale)
	for i, n := range vals {
		switch col.Type {
		case parquet.TypeDecimal:
			v.DecimalData.Data[i] = batch.Int128From(n)
		case parquet.TypeInt64:
			v.Int64Data[i] = n
		case parquet.TypeInt32:
			v.Int32Data[i] = int32(n)
		case parquet.TypeFloat64:
			v.Float64Data[i] = float64(n)
		default:
			t.Fatalf("dcBatch: unhandled type %s", col.Type)
		}
	}
	for _, i := range nulls {
		v.Nulls.SetNull(i)
	}
	return &batch.RecordBatch{
		Schema:  []parquet.Column{col},
		Columns: []*batch.Vector{v},
		Len:     len(vals),
	}
}

func dcDecimal(p, s int) parquet.Column {
	return parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: p, Scale: s, Nullable: true}
}

// TestDecimalCoerceRescalesTheCarrier is the operator's whole job: the
// unscaled integer MOVES, so the number does not.
func TestDecimalCoerceRescalesTheCarrier(t *testing.T) {
	// 12.75 and -0.01 at scale 2, restated at scale 4.
	in := dcBatch(t, dcDecimal(9, 2), []int64{1275, -1, 0}, 2)
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 18, Scale: 4}})
	out, err := op.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := out.Schema[0].Scale; got != 4 {
		t.Errorf("output declares scale %d, want 4", got)
	}
	if got := out.Schema[0].Precision; got != 18 {
		t.Errorf("output declares precision %d, want 18", got)
	}
	if got := out.Columns[0].DecimalData.Scale; got != 4 {
		t.Errorf("output VECTOR carries scale %d, want 4 — the schema and the vector must agree "+
			"or the shuffle header and the chunk describe different numbers", got)
	}
	for i, want := range []int64{127500, -100} {
		if got := out.Columns[0].DecimalData.Data[i].ToInt64(); got != want {
			t.Errorf("row %d unscaled = %d, want %d", i, got, want)
		}
	}
	if !out.Columns[0].Nulls.IsNull(2) {
		t.Error("row 2 was NULL on the way in and must still be")
	}
	// The INPUT batch is left alone: it is often its producer's pooled
	// storage, and a consumer upstream may still be reading it.
	if got := in.Columns[0].DecimalData.Data[0].ToInt64(); got != 1275 {
		t.Errorf("the input vector was mutated (row 0 unscaled = %d, want 1275)", got)
	}
	if got := in.Schema[0].Scale; got != 2 {
		t.Errorf("the input schema was mutated (scale %d, want 2)", got)
	}
}

// TestDecimalCoerceFromAnInteger covers the `numeric UNION ALL bigint` arm:
// an integer is a value at scale 0, so the whole output scale is the shift.
func TestDecimalCoerceFromAnInteger(t *testing.T) {
	for _, tc := range []struct {
		name string
		col  parquet.Column
	}{
		{"int64", parquet.Column{Name: "v", Type: parquet.TypeInt64, Nullable: true}},
		{"int32", parquet.Column{Name: "v", Type: parquet.TypeInt32, Nullable: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := dcBatch(t, tc.col, []int64{1, -3, 0}, 2)
			op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 18, Scale: 4}})
			out, err := op.Execute(context.Background(), in)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if out.Schema[0].Type != parquet.TypeDecimal {
				t.Fatalf("output type is %s, want DECIMAL", out.Schema[0].Type)
			}
			for i, want := range []int64{10000, -30000} {
				if got := out.Columns[0].DecimalData.Data[i].ToInt64(); got != want {
					t.Errorf("row %d unscaled = %d, want %d (an integer 1 is 1.0000, not 0.0001)", i, got, want)
				}
			}
			if !out.Columns[0].Nulls.IsNull(2) {
				t.Error("row 2 was NULL on the way in and must still be")
			}
		})
	}
}

// TestDecimalCoercePrecisionOnlyIsSchemaOnly: a scale that already agrees
// needs no data touched, only the declaration restated.
func TestDecimalCoercePrecisionOnlyIsSchemaOnly(t *testing.T) {
	in := dcBatch(t, dcDecimal(9, 2), []int64{1275})
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 21, Scale: 2}})
	out, err := op.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out.Schema[0].Precision != 21 || out.Schema[0].Scale != 2 {
		t.Errorf("output declares DECIMAL(%d,%d), want DECIMAL(21,2)", out.Schema[0].Precision, out.Schema[0].Scale)
	}
	if out.Columns[0] != in.Columns[0] {
		t.Error("a precision-only restatement reallocated the column; the carrier did not change")
	}
}

// TestDecimalCoerceOverflowIsAnError: a value with no Int128 at the output
// scale fails, rather than wrapping into a different number wearing the right
// type (ADR-0012 item 9's rule for SUM, for the same reason).
func TestDecimalCoerceOverflowIsAnError(t *testing.T) {
	// 10^18 at scale 2 restated at scale 30 needs 10^46.
	in := dcBatch(t, dcDecimal(38, 2), []int64{1_000_000_000_000_000_000})
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 38, Scale: 30}})
	_, err := op.Execute(context.Background(), in)
	if err == nil {
		t.Fatal("a value with no exact carrier at the output scale must be an error")
	}
	if !strings.Contains(err.Error(), "overflow") || !strings.Contains(err.Error(), `"v"`) {
		t.Errorf("error should name the overflow and the column, got %q", err)
	}
}

// TestDecimalCoerceRefusesToNarrow: the planner's target scale is the maximum
// over the arms, so a downward move is a planner defect, not a rounding
// decision this operator gets to make.
func TestDecimalCoerceRefusesToNarrow(t *testing.T) {
	in := dcBatch(t, dcDecimal(18, 4), []int64{127501})
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 9, Scale: 2}})
	_, err := op.Execute(context.Background(), in)
	if err == nil {
		t.Fatal("scaling DOWN drops digits and must be refused")
	}
	if !strings.Contains(err.Error(), "drop digits") {
		t.Errorf("error should say what it refused, got %q", err)
	}
}

// TestDecimalCoerceRefusesAnUncoercibleType: the planner only asks for
// DECIMAL and integer sources, so anything else is a spec that does not match
// the plan and is reported rather than guessed at.
func TestDecimalCoerceRefusesAnUncoercibleType(t *testing.T) {
	in := dcBatch(t, parquet.Column{Name: "v", Type: parquet.TypeFloat64, Nullable: true}, []int64{1})
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 18, Scale: 4}})
	if _, err := op.Execute(context.Background(), in); err == nil {
		t.Fatal("a FLOAT64 source has no exact DECIMAL carrier and must be refused")
	}
	miss := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "absent", Precision: 18, Scale: 4}})
	if _, err := miss.Execute(context.Background(), dcBatch(t, dcDecimal(9, 2), []int64{1})); err == nil {
		t.Fatal("a column the input does not have must be refused, not skipped")
	}
}

// TestDecimalCoerceCatchesAMidStreamChange. The operator resolves against the
// first batch; a LATER batch whose column arrives at a different scale is the
// very defect this exists for, so it is reported instead of being coerced
// under the first batch's assumption.
func TestDecimalCoerceCatchesAMidStreamChange(t *testing.T) {
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 38, Scale: 10}})
	if _, err := op.Execute(context.Background(), dcBatch(t, dcDecimal(9, 2), []int64{1275})); err != nil {
		t.Fatalf("first batch: %v", err)
	}
	_, err := op.Execute(context.Background(), dcBatch(t, dcDecimal(18, 4), []int64{127500}))
	if err == nil {
		t.Fatal("a column that changes scale mid-stream must be reported")
	}
	if !strings.Contains(err.Error(), "mid-stream") {
		t.Errorf("error should say the column changed mid-stream, got %q", err)
	}
}

// TestDecimalCoerceThroughAView: a view owns no storage and reads its values
// through a base, and the SCALE it carries is the base's.
func TestDecimalCoerceThroughAView(t *testing.T) {
	base := batch.NewVectorWithScale(parquet.TypeDecimal, 3, 2)
	base.DecimalData.Data[0] = batch.Int128From(1275)
	base.DecimalData.Data[1] = batch.Int128From(-1)
	base.DecimalData.Data[2] = batch.Int128From(7)
	base.Nulls.SetNull(2)
	view := batch.NewViewVector(base, []uint32{2, 0, 1})
	in := &batch.RecordBatch{
		Schema:  []parquet.Column{dcDecimal(9, 2)},
		Columns: []*batch.Vector{view},
		Len:     3,
	}
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 18, Scale: 4}})
	out, err := op.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !out.Columns[0].Nulls.IsNull(0) {
		t.Error("row 0 reads a NULL through the view and must still be NULL")
	}
	for i, want := range map[int]int64{1: 127500, 2: -100} {
		if got := out.Columns[0].DecimalData.Data[i].ToInt64(); got != want {
			t.Errorf("row %d unscaled = %d, want %d", i, got, want)
		}
	}
}

// TestDecimalCoerceKeepsTheSelectionVector: rows a selection excludes are
// converted too, so the caller's Sel still indexes the same rows.
func TestDecimalCoerceKeepsTheSelectionVector(t *testing.T) {
	in := dcBatch(t, dcDecimal(9, 2), []int64{1275, 300, -1})
	in.Sel = []uint32{2, 0}
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 18, Scale: 4}})
	out, err := op.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(out.Sel) != 2 || out.Sel[0] != 2 || out.Sel[1] != 0 {
		t.Fatalf("selection vector changed: %v", out.Sel)
	}
	if got := out.Columns[0].DecimalData.Data[2].ToInt64(); got != -100 {
		t.Errorf("selected row 2 unscaled = %d, want -100", got)
	}
	if got := out.Columns[0].DecimalData.Data[0].ToInt64(); got != 127500 {
		t.Errorf("selected row 0 unscaled = %d, want 127500", got)
	}
}

// TestDecimalCoerceIsCloneable: a fragment carrying this operator must still
// be able to run its morsel workers in parallel, which Pipeline gates on
// every operator implementing Cloneable.
func TestDecimalCoerceIsCloneable(t *testing.T) {
	op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: 18, Scale: 4}})
	var c Cloneable = op
	clone, ok := c.Clone().(*DecimalCoerce)
	if !ok {
		t.Fatal("Clone did not return a *DecimalCoerce")
	}
	if clone == op {
		t.Fatal("Clone returned the same operator; a morsel worker needs its own resolution state")
	}
	out, err := clone.Execute(context.Background(), dcBatch(t, dcDecimal(9, 2), []int64{1275}))
	if err != nil {
		t.Fatalf("clone Execute: %v", err)
	}
	if got := out.Columns[0].DecimalData.Data[0].ToInt64(); got != 127500 {
		t.Errorf("clone produced unscaled %d, want 127500", got)
	}
}

// dcInt128 builds an Int128 from decimal text, for the values that do not fit
// an int64 literal.
func dcInt128(t *testing.T, text string) batch.Int128 {
	t.Helper()
	v, ok := new(big.Int).SetString(text, 10)
	if !ok {
		t.Fatalf("dcInt128: %q is not an integer", text)
	}
	neg := v.Sign() < 0
	mag := new(big.Int).Abs(v)
	lo := new(big.Int).And(mag, new(big.Int).SetUint64(^uint64(0)))
	hi := new(big.Int).Rsh(mag, 64)
	out := batch.Int128{Hi: hi.Int64(), Lo: lo.Uint64()}
	if neg {
		out = out.Neg()
	}
	return out
}

// dcDecimalBatch is dcBatch for a single Int128 the int64 helper cannot carry.
func dcDecimalBatch(col parquet.Column, v batch.Int128) *batch.RecordBatch {
	vec := batch.NewVectorWithScale(parquet.TypeDecimal, 1, col.Scale)
	vec.DecimalData.Data[0] = v
	return &batch.RecordBatch{
		Schema:  []parquet.Column{col},
		Columns: []*batch.Vector{vec},
		Len:     1,
	}
}

// TestDecimalCoerceChecksTheDeclaredPrecisionNotTheCarrier.
//
// The Int128 carrier and the DECLARED precision are two different bounds, and
// only the second one is the type. Between them sits a band — an unscaled
// magnitude in [10^38, 2^127-1] under a DECIMAL(38,s), and everything above
// 10^p under any narrower p — where the multiplication succeeds and the value
// still does not fit the column it is being written into. Admitting it writes
// a number the declared type cannot hold into a column the parquet writer
// sizes from that precision (ADR-0018 §4) and every consumer reads back as
// in-range.
func TestDecimalCoerceChecksTheDeclaredPrecisionNotTheCarrier(t *testing.T) {
	for _, tc := range []struct {
		name     string
		src      parquet.Column
		unscaled string
		wantP    int
		wantS    int
		ok       bool
	}{
		// The band the carrier check misses at the widest declaration:
		// 1.5e37 at scale 9 shifts to 1.5e38, which is under 2^127-1
		// (~1.70e38) and over 10^38.
		{"band_at_38", dcDecimal(38, 9), "15000000000000000000000000000000000000", 38, 10, false},
		// One ulp under the bound is admitted: 10^38-1 at scale 10 is the
		// largest DECIMAL(38,10) there is.
		{"largest_representable", dcDecimal(38, 9), "9999999999999999999999999999999999999", 38, 10, true},
		// A narrower declaration is 10^11 short of the carrier, so the band
		// there is enormous and trivially reachable.
		{"narrow_precision", dcDecimal(9, 2), "10000000000", 11, 4, false},
		{"narrow_precision_fits", dcDecimal(9, 2), "9999999", 11, 4, true},
		// Negative magnitudes take the same bound.
		{"negative_band", dcDecimal(38, 9), "-15000000000000000000000000000000000000", 38, 10, false},
		{"negative_fits", dcDecimal(9, 2), "-9999999", 11, 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := dcDecimalBatch(tc.src, dcInt128(t, tc.unscaled))
			op := NewDecimalCoerce([]DecimalCoerceColumn{{Name: "v", Precision: tc.wantP, Scale: tc.wantS}})
			out, err := op.Execute(context.Background(), in)
			if tc.ok {
				if err != nil {
					t.Fatalf("a value inside DECIMAL(%d,%d) was rejected: %v", tc.wantP, tc.wantS, err)
				}
				if out.Columns[0].DecimalData.Scale != tc.wantS {
					t.Errorf("output scale = %d, want %d", out.Columns[0].DecimalData.Scale, tc.wantS)
				}
				return
			}
			if err == nil {
				t.Fatalf("a value outside DECIMAL(%d,%d) was admitted as %v — the check is on the "+
					"declared precision, not on what an Int128 happens to hold",
					tc.wantP, tc.wantS, out.Columns[0].DecimalData.Data[0])
			}
			for _, want := range []string{"numeric field overflow", `"v"`, "precision"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error should mention %q; got %q", want, err)
				}
			}
		})
	}
}

// TestDecimalPrecisionLimit pins the bound itself, including the edge the
// 38-digit cap makes the widest reachable one.
//
// A precision PAST the carrier's width now CLAMPS to 10^38 instead of
// answering "no bound to check". The skip was the wrong direction: a
// DECIMAL(50,2) declaration does not make an Int128 hold 10^50, so the values
// the skip admitted were exactly the ones with no carrier at all (ADR-0024
// item 4; the R12 row of the survey).
func TestDecimalPrecisionLimit(t *testing.T) {
	const tenPow38 = "100000000000000000000000000000000000000"
	for _, tc := range []struct {
		p    int
		want string
		ok   bool
	}{
		{1, "10", true},
		{18, "1000000000000000000", true},
		{38, tenPow38, true},
		{39, tenPow38, true},
		{50, tenPow38, true},
		{0, "", false},
		{-1, "", false},
	} {
		got, ok := batch.DecimalPrecisionLimit(tc.p)
		if ok != tc.ok {
			t.Errorf("batch.DecimalPrecisionLimit(%d) ok = %v, want %v", tc.p, ok, tc.ok)
			continue
		}
		if ok && got.String() != tc.want {
			t.Errorf("batch.DecimalPrecisionLimit(%d) = %s, want %s", tc.p, got.String(), tc.want)
		}
	}
}
