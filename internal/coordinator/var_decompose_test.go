package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestDecomposeVar_AllFourSpellings pins the rewrite every distributed
// variance query depends on (#339). The kind is carried in the synthetic
// column NAME because the partial state is identical for all four functions
// — one (count, mean, M2) triple — and only the final fold has to know
// which of them the query asked for.
//
// The sample/population mapping is the settled semantics: bare STDDEV and
// VARIANCE are the SAMPLE forms, matching DuckDB and PostgreSQL.
func TestDecomposeVar_AllFourSpellings(t *testing.T) {
	for _, tc := range []struct {
		fn       string
		wantKind string
	}{
		{"stddev", "stddev_samp"},
		{"STDDEV_SAMP", "stddev_samp"},
		{"variance", "var_samp"},
		{"var_samp", "var_samp"},
		{"stddev_pop", "stddev_pop"},
		{" VAR_POP ", "var_pop"},
	} {
		got := decomposeVar([]distributed.AggSpec{{
			Func: tc.fn, InputCol: "v", OutputCol: "s", OutputType: int(parquet.TypeFloat64),
		}})
		if len(got) != 1 {
			t.Fatalf("%s: produced %d specs, want 1: %+v", tc.fn, len(got), got)
		}
		if got[0].Func != varStateFunc {
			t.Errorf("%s: func %q, want %q", tc.fn, got[0].Func, varStateFunc)
		}
		if want := varStatePrefix + tc.wantKind + "#s"; got[0].OutputCol != want {
			t.Errorf("%s: output column %q, want %q", tc.fn, got[0].OutputCol, want)
		}
		// The partial ships an encoded state, not a number: a float64
		// declaration here would hand the merge stage the wrong vector.
		if got[0].OutputType != int(parquet.TypeString) {
			t.Errorf("%s: output type %v, want %v", tc.fn, parquet.TypeID(got[0].OutputType), parquet.TypeString)
		}
		if got[0].InputCol != "v" {
			t.Errorf("%s: input column %q, want the original %q", tc.fn, got[0].InputCol, "v")
		}
	}
}

// TestDecomposeVar_PassesThroughOtherAggregates: anything that is not a
// variance-family aggregate must survive untouched, and a spec list with
// none of them must not be reallocated at all.
func TestDecomposeVar_PassesThroughOtherAggregates(t *testing.T) {
	in := []distributed.AggSpec{
		{Func: "sum", InputCol: "a", OutputCol: "s"},
		{Func: "count", OutputCol: "c"},
		{Func: "corr", InputCol: "a", OutputCol: "r"},
	}
	got := decomposeVar(in)
	if len(got) != len(in) {
		t.Fatalf("produced %d specs, want %d", len(got), len(in))
	}
	for i := range in {
		if got[i] != in[i] {
			t.Errorf("spec %d rewritten: %+v, want %+v", i, got[i], in[i])
		}
	}

	mixed := decomposeVar([]distributed.AggSpec{
		{Func: "sum", InputCol: "a", OutputCol: "s"},
		{Func: "stddev", InputCol: "b", OutputCol: "d"},
	})
	if len(mixed) != 2 {
		t.Fatalf("mixed list produced %d specs, want 2", len(mixed))
	}
	if mixed[0].Func != "sum" || mixed[0].OutputCol != "s" {
		t.Errorf("the SUM beside a STDDEV was rewritten: %+v", mixed[0])
	}
}

func TestDecomposeVarPhysical(t *testing.T) {
	got := decomposeVarPhysical([]physical.AggSpec{
		{Func: "stddev_pop", InputCol: "v", OutputCol: "s", OutputType: parquet.TypeFloat64},
	})
	if len(got) != 1 {
		t.Fatalf("produced %d specs, want 1: %+v", len(got), got)
	}
	if got[0].Func != varStateFunc || got[0].OutputType != parquet.TypeString {
		t.Errorf("got func %q type %v, want %q / %v",
			got[0].Func, got[0].OutputType, varStateFunc, parquet.TypeString)
	}
	if want := varStatePrefix + "stddev_pop#s"; got[0].OutputCol != want {
		t.Errorf("output column %q, want %q", got[0].OutputCol, want)
	}
}
