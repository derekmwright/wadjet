package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestDecomposeCovar_AllThreeSpellings pins the rewrite every distributed
// CORR/COVAR query depends on (#353). As with the variance family, the kind
// rides the synthetic column NAME: one (count, meanX, meanY, C, M2x, M2y)
// sextuple serves all three functions, and only the final fold has to know
// which was asked for.
func TestDecomposeCovar_AllThreeSpellings(t *testing.T) {
	for _, tc := range []struct {
		fn       string
		wantKind string
	}{
		{"corr", "corr"},
		{"CORR", "corr"},
		{"covar_samp", "covar_samp"},
		{" COVAR_POP ", "covar_pop"},
	} {
		got := decomposeCovar([]distributed.AggSpec{{
			Func: tc.fn, InputCol: "x", InputCol2: "y", OutputCol: "c",
			OutputType: int(parquet.TypeFloat64),
		}})
		if len(got) != 1 {
			t.Fatalf("%s: produced %d specs, want 1: %+v", tc.fn, len(got), got)
		}
		if got[0].Func != covarStateFunc {
			t.Errorf("%s: func %q, want %q", tc.fn, got[0].Func, covarStateFunc)
		}
		if want := covarStatePrefix + tc.wantKind + "#c"; got[0].OutputCol != want {
			t.Errorf("%s: output column %q, want %q", tc.fn, got[0].OutputCol, want)
		}
		// The partial ships an encoded state, not a number.
		if got[0].OutputType != int(parquet.TypeString) {
			t.Errorf("%s: output type %v, want %v", tc.fn, parquet.TypeID(got[0].OutputType), parquet.TypeString)
		}
		// BOTH input columns have to survive: the state needs x and y per
		// row, and a dropped second column is a NULL answer, not an error.
		if got[0].InputCol != "x" || got[0].InputCol2 != "y" {
			t.Errorf("%s: inputs (%q, %q), want (\"x\", \"y\")", tc.fn, got[0].InputCol, got[0].InputCol2)
		}
	}
}

// TestDecomposeCovar_PassesThroughOthers: a spec list with no covariance
// aggregate comes back as the SAME slice — the rewrite must not allocate,
// reorder, or touch the other families.
func TestDecomposeCovar_PassesThroughOthers(t *testing.T) {
	in := []distributed.AggSpec{
		{Func: "sum", InputCol: "a", OutputCol: "s"},
		{Func: "stddev", InputCol: "b", OutputCol: "sd"},
		{Func: "median", InputCol: "c", OutputCol: "m"},
	}
	got := decomposeCovar(in)
	if &got[0] != &in[0] {
		t.Error("a list with no covariance aggregate should be returned unchanged")
	}

	// Mixed: only the covariance spec is rewritten, and its position holds.
	mixed := []distributed.AggSpec{
		{Func: "sum", InputCol: "a", OutputCol: "s"},
		{Func: "corr", InputCol: "x", InputCol2: "y", OutputCol: "c"},
		{Func: "count", OutputCol: "n"},
	}
	out := decomposeCovar(mixed)
	if len(out) != 3 {
		t.Fatalf("%d specs, want 3", len(out))
	}
	if out[0].Func != "sum" || out[2].Func != "count" {
		t.Errorf("neighbours changed: %+v", out)
	}
	if out[1].Func != covarStateFunc {
		t.Errorf("spec 1 func %q, want %q", out[1].Func, covarStateFunc)
	}
}

// TestWireAggSpecs_CarriesEveryArgument: the wire conversion is the one
// place all four dispatch paths share, and an argument it drops is an
// aggregate that behaves differently depending on which shape the query
// took. MIN_BY's ordering column, STRING_AGG's separator and
// PERCENTILE_CONT's fraction all reached the worker through nothing before
// #353.
func TestWireAggSpecs_CarriesEveryArgument(t *testing.T) {
	in := []physical.AggSpec{
		{Func: "min_by", InputCol: "label", InputCol2: "k", OutputCol: "mn", OutputType: parquet.TypeString},
		{Func: "string_agg", InputCol: "p", Separator: "::", OutputCol: "s"},
		{Func: "percentile_cont", InputCol: "v", Percentile: 0.9, OutputCol: "p90"},
	}
	got := wireAggSpecs(in)
	if len(got) != len(in) {
		t.Fatalf("%d specs, want %d", len(got), len(in))
	}
	if got[0].InputCol2 != "k" {
		t.Errorf("min_by ordering column %q, want \"k\"", got[0].InputCol2)
	}
	if got[0].OutputType != int(parquet.TypeString) {
		t.Errorf("min_by output type %v, want %v", parquet.TypeID(got[0].OutputType), parquet.TypeString)
	}
	if got[1].Separator != "::" {
		t.Errorf("string_agg separator %q, want \"::\"", got[1].Separator)
	}
	if got[2].Percentile != 0.9 {
		t.Errorf("percentile fraction %v, want 0.9", got[2].Percentile)
	}
}
