// Regression tests for #339: STDDEV/VARIANCE answered from part of their
// input, because a partial aggregate's (count, mean, M2) state was dropped
// at every merge rather than combined.
//
// Nothing here compares against a hand-copied constant. Each case either
// recomputes Welford over the same values inside the test, or uses input
// whose variance is exact by construction — a reference that stays honest
// if the fixture or the batch size ever changes.
package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// welfordRef is the independent reference: one sequential pass over the
// values, no partials, no merges.
func welfordRef(vals []float64) (n int64, mean, m2 float64) {
	for _, x := range vals {
		n++
		d := x - mean
		mean += d / float64(n)
		m2 += d * (x - mean)
	}
	return n, mean, m2
}

func refStddevSamp(vals []float64) float64 {
	n, _, m2 := welfordRef(vals)
	if n < 2 {
		return math.NaN()
	}
	return math.Sqrt(m2 / float64(n-1))
}

// priceLike builds n values shaped like o_totalprice: a mean two orders of
// magnitude above the spread, which is the case that separates a stable
// accumulator from a sum-of-squares one. Deterministic, no RNG seed to
// drift.
func priceLike(n int) []float64 {
	out := make([]float64, n)
	x := 12345.0
	for i := range out {
		// LCG in float, kept exact (all values < 2^53).
		x = math.Mod(x*1103515245+12345, 2147483648)
		out[i] = 250000 + (x/2147483648)*250000
	}
	return out
}

// TestVarianceStateMergePairwise: partials over disjoint, deliberately
// UNEQUAL slices must combine to the same answer one sequential pass gives.
// Unequal on purpose — summing M2 (the naive merge) is exact only when the
// partial means coincide, and equal splits of uniform data very nearly make
// them coincide.
func TestVarianceStateMergePairwise(t *testing.T) {
	vals := priceLike(50000) // ~24 batches at 2048
	wantN, wantMean, wantM2 := welfordRef(vals)

	for _, split := range [][]int{
		{50000},
		{2048, 47952},
		{1, 49999},
		{7, 13, 2048, 4096, 43836},
		{10000, 10000, 10000, 10000, 10000},
	} {
		var merged varianceState
		off := 0
		for _, size := range split {
			var part varianceState
			for _, x := range vals[off : off+size] {
				part.update(x)
			}
			off += size
			merged.merge(&part)
		}
		if off != len(vals) {
			t.Fatalf("split covers %d of %d values", off, len(vals))
		}
		if merged.count != wantN {
			t.Errorf("split %v: count %d, want %d", split, merged.count, wantN)
		}
		if rel := math.Abs(merged.mean-wantMean) / math.Abs(wantMean); rel > 1e-12 {
			t.Errorf("split %v: mean %.17g, want %.17g (rel %g)", split, merged.mean, wantMean, rel)
		}
		if rel := math.Abs(merged.m2-wantM2) / wantM2; rel > 1e-12 {
			t.Errorf("split %v: M2 %.17g, want %.17g (rel %g)\n"+
				"  a merge that sums M2 without the between-partial term lands here",
				split, merged.m2, wantM2, rel)
		}
	}
}

// TestVarianceStateMergeExactVariance pins the merge against an EXACT
// answer rather than another floating-point pass: values alternate M-1 and
// M+1, so the population variance is exactly 1 and the sample variance
// exactly n/(n-1), whatever M is. With M = 1e9 the sum-of-squares form
// (E[x²] − E[x]²) subtracts two numbers that agree to 18 digits and returns
// noise — this is the input that discriminates the two implementations.
func TestVarianceStateMergeExactVariance(t *testing.T) {
	const m = 1e9
	const n = 40000
	vals := make([]float64, n)
	for i := range vals {
		if i%2 == 0 {
			vals[i] = m - 1
		} else {
			vals[i] = m + 1
		}
	}

	var merged varianceState
	for i := 0; i < n; i += 3000 {
		end := i + 3000
		if end > n {
			end = n
		}
		var part varianceState
		for _, x := range vals[i:end] {
			part.update(x)
		}
		merged.merge(&part)
	}

	if got := merged.variancePop(); math.Abs(got-1) > 1e-9 {
		t.Errorf("population variance %.17g, want exactly 1", got)
	}
	wantSamp := float64(n) / float64(n-1)
	if got := merged.varianceSamp(); math.Abs(got-wantSamp) > 1e-9 {
		t.Errorf("sample variance %.17g, want %.17g", got, wantSamp)
	}

	// The control: what a sum-of-squares accumulator returns on the same
	// input. It is not required to be wrong for the test to pass, but if it
	// ever stops being wrong the input has lost its discriminating power
	// and this test is no longer proving what it claims.
	var s, sq float64
	for _, x := range vals {
		s += x
		sq += x * x
	}
	naive := sq/n - (s/n)*(s/n)
	if math.Abs(naive-1) < 1e-6 {
		t.Errorf("the sum-of-squares control returned %.17g — accurate, so this input no longer "+
			"discriminates a stable accumulator from a naive one; raise the magnitude", naive)
	}
}

// TestCovarianceStateMergePairwise holds the covariance/correlation state to
// the same standard: partials over unequal slices must reproduce what one
// sequential pass gives. CORR/COVAR share the extraState slot the variance
// family rides, so they shared its dropped merge.
func TestCovarianceStateMergePairwise(t *testing.T) {
	xs := priceLike(20000)
	ys := make([]float64, len(xs))
	for i, x := range xs {
		// Correlated but not collinear, so c, m2x and m2y are all distinct
		// and a merge that mishandles any one of them shows up.
		ys[i] = 3*x + float64((i*7919)%1000)
	}

	var ref covarianceState
	for i := range xs {
		ref.update(xs[i], ys[i])
	}

	var merged covarianceState
	for _, b := range [][2]int{{0, 137}, {137, 5000}, {5000, 5001}, {5001, 20000}} {
		var part covarianceState
		for i := b[0]; i < b[1]; i++ {
			part.update(xs[i], ys[i])
		}
		merged.merge(&part)
	}

	if merged.count != ref.count {
		t.Fatalf("count %d, want %d", merged.count, ref.count)
	}
	for _, f := range []struct {
		name      string
		got, want float64
	}{
		{"covar_samp", merged.covarSamp(), ref.covarSamp()},
		{"covar_pop", merged.covarPop(), ref.covarPop()},
		{"corr", merged.correlation(), ref.correlation()},
		{"meanX", merged.meanX, ref.meanX},
		{"meanY", merged.meanY, ref.meanY},
	} {
		if rel := math.Abs(f.got-f.want) / math.Abs(f.want); rel > 1e-12 {
			t.Errorf("%s = %.17g, want %.17g (rel %g)", f.name, f.got, f.want, rel)
		}
	}
}

func TestVarianceStateEncodeRoundTrip(t *testing.T) {
	cases := []varianceState{
		{},
		{count: 1, mean: 42, m2: 0},
		{count: 15000, mean: 250152.7316420002, m2: 3.11e14},
		{count: -1, mean: math.Inf(-1), m2: math.MaxFloat64},
	}
	for _, want := range cases {
		enc := want.encode()
		if len(enc) != varianceStateWidth {
			t.Errorf("encode(%+v) is %d chars, want %d", want, len(enc), varianceStateWidth)
		}
		got, ok := decodeVarianceState(enc)
		if !ok {
			t.Fatalf("decode(%q) failed", enc)
		}
		if got != want {
			t.Errorf("round trip: got %+v, want %+v", got, want)
		}
	}
	for _, bad := range []string{"", "zz", "not-hex-not-hex-not-hex-not-hex-not-hex-not-hex!"} {
		if _, ok := decodeVarianceState(bad); ok {
			t.Errorf("decode(%q) reported ok; a corrupt state must not read as an empty partial", bad)
		}
	}
}

// TestHashAggregateMergeSinkVariance is the defect itself, at the level it
// bit: two morsel-parallel clones over disjoint halves of one group, merged
// into a parent that never consumed a row. Before the fix the parent adopted
// the first clone's state wholesale and dropped every later one, so STDDEV
// over 15000 rows was answered from the rows one clone happened to see —
// a number wrong in the fourth digit that no row count could catch.
func TestHashAggregateMergeSinkVariance(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	vals := priceLike(6000)
	want := refStddevSamp(vals)

	parent := NewHashAggregate([]string{"g"}, []AggColumn{
		{Func: AggStddev, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeFloat64},
	})
	if err := parent.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Three clones over unequal slices, all writing the SAME group.
	bounds := []int{0, 1000, 3500, 6000}
	for i := 0; i < len(bounds)-1; i++ {
		w := parent.CloneSink().(*HashAggregate)
		if err := w.Init(ctx); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, 0, bounds[i+1]-bounds[i])
		for _, x := range vals[bounds[i]:bounds[i+1]] {
			rows = append(rows, map[string]any{"g": "all", "v": x})
		}
		if err := w.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
		parent.MergeSink(w)
	}

	out, err := parent.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("no output batch")
	}
	got := out.ToRows()
	if len(got) != 1 {
		t.Fatalf("%d rows, want 1", len(got))
	}
	s, ok := got[0]["s"].(float64)
	if !ok {
		t.Fatalf("s = %v (%T), want float64", got[0]["s"], got[0]["s"])
	}
	if rel := math.Abs(s-want) / want; rel > 1e-9 {
		t.Errorf("STDDEV over three merged partials = %.17g, want %.17g (rel %g)\n"+
			"  the reference is a single Welford pass over the identical 6000 values;\n"+
			"  a partial state dropped at merge lands within a few tenths of a percent of the truth",
			s, want, rel)
	}
}

// TestHashAggregateVarStateRoundTrip runs the distributed shape in one
// process: partial aggregates emit VAR_STATE columns, a merge aggregate
// combines them with VAR_STATE_MERGE, and the finished value must equal a
// sequential Welford pass. This is the path a coordinator builds across
// workers (coordinator/var_decompose.go → worker/var_fold.go); running it
// here pins the exec half without a cluster.
func TestHashAggregateVarStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	vals := priceLike(9000)
	want := refStddevSamp(vals)

	// Three partial tasks over unequal slices.
	bounds := []int{0, 500, 4000, 9000}
	partialRows := make([]map[string]any, 0, len(bounds))
	for i := 0; i < len(bounds)-1; i++ {
		agg := NewHashAggregate([]string{"g"}, []AggColumn{
			{Func: AggVarState, InputCol: "v", OutputCol: "st", OutputType: parquet.TypeString},
		})
		if err := agg.Init(ctx); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, 0, bounds[i+1]-bounds[i])
		for _, x := range vals[bounds[i]:bounds[i+1]] {
			rows = append(rows, map[string]any{"g": "all", "v": x})
		}
		if err := agg.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
		out, err := agg.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := out.ToRows()
		if len(got) != 1 {
			t.Fatalf("partial %d emitted %d rows, want 1", i, len(got))
		}
		if _, ok := got[0]["st"].(string); !ok {
			t.Fatalf("partial %d emitted st = %v (%T), want the encoded state string", i, got[0]["st"], got[0]["st"])
		}
		partialRows = append(partialRows, got[0])
	}

	// The merge stage: input is the partials' output schema.
	mergeSchema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "st", Type: parquet.TypeString},
	}
	merger := NewHashAggregate([]string{"g"}, []AggColumn{
		{Func: AggVarStateMerge, InputCol: "st", OutputCol: "st", OutputType: parquet.TypeString},
	})
	if err := merger.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := merger.Consume(ctx, batch.FromRows(mergeSchema, partialRows)); err != nil {
		t.Fatal(err)
	}
	out, err := merger.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	merged := out.ToRows()
	if len(merged) != 1 {
		t.Fatalf("merge emitted %d rows, want 1", len(merged))
	}

	got, ok := FinalizeVarianceState(merged[0]["st"].(string), VarKindStddevSamp)
	if !ok {
		t.Fatalf("FinalizeVarianceState rejected the merged state %q", merged[0]["st"])
	}
	if rel := math.Abs(got-want) / want; rel > 1e-9 {
		t.Errorf("STDDEV via VAR_STATE round trip = %.17g, want %.17g (rel %g)", got, want, rel)
	}

	// The population form comes off the same state — one partial shape
	// serves all four spellings.
	n, _, m2 := welfordRef(vals)
	wantPop := math.Sqrt(m2 / float64(n))
	gotPop, ok := FinalizeVarianceState(merged[0]["st"].(string), VarKindStddevPop)
	if !ok {
		t.Fatal("FinalizeVarianceState rejected the population kind")
	}
	if rel := math.Abs(gotPop-wantPop) / wantPop; rel > 1e-9 {
		t.Errorf("STDDEV_POP via VAR_STATE round trip = %.17g, want %.17g", gotPop, wantPop)
	}
}

// TestFinalizeVarianceStateNulls: the thresholds that make a variance NULL
// rather than zero, applied on the distributed path's fold.
func TestFinalizeVarianceStateNulls(t *testing.T) {
	empty := (&varianceState{}).encode()
	one := &varianceState{}
	one.update(7)

	for _, tc := range []struct {
		name    string
		encoded string
		kind    string
		wantOK  bool
	}{
		{"empty/samp", empty, VarKindStddevSamp, false},
		{"empty/pop", empty, VarKindVarPop, false},
		{"single row/samp", one.encode(), VarKindVarSamp, false},
		{"single row/pop", one.encode(), VarKindStddevPop, true},
		{"corrupt", "nope", VarKindStddevSamp, false},
		{"unknown kind", one.encode(), "median", false},
	} {
		if _, ok := FinalizeVarianceState(tc.encoded, tc.kind); ok != tc.wantOK {
			t.Errorf("%s: ok=%v, want %v", tc.name, ok, tc.wantOK)
		}
	}
	// A one-row population standard deviation is 0, not NULL.
	if v, _ := FinalizeVarianceState(one.encode(), VarKindStddevPop); v != 0 {
		t.Errorf("single-row STDDEV_POP = %v, want 0", v)
	}
}

// TestHashAggregateMergeSinkExtraStateFamily: every aggregate that keeps its
// state in extraState rode the same dropped merge. One group, split across
// two clones, one assertion per family.
func TestHashAggregateMergeSinkExtraStateFamily(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeFloat64},
		{Name: "w", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeBool},
	}
	left := []map[string]any{
		{"g": "a", "v": 1.0, "w": 2.0, "b": true},
		{"g": "a", "v": 2.0, "w": 4.0, "b": true},
	}
	right := []map[string]any{
		{"g": "a", "v": 3.0, "w": 6.0, "b": false},
		{"g": "a", "v": 4.0, "w": 8.0, "b": false},
	}

	parent := NewHashAggregate([]string{"g"}, []AggColumn{
		{Func: AggVariance, InputCol: "v", OutputCol: "var", OutputType: parquet.TypeFloat64},
		{Func: AggCorr, InputCol: "v", InputCol2: "w", OutputCol: "corr", OutputType: parquet.TypeFloat64},
		{Func: AggMedian, InputCol: "v", OutputCol: "med", OutputType: parquet.TypeFloat64},
		{Func: AggMaxBy, InputCol: "v", InputCol2: "w", OutputCol: "mx", OutputType: parquet.TypeFloat64},
		{Func: AggBoolOr, InputCol: "b", OutputCol: "any", OutputType: parquet.TypeBool},
		{Func: AggBoolAnd, InputCol: "b", OutputCol: "all", OutputType: parquet.TypeBool},
	})
	if err := parent.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]map[string]any{left, right} {
		w := parent.CloneSink().(*HashAggregate)
		if err := w.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := w.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
		parent.MergeSink(w)
	}

	out, err := parent.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := out.ToRows()
	if len(got) != 1 {
		t.Fatalf("%d rows, want 1", len(got))
	}
	row := got[0]

	// VARIANCE over 1,2,3,4 (sample) = 5/3.
	if v := row["var"].(float64); math.Abs(v-5.0/3.0) > 1e-12 {
		t.Errorf("VARIANCE = %v, want %v (the four values, not one clone's two)", v, 5.0/3.0)
	}
	// w = 2v exactly, so the correlation over all four rows is 1.
	if v := row["corr"].(float64); math.Abs(v-1) > 1e-12 {
		t.Errorf("CORR = %v, want 1", v)
	}
	// MEDIAN of 1,2,3,4 = 2.5; of either clone alone it is 1.5 or 3.5.
	if v := row["med"].(float64); math.Abs(v-2.5) > 1e-12 {
		t.Errorf("MEDIAN = %v, want 2.5", v)
	}
	// MAX_BY(v, w): the largest w is 8, on the row where v = 4.
	if v := row["mx"].(float64); v != 4 {
		t.Errorf("MAX_BY = %v, want 4", v)
	}
	if v := row["any"].(bool); !v {
		t.Error("BOOL_OR = false, want true (the first clone's rows are true)")
	}
	if v := row["all"].(bool); v {
		t.Error("BOOL_AND = true, want false (the second clone's rows are false)")
	}
}
