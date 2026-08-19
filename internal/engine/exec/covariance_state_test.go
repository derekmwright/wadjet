// Regression tests for #353: CORR, COVAR_SAMP and COVAR_POP had no way to
// cross a partial/final aggregate split. On the stage DAG the worker had no
// case for their function names at all, so they fell to its `default:
// AggSum` and answered with the sum of their first argument.
//
// The fix is the one #339 built for the variance family: the partial ships
// its (count, meanX, meanY, C, M2x, M2y) sextuple, merge stages combine
// them pairwise, and the final stage finishes. These tests hold that round
// trip against a reference computed here, over the same values.
package exec

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// covarRef is the independent reference: one sequential pass, no partials.
func covarRef(xs, ys []float64) (corr, samp, pop float64) {
	var n int64
	var meanX, meanY, c, m2x, m2y float64
	for i := range xs {
		n++
		fn := float64(n)
		dx := xs[i] - meanX
		meanX += dx / fn
		dy := ys[i] - meanY
		meanY += dy / fn
		c += dx * (ys[i] - meanY)
		m2x += dx * (xs[i] - meanX)
		m2y += dy * (ys[i] - meanY)
	}
	if n == 0 {
		return 0, 0, 0
	}
	pop = c / float64(n)
	if n >= 2 {
		samp = c / float64(n-1)
	}
	if m2x != 0 && m2y != 0 {
		corr = c / math.Sqrt(m2x*m2y)
	}
	return corr, samp, pop
}

// covarPair builds two correlated price-shaped columns: a mean far above
// the spread, which is where a naive co-moment loses its digits.
func covarPair(n int) (xs, ys []float64) {
	xs = priceLike(n)
	ys = make([]float64, n)
	for i, x := range xs {
		// A deterministic function of x with its own offset, so the
		// correlation is strong but not 1 and the two means differ by an
		// order of magnitude.
		ys[i] = 3000 + x/7 + math.Mod(float64(i)*37, 900)
	}
	return xs, ys
}

// TestCovarianceStateEncodeRoundTrip: the wire encoding is exact. A partial
// state that changes value in transit is a wrong answer with no symptom.
func TestCovarianceStateEncodeRoundTrip(t *testing.T) {
	xs, ys := covarPair(500)
	var st covarianceState
	for i := range xs {
		st.update(xs[i], ys[i])
	}
	enc := st.encode()
	if len(enc) != covarianceStateWidth {
		t.Fatalf("encoded width %d, want %d", len(enc), covarianceStateWidth)
	}
	back, ok := decodeCovarianceState(enc)
	if !ok {
		t.Fatalf("decode rejected %q", enc)
	}
	if back != st {
		t.Errorf("round trip changed the state:\n got %+v\nwant %+v", back, st)
	}

	// Anything that is not this encoding must be rejected rather than read
	// as a valid empty partial — a state column that lost its encoding is
	// not "no rows".
	for _, bad := range []string{"", "nope", enc[:len(enc)-2], enc + "00", "zz" + enc[2:]} {
		if _, ok := decodeCovarianceState(bad); ok {
			t.Errorf("decode accepted %q", bad)
		}
	}
}

// TestCovarianceStateMergeUnequalSlices: partials over disjoint, unequal
// slices must combine to what one sequential pass gives. Unequal on
// purpose — dropping the between-partial term is exact only when the
// partial means coincide, which equal splits of uniform data very nearly
// arrange.
func TestCovarianceStateMergeUnequalSlices(t *testing.T) {
	xs, ys := covarPair(9000)
	wantCorr, wantSamp, wantPop := covarRef(xs, ys)

	bounds := []int{0, 137, 900, 5000, 9000}
	var merged covarianceState
	for i := 0; i < len(bounds)-1; i++ {
		var part covarianceState
		for j := bounds[i]; j < bounds[i+1]; j++ {
			part.update(xs[j], ys[j])
		}
		merged.merge(&part)
	}
	if merged.count != int64(len(xs)) {
		t.Fatalf("merged count %d, want %d", merged.count, len(xs))
	}
	for _, tc := range []struct {
		name      string
		got, want float64
	}{
		{"CORR", merged.correlation(), wantCorr},
		{"COVAR_SAMP", merged.covarSamp(), wantSamp},
		{"COVAR_POP", merged.covarPop(), wantPop},
	} {
		if rel := math.Abs(tc.got-tc.want) / math.Abs(tc.want); rel > 1e-9 {
			t.Errorf("%s after merge = %.17g, want %.17g (rel %g)", tc.name, tc.got, tc.want, rel)
		}
	}
}

// TestHashAggregateCovarStateRoundTrip is the distributed shape end to end
// inside one process: three partial aggregates emit COVAR_STATE, a merge
// aggregate combines the encoded states with COVAR_STATE_MERGE, and
// FinalizeCovarianceState finishes all three kinds off the one state.
func TestHashAggregateCovarStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "x", Type: parquet.TypeFloat64},
		{Name: "y", Type: parquet.TypeFloat64},
	}
	xs, ys := covarPair(9000)
	wantCorr, wantSamp, wantPop := covarRef(xs, ys)

	bounds := []int{0, 500, 4000, 9000}
	partialRows := make([]map[string]any, 0, len(bounds))
	for i := 0; i < len(bounds)-1; i++ {
		agg := NewHashAggregate([]string{"g"}, []AggColumn{
			{Func: AggCovarState, InputCol: "x", InputCol2: "y", OutputCol: "st", OutputType: parquet.TypeString},
		})
		if err := agg.Init(ctx); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, 0, bounds[i+1]-bounds[i])
		for j := bounds[i]; j < bounds[i+1]; j++ {
			rows = append(rows, map[string]any{"g": "all", "x": xs[j], "y": ys[j]})
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

	mergeSchema := []parquet.Column{
		{Name: "g", Type: parquet.TypeString},
		{Name: "st", Type: parquet.TypeString},
	}
	merger := NewHashAggregate([]string{"g"}, []AggColumn{
		{Func: AggCovarStateMerge, InputCol: "st", OutputCol: "st", OutputType: parquet.TypeString},
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
	enc, ok := merged[0]["st"].(string)
	if !ok {
		t.Fatalf("merge emitted st = %v (%T), want the encoded state string", merged[0]["st"], merged[0]["st"])
	}

	for _, tc := range []struct {
		kind string
		want float64
	}{
		{CovarKindCorr, wantCorr},
		{CovarKindCovarSamp, wantSamp},
		{CovarKindCovarPop, wantPop},
	} {
		got, ok := FinalizeCovarianceState(enc, tc.kind)
		if !ok {
			t.Fatalf("FinalizeCovarianceState rejected the merged state for %s", tc.kind)
		}
		if rel := math.Abs(got-tc.want) / math.Abs(tc.want); rel > 1e-9 {
			t.Errorf("%s via COVAR_STATE round trip = %.17g, want %.17g (rel %g)", tc.kind, got, tc.want, rel)
		}
	}
}

// TestFinalizeCovarianceStateNulls: the thresholds that make a covariance
// NULL rather than zero, applied on the distributed path's fold.
func TestFinalizeCovarianceStateNulls(t *testing.T) {
	empty := (&covarianceState{}).encode()
	one := &covarianceState{}
	one.update(7, 11)

	for _, tc := range []struct {
		name    string
		encoded string
		kind    string
		wantOK  bool
	}{
		{"empty/corr", empty, CovarKindCorr, false},
		{"empty/samp", empty, CovarKindCovarSamp, false},
		{"empty/pop", empty, CovarKindCovarPop, false},
		{"single row/corr", one.encode(), CovarKindCorr, false},
		{"single row/samp", one.encode(), CovarKindCovarSamp, false},
		{"single row/pop", one.encode(), CovarKindCovarPop, true},
		{"corrupt", "nope", CovarKindCorr, false},
		{"unknown kind", one.encode(), "stddev_samp", false},
	} {
		if _, ok := FinalizeCovarianceState(tc.encoded, tc.kind); ok != tc.wantOK {
			t.Errorf("%s: ok=%v, want %v", tc.name, ok, tc.wantOK)
		}
	}
	// A one-row population covariance is 0, not NULL.
	if v, _ := FinalizeCovarianceState(one.encode(), CovarKindCovarPop); v != 0 {
		t.Errorf("single-row COVAR_POP = %v, want 0", v)
	}
}

// TestMinByMaxByFollowInputType: MIN_BY/MAX_BY return a value taken from
// their FIRST argument, so the output column has to be that column's type.
// Declared float64, the string they selected was dropped and every row came
// back 0 — the same defect #345 fixed for the window value functions.
func TestMinByMaxByFollowInputType(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "label", Type: parquet.TypeString},
		{Name: "k", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"label": "cheap", "k": 1.0},
		{"label": "mid", "k": 5.0},
		{"label": "dear", "k": 9.0},
	}
	agg := NewHashAggregate(nil, []AggColumn{
		// OutputType float64 on purpose: this is what the planner declared
		// before the fix, and the operator has to correct it from the
		// vector it observes.
		{Func: AggMinBy, InputCol: "label", InputCol2: "k", OutputCol: "mn", OutputType: parquet.TypeFloat64},
		{Func: AggMaxBy, InputCol: "label", InputCol2: "k", OutputCol: "mx", OutputType: parquet.TypeFloat64},
	})
	if err := agg.Init(ctx); err != nil {
		t.Fatal(err)
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
		t.Fatalf("%d rows, want 1", len(got))
	}
	if got[0]["mn"] != "cheap" {
		t.Errorf("MIN_BY = %v (%T), want \"cheap\"", got[0]["mn"], got[0]["mn"])
	}
	if got[0]["mx"] != "dear" {
		t.Errorf("MAX_BY = %v (%T), want \"dear\"", got[0]["mx"], got[0]["mx"])
	}
}

// TestMinByOverDateOrdering: a DATE ordering column is days-since-epoch in
// Int32Data and orders like the integer it is. Its absence from the float64
// extractor table made every row skip, so the answer was NULL.
func TestMinByOverDateOrdering(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "label", Type: parquet.TypeString},
		{Name: "d", Type: parquet.TypeDate},
	}
	rows := []map[string]any{
		{"label": "later", "d": int32(19000)},
		{"label": "earlier", "d": int32(9000)},
		{"label": "latest", "d": int32(20000)},
	}
	agg := NewHashAggregate(nil, []AggColumn{
		{Func: AggMinBy, InputCol: "label", InputCol2: "d", OutputCol: "mn"},
		{Func: AggMaxBy, InputCol: "label", InputCol2: "d", OutputCol: "mx"},
	})
	if err := agg.Init(ctx); err != nil {
		t.Fatal(err)
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
		t.Fatalf("%d rows, want 1", len(got))
	}
	if got[0]["mn"] != "earlier" {
		t.Errorf("MIN_BY over a DATE ordering column = %v, want \"earlier\"", got[0]["mn"])
	}
	if got[0]["mx"] != "latest" {
		t.Errorf("MAX_BY over a DATE ordering column = %v, want \"latest\"", got[0]["mx"])
	}
}
