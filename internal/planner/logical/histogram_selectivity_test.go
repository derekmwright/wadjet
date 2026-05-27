package logical

import (
	"testing"
)

// fakeHistogram is a minimal HistogramStats implementation for testing.
type fakeHistogram struct {
	le map[int64]float64 // pre-canned SelectivityLE values
	eq map[int64]float64
}

func (f *fakeHistogram) SelectivityLE(v any) float64 {
	k, ok := v.(int64)
	if !ok {
		return 0
	}
	return f.le[k]
}
func (f *fakeHistogram) SelectivityLT(v any) float64 { return f.SelectivityLE(v) }
func (f *fakeHistogram) SelectivityRange(lo, hi any) float64 {
	return f.SelectivityLE(hi) - f.SelectivityLE(lo)
}
func (f *fakeHistogram) SelectivityEQ(v any) float64 {
	k, ok := v.(int64)
	if !ok {
		return 0
	}
	return f.eq[k]
}

func TestHistogramSelectivityEngages(t *testing.T) {
	colHist := map[string]any{
		"x": &fakeHistogram{
			le: map[int64]float64{100: 0.286, 50: 0.143},
			eq: map[int64]float64{42: 0.001},
		},
	}
	cases := []struct {
		name string
		pred Predicate
		want float64
	}{
		{"<= 100 (histogram says 0.286)", Predicate{Column: "x", Op: "<=", Value: int64(100)}, 0.286},
		{"> 100 (1 - 0.286)", Predicate{Column: "x", Op: ">", Value: int64(100)}, 0.714},
		{"= 42 (histogram says 0.001)", Predicate{Column: "x", Op: "=", Value: int64(42)}, 0.001},
		{"unknown col falls back to heuristic", Predicate{Column: "y", Op: "<=", Value: int64(100)}, 0.33},
		{"no histogram for non-matching col", Predicate{Column: "z", Op: "=", Value: int64(7)}, 0.1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := estimatePredSelectivityWithHist([]Predicate{c.pred}, colHist)
			if abs64(got-c.want) > 0.01 {
				t.Errorf("got %.4f want %.4f", got, c.want)
			}
		})
	}
}

func abs64(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
