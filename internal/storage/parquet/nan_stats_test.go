package parquet

import (
	"bytes"
	"math"
	"testing"
)

// #928 regression: the writer must never emit a NaN float min/max, and a
// column of only NaN (and nulls) must write NO float bound at all — the
// Parquet spec excludes NaN from min/max. On e17e2b92 updateStatsF32/F64
// seeded min=max=NaN when the first value was NaN and every later value
// failed `v < NaN`/`v > NaN`, so the bounds stuck at NaN and buildStats wrote
// those bits into Statistics.MinValue/MaxValue — poisoning row-group pruning
// (in this engine and in any other reader).
//
// FAILS on e17e2b92: the NaN-first and all-NaN cases decode a NaN bound.
// Passes after: NaN is skipped, and an all-NaN column leaves min/max absent.
func TestWriterNeverEmitsNaNFloatStats(t *testing.T) {
	f32 := func(xs []float64) []float32 {
		out := make([]float32, len(xs))
		for i, x := range xs {
			out[i] = float32(x)
		}
		return out
	}
	nan := math.NaN()

	cases := []struct {
		name       string
		vals       []float64
		wantMin    float64
		wantMax    float64
		wantAbsent bool // no float bound at all (all-NaN column)
	}{
		{"nan_first", []float64{nan, 1, 2}, 1, 2, false},
		{"nan_middle", []float64{1, nan, 3}, 1, 3, false},
		{"nan_last", []float64{4, 5, nan}, 4, 5, false},
		{"all_nan", []float64{nan, nan}, 0, 0, true},
		{"single_nan", []float64{nan}, 0, 0, true},
		{"no_nan", []float64{-2, 7, 3}, -2, 7, false},
	}

	for _, c := range cases {
		for _, phys := range []string{"f64", "f32"} {
			t.Run(c.name+"_"+phys, func(t *testing.T) {
				col := Column{Name: "x", Type: TypeFloat64}
				if phys == "f32" {
					col.Type = TypeFloat32
				}
				s := Schema{Columns: []Column{col}}
				var b bytes.Buffer
				w, err := NewWriter(&b, s, DefaultWriterConfig())
				if err != nil {
					t.Fatal(err)
				}
				rows := make([]map[string]any, len(c.vals))
				if phys == "f32" {
					fs := f32(c.vals)
					for i := range rows {
						rows[i] = map[string]any{"x": fs[i]}
					}
				} else {
					for i := range rows {
						rows[i] = map[string]any{"x": c.vals[i]}
					}
				}
				if err := w.WriteRows(rows); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				fr, err := OpenFileReaderFromBytes(b.Bytes())
				if err != nil {
					t.Fatal(err)
				}
				st := fr.RowGroupStats(0).Columns["x"]
				if c.wantAbsent {
					if st.MinValue != nil || st.MaxValue != nil {
						t.Fatalf("all-NaN column wrote a float bound: min=%#v max=%#v, want absent",
							st.MinValue, st.MaxValue)
					}
					return
				}
				mn, okMn := st.MinValue.(float64)
				mx, okMx := st.MaxValue.(float64)
				if !okMn || !okMx {
					t.Fatalf("min/max not decoded as float64: min=%#v max=%#v", st.MinValue, st.MaxValue)
				}
				if math.IsNaN(mn) || math.IsNaN(mx) {
					t.Fatalf("wrote NaN bound: min=%v max=%v", mn, mx)
				}
				if mn != c.wantMin || mx != c.wantMax {
					t.Fatalf("bounds = %v/%v, want %v/%v", mn, mx, c.wantMin, c.wantMax)
				}
			})
		}
	}
}

// The ±0.0 spec rule: min prefers -0.0, max prefers +0.0. A column carrying
// both signed zeros (and nothing else) must record min=-0.0, max=+0.0 so the
// bounds are the spec's canonical ones, not whichever zero arrived first.
func TestWriterSignedZeroFloatBounds(t *testing.T) {
	for _, phys := range []string{"f64", "f32"} {
		t.Run(phys, func(t *testing.T) {
			col := Column{Name: "x", Type: TypeFloat64}
			var vals []any
			if phys == "f32" {
				col.Type = TypeFloat32
				vals = []any{float32(math.Copysign(0, 1)), float32(math.Copysign(0, -1))}
			} else {
				vals = []any{math.Copysign(0, 1), math.Copysign(0, -1)}
			}
			s := Schema{Columns: []Column{col}}
			var b bytes.Buffer
			w, err := NewWriter(&b, s, DefaultWriterConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := w.WriteRows([]map[string]any{{"x": vals[0]}, {"x": vals[1]}}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			fr, err := OpenFileReaderFromBytes(b.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			st := fr.RowGroupStats(0).Columns["x"]
			mn := st.MinValue.(float64)
			mx := st.MaxValue.(float64)
			if !math.Signbit(mn) {
				t.Errorf("min = %v (signbit %v), want -0.0", mn, math.Signbit(mn))
			}
			if math.Signbit(mx) {
				t.Errorf("max = %v (signbit %v), want +0.0", mx, math.Signbit(mx))
			}
		})
	}
}
