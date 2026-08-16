package expr

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

func TestCosineSimilarity(t *testing.T) {
	tests := []struct {
		name string
		a, b []float32
		want float64
	}{
		{"identical", []float32{1, 0, 0}, []float32{1, 0, 0}, 1.0},
		{"orthogonal", []float32{1, 0, 0}, []float32{0, 1, 0}, 0.0},
		{"opposite", []float32{1, 0}, []float32{-1, 0}, -1.0},
		{"similar", []float32{1, 2, 3}, []float32{1, 2, 3}, 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fnCosineSimilarity([]any{tt.a, tt.b})
			if result == nil {
				t.Fatal("got nil")
			}
			got := result.(float64)
			if math.Abs(got-tt.want) > 1e-6 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestL2Distance(t *testing.T) {
	tests := []struct {
		name string
		a, b []float32
		want float64
	}{
		{"same", []float32{0, 0}, []float32{0, 0}, 0},
		{"unit", []float32{0, 0}, []float32{3, 4}, 5},
		{"3d", []float32{1, 2, 3}, []float32{4, 6, 3}, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fnL2Distance([]any{tt.a, tt.b})
			got := result.(float64)
			if math.Abs(got-tt.want) > 1e-6 {
				t.Errorf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestDotProduct(t *testing.T) {
	result := fnDotProduct([]any{
		[]float32{1, 2, 3},
		[]float32{4, 5, 6},
	})
	got := result.(float64)
	// 1*4 + 2*5 + 3*6 = 32
	if got != 32.0 {
		t.Errorf("got %f, want 32.0", got)
	}
}

func TestVectorNorm(t *testing.T) {
	result := fnVectorNorm([]any{[]float32{3, 4}})
	got := result.(float64)
	if math.Abs(got-5.0) > 1e-6 {
		t.Errorf("got %f, want 5.0", got)
	}
}

func TestVectorDims(t *testing.T) {
	result := fnVectorDims([]any{[]float32{1, 2, 3, 4}})
	got := result.(int64)
	if got != 4 {
		t.Errorf("got %d, want 4", got)
	}
}

func TestVectorFuncsNilHandling(t *testing.T) {
	for _, fn := range []ScalarFunc{fnCosineSimilarity, fnL2Distance, fnDotProduct} {
		if fn([]any{nil, nil}) != nil {
			t.Error("expected nil for nil inputs")
		}
		if fn([]any{[]float32{1}, nil}) != nil {
			t.Error("expected nil for one nil input")
		}
	}
	if fnVectorNorm([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
	if fnVectorDims([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestVectorFuncsMismatchedDims(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{1, 2}
	for _, fn := range []ScalarFunc{fnCosineSimilarity, fnL2Distance, fnDotProduct} {
		if fn([]any{a, b}) != nil {
			t.Error("expected nil for mismatched dimensions")
		}
	}
}

func TestVecCosineSimilarity(t *testing.T) {
	dim := 3
	n := 4
	a := batch.NewVectorVector(n, dim)
	b := batch.NewVectorVector(n, dim)
	out := batch.NewVector(batch.TypeFloat64, n)

	// Row 0: identical
	a.SetVector(0, []float32{1, 0, 0})
	b.SetVector(0, []float32{1, 0, 0})
	// Row 1: orthogonal
	a.SetVector(1, []float32{1, 0, 0})
	b.SetVector(1, []float32{0, 1, 0})
	// Row 2: null a
	a.Nulls.SetNull(2)
	b.SetVector(2, []float32{1, 1, 1})
	// Row 3: parallel
	a.SetVector(3, []float32{2, 4, 6})
	b.SetVector(3, []float32{1, 2, 3})

	vecCosineSimilarity([]*batch.Vector{a, b}, out, n)

	if math.Abs(out.Float64Data[0]-1.0) > 1e-6 {
		t.Errorf("row 0: got %f, want 1.0", out.Float64Data[0])
	}
	if math.Abs(out.Float64Data[1]) > 1e-6 {
		t.Errorf("row 1: got %f, want 0.0", out.Float64Data[1])
	}
	if !out.Nulls.IsNullFast(2) {
		t.Error("row 2: expected null")
	}
	if math.Abs(out.Float64Data[3]-1.0) > 1e-6 {
		t.Errorf("row 3: got %f, want 1.0", out.Float64Data[3])
	}
}

func TestToFloat32SliceFromAny(t *testing.T) {
	// Test that []any with float64 elements works
	result := toFloat32Slice([]any{float64(1.5), float64(2.5), float64(3.5)})
	if len(result) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(result))
	}
	if result[0] != 1.5 {
		t.Errorf("got %f, want 1.5", result[0])
	}
}
