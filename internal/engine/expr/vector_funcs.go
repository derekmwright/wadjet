package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

func init() {
	// The distance functions return a float64 score; vector_dims a count.
	// The vec kernels below write out.Float64Data, so a String declaration
	// here is a process-killing panic (it was one, until the declaration
	// moved next to the registration — #310).
	fns := map[string]builtin{
		"cosine_similarity": {fnCosineSimilarity, RetFloat64},
		"l2_distance":       {fnL2Distance, RetFloat64},
		"dot_product":       {fnDotProduct, RetFloat64},
		"vector_norm":       {fnVectorNorm, RetFloat64},
		"vector_dims":       {fnVectorDims, RetInt64},
	}
	for name, b := range fns {
		DefaultRegistry.Register(name, b.fn, b.ret)
	}

	vecFns := map[string]VecScalarFunc{
		"cosine_similarity": vecCosineSimilarity,
		"l2_distance":       vecL2Distance,
		"dot_product":       vecDotProduct,
	}
	for name, fn := range vecFns {
		DefaultRegistry.RegisterVec(name, fn)
	}
}

// toFloat32Slice extracts a []float32 from various representations.
func toFloat32Slice(v any) []float32 {
	switch tv := v.(type) {
	case []float32:
		return tv
	case []any:
		out := make([]float32, len(tv))
		for i, elem := range tv {
			switch fv := elem.(type) {
			case float32:
				out[i] = fv
			case float64:
				out[i] = float32(fv)
			case int64:
				out[i] = float32(fv)
			case int:
				out[i] = float32(fv)
			}
		}
		return out
	default:
		return nil
	}
}

// fnCosineSimilarity computes cosine similarity between two vectors.
// Returns a float64 in [-1, 1].
func fnCosineSimilarity(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	a := toFloat32Slice(args[0])
	b := toFloat32Slice(args[1])
	if len(a) == 0 || len(a) != len(b) {
		return nil
	}
	return cosine(a, b)
}

// fnL2Distance computes Euclidean distance between two vectors.
func fnL2Distance(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	a := toFloat32Slice(args[0])
	b := toFloat32Slice(args[1])
	if len(a) == 0 || len(a) != len(b) {
		return nil
	}
	return l2dist(a, b)
}

// fnDotProduct computes the dot product of two vectors.
func fnDotProduct(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	a := toFloat32Slice(args[0])
	b := toFloat32Slice(args[1])
	if len(a) == 0 || len(a) != len(b) {
		return nil
	}
	return dot(a, b)
}

// fnVectorNorm computes the L2 norm of a vector.
func fnVectorNorm(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	a := toFloat32Slice(args[0])
	if len(a) == 0 {
		return nil
	}
	var sum float64
	for _, v := range a {
		sum += float64(v) * float64(v)
	}
	return math.Sqrt(sum)
}

// fnVectorDims returns the dimensionality of a vector.
func fnVectorDims(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	a := toFloat32Slice(args[0])
	return int64(len(a))
}

// --- Vectorized implementations ---

func vecCosineSimilarity(args []*batch.Vector, out *batch.Vector, n int) {
	if len(args) < 2 {
		return
	}
	a, b := args[0], args[1]
	if a.Type != batch.TypeVector || b.Type != batch.TypeVector {
		return
	}
	dim := a.VectorDim
	if dim <= 0 || dim != b.VectorDim {
		return
	}
	for i := 0; i < n; i++ {
		if a.Nulls.IsNullFast(i) || b.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		av := a.Float32Data[i*dim : (i+1)*dim]
		bv := b.Float32Data[i*dim : (i+1)*dim]
		out.Float64Data[i] = cosine(av, bv)
		out.Nulls.SetValid(i)
	}
}

func vecL2Distance(args []*batch.Vector, out *batch.Vector, n int) {
	if len(args) < 2 {
		return
	}
	a, b := args[0], args[1]
	if a.Type != batch.TypeVector || b.Type != batch.TypeVector {
		return
	}
	dim := a.VectorDim
	if dim <= 0 || dim != b.VectorDim {
		return
	}
	for i := 0; i < n; i++ {
		if a.Nulls.IsNullFast(i) || b.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		av := a.Float32Data[i*dim : (i+1)*dim]
		bv := b.Float32Data[i*dim : (i+1)*dim]
		out.Float64Data[i] = l2dist(av, bv)
		out.Nulls.SetValid(i)
	}
}

func vecDotProduct(args []*batch.Vector, out *batch.Vector, n int) {
	if len(args) < 2 {
		return
	}
	a, b := args[0], args[1]
	if a.Type != batch.TypeVector || b.Type != batch.TypeVector {
		return
	}
	dim := a.VectorDim
	if dim <= 0 || dim != b.VectorDim {
		return
	}
	for i := 0; i < n; i++ {
		if a.Nulls.IsNullFast(i) || b.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		av := a.Float32Data[i*dim : (i+1)*dim]
		bv := b.Float32Data[i*dim : (i+1)*dim]
		out.Float64Data[i] = dot(av, bv)
		out.Nulls.SetValid(i)
	}
}

// --- Core math (operate on float32 slices, return float64) ---

func dot(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

func l2dist(a, b []float32) float64 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}

func cosine(a, b []float32) float64 {
	var dotAB, normA, normB float64
	for i := range a {
		ai, bi := float64(a[i]), float64(b[i])
		dotAB += ai * bi
		normA += ai * ai
		normB += bi * bi
	}
	denom := math.Sqrt(normA) * math.Sqrt(normB)
	if denom == 0 {
		return 0
	}
	return dotAB / denom
}
