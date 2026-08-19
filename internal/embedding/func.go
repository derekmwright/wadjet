package embedding

import (
	"fmt"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// globalProvider is the configured embedding provider. Set via SetProvider.
var (
	globalMu       sync.RWMutex
	globalProvider Provider
)

// SetProvider configures the global embedding provider used by embed().
func SetProvider(p Provider) {
	globalMu.Lock()
	globalProvider = p
	globalMu.Unlock()
}

// GetProvider returns the current global provider, or nil.
func GetProvider() Provider {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalProvider
}

// RegisterFunctions registers embed() and embed_dim() in the expression registry.
func RegisterFunctions() {
	expr.DefaultRegistry.Register("embed", fnEmbed, expr.RetVector)
	expr.DefaultRegistry.Register("embed_model", fnEmbedModel, expr.RetString)
	expr.DefaultRegistry.Register("embed_dim", fnEmbedDim, expr.RetInt64)

	// SQL-level batching: the vectorized path collects every text in a record
	// batch into a single provider.Embed() call instead of one call per row.
	expr.DefaultRegistry.RegisterVec("embed", vecEmbed)
	// embed() declares VECTOR above; its output *dimension* is the dynamic
	// half — it comes from the live provider, so the planner asks for it here
	// at plan time rather than reading it off a constant.
	expr.DefaultRegistry.RegisterVecReturn("embed", func() int {
		if p := GetProvider(); p != nil {
			return p.Dimension()
		}
		return 0
	})
}

// vecEmbed is the vectorized implementation of embed(text) → VECTOR. It gathers
// all non-null inputs in the batch and issues a single provider.Embed() call,
// then scatters the results back into out (a TypeVector vector whose VectorDim
// is set from the provider dimension at plan time). On any error every targeted
// row is set NULL — matching the per-row fnEmbed null-on-failure behavior.
func vecEmbed(args []*batch.Vector, out *batch.Vector, n int) {
	if len(args) < 1 || n == 0 {
		return
	}
	in := args[0]

	p := GetProvider()
	if p == nil {
		for i := 0; i < n; i++ {
			out.Nulls.SetNull(i)
		}
		return
	}

	texts := make([]string, 0, n)
	rows := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if in.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		v := in.GetValue(i)
		s, ok := v.(string)
		if !ok {
			s = fmt.Sprint(v)
		}
		texts = append(texts, s)
		rows = append(rows, i)
	}
	if len(texts) == 0 {
		return
	}

	vectors, err := p.Embed(texts)
	if err != nil {
		for _, i := range rows {
			out.Nulls.SetNull(i)
		}
		return
	}

	// The authoritative width is what the provider actually returned. When the
	// plan-time dimension is unset, derive it from the first non-empty vector;
	// otherwise honor the plan-time width. Either way, a row whose vector
	// doesn't match that width is set NULL rather than truncated or left
	// holding stale data from a previous batch in the pooled output buffer.
	dim := out.VectorDim
	if dim <= 0 {
		for _, vec := range vectors {
			if len(vec) > 0 {
				dim = len(vec)
				break
			}
		}
	}
	if dim <= 0 {
		for _, i := range rows {
			out.Nulls.SetNull(i)
		}
		return
	}
	out.VectorDim = dim
	if need := n * dim; len(out.Float32Data) < need {
		out.Float32Data = make([]float32, need)
	}

	for k, i := range rows {
		if k < len(vectors) && len(vectors[k]) == dim {
			out.SetVector(i, vectors[k])
			out.Nulls.SetValid(i)
		} else {
			out.Nulls.SetNull(i)
		}
	}
}

// fnEmbed implements embed(text) → VECTOR.
// Returns a []float32 embedding for the input text.
// If no provider is configured, returns NULL.
func fnEmbed(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}

	text, ok := args[0].(string)
	if !ok {
		text = fmt.Sprint(args[0])
	}

	p := GetProvider()
	if p == nil {
		return nil // no provider configured
	}

	vectors, err := p.Embed([]string{text})
	if err != nil || len(vectors) == 0 {
		return nil
	}
	return vectors[0]
}

// fnEmbedModel returns the current embedding model name.
func fnEmbedModel(args []any) any {
	p := GetProvider()
	if p == nil {
		return nil
	}
	return p.Model()
}

// fnEmbedDim returns the current embedding dimension.
func fnEmbedDim(args []any) any {
	p := GetProvider()
	if p == nil {
		return nil
	}
	return int64(p.Dimension())
}
