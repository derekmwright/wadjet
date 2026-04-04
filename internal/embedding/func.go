package embedding

import (
	"fmt"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/expr"
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
	expr.DefaultRegistry.Register("embed", fnEmbed)
	expr.DefaultRegistry.Register("embed_model", fnEmbedModel)
	expr.DefaultRegistry.Register("embed_dim", fnEmbedDim)
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
