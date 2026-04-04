package embedding

import (
	"testing"
)

// mockProvider returns deterministic embeddings for testing.
type mockProvider struct {
	model string
	dim   int
	calls int
}

func (m *mockProvider) Embed(texts []string) ([][]float32, error) {
	m.calls++
	results := make([][]float32, len(texts))
	for i, text := range texts {
		vec := make([]float32, m.dim)
		// Deterministic: first char as seed
		seed := float32(0.1)
		if len(text) > 0 {
			seed = float32(text[0]) / 256.0
		}
		for j := range vec {
			vec[j] = seed + float32(j)*0.01
		}
		results[i] = vec
	}
	return results, nil
}

func (m *mockProvider) Dimension() int { return m.dim }
func (m *mockProvider) Model() string  { return m.model }

func TestEmbedFunction(t *testing.T) {
	mock := &mockProvider{model: "test-model", dim: 4}
	SetProvider(mock)
	defer SetProvider(nil)

	RegisterFunctions()

	// embed(text) returns []float32
	result := fnEmbed([]any{"hello"})
	vec, ok := result.([]float32)
	if !ok {
		t.Fatalf("embed() returned %T, want []float32", result)
	}
	if len(vec) != 4 {
		t.Fatalf("embed() dimension = %d, want 4", len(vec))
	}

	// embed(nil) returns nil
	if fnEmbed([]any{nil}) != nil {
		t.Error("embed(nil) should return nil")
	}

	// embed_model() returns model name
	model := fnEmbedModel(nil)
	if model != "test-model" {
		t.Errorf("embed_model() = %v, want test-model", model)
	}

	// embed_dim() returns dimension
	dim := fnEmbedDim(nil)
	if dim != int64(4) {
		t.Errorf("embed_dim() = %v, want 4", dim)
	}
}

func TestEmbedNoProvider(t *testing.T) {
	SetProvider(nil)

	if fnEmbed([]any{"hello"}) != nil {
		t.Error("embed() with no provider should return nil")
	}
	if fnEmbedModel(nil) != nil {
		t.Error("embed_model() with no provider should return nil")
	}
	if fnEmbedDim(nil) != nil {
		t.Error("embed_dim() with no provider should return nil")
	}
}

func TestCache(t *testing.T) {
	cache := NewCache(3)

	// Miss
	if cache.Get("model", "hello") != nil {
		t.Error("expected cache miss")
	}

	// Put + hit
	vec := []float32{1.0, 2.0, 3.0}
	cache.Put("model", "hello", vec)
	got := cache.Get("model", "hello")
	if got == nil || got[0] != 1.0 {
		t.Errorf("cache hit = %v, want [1 2 3]", got)
	}

	// Different model = miss
	if cache.Get("other", "hello") != nil {
		t.Error("different model should miss")
	}

	// Eviction at capacity
	cache.Put("model", "text2", []float32{4, 5, 6})
	cache.Put("model", "text3", []float32{7, 8, 9})
	if cache.Len() != 3 {
		t.Fatalf("cache len = %d, want 3", cache.Len())
	}

	// Adding a 4th should evict "hello"
	cache.Put("model", "text4", []float32{10, 11, 12})
	if cache.Get("model", "hello") != nil {
		t.Error("expected 'hello' to be evicted")
	}
	if cache.Len() != 3 {
		t.Fatalf("cache len = %d after eviction, want 3", cache.Len())
	}
}

func TestCachedProvider(t *testing.T) {
	_ = NewCache(100) // cache tested separately; here we test the function path
	mock := &mockProvider{model: "test", dim: 3}
	SetProvider(mock)
	defer SetProvider(nil)

	// First call: cache miss → calls provider
	result1 := fnEmbed([]any{"hello"})
	if result1 == nil {
		t.Fatal("first embed() returned nil")
	}
	if mock.calls != 1 {
		t.Fatalf("expected 1 API call, got %d", mock.calls)
	}

	// embed() uses the global provider which has no cache by default.
	// The OpenAI provider wraps a cache, but mock doesn't.
	// This test validates the mock works — cache integration is tested
	// via the OpenAI provider test with httptest.
}
