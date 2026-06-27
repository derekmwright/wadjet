// Package embedding provides text embedding via external APIs (OpenAI, Ollama).
package embedding

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// Provider generates vector embeddings from text.
type Provider interface {
	// Embed returns embedding vectors for one or more texts.
	// The returned slice has the same length as texts.
	Embed(texts []string) ([][]float32, error)

	// Dimension returns the output vector dimension.
	Dimension() int

	// Model returns the model identifier.
	Model() string
}

// Cache is an LRU cache for embeddings keyed on (model, text_hash).
type Cache struct {
	mu       sync.Mutex
	entries  map[string][]float32
	order    []string // LRU order (oldest first)
	maxItems int
}

// NewCache creates an embedding cache with the given max entries.
func NewCache(maxItems int) *Cache {
	if maxItems <= 0 {
		maxItems = 50000
	}
	return &Cache{
		entries:  make(map[string][]float32, maxItems),
		order:    make([]string, 0, maxItems),
		maxItems: maxItems,
	}
}

func cacheKey(model, text string) string {
	h := sha256.Sum256([]byte(text))
	return model + ":" + hex.EncodeToString(h[:16])
}

// Get returns a cached embedding, or nil if not found.
func (c *Cache) Get(model, text string) []float32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[cacheKey(model, text)]
}

// Put stores an embedding in the cache, evicting oldest if full.
func (c *Cache) Put(model, text string, vec []float32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheKey(model, text)
	if _, ok := c.entries[key]; ok {
		return // already cached
	}
	if len(c.order) >= c.maxItems {
		// Evict oldest
		evict := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, evict)
	}
	c.entries[key] = vec
	c.order = append(c.order, key)
}

// Len returns the number of cached entries.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// embedWithCache resolves embeddings for texts: cache hits are served directly
// and the uncached misses are issued as a single batched callAPI. nil vectors
// (the provider returned no embedding for a slot) are left nil in the result
// and NOT cached — caching nil would persist a permanent miss for that text.
// Shared by every Provider so the cache/batch policy lives in one place.
func embedWithCache(cache *Cache, model string, texts []string, callAPI func([]string) ([][]float32, error)) ([][]float32, error) {
	results := make([][]float32, len(texts))

	var misses []int
	for i, text := range texts {
		if cached := cache.Get(model, text); cached != nil {
			results[i] = cached
		} else {
			misses = append(misses, i)
		}
	}
	if len(misses) == 0 {
		return results, nil
	}

	missTexts := make([]string, len(misses))
	for i, idx := range misses {
		missTexts[i] = texts[idx]
	}

	vectors, err := callAPI(missTexts)
	if err != nil {
		return nil, err
	}

	for i, idx := range misses {
		if i < len(vectors) && vectors[i] != nil {
			results[idx] = vectors[i]
			cache.Put(model, texts[idx], vectors[i])
		}
	}
	return results, nil
}
