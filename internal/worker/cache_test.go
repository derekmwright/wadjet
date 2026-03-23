package worker

import "testing"

func TestLRUCache_BasicOps(t *testing.T) {
	cache := NewLRUCache(100)

	cache.Put("k1", []byte("hello"))
	cache.Put("k2", []byte("world"))

	v, ok := cache.Get("k1")
	if !ok || string(v) != "hello" {
		t.Fatalf("expected 'hello', got %q ok=%v", v, ok)
	}

	_, ok = cache.Get("missing")
	if ok {
		t.Fatal("expected miss for 'missing'")
	}

	if cache.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", cache.Len())
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	cache := NewLRUCache(20) // small cache

	cache.Put("k1", make([]byte, 10))
	cache.Put("k2", make([]byte, 10))

	// Cache is full at 20 bytes. Adding another should evict k1 (LRU)
	cache.Put("k3", make([]byte, 10))

	_, ok := cache.Get("k1")
	if ok {
		t.Fatal("expected k1 to be evicted")
	}

	_, ok = cache.Get("k2")
	if !ok {
		t.Fatal("expected k2 to still be present")
	}
}

func TestLRUCache_CopyOnRead(t *testing.T) {
	cache := NewLRUCache(100)
	cache.Put("k1", []byte("original"))

	// Get returns a copy — mutating it must not affect the cached value
	v1, _ := cache.Get("k1")
	v1[0] = 'X'

	v2, _ := cache.Get("k1")
	if string(v2) != "original" {
		t.Fatalf("cache corrupted: expected 'original', got %q", v2)
	}
}

func TestLRUCache_Update(t *testing.T) {
	cache := NewLRUCache(100)

	cache.Put("k1", []byte("v1"))
	cache.Put("k1", []byte("v2-updated"))

	v, ok := cache.Get("k1")
	if !ok || string(v) != "v2-updated" {
		t.Fatalf("expected 'v2-updated', got %q", v)
	}

	if cache.Len() != 1 {
		t.Fatalf("expected 1 entry after update, got %d", cache.Len())
	}
}
