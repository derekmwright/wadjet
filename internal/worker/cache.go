// Package worker implements the distributed task execution worker.
package worker

import (
	"container/list"
	"sync"
)

// LRUCache is a bounded cache for Parquet footers and column chunks.
type LRUCache struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	items    map[string]*list.Element
	evict    *list.List
}

type cacheEntry struct {
	key  string
	data []byte
}

// NewLRUCache creates a new LRU cache with the given byte limit.
func NewLRUCache(maxBytes int64) *LRUCache {
	return &LRUCache{
		maxBytes: maxBytes,
		items:    make(map[string]*list.Element),
		evict:    list.New(),
	}
}

// Get retrieves a copy of the cached value. The returned slice is safe to
// mutate (e.g., decompress) without corrupting the cached data.
func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.evict.MoveToFront(elem)
		src := elem.Value.(*cacheEntry).data
		cp := make([]byte, len(src))
		copy(cp, src)
		return cp, true
	}
	return nil, false
}

// GetRef retrieves the cached value without copying. The caller MUST NOT
// modify the returned slice. Safe for read-only use (e.g., Parquet decoding)
// because Go's GC keeps the backing array alive even if the cache evicts
// the entry while the caller still holds a reference.
func (c *LRUCache) GetRef(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.evict.MoveToFront(elem)
		return elem.Value.(*cacheEntry).data, true
	}
	return nil, false
}

// Put adds a value to the cache, evicting old entries if needed.
func (c *LRUCache) Put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.evict.MoveToFront(elem)
		old := elem.Value.(*cacheEntry)
		c.curBytes -= int64(len(old.data))
		old.data = data
		c.curBytes += int64(len(data))
		return
	}

	for c.curBytes+int64(len(data)) > c.maxBytes && c.evict.Len() > 0 {
		back := c.evict.Back()
		if back == nil {
			break
		}
		entry := back.Value.(*cacheEntry)
		c.curBytes -= int64(len(entry.data))
		delete(c.items, entry.key)
		c.evict.Remove(back)
	}

	entry := &cacheEntry{key: key, data: data}
	elem := c.evict.PushFront(entry)
	c.items[key] = elem
	c.curBytes += int64(len(data))
}

// Size returns the current cache size in bytes.
func (c *LRUCache) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// Len returns the number of entries in the cache.
func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
