package catalog

import (
	"sort"
	"strings"
	"sync"
)

// MemKV is an in-memory MetaKV implementation for tests and embedded use.
type MemKV struct {
	mu      sync.RWMutex
	data    map[string][]byte
	revs    map[string]uint64
	nextRev uint64
}

// NewMemKV creates a new in-memory KV store.
func NewMemKV() *MemKV {
	return &MemKV{
		data:    make(map[string][]byte),
		revs:    make(map[string]uint64),
		nextRev: 1,
	}
}

func (m *MemKV) Get(key string) ([]byte, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.data[key]
	if !ok {
		return nil, 0, ErrKeyNotFound
	}
	// Return a copy to prevent mutation
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, m.revs[key], nil
}

// Revision implements RevisionReader: the key's revision without copying
// its value, so a catalog cache can revalidate for the price of a map
// lookup.
func (m *MemKV) Revision(key string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rev, ok := m.revs[key]
	if !ok {
		return 0, ErrKeyNotFound
	}
	return rev, nil
}

func (m *MemKV) Put(key string, value []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]byte, len(value))
	copy(cp, value)
	m.data[key] = cp
	rev := m.nextRev
	m.revs[key] = rev
	m.nextRev++
	return rev, nil
}

func (m *MemKV) Update(key string, value []byte, expectedRev uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	currentRev, ok := m.revs[key]
	if !ok || currentRev != expectedRev {
		return 0, ErrRevisionMismatch
	}

	cp := make([]byte, len(value))
	copy(cp, value)
	m.data[key] = cp
	rev := m.nextRev
	m.revs[key] = rev
	m.nextRev++
	return rev, nil
}

func (m *MemKV) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	delete(m.revs, key)
	return nil
}

func (m *MemKV) List(prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for k := range m.data {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}
