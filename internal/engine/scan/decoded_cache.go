package scan

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// DecodedChunkCache is a worker-lifetime cache of decoded parquet column
// chunks: one entry per (object identity, row group, leaf column, catalog
// type) holding a cache-owned *batch.Vector clone. It attacks the zstd
// decompress + decode-kernel bill (~24% + ~7% of SF100 worker CPU) for
// re-reads of the same immutable base-table bytes — cross-query within a
// run and across benchmark runs. See docs/design/decoded-rowgroup-cache.md.
//
// Correctness model (v1, copy discipline): consumers NEVER share storage
// with the cache. A hit copies the cached vector into the caller's batch
// slot; an admit clones the freshly decoded vector into cache-owned
// storage. Entry vectors are immutable once inserted, so Get may return
// the entry pointer and the caller copies outside the cache lock (an
// eviction during the copy is safe — the GC keeps the clone alive).
//
// Ledger model (ADR-0006): the cache OWNS its bytes once, surfaced through
// Size for a hard system reservoir (memory.NewReservoirFunc). Consumers'
// copies are ordinary scan output charged exactly as today. The cache also
// implements memory.AccountedOperator so RequestRelief can shed it —
// eviction is the cheapest relief in the process — before any operator
// pays a real spill.
//
// Eviction is segmented LRU (probation/protected) with second-touch ghost
// admission: a key's first decode registers a ghost; the clone is stored
// on the second decode; hits promote probation entries to protected.
// Sequential scan floods (a cold first pass over a table) fill probation
// without displacing the protected hot set.
type DecodedChunkCache struct {
	capBytes  int64
	protCap   int64 // protected-segment bound: 80% of capBytes
	maxEntry  int64 // entries larger than this are never admitted (capBytes/8)
	maxGhosts int

	mu        sync.Mutex
	entries   map[decodedChunkKey]*list.Element // element value: *decodedChunkEntry
	probation *list.List                        // front = MRU
	protected *list.List
	protBytes int64
	ghosts    map[decodedChunkKey]*list.Element // element value: decodedChunkKey
	ghostLRU  *list.List

	size atomic.Int64 // total cached bytes; lock-free reservoir accessor

	instanceID uint64

	// Counters (atomic; surfaced by Stats for the 60s worker ticker).
	hits, misses, hitBytes         atomic.Int64
	admitted, ghostRegistered      atomic.Int64
	evictions, reliefEvictedBytes  atomic.Int64
	rejectedTooLarge, cloneSkipped atomic.Int64
}

type decodedChunkKey struct {
	identity    string // FileReader.CacheIdentity — "<bucket>/<key>#<size>"
	rgIdx       int
	colIdx      int // leaf column index in the file schema
	catalogType pqt.TypeID
	scale       int // DECIMAL scale (0 otherwise)
}

const (
	segProbation = iota
	segProtected
)

type decodedChunkEntry struct {
	key   decodedChunkKey
	vec   *batch.Vector // immutable once stored
	bytes int64
	seg   int
}

// NewDecodedChunkCache returns a cache bounded to capBytes. capBytes <= 0
// returns nil — a nil *DecodedChunkCache is valid and inert on every method.
func NewDecodedChunkCache(capBytes int64) *DecodedChunkCache {
	if capBytes <= 0 {
		return nil
	}
	return &DecodedChunkCache{
		capBytes:   capBytes,
		protCap:    capBytes * 8 / 10,
		maxEntry:   capBytes / 8,
		maxGhosts:  1 << 16,
		entries:    make(map[decodedChunkKey]*list.Element),
		probation:  list.New(),
		protected:  list.New(),
		ghosts:     make(map[decodedChunkKey]*list.Element),
		ghostLRU:   list.New(),
		instanceID: memory.NextInstanceID(),
	}
}

// Size returns the cached bytes (the reservoir's live accessor). Nil-safe.
func (c *DecodedChunkCache) Size() int64 {
	if c == nil {
		return 0
	}
	return c.size.Load()
}

// keyFor builds the cache key for one leaf column read, or ok=false when
// the cache is nil, the reader carries no identity, or the column type is
// outside the supported clone set.
func (c *DecodedChunkCache) keyFor(fr *pqt.FileReader, rgIdx, colIdx int, col pqt.Column) (decodedChunkKey, bool) {
	if c == nil {
		return decodedChunkKey{}, false
	}
	id := fr.CacheIdentity()
	if id == "" || !cloneableChunkType(col.Type) {
		return decodedChunkKey{}, false
	}
	return decodedChunkKey{
		identity:    id,
		rgIdx:       rgIdx,
		colIdx:      colIdx,
		catalogType: col.Type,
		scale:       col.Scale,
	}, true
}

// fillFromCache copies the cached chunk for key into dst (a freshly minted
// batch column of the right type and length) and reports whether it was a
// hit. dst is only written on a hit.
func (c *DecodedChunkCache) fillFromCache(dst *batch.Vector, key decodedChunkKey, numRows int) bool {
	c.mu.Lock()
	el, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		c.misses.Add(1)
		return false
	}
	e := el.Value.(*decodedChunkEntry)
	c.touchLocked(el, e)
	c.mu.Unlock()

	// Copy outside the lock: e.vec is immutable and GC-pinned by e.
	if e.vec.Len != numRows || !copyChunkVector(dst, e.vec, numRows) {
		// Shape mismatch (shouldn't happen — identity is content-stable) or
		// unsupported copy: treat as miss, decode normally.
		c.misses.Add(1)
		return false
	}
	c.hits.Add(1)
	c.hitBytes.Add(e.bytes)
	return true
}

// touchLocked applies the SLRU hit policy: protected entries move to front;
// probation entries promote to protected (demoting the protected tail back
// to probation while the protected segment exceeds its bound).
func (c *DecodedChunkCache) touchLocked(el *list.Element, e *decodedChunkEntry) {
	if e.seg == segProtected {
		c.protected.MoveToFront(el)
		return
	}
	c.probation.Remove(el)
	e.seg = segProtected
	c.entries[e.key] = c.protected.PushFront(e)
	c.protBytes += e.bytes
	for c.protBytes > c.protCap {
		tail := c.protected.Back()
		if tail == nil {
			break
		}
		te := tail.Value.(*decodedChunkEntry)
		c.protected.Remove(tail)
		te.seg = segProbation
		c.entries[te.key] = c.probation.PushFront(te)
		c.protBytes -= te.bytes
	}
}

// Offer presents a freshly decoded chunk vector for admission. First touch
// registers a ghost; second touch clones vec into cache-owned storage. The
// clone runs outside the cache lock.
func (c *DecodedChunkCache) Offer(key decodedChunkKey, vec *batch.Vector, numRows int) {
	// Pre-filter on the decoded vector's footprint; the entry is charged at
	// the clone's own (exactly sized) footprint below.
	if est := vec.MemBytes(); est <= 0 || est > c.maxEntry {
		c.rejectedTooLarge.Add(1)
		return
	}

	c.mu.Lock()
	if _, present := c.entries[key]; present {
		c.mu.Unlock()
		return
	}
	gel, wasGhost := c.ghosts[key]
	if !wasGhost {
		// First touch: ghost only.
		c.ghosts[key] = c.ghostLRU.PushFront(key)
		for len(c.ghosts) > c.maxGhosts {
			tail := c.ghostLRU.Back()
			delete(c.ghosts, tail.Value.(decodedChunkKey))
			c.ghostLRU.Remove(tail)
		}
		c.ghostRegistered.Add(1)
		c.mu.Unlock()
		return
	}
	c.ghostLRU.Remove(gel)
	delete(c.ghosts, key)
	c.mu.Unlock()

	clone := cloneChunkVector(vec, numRows, key.scale)
	if clone == nil {
		c.cloneSkipped.Add(1)
		return
	}
	bytes := clone.MemBytes()
	if bytes <= 0 || bytes > c.maxEntry {
		c.rejectedTooLarge.Add(1)
		return
	}

	c.mu.Lock()
	if _, present := c.entries[key]; present {
		c.mu.Unlock() // concurrent Offer won the race; drop our clone
		return
	}
	e := &decodedChunkEntry{key: key, vec: clone, bytes: bytes, seg: segProbation}
	c.entries[key] = c.probation.PushFront(e)
	c.size.Add(bytes)
	c.admitted.Add(1)
	c.evictToCapLocked()
	c.mu.Unlock()
}

// evictToCapLocked evicts probation-tail-first (then protected tail) until
// total size fits capBytes.
func (c *DecodedChunkCache) evictToCapLocked() {
	for c.size.Load() > c.capBytes {
		if c.evictOneLocked() == 0 {
			return
		}
	}
}

// evictOneLocked removes the coldest entry and returns its byte size (0 when
// the cache is empty).
func (c *DecodedChunkCache) evictOneLocked() int64 {
	tail := c.probation.Back()
	fromProtected := false
	if tail == nil {
		tail = c.protected.Back()
		fromProtected = true
	}
	if tail == nil {
		return 0
	}
	e := tail.Value.(*decodedChunkEntry)
	if fromProtected {
		c.protected.Remove(tail)
		c.protBytes -= e.bytes
	} else {
		c.probation.Remove(tail)
	}
	delete(c.entries, e.key)
	c.size.Add(-e.bytes)
	c.evictions.Add(1)
	return e.bytes
}

// --- memory.AccountedOperator: relief integration -------------------------
//
// The cache registers with the worker's shared SpillManager so
// RequestRelief can shed cached bytes — dropping a cache entry is the
// cheapest relief in the process. SpillableBytes is the full cache size:
// every entry is reconstructible from the (already locally cached)
// compressed bytes.

// Inspect implements memory.AccountedOperator.
func (c *DecodedChunkCache) Inspect() memory.OperatorFootprint {
	sz := c.Size()
	state := memory.OpActive
	if sz == 0 {
		state = memory.OpRegistered
	}
	return memory.OperatorFootprint{
		OwnedBytes:     sz,
		RetainedBytes:  sz,
		SpillableBytes: sz,
		State:          state,
		InstanceID:     c.instanceID,
		Name:           "decoded-rowgroup-cache",
	}
}

// EstimateRelief implements memory.AccountedOperator (pure read).
func (c *DecodedChunkCache) EstimateRelief(target int64) int64 {
	if sz := c.Size(); sz < target {
		return sz
	}
	return target
}

// SpillSome implements memory.AccountedOperator: "spilling" a cache is
// eviction — entries re-decode from local compressed bytes on next miss.
func (c *DecodedChunkCache) SpillSome(target int64) (int64, error) {
	var freed int64
	c.mu.Lock()
	for freed < target {
		n := c.evictOneLocked()
		if n == 0 {
			break
		}
		freed += n
	}
	c.mu.Unlock()
	c.reliefEvictedBytes.Add(freed)
	return freed, nil
}

// DecodedChunkCacheStats is the counter snapshot for the worker stats ticker.
type DecodedChunkCacheStats struct {
	Hits, Misses, HitBytes       int64
	Admitted, GhostRegistered    int64
	Evictions, ReliefBytes       int64
	RejectedTooLarge, CloneSkips int64
	SizeBytes, CapBytes          int64
	Entries                      int
}

// Stats returns a point-in-time counter snapshot. Nil-safe.
func (c *DecodedChunkCache) Stats() DecodedChunkCacheStats {
	if c == nil {
		return DecodedChunkCacheStats{}
	}
	c.mu.Lock()
	entries := len(c.entries)
	c.mu.Unlock()
	return DecodedChunkCacheStats{
		Hits:             c.hits.Load(),
		Misses:           c.misses.Load(),
		HitBytes:         c.hitBytes.Load(),
		Admitted:         c.admitted.Load(),
		GhostRegistered:  c.ghostRegistered.Load(),
		Evictions:        c.evictions.Load(),
		ReliefBytes:      c.reliefEvictedBytes.Load(),
		RejectedTooLarge: c.rejectedTooLarge.Load(),
		CloneSkips:       c.cloneSkipped.Load(),
		SizeBytes:        c.Size(),
		CapBytes:         c.capBytes,
		Entries:          entries,
	}
}

// --- vector clone/copy helpers ---------------------------------------------

// cloneableChunkType reports whether the decoded-vector clone/copy helpers
// support this catalog type. The set matches what ReadRowGroupNative's flat
// leaf path materializes; nested types, ROW children and VECTOR embeddings
// stay uncached (they fall through to a normal decode).
func cloneableChunkType(t pqt.TypeID) bool {
	switch t {
	case pqt.TypeBool,
		pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate,
		pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration,
		pqt.TypeFloat32, pqt.TypeFloat64,
		pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID,
		pqt.TypeDecimal:
		return true
	}
	return false
}

// cloneChunkVector deep-copies a freshly decoded chunk vector into new
// cache-owned storage. Returns nil for shapes the copy set does not cover
// (views, unexpected typed storage) — the caller skips admission.
func cloneChunkVector(src *batch.Vector, numRows int, scale int) *batch.Vector {
	if src == nil || src.Base != nil || src.Len != numRows {
		return nil
	}
	dst := batch.NewVectorWithScale(src.Type, numRows, scale)
	if !copyChunkVector(dst, src, numRows) {
		return nil
	}
	return dst
}

// copyChunkVector copies numRows values (data + nulls) from an immutable
// source chunk vector into dst, which must have been allocated with the
// same type/scale and length numRows (batch.NewRecordBatch or
// cloneChunkVector shapes). Returns false when the type is outside the
// supported set or the source storage doesn't match its declared type —
// callers treat false as "decode normally".
func copyChunkVector(dst, src *batch.Vector, numRows int) bool {
	if src.Base != nil || dst.Base != nil || dst.Type != src.Type {
		return false
	}
	switch src.Type {
	case batch.TypeBool:
		if len(src.BoolData) < numRows || len(dst.BoolData) < numRows {
			return false
		}
		copy(dst.BoolData, src.BoolData[:numRows])
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		if len(src.Int32Data) < numRows || len(dst.Int32Data) < numRows {
			return false
		}
		copy(dst.Int32Data, src.Int32Data[:numRows])
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		if len(src.Int64Data) < numRows || len(dst.Int64Data) < numRows {
			return false
		}
		copy(dst.Int64Data, src.Int64Data[:numRows])
	case batch.TypeFloat32:
		if len(src.Float32Data) < numRows || len(dst.Float32Data) < numRows {
			return false
		}
		copy(dst.Float32Data, src.Float32Data[:numRows])
	case batch.TypeFloat64:
		if len(src.Float64Data) < numRows || len(dst.Float64Data) < numRows {
			return false
		}
		copy(dst.Float64Data, src.Float64Data[:numRows])
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		if src.BytesData.Len() < numRows || dst.BytesData.Len() < numRows {
			return false
		}
		if len(dst.BytesData.Data) != 0 {
			// dst must be a fresh (or Reset) column: BulkCopy appends.
			return false
		}
		dst.BytesData.BulkCopy(0, &src.BytesData, 0, numRows)
	case batch.TypeDecimal:
		if len(src.DecimalData.Data) < numRows || len(dst.DecimalData.Data) < numRows ||
			dst.DecimalData.Scale != src.DecimalData.Scale {
			return false
		}
		copy(dst.DecimalData.Data, src.DecimalData.Data[:numRows])
	default:
		return false
	}
	dst.Nulls.CopyFrom(&src.Nulls, numRows)
	dst.Len = numRows
	return true
}
