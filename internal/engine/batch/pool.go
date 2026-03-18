package batch

import (
	"sync"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// NOTE: When Go arenas reach GA (currently experimental behind GOEXPERIMENT=arenas),
// the BatchPool backing allocator should be swapped to arena-based allocation.
// Arena lifecycle maps naturally to batch lifecycle: allocate vectors/bitmaps from
// an arena, free the entire arena when the batch is released. This would eliminate
// GC pressure on the batch processing path entirely, giving an estimated 10-15%
// throughput improvement on large aggregations and sorts.
//
// The pool abstraction here makes that swap straightforward — only Get() and the
// underlying NewRecordBatch call need to change. Operator code stays the same.

const (
	// maxPoolPerClass is the maximum number of batches cached per size class.
	maxPoolPerClass = 16
)

// BatchPool manages reusable RecordBatch allocations with size-class bucketing.
// Batches are pooled by their schema and row count to avoid allocation on the
// hot path. Thread-safe for concurrent operator use.
type BatchPool struct {
	schema    []parquet.Column
	batchSize int
	mu        sync.Mutex
	pool      []*RecordBatch
	maxPool   int
}

// NewBatchPool creates a pool for batches of the given schema and size.
func NewBatchPool(schema []parquet.Column, batchSize int) *BatchPool {
	return &BatchPool{
		schema:    schema,
		batchSize: batchSize,
		maxPool:   maxPoolPerClass,
	}
}

// BatchSize returns the row count this pool is configured for.
func (p *BatchPool) BatchSize() int { return p.batchSize }

// Get returns a batch from the pool, or allocates a new one.
func (p *BatchPool) Get() *RecordBatch {
	p.mu.Lock()
	if len(p.pool) > 0 {
		b := p.pool[len(p.pool)-1]
		p.pool = p.pool[:len(p.pool)-1]
		p.mu.Unlock()
		b.Reset(p.batchSize)
		return b
	}
	p.mu.Unlock()
	b := NewRecordBatch(p.schema, p.batchSize)
	b.pool = p
	return b
}

// Put returns a batch to the pool for reuse.
func (p *BatchPool) Put(b *RecordBatch) {
	p.mu.Lock()
	if len(p.pool) < p.maxPool {
		p.pool = append(p.pool, b)
	}
	p.mu.Unlock()
}

// GlobalPool provides shared batch pooling across operators with the same schema.
// This avoids each operator maintaining its own pool and improves reuse when
// multiple operators in a pipeline share a schema.
type GlobalPool struct {
	mu    sync.Mutex
	pools map[string]*BatchPool
}

// NewGlobalPool creates a new global pool.
func NewGlobalPool() *GlobalPool {
	return &GlobalPool{
		pools: make(map[string]*BatchPool),
	}
}

// ForSchema returns the pool for the given schema and batch size.
// Creates one if it doesn't exist yet.
func (gp *GlobalPool) ForSchema(schema []parquet.Column, batchSize int) *BatchPool {
	key := schemaKey(schema, batchSize)

	gp.mu.Lock()
	defer gp.mu.Unlock()

	if pool, ok := gp.pools[key]; ok {
		return pool
	}

	pool := NewBatchPool(schema, batchSize)
	gp.pools[key] = pool
	return pool
}

// schemaKey builds a cache key from schema columns and batch size.
func schemaKey(schema []parquet.Column, batchSize int) string {
	// Use a simple string key: "col1:type1,col2:type2@size"
	size := 0
	for _, c := range schema {
		size += len(c.Name) + 4 // name + separator + type digit
	}
	buf := make([]byte, 0, size+8)
	for i, c := range schema {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, c.Name...)
		buf = append(buf, ':')
		buf = append(buf, byte('0'+c.Type))
	}
	buf = append(buf, '@')
	buf = appendBatchSize(buf, batchSize)
	return string(buf)
}

func appendBatchSize(buf []byte, n int) []byte {
	if n == 0 {
		return append(buf, '0')
	}
	var tmp [10]byte
	i := len(tmp)
	for n > 0 {
		i--
		tmp[i] = byte('0' + n%10)
		n /= 10
	}
	return append(buf, tmp[i:]...)
}
