package batch

import "github.com/derekmwright/caelum/internal/storage/parquet"

// BatchPool manages reusable RecordBatch allocations per goroutine.
// Using a dedicated pool instead of sync.Pool for deterministic memory reuse.
type BatchPool struct {
	schema    []parquet.Column
	batchSize int
	pool      []*RecordBatch
	maxPool   int
}

// NewBatchPool creates a pool for batches of the given schema and size.
func NewBatchPool(schema []parquet.Column, batchSize int) *BatchPool {
	return &BatchPool{
		schema:    schema,
		batchSize: batchSize,
		maxPool:   8,
	}
}

// Get returns a batch from the pool, or allocates a new one.
func (p *BatchPool) Get() *RecordBatch {
	if len(p.pool) > 0 {
		b := p.pool[len(p.pool)-1]
		p.pool = p.pool[:len(p.pool)-1]
		b.Reset(p.batchSize)
		return b
	}
	b := NewRecordBatch(p.schema, p.batchSize)
	b.pool = p
	return b
}

// Put returns a batch to the pool for reuse.
func (p *BatchPool) Put(b *RecordBatch) {
	if len(p.pool) < p.maxPool {
		p.pool = append(p.pool, b)
	}
}
