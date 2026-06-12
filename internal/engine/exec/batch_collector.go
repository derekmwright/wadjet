package exec

import (
	"context"
	"os"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// SpillableBatchCollector is a Sink that buffers a child pipeline's entire
// output for later replay, with tracker accounting and spill-to-disk past
// memory pressure. It replaces the raw BatchSink in the join bridges
// (deferredJoinBridge, reverseBloomBridge), which pinned the full collected
// probe side in untracked heap while the downstream join simultaneously held
// its build side — double residency invisible to SpillManager victim
// selection (sweep finding #12; observed collecting probe batches at SF100).
//
// Lifecycle: use as a Pipeline Sink, then read back with Iterate (a
// non-consuming pre-scan, e.g. a bloom build) and/or NextReplay (a consuming
// stream that drops references and deletes spill scratch as it goes).
// Release frees everything still held; it is idempotent and must be called
// from the owner's Close and error paths.
type SpillableBatchCollector struct {
	Spill *memory.SpillManager // optional; nil = plain in-memory buffering

	mu         sync.Mutex
	schema     []parquet.Column
	batches    []*batch.RecordBatch
	spillFiles []string
	trackedMem int64
	totalRows  int

	replayReader *spillBatchReader
	replayFile   int
	replayIdx    int
}

func (c *SpillableBatchCollector) Init(_ context.Context) error { return nil }

func (c *SpillableBatchCollector) Consume(_ context.Context, b *batch.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schema == nil {
		c.schema = b.Schema
	}
	b.Detach() // prevent pool recycle — pipeline calls Release() after Consume()
	// Snapshot the selection vector — filter operators reuse their outSel
	// scratch across calls (see BatchSink.Consume).
	if b.Sel != nil {
		selCopy := make([]uint32, len(b.Sel))
		copy(selCopy, b.Sel)
		b.Sel = selCopy
	}
	c.batches = append(c.batches, b)
	c.totalRows += b.ActiveLen()

	if c.Spill == nil {
		return nil
	}
	cost := b.MemBytes()
	c.Spill.TrackBatch(cost)
	c.trackedMem += cost
	// Same drain shape as Sort.Consume: accumulate at least a run's worth
	// before flushing so spill files stay I/O-friendly.
	if c.Spill.ShouldSpillFor(memory.SpillCheap) && c.trackedMem >= minSortRunBytes {
		return c.drainToDiskLocked()
	}
	return nil
}

func (c *SpillableBatchCollector) Finalize(_ context.Context) error { return nil }
func (c *SpillableBatchCollector) Close() error                     { return nil }

// drainToDiskLocked writes the buffered batches to one spill file and
// releases their tracker charge. Caller holds c.mu.
func (c *SpillableBatchCollector) drainToDiskLocked() error {
	sw, err := newSpillBatchWriter(c.Spill.SpillDir(), "bridge-collect")
	if err != nil {
		return err
	}
	for _, b := range c.batches {
		if b.Sel != nil {
			// The columnar spill format writes all Len rows and carries no
			// selection vector — materialize the selection first.
			b = b.Compact()
		}
		if err := sw.writeBatch(b); err != nil {
			sw.abort()
			return err
		}
	}
	path, err := sw.close()
	if err != nil {
		return err
	}
	if path != "" {
		c.spillFiles = append(c.spillFiles, path)
	}
	c.batches = c.batches[:0]
	c.Spill.ReleaseTracking(c.trackedMem)
	c.trackedMem = 0
	return nil
}

// Rows returns the total active rows collected.
func (c *SpillableBatchCollector) Rows() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.totalRows
}

// Schema returns the schema of the first consumed batch (nil if none).
func (c *SpillableBatchCollector) Schema() []parquet.Column {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.schema
}

// Iterate streams every collected batch (spilled runs first, then the
// in-memory tail) through fn without consuming the collector. Use for
// pre-replay scans like bloom builds. Must not be called once NextReplay
// has started consuming.
func (c *SpillableBatchCollector) Iterate(fn func(*batch.RecordBatch) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range c.spillFiles {
		r, err := openSpillBatchReader(f)
		if err != nil {
			return err
		}
		for {
			b, err := r.Next()
			if err != nil {
				r.Close()
				return err
			}
			if b == nil {
				break
			}
			if err := fn(b); err != nil {
				r.Close()
				return err
			}
		}
		r.Close()
	}
	for _, b := range c.batches {
		if err := fn(b); err != nil {
			return err
		}
	}
	return nil
}

// NextReplay streams the collected batches in arrival order. Spilled runs
// are read back one batch at a time and each consumed file is deleted as
// soon as it is exhausted; in-memory batches have their references dropped
// and tracker charge returned as they are handed out, so peak residency
// falls monotonically during replay. Returns (nil, nil) when exhausted.
func (c *SpillableBatchCollector) NextReplay(_ context.Context) (*batch.RecordBatch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.replayReader != nil {
			b, err := c.replayReader.Next()
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			c.replayReader.Close()
			c.replayReader = nil
			os.Remove(c.spillFiles[c.replayFile])
			c.replayFile++
		}
		if c.replayFile >= len(c.spillFiles) {
			break
		}
		r, err := openSpillBatchReader(c.spillFiles[c.replayFile])
		if err != nil {
			return nil, err
		}
		c.replayReader = r
	}
	if c.replayIdx >= len(c.batches) {
		return nil, nil
	}
	b := c.batches[c.replayIdx]
	c.batches[c.replayIdx] = nil
	c.replayIdx++
	if c.Spill != nil {
		cost := b.MemBytes()
		if cost > c.trackedMem {
			cost = c.trackedMem
		}
		if cost > 0 {
			c.Spill.ReleaseTracking(cost)
			c.trackedMem -= cost
		}
	}
	return b, nil
}

// Release frees everything still held: the open replay reader, remaining
// spill scratch, buffered batch references, and the tracker reservation.
// Idempotent — call from the owner's Close and every error path.
func (c *SpillableBatchCollector) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replayReader != nil {
		c.replayReader.Close()
		c.replayReader = nil
	}
	if c.replayFile < len(c.spillFiles) {
		removeRunFiles(c.spillFiles[c.replayFile:])
	}
	c.spillFiles = nil
	c.replayFile = 0
	c.batches = nil
	c.replayIdx = 0
	if c.Spill != nil && c.trackedMem > 0 {
		c.Spill.ReleaseTracking(c.trackedMem)
		c.trackedMem = 0
	}
}
