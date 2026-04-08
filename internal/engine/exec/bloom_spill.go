package exec

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// SpillBloomSink is a Sink that streams its input batches straight to a
// columnar spill file while incrementally building a bloom filter from the
// values of one key column. It NEVER retains a reference to a consumed batch
// in memory — once a batch is hashed and written to disk, the sink drops it.
//
// This is the memory fix for reverseBloomBridge: previously the bridge ran
// its child probe pipeline into a BatchSink that buffered every batch in
// memory, then built a bloom from those buffered batches, then handed them
// to the next operator one at a time. For SF100 Q05 the buffered probe-side
// (lineitem after filter) was tens of GB live in heap. With this sink the
// buffered set is replaced by a sequential file on local disk and only one
// batch is in memory at a time.
//
// After Finalize, callers read back the batches via SpillBatchSource (or
// open the spill file path directly with OpenSpillBatchSource).
type SpillBloomSink struct {
	keyCol string

	mu        sync.Mutex
	writer    *spillBatchWriter
	spillPath string
	closed    bool

	// Bloom state. The bloom is sized once on the first batch using the
	// caller-provided estimate, then bits are set incrementally on every
	// consumed batch. We DELIBERATELY don't grow the bloom dynamically:
	// rehashing a bit-set bloom is not safe (we'd need to remember every
	// inserted key), and a slightly under-sized bloom just produces more
	// false positives, which only costs probe-side filter throughput, not
	// correctness.
	estRows   int
	bloom     []uint64
	bloomMask uint64
	rowsSeen  int
}

// NewSpillBloomSink creates a sink that writes consumed batches to a new
// spill file under dir/prefix and incrementally builds a bloom filter from
// keyCol. estRows should be the planner's row-count estimate for the probe
// side; it sizes the bloom once up front (≈10 bits/row, ~1% FPR).
func NewSpillBloomSink(dir, prefix, keyCol string, estRows int) (*SpillBloomSink, error) {
	w, err := newSpillBatchWriter(dir, prefix)
	if err != nil {
		return nil, fmt.Errorf("creating spill bloom sink: %w", err)
	}
	return &SpillBloomSink{
		keyCol:  keyCol,
		writer:  w,
		estRows: estRows,
	}, nil
}

func (s *SpillBloomSink) Init(_ context.Context) error { return nil }

func (s *SpillBloomSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	if b == nil || b.ActiveLen() == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("spill bloom sink already closed")
	}

	// Lazy-allocate the bloom on first batch using the planner's estimate.
	if s.bloom == nil {
		estRows := s.estRows
		if estRows < b.ActiveLen() {
			estRows = b.ActiveLen()
		}
		nSlots := 1
		for nSlots*64 < estRows*10 {
			nSlots *= 2
		}
		if nSlots < 8 {
			nSlots = 8
		}
		s.bloom = make([]uint64, nSlots)
		s.bloomMask = uint64(nSlots - 1)
	}

	// Hash the key column into the bloom.
	colIdx := b.ColumnIndex(s.keyCol)
	if colIdx >= 0 {
		col := b.Columns[colIdx]
		isInt := col.Type == batch.TypeInt32 || col.Type == batch.TypeInt64 ||
			col.Type == batch.TypePort || col.Type == batch.TypeProtocol ||
			col.Type == batch.TypeDate

		setBit := func(hash uint64) {
			h1 := hash & s.bloomMask
			h2 := (hash >> 17) & s.bloomMask
			b1 := hash & 63
			b2 := (hash >> 6) & 63
			s.bloom[h1] |= 1 << b1
			s.bloom[h2] |= 1 << b2
		}

		if b.Sel != nil {
			for _, si := range b.Sel {
				row := int(si)
				if col.Nulls.IsNull(row) {
					continue
				}
				if isInt {
					setBit(bloomHashInt(intValFromCol(col, row)))
				} else {
					setBit(bloomHashBytes(col.BytesData.Value(row)))
				}
			}
		} else {
			for row := 0; row < b.Len; row++ {
				if col.Nulls.IsNull(row) {
					continue
				}
				if isInt {
					setBit(bloomHashInt(intValFromCol(col, row)))
				} else {
					setBit(bloomHashBytes(col.BytesData.Value(row)))
				}
			}
		}
	}

	// Write the batch to disk and drop our reference. writeColumnarBatch
	// ignores Sel, so we MUST compact first when a selection vector is
	// present — otherwise the round-trip silently un-filters the batch and
	// downstream operators see all of the source rows. (This was the SF100
	// Q05 17x revenue regression: lineitem-side filters had Sel set, the
	// spill wrote everything, and the join produced ~17x the expected rows.)
	toWrite := b
	if b.Sel != nil {
		toWrite = b.Compact()
	}
	if err := s.writer.writeBatch(toWrite); err != nil {
		return fmt.Errorf("spill bloom sink write: %w", err)
	}
	s.rowsSeen += b.ActiveLen()
	return nil
}

func (s *SpillBloomSink) Finalize(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	path, err := s.writer.close()
	if err != nil {
		return fmt.Errorf("spill bloom sink finalize: %w", err)
	}
	s.spillPath = path
	return nil
}

// Close removes the spill file. Call after the source is done reading it.
func (s *SpillBloomSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.spillPath != "" {
		_ = os.Remove(s.spillPath)
		s.spillPath = ""
	}
	return nil
}

// Bloom returns the bloom filter built from consumed batches' keyCol.
// Returns (nil, 0) if no rows were consumed.
func (s *SpillBloomSink) Bloom() (bloom []uint64, mask uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rowsSeen == 0 {
		return nil, 0
	}
	return s.bloom, s.bloomMask
}

// SpillPath returns the path of the on-disk spill file (empty if no rows
// were consumed). Valid only after Finalize.
func (s *SpillBloomSink) SpillPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spillPath
}

// RowsSeen returns the total number of (active) rows consumed.
func (s *SpillBloomSink) RowsSeen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rowsSeen
}

// SpillBatchSource is a Source that streams batches back from a spill file
// written by SpillBloomSink (or any spillBatchWriter). One batch is read
// from disk per Next call.
type SpillBatchSource struct {
	path string
	mu   sync.Mutex
	r    *spillBatchReader
}

// OpenSpillBatchSource returns a Source that reads batches from a spill
// file. The file is opened lazily on Init.
func OpenSpillBatchSource(path string) *SpillBatchSource {
	return &SpillBatchSource{path: path}
}

func (s *SpillBatchSource) Init(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.path == "" {
		// No spill file → empty source.
		return nil
	}
	r, err := openSpillBatchReader(s.path)
	if err != nil {
		return err
	}
	s.r = r
	return nil
}

func (s *SpillBatchSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.r == nil {
		return nil, nil
	}
	return s.r.Next()
}

func (s *SpillBatchSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.r != nil {
		err := s.r.Close()
		s.r = nil
		return err
	}
	return nil
}
