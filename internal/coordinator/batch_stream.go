package coordinator

import (
	"context"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// BatchStream is a lazy, single-consumer iterator over result batches.
// Next returns (nil, nil) when exhausted. Implementations drop their
// reference to each batch as it is handed out, so a consumer that sends
// and releases batch-by-batch keeps peak residency at one batch plus
// whatever the producer still buffers.
//
// Close releases everything still held — buffered batches and, for
// spill-backed streams, replay scratch on disk. It is idempotent and must
// be called by whoever owns the stream, including on error and early-exit
// paths: an undrained spill-backed stream pins its scratch file until
// Close runs.
type BatchStream interface {
	Next(ctx context.Context) (*batch.RecordBatch, error)
	Close() error
}

// NewSliceStream adapts a materialized batch slice to BatchStream. The
// stream takes ownership of the slice (entries are nil'd as yielded).
func NewSliceStream(batches []*batch.RecordBatch) BatchStream {
	return &sliceStream{batches: batches}
}

// sliceStream adapts a materialized batch slice to BatchStream.
type sliceStream struct {
	batches []*batch.RecordBatch
	idx     int
}

func newSliceStream(batches []*batch.RecordBatch) *sliceStream {
	return &sliceStream{batches: batches}
}

func (s *sliceStream) Next(_ context.Context) (*batch.RecordBatch, error) {
	for s.idx < len(s.batches) {
		b := s.batches[s.idx]
		s.batches[s.idx] = nil
		s.idx++
		if b != nil {
			return b, nil
		}
	}
	s.batches = nil
	return nil, nil
}

func (s *sliceStream) Close() error {
	s.batches = nil
	return nil
}

// drainStream materializes the remainder of a stream into a slice. For use
// by consumers that genuinely need the full set at once (sort, re-aggregate
// with source back-pointers); streaming-capable consumers should iterate.
func drainStream(in BatchStream) ([]*batch.RecordBatch, error) {
	ctx := context.Background()
	var out []*batch.RecordBatch
	for {
		b, err := in.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return out, nil
		}
		out = append(out, b)
	}
}
