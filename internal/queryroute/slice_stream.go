package queryroute

import (
	"context"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// NewSliceStream adapts a materialized batch slice to Stream. The stream
// takes ownership of the slice — entries are dropped as they are yielded, so
// a consumer that sends and releases batch-by-batch keeps peak residency at
// one batch.
//
// Behaviour-for-behaviour the same as coordinator.NewSliceStream, which is
// the routed path's own: a nil entry is skipped rather than yielded, and the
// context is not consulted (the slice is already in memory; cancellation is
// the consumer's business).
func NewSliceStream(batches []*batch.RecordBatch) Stream {
	return &sliceStream{batches: batches}
}

type sliceStream struct {
	batches []*batch.RecordBatch
	idx     int
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
