package coordinator

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// maxGatherFrameBytes sanity-bounds a replayed frame's declared length.
// Frames are single GatherBatchMsg payloads, themselves bounded by the
// NATS payload limit (8 MB default); anything larger means scratch
// corruption and must fail rather than allocate unbounded.
const maxGatherFrameBytes = 64 << 20

// gatherReplayStream is the lazy BatchStream behind an over-budget gather
// result: the decoded in-memory prefix first (references dropped as they
// are handed out), then the raw WSHF payload frames replayed one at a time
// from the scratch file. The scratch file is deleted the moment it is
// exhausted — or on Close, whichever comes first. Single consumer, not
// goroutine-safe (same contract as BatchStream).
type gatherReplayStream struct {
	prefix  []*batch.RecordBatch
	idx     int
	path    string
	f       *os.File
	rd      *bufio.Reader
	pending []*batch.RecordBatch // decoded batches from the current frame
	pendIdx int
	renamer *batchRenamer // applied to replayed batches; prefix is pre-applied
	frame   []byte        // reusable frame read buffer
}

func newGatherReplayStream(prefix []*batch.RecordBatch, path string, renamer *batchRenamer) *gatherReplayStream {
	return &gatherReplayStream{prefix: prefix, path: path, renamer: renamer}
}

func (s *gatherReplayStream) Next(_ context.Context) (*batch.RecordBatch, error) {
	// In-memory prefix first.
	for s.idx < len(s.prefix) {
		b := s.prefix[s.idx]
		s.prefix[s.idx] = nil
		s.idx++
		if b != nil {
			return b, nil
		}
	}
	s.prefix = nil
	for {
		// Hand out the rest of the current frame's batches.
		for s.pendIdx < len(s.pending) {
			b := s.pending[s.pendIdx]
			s.pending[s.pendIdx] = nil
			s.pendIdx++
			if b != nil {
				return b, nil
			}
		}
		s.pending = nil
		s.pendIdx = 0

		if s.path == "" {
			return nil, nil // exhausted (or never spilled)
		}
		if s.f == nil {
			f, err := os.Open(s.path)
			if err != nil {
				return nil, fmt.Errorf("opening gather scratch: %w", err)
			}
			s.f = f
			s.rd = bufio.NewReaderSize(f, 1<<16)
		}
		var hdr [4]byte
		if _, err := io.ReadFull(s.rd, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				s.dropScratch()
				return nil, nil
			}
			return nil, fmt.Errorf("reading gather scratch frame header: %w", err)
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n == 0 || n > maxGatherFrameBytes {
			return nil, fmt.Errorf("gather scratch corrupt: frame length %d", n)
		}
		if cap(s.frame) < int(n) {
			s.frame = make([]byte, n)
		}
		s.frame = s.frame[:n]
		if _, err := io.ReadFull(s.rd, s.frame); err != nil {
			return nil, fmt.Errorf("reading gather scratch frame: %w", err)
		}
		decoded, err := readShuffleBatches(s.frame)
		if err != nil {
			return nil, fmt.Errorf("decoding gather scratch frame: %w", err)
		}
		if s.renamer != nil {
			for i, b := range decoded {
				decoded[i] = s.renamer.apply(b)
			}
		}
		s.pending = decoded
	}
}

// dropScratch closes and removes the scratch file.
func (s *gatherReplayStream) dropScratch() {
	if s.f != nil {
		s.f.Close()
		s.f = nil
		s.rd = nil
	}
	if s.path != "" {
		os.Remove(s.path)
		s.path = ""
	}
}

// Close releases the prefix, pending batches, and scratch file. Idempotent.
func (s *gatherReplayStream) Close() error {
	s.prefix = nil
	s.pending = nil
	s.dropScratch()
	return nil
}
