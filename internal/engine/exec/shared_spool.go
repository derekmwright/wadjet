// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// SharedSpool is ONE evaluation of a relation that several readers read, each
// at its own position, filled ON DEMAND — PostgreSQL's CTE Scan over a shared
// tuplestore (ADR-0021 §2d).
//
// The body is a pipeline (Source → Ops) that runs on its own goroutine into a
// sink that stores each batch and then WAITS until some reader asks for a
// batch the spool does not hold yet. So:
//
//   - the body is opened when the first reader first asks for a batch, never
//     when a reader is built: a spool nobody pulls never runs its body;
//   - a batch is computed only when a reader needs a row past what is stored:
//     a reader that stops early (LIMIT, EXISTS, a semi-join that found its
//     match) never forces rows nobody reads, and an error the body would
//     raise on a later row is never raised;
//   - the body's error is raised to the reader whose pull reached it, when it
//     reaches it — a reader still reading stored batches reads them first;
//   - every reader sees the same rows in the same order, whatever the order
//     the readers run in and however far each one reads.
//
// The stored batches are charged to the spill manager's tracker and written
// to run files past the budget, as SpillableBatchCollector's are; the files
// are immutable once written, so a reader reads them without the lock.
// Close ends the body (cancelling it if it is still running) and frees the
// runs and the charge; the owner calls it when the statement ends.
type SharedSpool struct {
	// Spill is the statement's spill manager (nil = in-memory only).
	Spill *memory.SpillManager
	// RunBytes, when >0, is the least a drain writes (see
	// SpillableBatchCollector.RunBytes).
	RunBytes int64
	// Source and Ops are the body's pipeline, built by the planner; the spool
	// owns them from here and closes them.
	Source Source
	Ops    []UnaryOperator

	mu      sync.Mutex
	changed chan struct{} // closed and replaced on every state change

	started bool // the body's goroutine was launched
	done    bool // the body ran to its end (err holds its error, if any)
	closed  bool
	err     error
	exited  chan struct{}
	cancel  context.CancelFunc

	want int // the most batches any reader has asked for
	n    int // batches stored
	rows int // rows stored

	mem      []*batch.RecordBatch // stored batches [memStart, n)
	memStart int
	runs     []spoolRun // stored batches [0, memStart), in order
	tracked  int64
}

type spoolRun struct {
	path         string
	start, count int
}

// errSpoolClosed is what a reader that outlives the statement reads.
var errSpoolClosed = errors.New("the shared evaluation was closed with the statement")

// NewReader returns a Source that reads the spool from its first batch.
func (s *SharedSpool) NewReader() *SpoolReader { return &SpoolReader{s: s} }

// Started reports whether the body was ever opened (a reader asked for a
// batch).
func (s *SharedSpool) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// Rows reports how many rows the body has produced so far.
func (s *SharedSpool) Rows() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows
}

// SpillRuns reports how many runs the spool has written to disk — the
// engagement a spill gate asserts.
func (s *SharedSpool) SpillRuns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.runs)
}

func (s *SharedSpool) signalLocked() {
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
}

func (s *SharedSpool) waitChanLocked() <-chan struct{} {
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

// startLocked launches the body. Its context keeps the first puller's values
// but not its cancellation: the evaluation is the statement's, and a reader
// that finishes (a LIMIT, an EXISTS) must not end it for the others. Close
// cancels it.
func (s *SharedSpool) startLocked(ctx context.Context) {
	s.started = true
	bctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel
	s.exited = make(chan struct{})
	go s.run(bctx)
}

func (s *SharedSpool) run(ctx context.Context) {
	defer close(s.exited)
	p := &Pipeline{Source: s.Source, Ops: s.Ops, Sink: &spoolSink{s: s}}
	err := p.Run(ctx)
	_ = p.Close()
	s.mu.Lock()
	s.done = true
	if !s.closed {
		s.err = err
	}
	s.signalLocked()
	s.mu.Unlock()
}

// store appends one batch and then waits until a reader asks for more than
// the spool holds (or the spool is closed).
func (s *SharedSpool) store(ctx context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errSpoolClosed
	}
	if b.ActiveLen() > 0 {
		FlattenForConsumer(b, nil) // retained past the batch cycle: views must not survive
		b.Detach()                 // the pipeline releases the batch after Consume
		if b.Sel != nil {
			b.Sel = append([]uint32(nil), b.Sel...)
		}
		s.mem = append(s.mem, b)
		s.n++
		s.rows += b.ActiveLen()
		if s.Spill != nil {
			cost := b.MemBytes()
			s.Spill.TrackBatch(cost)
			s.tracked += cost
			floor := minSortRunBytes
			if s.RunBytes > 0 && s.RunBytes < floor {
				floor = s.RunBytes
			}
			if s.Spill.ShouldSpillFor(memory.SpillCheap) && s.tracked >= floor {
				if err := s.drainLocked(); err != nil {
					s.mu.Unlock()
					return err
				}
			}
		}
		s.signalLocked()
	}
	for !s.closed && s.want <= s.n {
		ch := s.waitChanLocked()
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		s.mu.Lock()
	}
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errSpoolClosed
	}
	return nil
}

// drainLocked writes the in-memory batches to one run file and releases their
// charge. Caller holds s.mu.
func (s *SharedSpool) drainLocked() error {
	if len(s.mem) == 0 {
		return nil
	}
	sw, err := newSpillBatchWriter(s.Spill.SpillDir(), "cte-spool")
	if err != nil {
		return err
	}
	for _, b := range s.mem {
		if b.Sel != nil {
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
	s.runs = append(s.runs, spoolRun{path: path, start: s.memStart, count: len(s.mem)})
	s.memStart += len(s.mem)
	s.mem = nil
	s.Spill.ReleaseTracking(s.tracked)
	s.tracked = 0
	return nil
}

// Close ends the body and frees what the spool holds. Idempotent.
func (s *SharedSpool) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	started := s.started
	if s.cancel != nil {
		s.cancel()
	}
	s.signalLocked()
	s.mu.Unlock()
	if started {
		<-s.exited
	} else {
		// Never opened: the built body is still ours to close.
		_ = (&Pipeline{Source: s.Source, Ops: s.Ops, Sink: &spoolSink{s: s}}).Close()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	paths := make([]string, 0, len(s.runs))
	for _, r := range s.runs {
		paths = append(paths, r.path)
	}
	removeRunFiles(paths)
	s.runs = nil
	s.mem = nil
	if s.Spill != nil && s.tracked > 0 {
		s.Spill.ReleaseTracking(s.tracked)
		s.tracked = 0
	}
}

// spoolSink is the body pipeline's sink: it stores and waits for demand.
type spoolSink struct{ s *SharedSpool }

func (k *spoolSink) Init(context.Context) error { return nil }
func (k *spoolSink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	return k.s.store(ctx, b)
}
func (k *spoolSink) Finalize(context.Context) error { return nil }
func (k *spoolSink) Close() error                   { return nil }

// SpoolReader is one reader of a SharedSpool, at its own position. Next is
// mutex-guarded so a parallel pipeline can share one instance.
type SpoolReader struct {
	s *SharedSpool

	mu    sync.Mutex
	pos   int // the next batch this reader returns
	rd    *spillBatchReader
	rdRun int // index of the run rd reads
	rdPos int // the batch rd returns next
}

// Init rewinds the reader to the spool's first batch.
func (r *SpoolReader) Init(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeRunLocked()
	r.pos = 0
	return nil
}

func (r *SpoolReader) Next(ctx context.Context) (*batch.RecordBatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.s
	for {
		s.mu.Lock()
		if r.pos < s.n {
			if r.pos >= s.memStart {
				b := s.mem[r.pos-s.memStart]
				s.mu.Unlock()
				r.pos++
				// Shallow clone: shared column vectors (read-only by operator
				// contract), an own Sel so a filter's in-place assignment
				// cannot clobber the stored batch or another reader's.
				clone := &batch.RecordBatch{
					Schema:  b.Schema,
					Columns: append([]*batch.Vector(nil), b.Columns...),
					Len:     b.Len,
				}
				if b.Sel != nil {
					clone.Sel = append([]uint32(nil), b.Sel...)
				}
				return clone, nil
			}
			idx := 0
			for idx < len(s.runs) && r.pos >= s.runs[idx].start+s.runs[idx].count {
				idx++
			}
			run := s.runs[idx]
			s.mu.Unlock()
			b, err := r.readRunLocked(idx, run)
			if err != nil {
				return nil, err
			}
			r.pos++
			return b, nil
		}
		if s.done {
			err := s.err
			s.mu.Unlock()
			return nil, err
		}
		if s.closed {
			s.mu.Unlock()
			return nil, errSpoolClosed
		}
		if s.want < r.pos+1 {
			s.want = r.pos + 1
			s.signalLocked()
		}
		if !s.started {
			s.startLocked(ctx)
		}
		ch := s.waitChanLocked()
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// readRunLocked returns batch r.pos from run idx, reopening the run (and
// skipping to the position) when the open reader is not already there — the
// batches a reader read from memory may have been written to a run since.
func (r *SpoolReader) readRunLocked(idx int, run spoolRun) (*batch.RecordBatch, error) {
	if r.rd == nil || r.rdRun != idx || r.rdPos != r.pos {
		r.closeRunLocked()
		rd, err := openSpillBatchReader(run.path)
		if err != nil {
			return nil, err
		}
		r.rd, r.rdRun, r.rdPos = rd, idx, run.start
		for r.rdPos < r.pos {
			if _, err := r.rd.Next(); err != nil {
				return nil, err
			}
			r.rdPos++
		}
	}
	b, err := r.rd.Next()
	if err != nil {
		return nil, err
	}
	r.rdPos++
	if b == nil {
		return nil, errors.New("shared evaluation run ended before its recorded batch count")
	}
	return b, nil
}

func (r *SpoolReader) closeRunLocked() {
	if r.rd != nil {
		r.rd.Close()
		r.rd = nil
	}
}

func (r *SpoolReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeRunLocked()
	return nil
}
