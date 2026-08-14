// Package logio decouples log production from the log sink.
//
// Motivation (2026-08-14, frozen-spin/quiet-stall stall family): the
// worker logs to stderr, which in the benchmark deploy is a pipe into
// journald. When journald stalls, the 64KB pipe fills, the next
// slog write blocks WHILE HOLDING the handler mutex, and every
// goroutine that logs parks behind it — a whole-process freeze that
// self-heals in one burst when journald catches up (validation run
// 20260814-002639: 190s freeze on one worker, liveness markers stopped,
// coordinator saw a 36-result burst on recovery). Execution must never
// be gated on the log sink.
package logio

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncWriter is an io.Writer that hands records to a background
// drainer through a bounded channel. When the buffer is full, Write
// drops the record and counts it instead of blocking. Dropped totals
// are surfaced in-band by the drainer (at most once per second) so the
// log stream itself records the loss.
type AsyncWriter struct {
	sink     io.Writer
	ch       chan []byte
	dropped  atomic.Uint64
	reported uint64 // drainer-only: last dropped value surfaced
	closeMu  sync.RWMutex
	closed   bool
	done     chan struct{}
}

// NewAsyncWriter starts the drainer. depth is the buffered record
// count; at a few hundred bytes per record, 8192 records ≈ 2-3 MB and
// absorbs many minutes of normal log volume during a sink stall.
func NewAsyncWriter(sink io.Writer, depth int) *AsyncWriter {
	a := &AsyncWriter{
		sink: sink,
		ch:   make(chan []byte, depth),
		done: make(chan struct{}),
	}
	go a.drain()
	return a
}

// Write never blocks on the sink: it copies p (slog reuses its buffer)
// and enqueues, dropping on overflow. Always reports success to the
// caller — a dropped log line must not fail the operation that logged.
// The RLock guards the enqueue against a concurrent Close (send on
// closed channel); it is uncontended in steady state.
func (a *AsyncWriter) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	a.closeMu.RLock()
	if a.closed {
		a.closeMu.RUnlock()
		a.dropped.Add(1)
		return len(p), nil
	}
	select {
	case a.ch <- b:
	default:
		a.dropped.Add(1)
	}
	a.closeMu.RUnlock()
	return len(p), nil
}

// Dropped returns the number of records dropped so far.
func (a *AsyncWriter) Dropped() uint64 { return a.dropped.Load() }

// Close stops accepting writes, drains the buffer to the sink, and
// returns. Safe to call once; further Writes after Close are dropped.
func (a *AsyncWriter) Close() error {
	a.closeMu.Lock()
	if a.closed {
		a.closeMu.Unlock()
		return nil
	}
	a.closed = true
	close(a.ch)
	a.closeMu.Unlock()
	<-a.done
	return nil
}

func (a *AsyncWriter) drain() {
	defer close(a.done)
	var lastReport time.Time
	for b := range a.ch {
		_, _ = a.sink.Write(b)
		if d := a.dropped.Load(); d > a.reported && time.Since(lastReport) > time.Second {
			fmt.Fprintf(a.sink, "level=WARN msg=\"async log writer dropped records\" dropped_total=%d dropped_delta=%d\n",
				d, d-a.reported)
			a.reported = d
			lastReport = time.Now()
		}
	}
}
