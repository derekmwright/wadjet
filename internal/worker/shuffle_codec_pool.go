package worker

import (
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
)

// s2.Writer / s2.Reader carry sizable internal buffers (the encoder block
// buffer is ~64 KB; the decoder maintains its own scratch). Allocating a
// fresh codec for every shuffle compress/decompress call shows up at the
// top of the SF100 22Q profile — Q18 in particular spent ~25 % of CPU in
// shuffle-write under s2.encodeBlockGo plus its surrounding allocator
// pressure (see project_q18_cpu_profile_sf100_2026-05-22). The codec's
// own Reset(io.Writer)/Reset(io.Reader) APIs already let us recycle the
// internal state across calls; this pool puts the recycling in front of
// every caller in the shuffle hot path.

var (
	s2WriterPool = sync.Pool{New: func() any { return s2.NewWriter(nil) }}
	s2ReaderPool = sync.Pool{New: func() any { return s2.NewReader(nil) }}
)

// acquireS2Writer returns a pooled *s2.Writer attached to dst. The writer's
// internal block buffer is reused across calls, eliminating per-call
// allocation for the hot shuffle-write path. The caller MUST call Close
// before releaseS2Writer — Close flushes the final block; without it the
// produced stream is truncated.
func acquireS2Writer(dst io.Writer) *s2.Writer {
	w := s2WriterPool.Get().(*s2.Writer)
	w.Reset(dst)
	return w
}

// releaseS2Writer returns w to the pool after detaching it from its
// destination (Reset(nil)) so the pool never holds a reference to a
// caller's now-closed io.Writer. Safe to call after w.Close().
func releaseS2Writer(w *s2.Writer) {
	w.Reset(nil)
	s2WriterPool.Put(w)
}

// acquireS2Reader returns a pooled *s2.Reader attached to src. The reader's
// decompression scratch is reused across calls.
func acquireS2Reader(src io.Reader) *s2.Reader {
	r := s2ReaderPool.Get().(*s2.Reader)
	r.Reset(src)
	return r
}

// releaseS2Reader returns r to the pool after detaching it from src.
func releaseS2Reader(r *s2.Reader) {
	r.Reset(nil)
	s2ReaderPool.Put(r)
}
