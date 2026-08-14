package worker

import (
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
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

// zstd pools for the WSHZ envelope (docs/design/exchange-zstd-wire.md).
// Level pinned to SpeedFastest — higher levels measured 0.1–1pp better
// ratio for 1.3–1.6× the CPU on real shuffle payloads (see
// BenchmarkWSHCCompressionCodecs). Concurrency pinned to 1: klauspost's
// internal parallelism spawns per-codec goroutines that defeat pooling,
// and upload-path parallelism already comes from concurrent upload jobs.
// Encoders/decoders are goroutine-bound while acquired — never share one
// across goroutines.
var (
	zstdWriterPool = sync.Pool{New: func() any {
		w, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1))
		if err != nil {
			panic("zstd encoder init: " + err.Error()) // static options; cannot fail
		}
		return w
	}}
	zstdReaderPool = sync.Pool{New: func() any {
		r, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic("zstd decoder init: " + err.Error()) // static options; cannot fail
		}
		return r
	}}
)

// acquireZstdWriter returns a pooled *zstd.Encoder attached to dst. The
// caller MUST call Close before releaseZstdWriter — Close flushes the
// final frame; without it the produced stream is truncated.
func acquireZstdWriter(dst io.Writer) *zstd.Encoder {
	w := zstdWriterPool.Get().(*zstd.Encoder)
	w.Reset(dst)
	return w
}

// releaseZstdWriter returns w to the pool after detaching it from its
// destination. Safe to call after w.Close() (Reset re-arms a closed
// encoder for reuse).
func releaseZstdWriter(w *zstd.Encoder) {
	w.Reset(nil)
	zstdWriterPool.Put(w)
}

// acquireZstdReader returns a pooled *zstd.Decoder attached to src.
func acquireZstdReader(src io.Reader) (*zstd.Decoder, error) {
	r := zstdReaderPool.Get().(*zstd.Decoder)
	if err := r.Reset(src); err != nil {
		zstdReaderPool.Put(r)
		return nil, err
	}
	return r, nil
}

// releaseZstdReader returns r to the pool after detaching it from src.
// Reset(nil) parks the decoder without Close-ing it (Close would retire
// it permanently).
func releaseZstdReader(r *zstd.Decoder) {
	_ = r.Reset(nil) // nil input parks the decoder; error is by-design
	zstdReaderPool.Put(r)
}
