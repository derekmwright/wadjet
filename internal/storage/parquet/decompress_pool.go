package parquet

import "sync"

// zstd.DecodeAll always allocates a fresh output slice when its dst arg can't
// hold the decompressed payload. Every parquet page goes through this path,
// and the SF100 22Q worker CPU profile after PR #103 (s2 codec pool) ranked
// `zstd.sequenceDecs.decodeSync` at 14.3 % cum — roughly 2× the residual s2
// share. This pool reuses the output buffer across decompress calls so the
// per-page allocation falls off the profile.
//
// Lifetime contract: the caller of Decompress receives a slice whose backing
// array MAY come from this pool. PageData records the buffer and exposes
// Release() so the per-page consumer loop can return it after the values have
// been copied into their destination Vector. If Release is not called, the
// buffer is simply GC'd — correctness is preserved, the perf win is lost.

var zstdBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64*1024)
		return &b
	},
}

// Cap pool-resident buffers at 4 MiB. Parquet pages are typically <=1 MiB; a
// runaway page that exceeds this stays out of the pool so a single outlier
// can't pin a multi-megabyte buffer indefinitely.
const zstdBufPoolMaxCap = 4 * 1024 * 1024

func getZstdBuf(size int) []byte {
	bp := zstdBufPool.Get().(*[]byte)
	b := (*bp)[:0]
	if cap(b) < size {
		// Pool entry too small — let zstd.DecodeAll grow the slice itself
		// (it picks a sensible growth factor). Return the small one to the
		// pool so it stays usable for smaller pages.
		zstdBufPool.Put(bp)
		return make([]byte, 0, size)
	}
	return b
}

func putZstdBuf(b []byte) {
	if cap(b) == 0 || cap(b) > zstdBufPoolMaxCap {
		return
	}
	b = b[:0]
	zstdBufPool.Put(&b)
}
