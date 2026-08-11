package parquet

import (
	"sync"
	"sync/atomic"
)

// Pooled staging buffers for pread-mode column-chunk reads
// (docs/design/scan-pread-reads.md). A staged ColumnPageReader reads its
// whole column chunk — typically hundreds of KB to tens of MB — into one
// of these buffers with a single ranged read, decodes from it, and
// returns it on Close. Pooling matters here for the same reason as the
// zstd pool one layer down: every projected column of every row group
// takes one buffer, so an unpooled path would allocate the entire scan
// byte volume through the heap and turn the GC assist/pause budget into
// a function of scan throughput.
//
// Size classes are powers of two so a buffer serves any request up to
// its capacity; requests beyond chunkPoolMaxCap are allocated fresh and
// never pooled, keeping outlier chunks from pinning giant buffers.

const (
	chunkPoolMinCap = 64 << 10  // smallest pooled class
	chunkPoolMaxCap = 32 << 20  // largest pooled class; bigger chunks bypass the pool
)

// chunkPools[i] holds buffers of capacity chunkPoolMinCap << i.
var chunkPools = func() []*sync.Pool {
	var ps []*sync.Pool
	for c := chunkPoolMinCap; c <= chunkPoolMaxCap; c <<= 1 {
		c := c
		ps = append(ps, &sync.Pool{New: func() any {
			preadAllocs.Add(1)
			b := make([]byte, c)
			return &b
		}})
	}
	return ps
}()

// Staged-read engagement counters (wlog markers; see PreadStats).
var (
	preadChunks atomic.Int64 // column chunks staged via ranged read
	preadBytes  atomic.Int64 // bytes those reads covered
	preadAllocs atomic.Int64 // fresh buffer allocations (pool misses + oversized)
)

// PreadStats returns the staged-read engagement counters: column chunks
// staged, bytes read, and fresh buffer allocations. Deltas near zero for
// allocs with chunks climbing mean the pool is doing its job.
func PreadStats() (chunks, bytes, allocs int64) {
	return preadChunks.Load(), preadBytes.Load(), preadAllocs.Load()
}

// chunkClass returns the pool index for a buffer of at least n bytes, or
// -1 when n exceeds the largest pooled class.
func chunkClass(n int) int {
	c := chunkPoolMinCap
	for i := range chunkPools {
		if n <= c {
			return i
		}
		c <<= 1
	}
	return -1
}

// getChunkBuf returns a buffer with len == n. Oversized requests are
// allocated fresh and will be dropped to GC on putChunkBuf.
func getChunkBuf(n int) []byte {
	if i := chunkClass(n); i >= 0 {
		bp := chunkPools[i].Get().(*[]byte)
		return (*bp)[:n]
	}
	preadAllocs.Add(1)
	return make([]byte, n)
}

// putChunkBuf returns a buffer obtained from getChunkBuf. Buffers whose
// capacity is not an exact pooled class (oversized allocations) are
// dropped.
func putChunkBuf(b []byte) {
	c := cap(b)
	if c < chunkPoolMinCap || c > chunkPoolMaxCap || c&(c-1) != 0 {
		return
	}
	if i := chunkClass(c); i >= 0 {
		b = b[:c]
		chunkPools[i].Put(&b)
	}
}
