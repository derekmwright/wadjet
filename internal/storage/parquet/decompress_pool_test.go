package parquet

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func zstdEncode(t testing.TB, src []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	defer enc.Close()
	return enc.EncodeAll(src, nil)
}

// Pooled zstd decompress must produce bit-identical output to a fresh
// allocation, including across repeated calls that hit / miss the pool size
// class. Mirrors the round-trip invariant validated for PR #103's s2 pool.
func TestZstdBufPool_RoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	sizes := []int{0, 1, 32, 1024, 4096, 64*1024, 256*1024, 1024*1024}

	for _, size := range sizes {
		src := make([]byte, size)
		rng.Read(src)
		enc := zstdEncode(t, src)

		for i := 0; i < 4; i++ {
			got, err := decompressZstd(enc, size)
			if err != nil {
				t.Fatalf("size=%d iter=%d: %v", size, i, err)
			}
			if !bytes.Equal(got, src) {
				t.Fatalf("size=%d iter=%d: output mismatch", size, i)
			}
			putZstdBuf(got)
		}
	}
}

// Concurrent decompresses must not see cross-pollution from sibling
// goroutines' output buffers. This is the regression guard for the case where
// two callers Get the same backing array from the pool.
func TestZstdBufPool_Concurrent(t *testing.T) {
	const workers = 16
	const iters = 64

	payloads := make([][]byte, workers)
	encoded := make([][]byte, workers)
	rng := rand.New(rand.NewSource(42))
	for i := range payloads {
		size := 1024 + rng.Intn(64*1024)
		payloads[i] = make([]byte, size)
		rng.Read(payloads[i])
		encoded[i] = zstdEncode(t, payloads[i])
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				got, err := decompressZstd(encoded[i], len(payloads[i]))
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(got, payloads[i]) {
					errs <- &mismatchErr{worker: i, iter: j}
					putZstdBuf(got)
					return
				}
				putZstdBuf(got)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}

type mismatchErr struct{ worker, iter int }

func (e *mismatchErr) Error() string {
	return "concurrent decompress mismatch"
}

func BenchmarkDecompressZstd(b *testing.B) {
	// 64 KiB random-ish payload (compresses moderately) — roughly mid-range
	// for TPC-H parquet pages.
	rng := rand.New(rand.NewSource(7))
	src := make([]byte, 64*1024)
	for i := range src {
		src[i] = byte(rng.Intn(64))
	}
	enc := zstdEncode(b, src)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got, err := decompressZstd(enc, len(src))
		if err != nil {
			b.Fatal(err)
		}
		putZstdBuf(got)
	}
}

// putZstdBuf must drop oversized buffers so a single anomalous page can't
// pin a huge allocation in the pool indefinitely.
func TestZstdBufPool_RejectsOversized(t *testing.T) {
	huge := make([]byte, 0, zstdBufPoolMaxCap+1)
	putZstdBuf(huge) // must not panic, must not retain

	// Drain the pool a few times; we expect the new function (64 KB) to win
	// out, never the huge one.
	for i := 0; i < 8; i++ {
		bp := zstdBufPool.Get().(*[]byte)
		if cap(*bp) > zstdBufPoolMaxCap {
			t.Fatalf("oversized buffer leaked into pool: cap=%d", cap(*bp))
		}
		zstdBufPool.Put(bp)
	}
}

// PageData.Release must be safe to call multiple times and on a zero value.
func TestPageDataRelease_Idempotent(t *testing.T) {
	var p *PageData
	p.Release() // nil receiver

	p = &PageData{}
	p.Release() // no rawBuf

	buf := getZstdBuf(1024)
	p = &PageData{rawBuf: buf, codec: CodecZstd}
	p.Release()
	p.Release() // double-release must not double-put
}
