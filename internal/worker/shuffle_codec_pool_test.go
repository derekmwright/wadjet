package worker

import (
	"bytes"
	"crypto/rand"
	"io"
	"sync"
	"testing"
)

// TestS2WriterPoolRoundTrip confirms the writer pool produces output that
// round-trips through the reader pool, across many sequential reuses of
// the same pooled instance. Catches any state bleed across Reset boundaries.
func TestS2WriterPoolRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		{"small_1KB", 1024},
		{"medium_64KB", 64 * 1024},
		{"large_4MB", 4 * 1024 * 1024},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for iter := 0; iter < 5; iter++ {
				src := make([]byte, tc.size)
				if _, err := rand.Read(src); err != nil {
					t.Fatalf("rand: %v", err)
				}

				var enc bytes.Buffer
				w := acquireS2Writer(&enc)
				if _, err := w.Write(src); err != nil {
					t.Fatalf("write iter %d: %v", iter, err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("close iter %d: %v", iter, err)
				}
				releaseS2Writer(w)

				r := acquireS2Reader(bytes.NewReader(enc.Bytes()))
				got, err := io.ReadAll(r)
				if err != nil {
					t.Fatalf("decode iter %d: %v", iter, err)
				}
				releaseS2Reader(r)

				if !bytes.Equal(got, src) {
					t.Fatalf("iter %d: round-trip mismatch (%d → %d → %d bytes)",
						iter, tc.size, enc.Len(), len(got))
				}
			}
		})
	}
}

// TestS2PoolConcurrent exercises the pool from many goroutines simultaneously
// to confirm a worker holding one instance never observes another's state.
func TestS2PoolConcurrent(t *testing.T) {
	const goroutines = 16
	const itersPerG = 32
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed byte) {
			defer wg.Done()
			payload := bytes.Repeat([]byte{seed}, 8192)
			for i := 0; i < itersPerG; i++ {
				var buf bytes.Buffer
				w := acquireS2Writer(&buf)
				if _, err := w.Write(payload); err != nil {
					errs <- err
					return
				}
				if err := w.Close(); err != nil {
					errs <- err
					return
				}
				releaseS2Writer(w)

				r := acquireS2Reader(bytes.NewReader(buf.Bytes()))
				got, err := io.ReadAll(r)
				releaseS2Reader(r)
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(got, payload) {
					errs <- io.ErrUnexpectedEOF
					return
				}
			}
		}(byte(g + 1))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent round-trip: %v", err)
		}
	}
}

// BenchmarkCompressShuffleDataPool measures the steady-state allocation
// reduction from the pool on the hot shuffle-write path. Compare before/
// after a pool change with `benchstat`.
func BenchmarkCompressShuffleDataPool(b *testing.B) {
	src := make([]byte, 1<<20) // 1 MB compressible payload
	for i := range src {
		src[i] = byte(i % 251)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := CompressShuffleData(src)
		if len(out) == 0 {
			b.Fatal("empty output")
		}
	}
}

// BenchmarkDecompressShuffleDataPool measures the decompress side.
func BenchmarkDecompressShuffleDataPool(b *testing.B) {
	src := make([]byte, 1<<20)
	for i := range src {
		src[i] = byte(i % 251)
	}
	compressed := CompressShuffleData(src)
	if string(compressed[:4]) != "WSHC" {
		b.Fatalf("input not compressed (skipped threshold); got magic %q", compressed[:4])
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := DecompressShuffleData(compressed)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) != len(src) {
			b.Fatalf("len mismatch: got %d want %d", len(out), len(src))
		}
	}
}
