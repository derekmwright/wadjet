package parquet

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
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

// deepCopyForSnapshot makes a value safe to retain across a pool poison: any
// reference type that *could* alias a pooled decompression buffer is copied so
// the snapshot is immune to later corruption. Scalars are returned as-is.
func deepCopyForSnapshot(v any) any {
	switch t := v.(type) {
	case []byte:
		cp := make([]byte, len(t))
		copy(cp, t)
		return cp
	case string:
		return string([]byte(t))
	case []int64:
		cp := make([]int64, len(t))
		copy(cp, t)
		return cp
	case []float64:
		cp := make([]float64, len(t))
		copy(cp, t)
		return cp
	default:
		return v
	}
}

// TestZstdPoolPoison_ReaderAlias is the premature-release regression guard for
// the zstd decompress-buffer pool. It reads a zstd-compressed parquet file
// fully via the public reader, snapshots every value, then deterministically
// poisons the pool by overwriting the full capacity of every released buffer
// with 0xFF. If any consumer retained a slice that still aliases a pooled
// buffer past page.Release(), its bytes now read sentinel and the snapshot
// comparison fails. On correct (copy-out) code the snapshot is unchanged.
//
// This lives in the parquet package on purpose: only here can the test reach
// zstdBufPool directly to force a deterministic poison, and ReadRows runs
// single-goroutine so every released page buffer is resident in one pool when
// poisonPool drains it — no scheduling or sync.Pool timing luck.
func TestZstdPoolPoison_ReaderAlias(t *testing.T) {
	const nRows = 5000
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},       // PLAIN INT64 — Int64() unsafe-aliases rawBuf
			{Name: "i32", Type: TypeInt32},      // PLAIN INT32 — Int32() unsafe-aliases rawBuf
			{Name: "amount", Type: TypeFloat64}, // PLAIN DOUBLE — Double() aliases rawBuf
			{Name: "name", Type: TypeString},    // BYTE_ARRAY — exercises the string path
		},
	}
	rows := make([]map[string]any, nRows)
	for i := 0; i < nRows; i++ {
		rows[i] = map[string]any{
			"id":     int64(i)*1_000_003 + 7,
			"i32":    int32(i*31 - 5),
			"amount": float64(i) * 1.5,
			"name":   fmt.Sprintf("row-%08d-payload-value", i),
		}
	}

	// Force zstd compression and a small page buffer so the columns span
	// several pages — each page is its own decompress + Release cycle.
	cfg := DefaultWriterConfig()
	cfg.Compression = CompressionZstd
	cfg.PageBufferSize = 8 * 1024
	cfg.RowGroupSize = 1 << 20

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data := buf.Bytes()

	reader, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	got, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(got) != nRows {
		t.Fatalf("row count = %d, want %d", len(got), nRows)
	}

	// Snapshot every value with a deep copy, BEFORE poisoning the pool.
	snap := make([]map[string]any, len(got))
	for i, r := range got {
		m := make(map[string]any, len(r))
		for k, v := range r {
			m[k] = deepCopyForSnapshot(v)
		}
		snap[i] = m
	}

	poisonPool(t)

	for i := range got {
		if !reflect.DeepEqual(got[i], snap[i]) {
			t.Fatalf("row %d corrupted after pool poison — a consumer retained a "+
				"pool-aliased reference past Release:\n got=%#v\nwant=%#v",
				i, got[i], snap[i])
		}
	}

	// A clean re-read must also match the snapshot (the file itself is intact).
	reader2, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("NewReaderFromBytes (reread): %v", err)
	}
	reread, err := reader2.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows (reread): %v", err)
	}
	for i := range snap {
		if !reflect.DeepEqual(reread[i], snap[i]) {
			t.Fatalf("row %d differs on clean re-read: got=%#v want=%#v",
				i, reread[i], snap[i])
		}
	}
}

// poisonPool deterministically clobbers every buffer resident in zstdBufPool:
// it drains the pool, fills each buffer's full capacity with 0xFF, then returns
// them. Any slice still aliasing a released buffer now reads sentinel bytes.
func poisonPool(t *testing.T) {
	t.Helper()
	const drainLimit = 8192
	drained := make([]*[]byte, 0, 128)
	fresh := 0
	for i := 0; i < drainLimit; i++ {
		bp := zstdBufPool.Get().(*[]byte)
		b := *bp
		full := b[:cap(b)]
		for j := range full {
			full[j] = 0xFF
		}
		drained = append(drained, bp)
		// Pool.New mints empty 64 KiB buffers; once we've pulled a short streak
		// of those we've drained all real (payload-bearing) entries.
		if cap(b) == 64*1024 {
			fresh++
			if fresh >= 16 {
				break
			}
		} else {
			fresh = 0
		}
	}
	for _, bp := range drained {
		b := (*bp)[:0]
		zstdBufPool.Put(&b)
	}
}
