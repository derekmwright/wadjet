package worker

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/diskio"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestDropBehindWalk_RealMadvise walks a real >16 MiB WSHF mmap with the
// single-pass drop-behind active and verifies (a) decoded data is intact —
// MADV_DONTNEED behind a strictly-forward cursor can never corrupt what
// the batches already copied out — and (b) the walk actually dropped
// windows (the steady-regime displacement fix engages).
func TestDropBehindWalk_RealMadvise(t *testing.T) {
	origDrop := diskio.DropBehindEnabled()
	diskio.SetDropBehindEnabled(true)
	t.Cleanup(func() { diskio.SetDropBehindEnabled(origDrop) })

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	// ~2300 chunks × 2048 int64 rows ≈ 37 MB — several drop windows.
	const numChunks = 2300
	var wantSum int64
	next := int64(0)
	bb := batch.NewRecordBatch(schema, batch.DefaultBatchSize)
	for c := 0; c < numChunks; c++ {
		for i := 0; i < batch.DefaultBatchSize; i++ {
			bb.Columns[0].Int64Data[i] = next
			wantSum += next
			next++
		}
		if err := sw.writeChunk(bb.Columns, nil, batch.DefaultBatchSize); err != nil {
			t.Fatal(err)
		}
	}
	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data[4:], numChunks)

	path := filepath.Join(t.TempDir(), "walk.wshf")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	mm, err := syscall.Mmap(int(f.Fd()), 0, len(data), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(mm)

	cr, err := newShuffleChunkReader(mm)
	if err != nil {
		t.Fatal(err)
	}
	s := &cachedFileStreamSource{
		chunkReader: cr,
		mmapData:    mm,
		localPath:   path, // owned temp — eligible for drop-behind
	}

	var gotSum int64
	var rows int
	for {
		b, err := cr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		s.dropBehindWalk()
		for i := 0; i < b.Len; i++ {
			gotSum += b.Columns[0].Int64Data[i]
		}
		rows += b.Len
	}

	if rows != numChunks*batch.DefaultBatchSize {
		t.Fatalf("rows = %d, want %d", rows, numChunks*batch.DefaultBatchSize)
	}
	if gotSum != wantSum {
		t.Fatalf("sum = %d, want %d — data corrupted behind the walk", gotSum, wantSum)
	}
	if s.walkDropMark < walkDropWindowBytes {
		t.Fatalf("walkDropMark = %d — drop-behind never engaged on a %d-byte walk",
			s.walkDropMark, len(data))
	}
}

// TestDropBehindWalk_SkipsUnownedFiles pins the ownership gate: a
// LocalStageCache-owned file (localPath == "") may be walked again by
// another consumer or peer-served, so the walk must never drop its pages.
func TestDropBehindWalk_SkipsUnownedFiles(t *testing.T) {
	origDrop := diskio.DropBehindEnabled()
	diskio.SetDropBehindEnabled(true)
	t.Cleanup(func() { diskio.SetDropBehindEnabled(origDrop) })

	// Anonymous mapping: page-aligned like the real thing, and harmless if
	// a regression ever lets the madvise through.
	mm, err := syscall.Mmap(-1, 0, 4*walkDropWindowBytes,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(mm)

	s := &cachedFileStreamSource{
		chunkReader: &shuffleChunkReader{pos: 3 * walkDropWindowBytes},
		mmapData:    mm,
		localPath:   "",
	}
	s.dropBehindWalk()
	if s.walkDropMark != 0 {
		t.Fatalf("walkDropMark = %d on an unowned file", s.walkDropMark)
	}
}

// TestWillNeedAdviserClampsAndCounts: the adviser must page-align, clamp
// to the mapping, ignore out-of-range spans, and count only advised
// bytes. Uses a real anonymous mapping so Madvise succeeds.
func TestWillNeedAdviserClampsAndCounts(t *testing.T) {
	const size = 1 << 20
	data, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		t.Fatalf("anon mmap: %v", err)
	}
	defer unix.Munmap(data)

	before := readaheadAdviseBytes.Load()
	adv := willNeedAdviser(data)
	pg := int64(os.Getpagesize())

	adv(pg+123, 1000)        // unaligned interior span → aligned down, counted
	adv(-5, 100)             // negative offset → ignored
	adv(int64(size), 10)     // past the end → ignored
	adv(int64(size)-50, 500) // tail overrun → clamped to mapping end
	adv(0, 0)                // empty → ignored

	got := readaheadAdviseBytes.Load() - before
	span1 := int64(123 + 1000)                              // start aligns down pg+123 → pg; end stays pg+1123
	span4 := int64(size) - ((int64(size) - 50) &^ (pg - 1)) // tail: aligned-down start to mapping end
	want := span1 + span4
	if got != want {
		t.Fatalf("advised bytes = %d, want %d (span1 %d + span4 %d)", got, want, span1, span4)
	}
}
