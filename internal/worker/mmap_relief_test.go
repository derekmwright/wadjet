package worker

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// withReliefEnabled flips the relief flag on for a test and restores it + clears
// the registry afterward, so tests don't leak global state into each other.
func withReliefEnabled(t *testing.T) {
	t.Helper()
	prevEnabled := mmapReliefEnabled.Load()
	prevThresh := mmapReliefThresholdBytes.Load()
	SetMmapRelief(true, 1) // tiny threshold; tests drive relief directly
	t.Cleanup(func() {
		mmapReliefEnabled.Store(prevEnabled)
		mmapReliefThresholdBytes.Store(prevThresh)
		// drain any regions a test left registered
		mmapRegistry.mu.Lock()
		mmapRegistry.regions = make(map[*mmapRegion]struct{})
		mmapRegistry.mu.Unlock()
	})
}

func TestMmapRelief_DisabledIsNoOp(t *testing.T) {
	// With relief disabled (default), registerMmap returns nil and nothing is
	// tracked — the zero-cost dormant path.
	SetMmapRelief(false, 0)
	r := registerMmap([]byte("data"))
	if r != nil {
		t.Fatalf("registerMmap should return nil when disabled, got %v", r)
	}
	if got := liveMmapCount(); got != 0 {
		t.Fatalf("no regions should be tracked when disabled, got %d", got)
	}
	// touch on a nil region and relieveMmap when disabled are both safe no-ops.
	r.touch()
	if freed := relieveMmap(1 << 30); freed != 0 {
		t.Fatalf("relieveMmap should free 0 when disabled, got %d", freed)
	}
}

func TestMmapRelief_RegisterUnregister(t *testing.T) {
	withReliefEnabled(t)
	// Use real anonymous mmap-able slices so unix.Madvise has a valid address —
	// but for register/unregister bookkeeping plain slices suffice.
	a := registerMmap(make([]byte, 4096))
	b := registerMmap(make([]byte, 4096))
	if a == nil || b == nil {
		t.Fatal("registerMmap returned nil while enabled")
	}
	if got := liveMmapCount(); got != 2 {
		t.Fatalf("expected 2 tracked regions, got %d", got)
	}
	unregisterMmap(a)
	if got := liveMmapCount(); got != 1 {
		t.Fatalf("expected 1 after unregister, got %d", got)
	}
	unregisterMmap(a) // idempotent / nil-safe-ish (already removed)
	unregisterMmap(nil)
	unregisterMmap(b)
	if got := liveMmapCount(); got != 0 {
		t.Fatalf("expected 0 after unregistering all, got %d", got)
	}
}

func TestMmapRelief_TouchOrdersColdestFirst(t *testing.T) {
	withReliefEnabled(t)
	// relieveMmap sorts by lastTouch ascending (coldest first). Register three,
	// touch them in a known order, and assert the relief order via a custom
	// MADV that records instead of syscalling.
	r1 := registerMmap(make([]byte, 4096))
	r2 := registerMmap(make([]byte, 4096))
	r3 := registerMmap(make([]byte, 4096))
	// Make touch times strictly increasing and distinct.
	r1.lastTouch.Store(100)
	r2.lastTouch.Store(300)
	r3.lastTouch.Store(200)
	// Coldest-first order should be r1(100), r3(200), r2(300).
	order := sortRegionsByTouch()
	if len(order) != 3 || order[0] != r1 || order[1] != r3 || order[2] != r2 {
		t.Fatalf("coldest-first order wrong: got %v want [r1 r3 r2]", order)
	}
}

// TestMmapRelief_RealMadvise exercises the actual MADV_DONTNEED syscall on a
// real file mapping (mirroring the production PROT_READ MAP_SHARED path) and
// confirms relief reports the bytes freed and the data still reads correctly
// after the advise (pages re-fault from the backing file).
func TestMmapRelief_RealMadvise(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("MADV_DONTNEED path is Linux-only")
	}
	withReliefEnabled(t)

	// Create a backing file with known contents, larger than a page.
	const size = 64 * 4096
	path := filepath.Join(t.TempDir(), "mmap-test")
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer syscall.Munmap(data)

	r := registerMmap(data)
	if r == nil {
		t.Fatal("registerMmap returned nil while enabled")
	}

	freed := relieveMmap(size)
	if freed < size {
		t.Fatalf("relieveMmap freed %d, want >= %d", freed, size)
	}
	// Data must still be correct after MADV_DONTNEED (re-faulted from the file).
	for i := 0; i < size; i += 4096 {
		if data[i] != byte(i%251) {
			t.Fatalf("data corrupted after relief at %d: got %d want %d", i, data[i], byte(i%251))
		}
	}
	unregisterMmap(r)
}

func TestMmapRelief_ConcurrentRegisterTouch(t *testing.T) {
	withReliefEnabled(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := registerMmap(make([]byte, 4096))
			for j := 0; j < 100; j++ {
				r.touch()
			}
			unregisterMmap(r)
		}()
	}
	// concurrent reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 100; j++ {
			_ = liveMmapCount()
		}
	}()
	wg.Wait()
	if got := liveMmapCount(); got != 0 {
		t.Fatalf("all regions should be unregistered, got %d", got)
	}
}

// TestMmapRelief_ConsumedPrefixSelfRelief: a forward walker's consumed prefix
// is advised away in page-aligned hysteresis steps, the data still reads back
// correctly (file-backed re-fault), and the global relief pass credits only
// the remaining live bytes.
func TestMmapRelief_ConsumedPrefixSelfRelief(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("MADV_DONTNEED path is Linux-only")
	}
	withReliefEnabled(t)

	// Lower the hysteresis to 4 pages so the test file stays small.
	oldChunk := mmapSelfReliefChunkBytes
	mmapSelfReliefChunkBytes = 4 * mmapPageSize
	defer func() { mmapSelfReliefChunkBytes = oldChunk }()

	size := int(16 * mmapPageSize)
	path := filepath.Join(t.TempDir(), "mmap-prefix-test")
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer syscall.Munmap(data)
	r := registerMmap(data)
	defer unregisterMmap(r)

	// nil receiver (relief disabled path) is a no-op.
	var nilRegion *mmapRegion
	if got := nilRegion.relieveConsumedPrefix(1 << 30); got != 0 {
		t.Fatalf("nil region relieved %d bytes", got)
	}

	// Below the hysteresis step: nothing advised.
	if got := r.relieveConsumedPrefix(3 * mmapPageSize); got != 0 {
		t.Fatalf("sub-chunk consumed relieved %d bytes, want 0", got)
	}
	if r.relieved.Load() != 0 {
		t.Fatalf("relieved offset moved to %d without a full step", r.relieved.Load())
	}

	// Cursor mid-page past one full step: relief floors to the page boundary.
	consumed := 5*mmapPageSize + 123
	if got := r.relieveConsumedPrefix(consumed); got != 5*mmapPageSize {
		t.Fatalf("first step relieved %d, want %d", got, 5*mmapPageSize)
	}
	if r.relieved.Load() != 5*mmapPageSize {
		t.Fatalf("relieved offset = %d, want %d", r.relieved.Load(), 5*mmapPageSize)
	}

	// Re-advising the same cursor: no second step (monotonic + hysteresis).
	if got := r.relieveConsumedPrefix(consumed); got != 0 {
		t.Fatalf("repeat call relieved %d bytes, want 0", got)
	}

	// Cursor past EOF clamps to the page-aligned region end.
	if got := r.relieveConsumedPrefix(int64(size) + 999); got != 11*mmapPageSize {
		t.Fatalf("EOF clamp relieved %d, want %d", got, 11*mmapPageSize)
	}

	// Every byte must still read back correctly (re-fault from file).
	for i := 0; i < size; i++ {
		if data[i] != byte(i%251) {
			t.Fatalf("data corrupted after prefix relief at %d", i)
		}
	}

	// Global relief accounting: the region is fully self-relieved
	// (liveBytes < page), so the pass must skip it and credit nothing.
	if r.liveBytes() >= mmapPageSize {
		t.Fatalf("liveBytes = %d, want < page", r.liveBytes())
	}
	if freed := relieveMmap(1 << 30); freed != 0 {
		t.Fatalf("global relief freed %d from a drained region, want 0", freed)
	}
}

// TestMmapRelief_GlobalReliefCreditsLiveBytesOnly: a half-self-relieved
// region credits only its remaining live bytes toward the global target.
func TestMmapRelief_GlobalReliefCreditsLiveBytesOnly(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("MADV_DONTNEED path is Linux-only")
	}
	withReliefEnabled(t)
	oldChunk := mmapSelfReliefChunkBytes
	mmapSelfReliefChunkBytes = 4 * mmapPageSize
	defer func() { mmapSelfReliefChunkBytes = oldChunk }()

	size := int(8 * mmapPageSize)
	path := filepath.Join(t.TempDir(), "mmap-live-test")
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer syscall.Munmap(data)
	r := registerMmap(data)
	defer unregisterMmap(r)

	if got := r.relieveConsumedPrefix(4 * mmapPageSize); got != 4*mmapPageSize {
		t.Fatalf("prefix relief = %d, want %d", got, 4*mmapPageSize)
	}
	if freed := relieveMmap(1 << 30); freed != 4*mmapPageSize {
		t.Fatalf("global relief credited %d, want live %d", freed, 4*mmapPageSize)
	}
}

// TestMmapRelief_WSHFWalkWithSelfRelief: walking a real mmap'd WSHF file with
// per-batch consumed-prefix relief decodes the identical rows as a plain walk
// — locks the "consumed prefix is dead" invariant the self-relief depends on.
func TestMmapRelief_WSHFWalkWithSelfRelief(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("MADV_DONTNEED path is Linux-only")
	}
	withReliefEnabled(t)
	oldChunk := mmapSelfReliefChunkBytes
	mmapSelfReliefChunkBytes = mmapPageSize // relieve as aggressively as possible
	defer func() { mmapSelfReliefChunkBytes = oldChunk }()

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}
	const rowsPerChunk = 512
	const numChunks = 64 // ~ enough bytes to cross several relief steps
	src := batch.NewRecordBatch(schema, rowsPerChunk)
	for i := 0; i < rowsPerChunk; i++ {
		src.Columns[0].Int64Data[i] = int64(i)
		src.Columns[1].BytesData.Set(i, []byte(strings.Repeat("x", 32)))
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	for c := 0; c < numChunks; c++ {
		if err := sw.writeChunk(src.Columns, nil, rowsPerChunk); err != nil {
			t.Fatal(err)
		}
	}
	// Patch the chunk count into the header (same pattern as
	// TestShuffleFormatRoundTrip — the streaming writer's header is
	// finalized by its sink wrapper in production).
	fileBytes := buf.Bytes()
	binary.LittleEndian.PutUint32(fileBytes[4:], uint32(sw.numChunks))

	path := filepath.Join(t.TempDir(), "walk.wshf")
	if err := os.WriteFile(path, fileBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := syscall.Mmap(int(f.Fd()), 0, buf.Len(), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer syscall.Munmap(data)
	region := registerMmap(data)
	defer unregisterMmap(region)

	r, err := newShuffleChunkReader(data)
	if err != nil {
		t.Fatal(err)
	}
	var rows, relieved int64
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		// Mirror cachedFileStreamSource.Next: relieve behind the cursor
		// after every decoded batch.
		relieved += region.relieveConsumedPrefix(int64(r.pos))
		for i := 0; i < b.ActiveLen(); i++ {
			want := int64(i % rowsPerChunk)
			if b.Columns[0].Int64Data[i] != want {
				t.Fatalf("row %d decoded %d, want %d (after %d bytes relieved)",
					rows+int64(i), b.Columns[0].Int64Data[i], want, relieved)
			}
		}
		rows += int64(b.ActiveLen())
	}
	if rows != rowsPerChunk*numChunks {
		t.Fatalf("decoded %d rows, want %d", rows, rowsPerChunk*numChunks)
	}
	if relieved == 0 {
		t.Fatal("walk never self-relieved — test exercised nothing")
	}
}
