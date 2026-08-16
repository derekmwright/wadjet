package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/diskio"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setShufflePread flips the WSHF read-staging mode for one test, restoring
// the process default afterwards. Tests using it must not run in parallel.
func setShufflePread(t *testing.T, on bool) {
	t.Helper()
	prev := shufflePreadEnabled
	shufflePreadEnabled = on
	t.Cleanup(func() { shufflePreadEnabled = prev })
}

// buildTestWSHF produces a multi-chunk plain-WSHF payload of numChunks
// full batches over a single int64 column, returning the encoded bytes,
// the expected value sum, and the expected row count.
func buildTestWSHF(t *testing.T, numChunks int) (data []byte, wantSum int64, wantRows int) {
	t.Helper()
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64, Nullable: true}}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
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
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[4:], uint32(numChunks))
	return out, wantSum, numChunks * batch.DefaultBatchSize
}

// drainSum pulls the source dry and returns the sum and count of column 0.
func drainSum(t *testing.T, src *cachedFileStreamSource) (sum int64, rows int) {
	t.Helper()
	ctx := context.Background()
	for {
		bb, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if bb == nil {
			return sum, rows
		}
		for i := 0; i < bb.Len; i++ {
			sum += bb.Columns[0].Int64Data[i]
		}
		rows += bb.Len
	}
}

// TestShufflePread_StagedParityAcrossModes reads the same WSHF and WSHC
// objects through the S3-staged open path in both read-staging modes and
// requires identical results: the read()-streaming default and the
// WADJET_SHUFFLE_PREAD=0 mmap kill switch cannot diverge. Also pins the
// ownership contract — staged spill temps are unlinked once drained — and
// the engagement counters that gate the SF100 A/B.
func TestShufflePread_StagedParityAcrossModes(t *testing.T) {
	data, wantSum, wantRows := buildTestWSHF(t, 40)
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	compressed := CompressShuffleData(data)
	for key, payload := range map[string][]byte{
		"queries/q/p0/plain.wshf": data,
		"queries/q/p1/comp.wshf":  compressed,
	} {
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(payload), int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
	}
	keys := []string{"queries/q/p0/plain.wshf", "queries/q/p1/comp.wshf"}

	for _, mode := range []struct {
		name  string
		pread bool
	}{{"pread", true}, {"mmap-killswitch", false}} {
		t.Run(mode.name, func(t *testing.T) {
			setShufflePread(t, mode.pread)
			spill := t.TempDir()
			ex := &Executor{store: store, spillDir: spill}
			src := newCachedFileStreamSource(ex, "", bucket, keys)
			if err := src.Init(ctx); err != nil {
				t.Fatal(err)
			}
			sum, rows := drainSum(t, src)
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			if sum != 2*wantSum || rows != 2*wantRows {
				t.Errorf("sum=%d rows=%d, want sum=%d rows=%d", sum, rows, 2*wantSum, 2*wantRows)
			}
			files, bytesRead := ex.ShuffleFilePreadStats()
			if mode.pread {
				if files != 2 {
					t.Errorf("file_pread_files=%d, want 2", files)
				}
				// Both staged temps are plain WSHF and at least as large as
				// the uncompressed payload.
				if bytesRead < 2*int64(len(data)) {
					t.Errorf("file_pread_bytes=%d, want >= %d", bytesRead, 2*len(data))
				}
			} else if files != 0 {
				t.Errorf("file_pread_files=%d on kill-switch path, want 0", files)
			}
			// Owned staged temps must be unlinked once their file drained.
			ents, err := os.ReadDir(spill)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range ents {
				t.Errorf("leaked spill temp after drain: %s", e.Name())
			}
		})
	}
}

// TestShufflePread_LocalStageCacheNotUnlinked pins the tier-0 ownership
// contract on the read-staged path: a LocalStageCache-owned file is read
// via read()-streaming, counted on the local tier ledger by its real size,
// and NOT unlinked by the consumer — the cache owns cleanup.
func TestShufflePread_LocalStageCacheNotUnlinked(t *testing.T) {
	setShufflePread(t, true)
	data, wantSum, wantRows := buildTestWSHF(t, 8)
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	const key = "queries/q1/p0/task.wshf"

	cacheRoot := t.TempDir()
	staging := filepath.Join(t.TempDir(), "produced.wshf")
	if err := os.WriteFile(staging, data, 0o644); err != nil {
		t.Fatal(err)
	}
	cache := NewLocalStageCache(cacheRoot)
	adopted := cache.Adopt("q1", key, staging)
	if adopted == "" {
		t.Fatal("Adopt failed")
	}

	ex := &Executor{store: store, spillDir: t.TempDir(), localCache: cache}
	src := newCachedFileStreamSource(ex, "q1", bucket, []string{key})
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sum, rows := drainSum(t, src)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if sum != wantSum || rows != wantRows {
		t.Errorf("sum=%d rows=%d, want sum=%d rows=%d", sum, rows, wantSum, wantRows)
	}
	if _, err := os.Stat(adopted); err != nil {
		t.Errorf("cache-owned file gone after read: %v", err)
	}
	io := ex.ShuffleIOStats()
	if io.LocalFiles != 1 || io.LocalBytes != int64(len(data)) {
		t.Errorf("local tier ledger files=%d bytes=%d, want 1/%d", io.LocalFiles, io.LocalBytes, len(data))
	}
	if files, _ := ex.ShuffleFilePreadStats(); files != 1 {
		t.Errorf("file_pread_files=%d, want 1", files)
	}
}

// TestShufflePread_DropBehindEngages verifies the fd drop-behind wrapper
// engages on owned staged temps: a walk bigger than two 8 MiB windows must
// advance the diskio read-drop counter (the wlog rollout marker).
func TestShufflePread_DropBehindEngages(t *testing.T) {
	setShufflePread(t, true)
	origDrop := diskio.DropBehindEnabled()
	diskio.SetDropBehindEnabled(true)
	t.Cleanup(func() { diskio.SetDropBehindEnabled(origDrop) })

	// ~1200 chunks × 2048 int64 rows ≈ 20 MB > 2 drop windows.
	data, wantSum, wantRows := buildTestWSHF(t, 1200)
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	const key = "queries/q/p0/big.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	_, before := diskio.DropBehindStats()
	ex := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(ex, "", bucket, []string{key})
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sum, rows := drainSum(t, src)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if sum != wantSum || rows != wantRows {
		t.Errorf("sum=%d rows=%d, want sum=%d rows=%d", sum, rows, wantSum, wantRows)
	}
	_, after := diskio.DropBehindStats()
	if after <= before {
		t.Errorf("read drop-behind did not engage: before=%d after=%d", before, after)
	}
}
