package worker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// writeMultiGroupFixture writes n parquet files with groupsPerFile row
// groups of rowsPerGroup rows each ("id", "val" int64) and returns the
// keys and expected val-sum. Small row groups so decode-ahead has real
// in-file parallelism to exercise.
func writeMultiGroupFixture(t *testing.T, store objstore.Store, bucket string, n, groupsPerFile, rowsPerGroup int) ([]string, int64) {
	t.Helper()
	if err := store.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeInt64},
	}}
	cfg := parquet.DefaultWriterConfig()
	cfg.RowGroupSize = rowsPerGroup

	keys := make([]string, 0, n)
	var wantSum int64
	rowID := 0
	for f := 0; f < n; f++ {
		var buf bytes.Buffer
		w, err := parquet.NewWriter(&buf, schema, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for g := 0; g < groupsPerFile; g++ {
			rows := make([]map[string]any, rowsPerGroup)
			for r := range rows {
				v := int64(rowID * 7)
				rows[r] = map[string]any{"id": int64(rowID), "val": v}
				wantSum += v
				rowID++
			}
			if err := w.WriteRows(rows); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("tables/t/part-%04d.parquet", f)
		if _, err := store.Put(context.Background(), bucket, key, bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	return keys, wantSum
}

// TestScanDecodeAhead_MultiFileParity: with --scan-decode-ahead on, a
// multi-file multi-row-group scan over the prefetcher path yields exactly
// the rows the serial path yields, and the engagement counter moves.
func TestScanDecodeAhead_MultiFileParity(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	keys, wantSum := writeMultiGroupFixture(t, store, "b", 5, 4, 32)

	e := &Executor{store: store, spillDir: t.TempDir()}
	e.SetScanDecodeAhead(true, 0)
	src := newCachedFileStreamSource(e, "", "b", keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sum, rows := drainValSum(t, ctx, src)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if sum != wantSum || rows != 5*4*32 {
		t.Fatalf("decode-ahead read: sum=%d rows=%d, want sum=%d rows=%d", sum, rows, wantSum, 5*4*32)
	}
	if groups, _, _, _ := e.ScanDecodeAheadStats(); groups != 5*4 {
		t.Errorf("decode-ahead groups = %d, want %d", groups, 5*4)
	}

	// Serial control on the same objects.
	e2 := &Executor{store: store, spillDir: t.TempDir()}
	src2 := newCachedFileStreamSource(e2, "", "b", keys)
	if err := src2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src2.Close()
	sum2, rows2 := drainValSum(t, ctx, src2)
	if sum2 != sum || rows2 != rows {
		t.Fatalf("serial control disagrees: sum=%d rows=%d vs decode-ahead sum=%d rows=%d", sum2, rows2, sum, rows)
	}
}

// localPathTestStore serves every object from a local directory via the
// LocalPathStore interface — the deterministic base-cache stand-in for
// pre-open tests (no download timing involved).
type localPathTestStore struct {
	objstore.Store
	paths map[string]string // bucket+"/"+key -> local file
}

func (s *localPathTestStore) CachedLocalPath(bucket, key string) (string, bool) {
	p, ok := s.paths[bucket+"/"+key]
	return p, ok
}
func (s *localPathTestStore) HasCachedPath(bucket, key string) bool {
	_, ok := s.paths[bucket+"/"+key]
	return ok
}

// TestScanDecodeAhead_CrossFileContinuation: with every file resident in
// the local-path tier, the continuation must pre-open every file after
// the first (deterministically — no download races), deliver identical
// rows, and leave the cache-owned files on disk.
func TestScanDecodeAhead_CrossFileContinuation(t *testing.T) {
	ctx := context.Background()
	inner := objstore.NewMemStore()
	keys, wantSum := writeMultiGroupFixture(t, inner, "b", 4, 3, 32)

	// Materialize every object as a local file for the LocalPathStore tier.
	dir := t.TempDir()
	paths := make(map[string]string, len(keys))
	for i, k := range keys {
		rc, _, err := inner.Get(ctx, "b", k)
		if err != nil {
			t.Fatal(err)
		}
		local := filepath.Join(dir, fmt.Sprintf("cached-%d.parquet", i))
		data := make([]byte, 0)
		buf := make([]byte, 32<<10)
		for {
			n, rerr := rc.Read(buf)
			data = append(data, buf[:n]...)
			if rerr != nil {
				break
			}
		}
		rc.Close()
		if err := os.WriteFile(local, data, 0o644); err != nil {
			t.Fatal(err)
		}
		paths["b/"+k] = local
	}
	store := &localPathTestStore{Store: inner, paths: paths}

	e := &Executor{store: store, spillDir: t.TempDir()}
	e.SetScanDecodeAhead(true, 0)
	src := newCachedFileStreamSource(e, "", "b", keys)
	src.prefetchDisabled = true // isolate the local-path pre-open tier
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sum, rows := drainValSum(t, ctx, src)
	if sum != wantSum || rows != 4*3*32 {
		t.Fatalf("continuation read: sum=%d rows=%d, want sum=%d rows=%d", sum, rows, wantSum, 4*3*32)
	}
	if src.preOpens != len(keys)-1 {
		t.Errorf("pre-opened %d files, want %d (every file after the first)", src.preOpens, len(keys)-1)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	// Cache-owned files must survive the scan (localPath stays empty).
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("cache-owned file removed by scan: %s (%v)", p, err)
		}
	}
}

// TestScanDecodeAhead_CloseWithPendingFile: closing the source while a
// pre-opened next file is still pending must quiesce its decode workers
// and release its resources without touching delivered data. The -race
// build is the real assertion.
func TestScanDecodeAhead_CloseWithPendingFile(t *testing.T) {
	ctx := context.Background()
	inner := objstore.NewMemStore()
	keys, _ := writeMultiGroupFixture(t, inner, "b", 3, 2, 32)

	dir := t.TempDir()
	paths := make(map[string]string, len(keys))
	for i, k := range keys {
		rc, _, err := inner.Get(ctx, "b", k)
		if err != nil {
			t.Fatal(err)
		}
		var data []byte
		buf := make([]byte, 32<<10)
		for {
			n, rerr := rc.Read(buf)
			data = append(data, buf[:n]...)
			if rerr != nil {
				break
			}
		}
		rc.Close()
		local := filepath.Join(dir, fmt.Sprintf("cached-%d.parquet", i))
		if err := os.WriteFile(local, data, 0o644); err != nil {
			t.Fatal(err)
		}
		paths["b/"+k] = local
	}
	store := &localPathTestStore{Store: inner, paths: paths}

	e := &Executor{store: store, spillDir: t.TempDir()}
	e.SetScanDecodeAhead(true, 0)
	src := newCachedFileStreamSource(e, "", "b", keys)
	src.prefetchDisabled = true
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Pull batches until a pending file exists, then close mid-stream.
	for i := 0; i < 100 && src.nextParquet == nil; i++ {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
	}
	if src.nextParquet == nil {
		t.Fatal("fixture never produced a pending pre-opened file")
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if src.nextParquet != nil {
		t.Error("Close left the pending file live")
	}
}

// TestScanDecodeAhead_PerGroupTokens: the source's decode CPU is
// budgeted per row group against the executor's cpuToken pool — a
// drained pool still yields identical rows (cursor-exempt serial
// progress, token-stall counter fires), and with an ample pool NOTHING
// remains held after the scan drains mid-source or closes: no
// source-lifetime hoarding (the 2026-07-16 SF100 regression class).
func TestScanDecodeAhead_PerGroupTokens(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	keys, wantSum := writeMultiGroupFixture(t, store, "b", 3, 3, 32)

	// Drained pool: still correct, stall counter fires.
	e := &Executor{store: store, spillDir: t.TempDir()}
	e.SetScanDecodeAhead(true, 0)
	e.cpuTokens = newCPUTokens(0)
	src := newCachedFileStreamSource(e, "", "b", keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sum, rows := drainValSum(t, ctx, src)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if sum != wantSum || rows != 3*3*32 {
		t.Fatalf("starved read: sum=%d rows=%d, want sum=%d rows=%d", sum, rows, wantSum, 3*3*32)
	}
	if _, _, _, stalls := e.ScanDecodeAheadStats(); stalls == 0 {
		t.Error("token-stall counter did not fire under a drained pool")
	}
	if held := e.cpuTokens.InUse(); held != 0 {
		t.Errorf("tokens in use after starved scan = %d, want 0", held)
	}

	// Ample pool: after every delivered batch, held tokens can only be
	// the ones inside active decodes — and after Close, exactly zero.
	e2 := &Executor{store: store, spillDir: t.TempDir()}
	e2.SetScanDecodeAhead(true, 0)
	e2.cpuTokens = newCPUTokens(16)
	src2 := newCachedFileStreamSource(e2, "", "b", keys)
	if err := src2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sum2, rows2 := drainValSum(t, ctx, src2)
	if sum2 != wantSum || rows2 != rows {
		t.Fatalf("ample read: sum=%d rows=%d, want sum=%d rows=%d", sum2, rows2, wantSum, rows)
	}
	if err := src2.Close(); err != nil {
		t.Fatal(err)
	}
	if held := e2.cpuTokens.InUse(); held != 0 {
		t.Errorf("tokens in use after Close = %d, want 0 (hoarding)", held)
	}
}

// TestFilePrefetcher_TryTakePutsBackFailures: a tryTake that observes an
// err/skipped result must put it back so the authoritative take() still
// receives it — consuming it would leave take() blocked forever on the
// cap-1 channel.
func TestFilePrefetcher_TryTakePutsBackFailures(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	keys, _ := writeMultiGroupFixture(t, store, "b", 2, 1, 8)
	missing := "tables/t/missing.parquet"
	files := []string{keys[0], missing, keys[1]}

	e := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(e, "", "b", files)
	p := startFilePrefetcher(ctx, src)
	defer p.Close()

	// Poll tryTake on the missing file's index until its download resolves
	// to an error; every resolved observation must report unusable (nil).
	// Deadline-based — a count-bounded spin finishes before the prefetch
	// worker even runs on a slow CI runner.
	var resolved bool
	deadline := time.Now().Add(15 * time.Second)
	for !resolved && time.Now().Before(deadline) {
		var res *prefetchResult
		res, resolved = p.tryTake(1)
		if resolved && res != nil {
			t.Fatalf("tryTake returned a usable result for a missing object: %+v", res)
		}
		if !resolved {
			time.Sleep(time.Millisecond)
		}
	}
	if !resolved {
		t.Fatal("prefetch of missing object never resolved within deadline")
	}
	// The authoritative take must still observe the buffered failure
	// immediately (put-back happened) rather than blocking.
	done := make(chan *prefetchResult, 1)
	go func() { done <- p.take(ctx, 1) }()
	select {
	case res := <-done:
		if res.err == nil && !res.skipped {
			t.Fatalf("take after tryTake put-back: unexpected success %+v", res)
		}
	case <-ctx.Done():
		t.Fatal("ctx done")
	}
}
