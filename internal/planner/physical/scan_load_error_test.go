// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1710: a parquet file the scan cannot fetch or open must FAIL the scan,
// naming the file. Before the fix the row-group worker's load error was
// released and skipped (readRG returned nil with nothing on errCh), and the
// lazy file-level worker `continue`d past a Get / open failure, so the scan
// finished as success with that file's rows missing.

// loadFault is what faultStore does to one object.
type loadFault int

const (
	faultNone     loadFault = iota
	faultGetError           // the request fails
	faultTruncate           // the body stops half way
	faultFooter             // the body's trailing footer length + magic are garbage
)

func (f loadFault) String() string {
	return [...]string{"none", "get-error", "truncated", "corrupt-footer"}[f]
}

var errInjectedLoad = errors.New("injected object-store failure")

// faultStore wraps a real store (never a mock: objstore.NewMemStore or a
// FileStore) and damages ONE object's reads once armed. onReaderAt says
// whether GetReaderAt is damaged too: off, a footer read through ReaderAt
// (buildRGUnits' pruning pass) still sees the good object and only the data
// load fails, which is the boundary #1710 is about.
type faultStore struct {
	objstore.Store
	mu         sync.Mutex
	target     string
	fault      loadFault
	onReaderAt bool
}

func (s *faultStore) arm(target string, f loadFault, onReaderAt bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.target, s.fault, s.onReaderAt = target, f, onReaderAt
}

func (s *faultStore) faultFor(key string, readerAt bool) loadFault {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key != s.target || (readerAt && !s.onReaderAt) {
		return faultNone
	}
	return s.fault
}

func damage(data []byte, f loadFault) []byte {
	switch f {
	case faultTruncate:
		return append([]byte(nil), data[:len(data)/2]...)
	case faultFooter:
		out := append([]byte(nil), data...)
		for i := len(out) - 8; i < len(out); i++ {
			out[i] = 0xEE
		}
		return out
	}
	return data
}

func (s *faultStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	f := s.faultFor(key, false)
	if f == faultGetError {
		return nil, objstore.ObjectInfo{}, fmt.Errorf("get %s: %w", key, errInjectedLoad)
	}
	rc, info, err := s.Store.Get(ctx, bucket, key)
	if err != nil || f == faultNone {
		return rc, info, err
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, objstore.ObjectInfo{}, err
	}
	return io.NopCloser(bytes.NewReader(damage(data, f))), info, nil
}

type bytesReaderAt struct{ *bytes.Reader }

func (bytesReaderAt) Close() error { return nil }

func (s *faultStore) GetReaderAt(ctx context.Context, bucket, key string) (objstore.ReaderAtCloser, int64, error) {
	ras := s.Store.(objstore.ReaderAtStore)
	f := s.faultFor(key, true)
	switch f {
	case faultNone:
		return ras.GetReaderAt(ctx, bucket, key)
	case faultGetError:
		return nil, 0, fmt.Errorf("get reader %s: %w", key, errInjectedLoad)
	}
	ra, size, err := ras.GetReaderAt(ctx, bucket, key)
	if err != nil {
		return nil, 0, err
	}
	data := make([]byte, size)
	_, err = ra.ReadAt(data, 0)
	ra.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, err
	}
	d := damage(data, f)
	return bytesReaderAt{bytes.NewReader(d)}, int64(len(d)), nil
}

func (s *faultStore) StoreID() string { return objstore.StoreID(s.Store) }

const (
	loadErrFiles      = 4
	loadErrRGsPerFile = 3
	loadErrRGRows     = 100
)

// loadErrTable writes loadErrFiles parts of loadErrRGsPerFile row groups each
// through the given store, id = file*1000 + rg*100 + row.
func loadErrTable(t *testing.T, base objstore.Store) (*catalog.Catalog, *faultStore, []catalog.FileEntry) {
	t.Helper()
	ctx := context.Background()
	fs := &faultStore{Store: base}
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	cat := catalog.NewWithStore(fs, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	var entries []catalog.FileEntry
	for f := 0; f < loadErrFiles; f++ {
		var sets [][]map[string]any
		for rg := 0; rg < loadErrRGsPerFile; rg++ {
			rows := make([]map[string]any, loadErrRGRows)
			for r := range rows {
				rows[r] = map[string]any{"id": int64(f*1000 + rg*100 + r)}
			}
			sets = append(sets, rows)
		}
		data := writeTestParquetMultiRG(t, schema, sets...)
		path := fmt.Sprintf("tables/t/chunk_%04d.parquet", f)
		if _, err := fs.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		e := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)),
			NumRows: loadErrRGsPerFile * loadErrRGRows, CreatedAt: time.Now()}
		if err := cat.AddFiles(ctx, "t", map[string]string{}, "tables/t/", []catalog.FileEntry{e}); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
	}
	return cat, fs, entries
}

func loadErrWantSum() (n, sum int64) {
	for f := 0; f < loadErrFiles; f++ {
		for rg := 0; rg < loadErrRGsPerFile; rg++ {
			for r := 0; r < loadErrRGRows; r++ {
				n++
				sum += int64(f*1000 + rg*100 + r)
			}
		}
	}
	return n, sum
}

// scanCountSum drains a real catalog scan of t.id and returns its row count,
// the sum of id, and the scan's error. It consumes every batch the source
// hands over, whatever the worker count, so a test built on it never waits
// on an error a parked worker has not produced yet.
func scanCountSum(cat *catalog.Catalog, src *catalogScanSource, inspect func(*scanSourceInner)) (n, sum int64, err error) {
	ctx := context.Background()
	if err := src.Init(ctx); err != nil {
		return 0, 0, err
	}
	defer src.Close()
	if inspect != nil {
		defer func() { inspect(src.inner.(*scannerExecSource).scanner) }()
	}
	for {
		b, err := src.Next(ctx)
		if err != nil {
			return n, sum, err
		}
		if b == nil {
			return n, sum, nil
		}
		col := b.Columns[0]
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			n++
			sum += col.Int64Data[row]
		}
	}
}

// loadErrCase is one injected failure through one load mode.
type loadErrCase struct {
	mode       string
	fault      loadFault
	onReaderAt bool
	cold       bool // drop the process footer cache before the failing scan
}

// runLoadErrCase builds the table, primes the footer cache with an unfailed
// scan (which is also the control answer), arms the fault on one file and
// scans again through the named mode.
func runLoadErrCase(t *testing.T, c loadErrCase, target int) (n, sum int64, err error) {
	t.Helper()
	var base objstore.Store = objstore.NewMemStore()
	if c.mode == "staged-pread" {
		dir := t.TempDir()
		fsStore, ferr := objstore.NewFileStore(dir)
		if ferr != nil {
			t.Fatal(ferr)
		}
		base = fsStore
	}
	if c.mode == "whole-file" || c.mode == "deferred-whole-file" {
		prev := scanRowGroupBuffers.Set(false)
		t.Cleanup(func() { scanRowGroupBuffers.Set(prev) })
	} else {
		prev := scanRowGroupBuffers.Set(true)
		t.Cleanup(func() { scanRowGroupBuffers.Set(prev) })
	}
	cat, fs, entries := loadErrTable(t, base)
	mk := func() *catalogScanSource {
		src := &catalogScanSource{catalog: cat, tableName: "t", requiredCols: []string{"id"}}
		switch c.mode {
		case "file-worker":
			src.rowLimit = 1 << 30 // lazy file-level scanWorker; the limit itself never bites
		case "deferred", "deferred-whole-file":
			// A budget smaller than any row group refuses every read-ahead,
			// so each row group is handed over as a deferred marker and
			// decoded on the consumer's goroutine in next().
			src.memTracker = memory.NewTracker("scan-load-err", 1)
		}
		return src
	}
	wantN, wantSum := loadErrWantSum()
	// The control scan also proves the mode engaged: an arm that silently
	// fell onto another load path would be testing that path twice.
	rg0, wf0 := RowGroupLoadStats()
	var onDemand, fileLevel bool
	gn, gs, gerr := scanCountSum(cat, mk(), func(inner *scanSourceInner) {
		onDemand = inner.onDemand.Load()
		fileLevel = len(inner.rgUnits) == 0
	})
	if gerr != nil || gn != wantN || gs != wantSum {
		t.Fatalf("control scan: n=%d sum=%d err=%v, want n=%d sum=%d", gn, gs, gerr, wantN, wantSum)
	}
	rg1, wf1 := RowGroupLoadStats()
	rgLoads, wfLoads := rg1-rg0, wf1-wf0
	var engaged bool
	switch c.mode {
	case "row-group":
		engaged = rgLoads == loadErrFiles && wfLoads == 0 && !onDemand
	case "whole-file":
		engaged = wfLoads == loadErrFiles && rgLoads == 0 && !onDemand
	case "staged-pread": // the staged open returns before either counter
		engaged = rgLoads == 0 && wfLoads == 0 && !fileLevel
	case "file-worker":
		engaged = rgLoads == 0 && wfLoads == 0 && fileLevel
	case "deferred":
		engaged = rgLoads == loadErrFiles && onDemand
	case "deferred-whole-file":
		engaged = wfLoads == loadErrFiles && onDemand
	}
	if !engaged {
		t.Fatalf("mode %s did not engage: row-group loads %d, whole-file loads %d, on-demand %v, file-level %v",
			c.mode, rgLoads, wfLoads, onDemand, fileLevel)
	}
	fs.arm(entries[target].Path, c.fault, c.onReaderAt)
	if c.cold {
		parquet.ResetFooterCache()
	}
	return scanCountSum(cat, mk(), nil)
}

// TestScanLoadErrorFailsTheScan is #1710's gate: one file of four cannot be
// fetched or opened, and the scan must fail with an error naming it, through
// every load mode. A short answer returned as success is the defect.
func TestScanLoadErrorFailsTheScan(t *testing.T) {
	modes := []string{"row-group", "whole-file", "staged-pread", "file-worker", "deferred", "deferred-whole-file"}
	faults := []loadFault{faultGetError, faultTruncate, faultFooter}
	for _, mode := range modes {
		for _, f := range faults {
			for _, v := range []struct{ onRA, cold bool }{{false, false}, {true, false}, {true, true}} {
				// target the LAST file: its row groups are the scan's last.
				c := loadErrCase{mode: mode, fault: f, onReaderAt: v.onRA, cold: v.cold}
				t.Run(fmt.Sprintf("%s/%s/readerAt=%v/cold=%v", mode, f, v.onRA, v.cold), func(t *testing.T) {
					target := loadErrFiles - 1
					n, sum, err := runLoadErrCase(t, c, target)
					wantN, wantSum := loadErrWantSum()
					if err == nil {
						// The damage never reached the bytes this mode reads
						// (a row-group read never touches the footer bytes; a
						// ReaderAt read never calls Get): the full answer is
						// the right one. A short one is the defect.
						if n != wantN || sum != wantSum {
							t.Fatalf("scan succeeded with n=%d sum=%d (full answer n=%d sum=%d): a file the "+
								"store could not serve was dropped silently", n, sum, wantN, wantSum)
						}
						return
					}
					if errors.Is(err, context.Canceled) {
						t.Fatalf("a load error surfaced as a cancellation: %v", err)
					}
					if !strings.Contains(err.Error(), fmt.Sprintf("chunk_%04d.parquet", target)) {
						t.Fatalf("error %q does not name the file that failed", err)
					}
					t.Logf("error: %v", err)
				})
			}
		}
	}
}
