package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// classFailStore fails one operation class on demand while every other
// class keeps working against a real MemStore. It is the shape of the
// incident #798 reports: cleanup DELETEs time out against a slow S3 while
// the base tables are perfectly readable.
type classFailStore struct {
	*MemStore
	deleteErr error
	getErr    error
	putErr    error
	deletes   int
	gets      int
}

func (s *classFailStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	if s.putErr != nil {
		return "", fmt.Errorf("putting object: %w", s.putErr)
	}
	return s.MemStore.Put(ctx, bucket, key, r, size, contentType)
}

func (s *classFailStore) Delete(ctx context.Context, bucket, key string) error {
	s.deletes++
	if s.deleteErr != nil {
		return fmt.Errorf("deleting object: %w", s.deleteErr)
	}
	return s.MemStore.Delete(ctx, bucket, key)
}

func (s *classFailStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	s.gets++
	if s.getErr != nil {
		return nil, ObjectInfo{}, fmt.Errorf("getting object: %w", s.getErr)
	}
	return s.MemStore.Get(ctx, bucket, key)
}

func newClassFixture(t *testing.T) (*classFailStore, *CircuitStore) {
	t.Helper()
	inner := &classFailStore{MemStore: NewMemStore()}
	ctx := context.Background()
	if err := inner.MakeBucket(ctx, "b"); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	if _, err := inner.MemStore.Put(ctx, "b", "table/data.parquet", bytes.NewReader([]byte("rows")), 4, ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg := DefaultCircuitConfig()
	cfg.RequestTimeout = time.Second
	return inner, NewCircuitStore(inner, cfg, discardLogger())
}

// TestDeleteFailuresNeverFastFailTheReadPath is #798's invariant, and it is
// deliberately stated as the invariant rather than as the symptom: whatever
// error class a failing DELETE produces, a base-table READ that S3 can
// serve must still be served. Before the per-class split this failed with
// "circuit breaker open: S3 unavailable" on the first Get after the burst.
func TestDeleteFailuresNeverFastFailTheReadPath(t *testing.T) {
	inner, cs := newClassFixture(t)
	ctx := context.Background()
	cfg := cs.Config()

	// Twice the threshold of consecutive delete failures, the shape one slow
	// ResultCleaner.CleanQuery burst produces once its 30 s deadline expires.
	inner.deleteErr = context.DeadlineExceeded
	n := 2 * cfg.FailureThreshold
	for i := 0; i < n; i++ {
		if err := cs.Delete(ctx, "b", fmt.Sprintf("queries/q1/stage-0/part-%d", i)); err == nil {
			t.Fatalf("delete %d: want an error", i)
		}
	}

	// Every read shape must still reach the store.
	rc, _, err := cs.Get(ctx, "b", "table/data.parquet")
	if err != nil {
		t.Fatalf("Get after %d delete failures: %v", n, err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	if string(body) != "rows" {
		t.Fatalf("Get returned %q, want %q", body, "rows")
	}
	if _, err := cs.Head(ctx, "b", "table/data.parquet"); err != nil {
		t.Fatalf("Head after %d delete failures: %v", n, err)
	}
	if _, err := cs.List(ctx, "b", ListOptions{Prefix: "table/"}); err != nil {
		t.Fatalf("List after %d delete failures: %v", n, err)
	}
	ra, _, err := cs.GetReaderAt(ctx, "b", "table/data.parquet")
	if err != nil {
		t.Fatalf("GetReaderAt after %d delete failures: %v", n, err)
	}
	ra.Close()
}

// TestReadFailuresStillOpenTheReadBreaker is the mirror arm: the gate above
// must not be satisfiable by disabling the breaker. A genuinely unreachable
// S3 still has to fast-fail reads at the threshold.
func TestReadFailuresStillOpenTheReadBreaker(t *testing.T) {
	inner, cs := newClassFixture(t)
	ctx := context.Background()
	cfg := cs.Config()

	inner.getErr = context.DeadlineExceeded
	for i := 0; i < cfg.FailureThreshold; i++ {
		if _, _, err := cs.Get(ctx, "b", "table/data.parquet"); err == nil {
			t.Fatalf("get %d: want an error", i)
		}
	}
	if _, _, err := cs.Get(ctx, "b", "table/data.parquet"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("after %d read failures: want ErrCircuitOpen, got %v", cfg.FailureThreshold, err)
	}
	before := inner.gets
	if _, _, err := cs.Get(ctx, "b", "table/data.parquet"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second read while open: want ErrCircuitOpen, got %v", err)
	}
	if inner.gets != before {
		t.Fatalf("an open read breaker still dispatched to the store (%d -> %d)", before, inner.gets)
	}
}

// TestWriteFailuresDoNotFastFailTheReadPath is the second half of the
// invariant. An upload burst failing (a full disk, a throttled PUT quota)
// says nothing about whether a GET can be served.
func TestWriteFailuresDoNotFastFailTheReadPath(t *testing.T) {
	inner, cs := newClassFixture(t)
	ctx := context.Background()
	cfg := cs.Config()

	inner.putErr = context.DeadlineExceeded
	for i := 0; i < 2*cfg.FailureThreshold; i++ {
		if _, err := cs.Put(ctx, "b", "queries/q1/stage-0/out", bytes.NewReader([]byte("x")), 1, ""); err == nil {
			t.Fatalf("put %d: want an error", i)
		}
	}
	if _, _, err := cs.Get(ctx, "b", "table/data.parquet"); err != nil {
		t.Fatalf("Get after %d write failures: %v", 2*cfg.FailureThreshold, err)
	}
}

// TestNotFoundReadResetsTheFailureCounter is #821. A NotFound is a
// completed round trip: the service answered. While it was merely neutral,
// the by-design NotFound probes of the streaming-exchange fallback made the
// healthy intervals between failures invisible, so failures separated by
// successful round trips still accumulated to the threshold.
func TestNotFoundReadResetsTheFailureCounter(t *testing.T) {
	inner, cs := newClassFixture(t)
	ctx := context.Background()
	cfg := cs.Config()
	if cfg.FailureThreshold < 3 {
		t.Skipf("threshold %d too small for this shape", cfg.FailureThreshold)
	}

	fail := func(i int) {
		inner.getErr = context.DeadlineExceeded
		if _, _, err := cs.Get(ctx, "b", "table/data.parquet"); err == nil {
			t.Fatalf("get %d: want an error", i)
		}
		inner.getErr = nil
	}
	probeMissing := func() {
		if _, err := cs.Head(ctx, "b", "not/yet/uploaded"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("probe: want ErrNotFound, got %v", err)
		}
	}

	// threshold-1 failures, a healthy NotFound round trip, then
	// threshold-1 more. Nothing here is threshold consecutive failures.
	for i := 0; i < cfg.FailureThreshold-1; i++ {
		fail(i)
	}
	probeMissing()
	for i := 0; i < cfg.FailureThreshold-1; i++ {
		fail(i)
	}
	if _, _, err := cs.Get(ctx, "b", "table/data.parquet"); err != nil {
		t.Fatalf("read after failures separated by a NotFound probe: %v", err)
	}
}

// TestBreakerOpensPerClassAndCountsTheClass is #822's signal half: an
// operator has to be able to see WHICH class opened, and each class's
// counter has to be independent.
func TestBreakerOpensPerClassAndCountsTheClass(t *testing.T) {
	inner, cs := newClassFixture(t)
	ctx := context.Background()
	cfg := cs.Config()

	var opened []OpClass
	cs.SetOnOpen(func(c OpClass) { opened = append(opened, c) })

	inner.deleteErr = context.DeadlineExceeded
	for i := 0; i < cfg.FailureThreshold; i++ {
		_ = cs.Delete(ctx, "b", fmt.Sprintf("queries/q1/o-%d", i))
	}

	if got := cs.StateFor(OpDelete); got != CircuitOpen {
		t.Fatalf("delete class: got %s, want open", got)
	}
	if got := cs.StateFor(OpRead); got != CircuitClosed {
		t.Fatalf("read class: got %s, want closed", got)
	}
	if got := cs.StateFor(OpWrite); got != CircuitClosed {
		t.Fatalf("write class: got %s, want closed", got)
	}
	if got := cs.OpenedTotal(OpDelete); got != 1 {
		t.Fatalf("delete opened counter = %d, want 1", got)
	}
	if got := cs.OpenedTotal(OpRead); got != 0 {
		t.Fatalf("read opened counter = %d, want 0", got)
	}
	if len(opened) != 1 || opened[0] != OpDelete {
		t.Fatalf("SetOnOpen saw %v, want [delete]", opened)
	}
	if OpDelete.String() != "delete" || OpRead.String() != "read" || OpWrite.String() != "write" {
		t.Fatalf("class names drifted: %s %s %s", OpRead, OpWrite, OpDelete)
	}
}

// TestCircuitBreakerHasExactlyOneConstructionSite pins the scope the round-0
// dossier had to establish by hand: the breaker is process-wide, not per
// bucket, because NewCircuitStore is called once. A second construction
// site would give one process two independent breakers and silently change
// what every assertion in this file means — so it has to fail this test and
// be argued for, not discovered later.
func TestCircuitBreakerHasExactlyOneConstructionSite(t *testing.T) {
	root := repoRootFromTest(t)
	sites := []string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			// A directory whose name begins with "." or "_" holds no package
			// of this module — the rule the Go toolchain itself applies — and
			// skipping them is this gate's CORRECTNESS, not tidiness. A git
			// WORKTREE lives at .claude/worktrees/<name>/, NESTED inside the
			// module root, and is a full second copy of this source tree. The
			// first draft of this walker skipped only ".git", "dist",
			// "testdata" and "node_modules", so it counted every checked-out
			// branch's construction site beside its own and the verdict
			// depended on whether anybody happened to have a worktree open:
			// it passed in the agent worktree that wrote it and failed on
			// main, which has them. That is the same defect 61eba248 fixed in
			// exec/clone_fence_callers_test.go one commit before this arc's
			// base, reintroduced here.
			if path != root {
				if n := d.Name(); strings.HasPrefix(n, ".") || strings.HasPrefix(n, "_") ||
					n == "node_modules" || n == "dist" || n == "testdata" {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		for i, line := range strings.Split(string(body), "\n") {
			if strings.Contains(line, "NewCircuitStore(") && !strings.Contains(line, "func NewCircuitStore") {
				rel, _ := filepath.Rel(root, path)
				sites = append(sites, fmt.Sprintf("%s:%d", rel, i+1))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if len(sites) != 1 {
		t.Fatalf("NewCircuitStore has %d non-test construction sites %v; want exactly 1.\n"+
			"The breaker's counters are process-wide by construction, and every #798 gate "+
			"assumes it. A second site needs its own argument and its own gates.", len(sites), sites)
	}
}

// repoRootFromTest walks up from the test's working directory to the module
// root (the directory holding go.mod) - the checkout the test binary was
// built from. SIBLING worktrees are outside it; NESTED ones (this repo puts
// them at .claude/worktrees/<name>/) are inside it, and are excluded by the
// walker's dot-prefix rule above rather than by this function.
func repoRootFromTest(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod above %s", dir)
		}
		dir = parent
	}
}
