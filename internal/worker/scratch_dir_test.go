package worker

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestTwoExecutorsRunningOneTaskIDKeepSeparateScratch is #833's gate.
//
// A task ID identifies a task within a QUERY. Two executors on one host can
// legitimately hold the same one — two workers sharing a box, or two `go test`
// processes in this package, which reuse fixed task IDs. The scratch path used
// to be built from that ID alone (`<spillDir>/stage-<taskID>`, or a bare
// `/tmp/stage-<taskID>`), so the two wrote into one directory and each
// `os.RemoveAll`'d it at finalize.
//
// Both arms of the collision are attempted here: the SHARED CONFIGURED spill
// directory (two co-located workers pointed at one NVMe volume) and the
// UNCONFIGURED one (the CI shape — os.TempDir(), which every process shares).
// Reverting stageSpillDir to `filepath.Join(base, "stage-"+task.ID)` fails
// both.
func TestTwoExecutorsRunningOneTaskIDKeepSeparateScratch(t *testing.T) {
	task := distributed.Task{ID: "frag-morsel-agg", QueryID: "q-morsel-agg"}

	for _, arm := range []struct {
		name     string
		spillDir func(t *testing.T) string
	}{
		{"shared configured spill dir", func(t *testing.T) string { return t.TempDir() }},
		{"unconfigured (both fall back to os.TempDir)", func(t *testing.T) string { return "" }},
	} {
		t.Run(arm.name, func(t *testing.T) {
			dir := arm.spillDir(t)
			a := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
			b := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
			a.SetMemoryBudget(0, dir)
			b.SetMemoryBudget(0, dir)
			t.Cleanup(a.RemoveScratchRoot)
			t.Cleanup(b.RemoveScratchRoot)

			pathA, pathB := stageSpillDir(a, task), stageSpillDir(b, task)
			if pathA == pathB {
				t.Fatalf("two executors derived ONE scratch path for task %q: %s\n"+
					"each finalize does os.RemoveAll on it, so whichever finishes first "+
					"deletes the other's partition files (#833)", task.ID, pathA)
			}
			for _, p := range []string{pathA, pathB} {
				if err := os.MkdirAll(p, 0o755); err != nil {
					t.Fatalf("MkdirAll %s: %v", p, err)
				}
				if err := os.WriteFile(filepath.Join(p, "partition=0000.wshf"), []byte(p), 0o644); err != nil {
					t.Fatalf("write %s: %v", p, err)
				}
			}
			// The finalize of one task must leave the other's scratch alone.
			if err := os.RemoveAll(pathA); err != nil {
				t.Fatalf("RemoveAll %s: %v", pathA, err)
			}
			got, err := os.ReadFile(filepath.Join(pathB, "partition=0000.wshf"))
			if err != nil {
				t.Fatalf("executor A's finalize removed executor B's scratch file: %v\n"+
					"  A: %s\n  B: %s", err, pathA, pathB)
			}
			if string(got) != pathB {
				t.Fatalf("executor B's partition file holds %q, want %q", got, pathB)
			}
		})
	}
}

// TestOneExecutorKeepsOneTasksScratchApartFromAnothers is the other side of
// the same claim: within ONE executor the path still separates tasks, and
// separates two queries that happen to spell a task ID the same way.
func TestOneExecutorKeepsOneTasksScratchApartFromAnothers(t *testing.T) {
	e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, t.TempDir())
	t.Cleanup(e.RemoveScratchRoot)

	seen := map[string]string{}
	for _, tc := range []struct{ name, query, id, kind string }{
		{"q1/t1 stage", "q-1", "t-1", "stage"},
		{"q1/t2 stage", "q-1", "t-2", "stage"},
		{"q2/t1 stage", "q-2", "t-1", "stage"},
		{"q1/t1 shuffle", "q-1", "t-1", "shuffle"},
		{"no query id", "", "t-1", "stage"},
	} {
		p := e.taskScratchDir(distributed.Task{ID: tc.id, QueryID: tc.query}, tc.kind)
		if prev, dup := seen[p]; dup {
			t.Fatalf("%s and %s share the scratch path %s", prev, tc.name, p)
		}
		seen[p] = tc.name
		if !strings.HasPrefix(p, e.scratchRoot()+string(os.PathSeparator)) {
			t.Fatalf("%s: scratch path %s is not under this executor's root %s", tc.name, p, e.scratchRoot())
		}
	}
}

// TestScratchPathSegmentCannotEscapeTheRoot: a query or task id crosses the
// wire from a coordinator, and it becomes a path segment here. Method 10 — the
// design claims ids are tame, so the corpus attempts the ones that are not.
func TestScratchPathSegmentCannotEscapeTheRoot(t *testing.T) {
	e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	root := t.TempDir()
	e.SetMemoryBudget(0, root)
	t.Cleanup(e.RemoveScratchRoot)

	for _, id := range []string{"../../etc", "a/b", "..", ".", "", "a\x00b", "/absolute"} {
		p := e.taskScratchDir(distributed.Task{ID: id, QueryID: id}, "stage")
		clean := filepath.Clean(p)
		if !strings.HasPrefix(clean, e.scratchRoot()+string(os.PathSeparator)) {
			t.Fatalf("task id %q produced %q, which is outside this executor's scratch root %q",
				id, clean, e.scratchRoot())
		}
	}
}

// TestScratchRootOwnerPIDReadsBothRootKinds: the sweeper reclaims a root whose
// owning process is gone, and it decides ownership by reading a pid out of the
// directory NAME. Both root kinds have to be readable by it — the per-process
// one a worker makes, and the per-instance one an executor makes — and nothing
// else may be, because a name it misreads is a directory it might delete from
// under a live worker.
func TestScratchRootOwnerPIDReadsBothRootKinds(t *testing.T) {
	for _, tc := range []struct {
		name string
		want int
		ok   bool
	}{
		{"wadjet-worker-4242", 4242, true},
		{"wadjet-exec-4242-1837465", 4242, true},
		{"wadjet-exec-4242-", 4242, true},
		{"wadjet-worker-", 0, false},
		{"wadjet-exec-", 0, false},
		{"wadjet-exec-notapid-1", 0, false},
		{"wadjet-worker-0", 0, false},
		{"wadjet-worker--3", 0, false},
		{"wadjet-spill", 0, false},
		{"some-other-dir", 0, false},
	} {
		got, ok := scratchRootOwnerPID(tc.name)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("scratchRootOwnerPID(%q) = (%d, %v), want (%d, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
	// And the real thing round-trips: the root this executor just made names
	// this process, so a sweeper in another process can tell it is alive.
	e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, t.TempDir())
	t.Cleanup(e.RemoveScratchRoot)
	pid, ok := scratchRootOwnerPID(filepath.Base(e.scratchRoot()))
	if !ok || pid != os.Getpid() {
		t.Fatalf("the executor's own scratch root %q does not name this process (%d); "+
			"an abandoned one could never be reclaimed", e.scratchRoot(), os.Getpid())
	}
}

// TestRemoveScratchRootLeavesTheConfiguredDirectoryAlone: the fallback when
// MkdirTemp fails is the base directory itself, and removing THAT would delete
// the operator's spill volume.
func TestRemoveScratchRootLeavesTheConfiguredDirectoryAlone(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "someone-elses.wshf"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, base)
	e.scratchOnce.Do(func() { e.scratchDir = base }) // the MkdirTemp-failed fallback
	e.RemoveScratchRoot()
	if _, err := os.Stat(filepath.Join(base, "someone-elses.wshf")); err != nil {
		t.Fatalf("RemoveScratchRoot deleted the configured spill directory's contents: %v", err)
	}
}

// TestTwoExecutorsRunOneTaskIDConcurrentlyAndBothAnswer is the end-to-end arm:
// the same fragment task, same ID, on two executors at once, ten times. This
// is the shape the flake was observed as — "four internal/worker morsel tests
// fail in a combined run while a second test process is using
// /tmp/stage-frag-morsel-agg/, and the package passes alone, three times".
func TestTwoExecutorsRunOneTaskIDConcurrentlyAndBothAnswer(t *testing.T) {
	const numFiles, rowsPerFile, groups, minV = 4, 500, 20, 100
	bucket := "scratch-collide"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	keys, want := putGroupedFiles(t, store, bucket, numFiles, rowsPerFile, groups, minV)

	// Both executors share ONE spill directory, which is what two co-located
	// workers pointed at one volume have.
	shared := t.TempDir()
	run := func(t *testing.T, e *Executor, resultPrefix string) map[int64]float64 {
		task := aggFragmentTask(bucket, keys, minV)
		task.ResultPrefix = resultPrefix
		result := &distributed.ResultNotification{TaskID: task.ID}
		if err := e.executeStage(context.Background(), task, result); err != nil {
			t.Errorf("executeStage: %v", err)
			return nil
		}
		if len(result.ResultFiles) != 1 {
			t.Errorf("expected 1 output file, got %d", len(result.ResultFiles))
			return nil
		}
		return readGroupTotals(t, store, bucket, result.ResultFiles[0])
	}

	for iter := 0; iter < 10; iter++ {
		a := NewExecutor(store, NewLRUCache(4<<20), nil)
		b := NewExecutor(store, NewLRUCache(4<<20), nil)
		a.SetMemoryBudget(0, shared)
		b.SetMemoryBudget(0, shared)
		var wg sync.WaitGroup
		got := make([]map[int64]float64, 2)
		for i, e := range []*Executor{a, b} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got[i] = run(t, e, "out/agg-"+strconv.Itoa(iter)+"-"+strconv.Itoa(i)+"/")
			}()
		}
		wg.Wait()
		a.RemoveScratchRoot()
		b.RemoveScratchRoot()
		if t.Failed() {
			t.Fatalf("iteration %d: two executors running task ID %q at once", iter, "frag-morsel-agg")
		}
		for i, g := range got {
			if len(g) != len(want) {
				t.Fatalf("iteration %d executor %d: %d groups, want %d", iter, i, len(g), len(want))
			}
			for k, v := range want {
				if g[k] != v {
					t.Fatalf("iteration %d executor %d: group %d = %v, want %v", iter, i, k, g[k], v)
				}
			}
		}
	}
}

// TestNoRuntimeScratchPathIsHardcodedUnderTmp closes the CLASS #833 is an
// instance of: a runtime scratch path that is a fixed absolute location rather
// than one derived from the configured scratch root.
//
// The scope is the packages that execute a QUERY. Operator-facing tools —
// internal/harness, cmd/ — legitimately default to a path under /tmp, because
// that is a default a human overrides on the command line, not a per-task
// directory two processes race on. That boundary is the gate's, not an
// allowlist of files: a new bare-/tmp literal anywhere in the engine fails it.
//
// The walk skips directories starting with "." or "_" — what the Go toolchain
// itself skips. A git worktree lives at .claude/worktrees/<name>/ INSIDE the
// module root and is a full second copy of the source, so a walk without that
// rule reports every hit twice and its verdict depends on whether anybody has
// a worktree open (CLAUDE.md, "Test patterns").
func TestNoRuntimeScratchPathIsHardcodedUnderTmp(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	// The query-executing packages. Everything outside them is out of scope by
	// construction, which is why this gate needs no per-file exemptions.
	scope := []string{
		filepath.Join(root, "internal", "worker"),
		filepath.Join(root, "internal", "coordinator"),
		filepath.Join(root, "internal", "engine"),
		filepath.Join(root, "internal", "planner"),
		filepath.Join(root, "internal", "storage"),
		filepath.Join(root, "internal", "server"),
	}
	var skippedDirs int
	var offenders []string
	for _, dir := range scope {
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name := d.Name(); path != dir && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
					skippedDirs++
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for i, line := range strings.Split(string(src), "\n") {
				if bytes.Contains([]byte(line), []byte(`"/tmp/`)) || bytes.Contains([]byte(line), []byte(`"/tmp"`)) {
					rel, _ := filepath.Rel(root, path)
					offenders = append(offenders, rel+":"+strconv.Itoa(i+1)+": "+strings.TrimSpace(line))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("a fixed /tmp path on a runtime scratch site (#833) — derive it from the "+
			"configured scratch root instead (worker.Executor.taskScratchDir):\n  %s",
			strings.Join(offenders, "\n  "))
	}
	t.Logf("scanned %d package trees, skipped %d dot/underscore directories", len(scope), skippedDirs)
}

// TestAnAbandonedScratchRootUnderAConfiguredSpillDirIsReclaimed is the
// disk-leak gate for #833's own fix, lifted from the round-0 review.
//
// Moving per-task scratch under a private root moved it out of reach of the
// only sweeper that had ever reclaimed it on the production deployment.
// Before, a hard-killed worker left `<spillDir>/stage-<taskID>/` and
// `<spillDir>/shuffle-<taskID>/`, which `sweepStaleBuildCacheFiles` matches by
// its top-level `stage-`/`shuffle-` arms — "this is the only path that
// reclaims those", as its own comment says. After, the same files live at
// `<spillDir>/wadjet-exec-<pid>-<rand>/<query>/stage-<task>/`, a level below
// that match; and `sweepAbandonedScratchRoots` opened the system temp dir and
// nothing else, so with `--spill-dir=/mnt/nvme` it never looked where the root
// was. Nothing reclaimed it. That is the failure ADR-0009 exists for.
//
// The gate is the pair, because either half alone passes: the OLD shape is
// reclaimed by the old sweeper, the NEW shape by the widened one, and a LIVE
// owner's root by neither. Every scratch-sweep test that existed set TMPDIR,
// which is exactly why the regression was invisible to the suite — so this one
// sets a spill dir that is NOT under the temp dir.
func TestAnAbandonedScratchRootUnderAConfiguredSpillDirIsReclaimed(t *testing.T) {
	spill := t.TempDir() // stands in for --spill-dir=/mnt/nvme
	t.Setenv("TMPDIR", t.TempDir())

	dead := aDeadPID(t)
	live := os.Getpid()

	// Three roots: what the tree left before this arc, what it leaves now, and
	// one whose owner is still running and must not be touched.
	oldShape := filepath.Join(spill, "stage-frag-morsel-agg")
	deadRoot := filepath.Join(spill, execScratchPrefix+strconv.Itoa(dead)+"-abc123")
	deadShape := filepath.Join(deadRoot, "q-1", "stage-frag-morsel-agg")
	liveRoot := filepath.Join(spill, execScratchPrefix+strconv.Itoa(live)+"-def456")
	liveShape := filepath.Join(liveRoot, "q-2", "stage-frag-morsel-agg")
	for _, d := range []string{oldShape, deadShape, liveShape} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "p0.wshf"), make([]byte, 1<<20), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	w := &Worker{logger: slog.Default()}
	w.config.SpillDir = spill
	w.sweepStaleBuildCacheFiles()
	w.sweepAbandonedScratchRoots()

	if _, err := os.Stat(oldShape); err == nil {
		t.Errorf("the PRE-ARC shape %s survived both sweeps — the sweeper that reclaimed it "+
			"before this arc must keep doing so", oldShape)
	}
	if _, err := os.Stat(deadRoot); err == nil {
		t.Errorf("LEAK: the orphaned scratch root %s (owner pid %d is dead) survived both "+
			"sweeps. On a configured --spill-dir nothing else ever reclaims it, so a worker "+
			"that is OOM-killed leaks its per-task scratch forever (#833's own fix caused "+
			"this; ADR-0009 is the record)", deadRoot, dead)
	}
	if _, err := os.Stat(liveShape); err != nil {
		t.Errorf("the LIVE owner's scratch %s was deleted (%v) — a co-located worker may be "+
			"writing into it right now; ownership is decided by pid liveness for this reason",
			liveShape, err)
	}
}

// TestScratchFallbackShapeStaysReclaimable: when MkdirTemp cannot make a
// private root, scratch degrades to the FLAT pre-#833 shape rather than to a
// nested one under a `<query>` directory. The flat one is what
// sweepStaleBuildCacheFiles reclaims; a nested one would be a leak with no
// sweeper at all, which is a worse failure than the collision the fallback
// admits.
func TestScratchFallbackShapeStaysReclaimable(t *testing.T) {
	spill := t.TempDir()
	e := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, spill)
	// Force the fallback: pretend MkdirTemp failed and the root IS the base.
	e.scratchOnce.Do(func() { e.scratchDir = spill })

	dir := e.taskScratchDir(distributed.Task{ID: "frag-morsel-agg", QueryID: "q-1"}, "stage")
	if got, want := dir, filepath.Join(spill, "stage-frag-morsel-agg"); got != want {
		t.Fatalf("the fallback scratch path is %s, want the flat pre-#833 shape %s — a nested "+
			"path here is reclaimed by nothing after a hard kill", got, want)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "p0.wshf"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	w := &Worker{logger: slog.Default()}
	w.config.SpillDir = spill
	w.sweepStaleBuildCacheFiles()
	if _, err := os.Stat(dir); err == nil {
		t.Fatalf("the fallback's scratch %s survived the stale-artifact sweep", dir)
	}
}

// aDeadPID finds a pid no process holds, so the sweeper's liveness check has
// something to call abandoned. Skips rather than guesses when it cannot.
func aDeadPID(t *testing.T) int {
	t.Helper()
	for pid := 40000; pid < 60000; pid++ {
		if !processAlive(pid) {
			return pid
		}
	}
	t.Skip("no dead pid available on this machine")
	return 0
}
