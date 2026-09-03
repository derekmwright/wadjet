package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// Per-task scratch lives under THIS executor's own root, never at a path
// derived from the task ID alone.
//
// A task ID is unique to a QUERY, not to a machine. The stage sinks used to
// build their scratch directory as `<spillDir>/stage-<taskID>` — or, with no
// spill directory configured, as a bare `/tmp/stage-<taskID>` — so any two
// executors that ran the same task ID on one host wrote into and, on
// finalize, `os.RemoveAll`'d each other's directory. Two workers co-located on
// a box is the production shape of that; two `go test` processes in one
// package, which reuse fixed task IDs, is the CI shape, and it is where it was
// found: four `internal/worker` morsel tests fail in a combined run while a
// second test process is using `/tmp/stage-frag-morsel-agg/`, and the package
// passes alone (#833).
//
// So the path carries the three things that actually distinguish one
// directory's owner from another's: the PROCESS-and-instance (this root), the
// QUERY, and the TASK. The root is created once per Executor with
// os.MkdirTemp, which is atomic and collision-free by construction — a PID is
// not, because PIDs are reused and two Executors can live in one process.
// Tests get their root from t.TempDir() by passing it as the spill directory,
// exactly like every other spill artifact.
//
// TestNoRuntimeScratchPathIsHardcodedUnderTmp keeps the class closed: a new
// bare-/tmp literal on a runtime scratch path fails it.

// execScratchPrefix names this executor's root under the scratch directory.
// The pid it carries is what lets sweepAbandonedScratchRoots reap one left by
// a process that died before Worker.Stop — the same rule the per-PROCESS root
// (scratchRootPrefix, worker.go) already lives by; the random suffix
// os.MkdirTemp appends after it is what makes the root per-INSTANCE, which the
// pid alone cannot be when two executors run in one process.
const execScratchPrefix = "wadjet-exec-"

// scratchRoot returns this Executor's private scratch root, creating it on
// first use. It is under the configured spill directory when there is one and
// under os.TempDir() otherwise, and it is unique to this Executor instance.
//
// A creation failure is not fatal here: the caller's own MkdirAll reports it
// with the context of what it was making room for. The fallback is the base
// directory, which is where the scratch used to live — no worse than before,
// and still under the configured root.
func (e *Executor) scratchRoot() string {
	e.scratchOnce.Do(func() {
		base := e.spillDir
		if base == "" {
			base = os.TempDir()
		}
		dir, err := os.MkdirTemp(base, fmt.Sprintf("%s%d-", execScratchPrefix, os.Getpid()))
		if err != nil {
			e.logger.Warn("could not create a private scratch root; per-task scratch "+
				"falls back to the base directory and may collide with another executor on this host",
				"base", base, "error", err)
			dir = base
		}
		e.scratchMu.Lock()
		e.scratchDir = dir
		e.scratchMu.Unlock()
	})
	e.scratchMu.Lock()
	defer e.scratchMu.Unlock()
	return e.scratchDir
}

// taskScratchDir is the directory one task's `kind` scratch (stage-sink
// partition files, shuffle partition files) belongs in: per process-instance,
// per query, per task. The caller creates and removes it.
//
// The query segment is what keeps one query's scratch together on disk, which
// is what a human reading a full spill volume needs; the task segment is what
// keeps two tasks of one query apart. A task with no QueryID — a hand-built
// fragment in a test — gets a fixed segment rather than being flattened into
// the root, so the shape of the path does not depend on the field being set.
func (e *Executor) taskScratchDir(task distributed.Task, kind string) string {
	query := task.QueryID
	if query == "" {
		query = "no-query"
	}
	return filepath.Join(e.scratchRoot(), scratchPathSegment(query), kind+"-"+scratchPathSegment(task.ID))
}

// scratchPathSegment makes an id safe as one path segment. Query and task ids
// are generated internally and are already tame, but they cross the wire from
// a coordinator, and a segment holding a separator or ".." would put the
// scratch somewhere other than under this root.
func scratchPathSegment(id string) string {
	out := make([]byte, 0, len(id))
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "_"
	}
	return string(out)
}

// scratchRootOwnerPID reads the owning process out of a scratch-root
// directory name, for both root kinds: the per-PROCESS root
// `wadjet-worker-<pid>` a worker creates when no SpillDir is configured, and
// the per-INSTANCE root `wadjet-exec-<pid>-<random>` an Executor creates under
// it. Ownership is a pid question in both cases, which is what lets
// sweepAbandonedScratchRoots reclaim a root whose owner is gone without ever
// touching one whose owner may be writing to it right now.
//
// Returns false for any name it cannot read a pid out of, so an unrecognised
// directory is left alone.
func scratchRootOwnerPID(name string) (int, bool) {
	var rest string
	switch {
	case strings.HasPrefix(name, execScratchPrefix):
		rest = strings.TrimPrefix(name, execScratchPrefix)
		if i := strings.IndexByte(rest, '-'); i >= 0 {
			rest = rest[:i]
		}
	case strings.HasPrefix(name, scratchRootPrefix):
		rest = strings.TrimPrefix(name, scratchRootPrefix)
	default:
		return 0, false
	}
	pid, err := strconv.Atoi(rest)
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// RemoveScratchRoot deletes this executor's scratch root. Called from
// Worker.Stop: every per-task directory under it is removed by the sink that
// made it, so this reaps the root itself and whatever an aborted task left.
func (e *Executor) RemoveScratchRoot() {
	e.scratchMu.Lock()
	defer e.scratchMu.Unlock()
	if e.scratchDir == "" || e.scratchDir == e.spillDir || e.scratchDir == os.TempDir() {
		return // never created, or fell back to the base directory
	}
	if err := os.RemoveAll(e.scratchDir); err != nil {
		e.logger.Warn("scratch root cleanup failed; disk space may leak",
			"dir", e.scratchDir, "error", err)
	}
	e.scratchDir = ""
}
