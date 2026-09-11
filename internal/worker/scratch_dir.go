package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// Task scratch names must distinguish executor instance, query and task (#833).
// Create one executor root with atomic MkdirTemp under configured spill dir or
// os.TempDir; PID alone cannot distinguish instances or survive PID reuse.
// Every creatable root must be reachable by sweepAbandonedScratchRoots in BOTH
// locations, using PID liveness. MkdirTemp failure falls back to the flat shape
// already reclaimed by sweepStaleBuildCacheFiles, with caller MkdirAll errors.
// TestNoRuntimeScratchPathIsHardcodedUnderTmp and
// TestAnAbandonedScratchRootUnderAConfiguredSpillDirIsReclaimed gate this pair.
// See docs/internals/worker-executor-scratch-ownership.md for the design.

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
				"falls back to the pre-#833 shape, which collides with another executor "+
				"running the same task ID on this host",
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
	root := e.scratchRoot()
	if root == e.spillDir || root == os.TempDir() {
		// The MkdirTemp-failure fallback. Degrade to the PRE-#833 shape —
		// `<base>/<kind>-<task>` — rather than to `<base>/<query>/<kind>-<task>`:
		// the flat one is what Worker.sweepStaleBuildCacheFiles reclaims after
		// a hard kill (its top-level `stage-`/`shuffle-` arms), and a nested
		// one under a `<query>` directory nothing removes would leak forever.
		// A collision is recoverable; an unreclaimable multi-GB orphan on a
		// spill volume is the failure ADR-0009 exists for.
		return filepath.Join(root, kind+"-"+scratchPathSegment(task.ID))
	}
	query := task.QueryID
	if query == "" {
		query = "no-query"
	}
	return filepath.Join(root, scratchPathSegment(query), kind+"-"+scratchPathSegment(task.ID))
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
