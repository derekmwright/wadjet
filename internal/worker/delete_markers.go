package worker

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/scan"
)

// taskDeleteSets decodes a task's merge-on-read delete markers into the
// per-file form the scan source consults (#491).
//
// The map is keyed by S3 key, which is why ONE map serves every source a
// task builds: a marker belongs to the file, not to the alias reading it,
// so a self-join's two sides skip the same rows and a stage-output key
// simply never matches. Callers pass it to every source they construct;
// keys naming files a given source never opens cost a lookup that misses.
//
// A malformed payload is an ERROR, never a silently empty set. The whole
// point of the field is to remove rows, so a task that cannot read it must
// fail rather than answer with the deleted rows still in it — the #308
// position (a loud failure over a silently different answer).
//
// Returns nil for the common case of a task over tables with no deletes,
// so every caller's fast path is a nil map.
func taskDeleteSets(task distributed.Task) (map[string]*scan.DeleteSet, error) {
	if len(task.DeleteMarkers) == 0 {
		return nil, nil
	}
	out := make(map[string]*scan.DeleteSet, len(task.DeleteMarkers))
	for _, dm := range task.DeleteMarkers {
		if dm.File == "" {
			continue
		}
		set, err := scan.DecodeDeleteSet(dm.Runs)
		if err != nil {
			return nil, fmt.Errorf("task %s: delete markers for %s: %w", task.ID, dm.File, err)
		}
		if set.Empty() {
			continue
		}
		out[dm.File] = set
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
