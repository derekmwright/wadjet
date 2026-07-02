package distributed

import "strings"

// ScratchQueryID extracts the root query ID from a query-scratch object key
// or prefix ("queries/<id>/..."), or "" when the key isn't query scratch
// (e.g. a table file). The root ID is the stable identity shared by every
// stage of one query — task QueryIDs are stage-scoped ("st-<stage>-<id>")
// and differ between the producer and consumer of a stage boundary, so
// cross-stage state (stage-output caches, streaming-exchange tokens and
// location hints) must key on the root, and the key is where to get it.
func ScratchQueryID(key string) string {
	rest, ok := strings.CutPrefix(key, "queries/")
	if !ok {
		return ""
	}
	id, _, ok := strings.Cut(rest, "/")
	if !ok {
		return ""
	}
	return id
}

// TaskRootQueryID derives a task's root query ID from its scratch prefixes
// and input file lists (all of which carry "queries/<id>/..." paths for
// stage-DAG tasks). Returns "" for tasks with no query-scratch anchors
// (e.g. legacy full-SQL pipeline tasks) — callers skip root-scoped
// bookkeeping for those.
func TaskRootQueryID(t *Task) string {
	if id := ScratchQueryID(t.ResultPrefix); id != "" {
		return id
	}
	if id := ScratchQueryID(t.Output); id != "" {
		return id
	}
	lists := [][]string{t.InputFiles, t.BuildFiles, t.Files}
	for _, l := range lists {
		for _, f := range l {
			if id := ScratchQueryID(f); id != "" {
				return id
			}
		}
	}
	for _, l := range t.Inputs {
		for _, f := range l {
			if id := ScratchQueryID(f); id != "" {
				return id
			}
		}
	}
	for _, l := range t.PreScannedInputs {
		for _, f := range l {
			if id := ScratchQueryID(f); id != "" {
				return id
			}
		}
	}
	for i := range t.FusedJoins {
		for _, f := range t.FusedJoins[i].BuildFiles {
			if id := ScratchQueryID(f); id != "" {
				return id
			}
		}
	}
	for k := range t.InputLocations {
		if id := ScratchQueryID(k); id != "" {
			return id
		}
	}
	return ""
}
