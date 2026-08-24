package coordinator

import (
	"context"
	"sort"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// Merge-on-read deletes on the stage DAG: one stamp, every carrier.
//
// A DELETE marks file-absolute row indices in the manifest instead of
// rewriting parquet, and every scan of the marked file has to skip them.
// The single-process engine reads the manifest at scan Init; the DAG's
// workers must be TOLD, because a worker reading the catalog itself would
// let two tasks of one stage see different revisions — a join would then
// find a row on one side and not the other.
//
// Where the declaration is attached is the whole design question. #423's
// declared-schema fix needed THREE carriers (OpSpec.ColumnTypes,
// OpSpec.BuildColumnTypes, Task.ColumnTypes) because a type belongs to an
// ALIAS, and every dispatcher that invents a new way to reach a base table
// has to pick one — a standing trap the internals map documents. A delete
// marker belongs to the FILE, not the alias, so it needs none of that:
// stampTaskDeleteMarkers walks every file list a task can carry and emits
// one task-level list. That is why it can live at the single choke point
// every dispatcher and every retry already passes through
// (Scheduler.PublishTasks), and why a future dispatcher gets it for free.
//
// The map itself is the plan's, not a fresh catalog read: executeStageDAG
// unions the stages' ScanDeletes (annotated at plan time from the same
// manifest object that produced their file lists) and parks it on the
// context for the dispatch subtree.

// queryDeleteMarkersKey is the context key for a query's plan-time delete
// state, keyed by data-file path.
type queryDeleteMarkersKey struct{}

// withQueryDeleteMarkers parks the query's delete state on ctx so every
// task published under it — by any dispatcher, on any retry — is stamped.
// A nil/empty map leaves ctx untouched: the overwhelmingly common case is a
// table with no deletes, and it must cost nothing.
func withQueryDeleteMarkers(ctx context.Context, deletes map[string][]int64) context.Context {
	if len(deletes) == 0 {
		return ctx
	}
	return context.WithValue(ctx, queryDeleteMarkersKey{}, deletes)
}

func queryDeleteMarkersFromContext(ctx context.Context) map[string][]int64 {
	if ctx == nil {
		return nil
	}
	d, _ := ctx.Value(queryDeleteMarkersKey{}).(map[string][]int64)
	return d
}

// collectStageDeletes unions every stage's plan-time delete state into one
// file-keyed map. Stages of one query may scan several tables and the same
// table twice (a self-join plans two scan stages); paths are unique per
// object, so one flat map answers for all of them.
func collectStageDeletes(stages []physical.Stage) map[string][]int64 {
	var out map[string][]int64
	for i := range stages {
		for file, rows := range stages[i].ScanDeletes {
			if len(rows) == 0 {
				continue
			}
			if out == nil {
				out = make(map[string][]int64, len(stages[i].ScanDeletes))
			}
			if _, seen := out[file]; seen {
				continue // same snapshot, same answer
			}
			out[file] = rows
		}
	}
	return out
}

// stampTaskDeleteMarkers attaches the delete markers for every base-table
// file this task reads, under whichever carrier it arrives on. Idempotent
// and cheap: a task whose files carry no markers gets nothing.
//
// The walk deliberately mirrors annotateTaskPeerLocations' — the same set
// of file-bearing fields, for the same reason (a dispatcher that invents a
// new home for input keys must teach both). The difference is the failure
// mode: a missed peer hint costs one failed fetch, a missed delete marker
// silently returns deleted rows.
func stampTaskDeleteMarkers(t *distributed.Task, deletes map[string][]int64) {
	if t == nil || len(deletes) == 0 {
		return
	}
	var specs []distributed.DeleteSpec
	seen := make(map[string]bool, 4)
	addAll := func(files []string) {
		for _, f := range files {
			rows, ok := deletes[f]
			if !ok || seen[f] {
				continue
			}
			seen[f] = true
			runs := scan.EncodeDeleteRuns(rows)
			if len(runs) == 0 {
				continue
			}
			specs = append(specs, distributed.DeleteSpec{File: f, Runs: runs})
		}
	}
	addAll(t.Files)
	addAll(t.InputFiles)
	addAll(t.BuildFiles)
	for _, fs := range t.Inputs {
		addAll(fs)
	}
	for _, fs := range t.PreScannedInputs {
		addAll(fs)
	}
	for _, fs := range t.ScanFileFilter {
		addAll(fs)
	}
	for i := range t.FusedJoins {
		addAll(t.FusedJoins[i].BuildFiles)
	}
	for i := range t.Operators {
		addAll(t.Operators[i].InputFiles)
		addAll(t.Operators[i].BuildFiles)
	}
	// Three of the walked fields are maps, so the order the specs come out
	// in is Go's map order. Sorting costs nothing at these sizes and makes a
	// task's serialized bytes reproducible, which is what a DLQ entry or a
	// dispatch diff is worth reading for.
	sort.Slice(specs, func(i, j int) bool { return specs[i].File < specs[j].File })
	t.DeleteMarkers = specs
}
