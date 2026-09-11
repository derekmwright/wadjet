package coordinator

import (
	"context"
	"sort"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// Every DAG scan must skip the manifest's file-absolute deleted row indices.
// Use the plan-time manifest, never worker catalog rereads that could split revisions.
// executeStageDAG unions ScanDeletes from the same manifests as the file lists
// and carries the map on the dispatch context.
// Delete identity belongs to the FILE, not the alias (unlike #423's type carriers).
// stampTaskDeleteMarkers walks every task file list at Scheduler.PublishTasks,
// so every dispatcher and every retry gets one task-level stamp.
// See docs/internals/dag-plan-time-delete-marker-stamp.md for the design.

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
	// PreComputedAggregates' CacheFiles are, like PreScannedInputs, always
	// the query-scoped .wshf output of a TaskTypePipeline sub-query that
	// already applied deletes when producing them (aggregate_shuffle.go's
	// preComputeDerivedAggregate) — never base-table parquet. Walked
	// anyway for the same reason PreScannedInputs is: cheap, and it keeps
	// every *Files field a task can carry covered by one classification
	// (task_field_carrier_coverage_test.go) instead of a documented
	// exception.
	for i := range t.PreComputedAggregates {
		addAll(t.PreComputedAggregates[i].CacheFiles)
	}
	// Three of the walked fields are maps, so the order the specs come out
	// in is Go's map order. Sorting costs nothing at these sizes and makes a
	// task's serialized bytes reproducible, which is what a DLQ entry or a
	// dispatch diff is worth reading for.
	sort.Slice(specs, func(i, j int) bool { return specs[i].File < specs[j].File })
	t.DeleteMarkers = specs
}
