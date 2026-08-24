package coordinator

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// The three carriers #423 needed a separate field for — OpSpec.ColumnTypes
// (fragment source), OpSpec.BuildColumnTypes (join build), Task.ColumnTypes
// (shuffle's implicit scan) — plus every other file list a task can hold.
// One stamp has to cover all of them, because a marker missed on any one of
// them is deleted rows coming back through that carrier only.
func TestStampTaskDeleteMarkersCoversEveryFileCarrier(t *testing.T) {
	const marked = "tables/t/marked.parquet"
	const clean = "tables/t/clean.parquet"
	deletes := map[string][]int64{marked: {1, 2, 3}}

	carriers := map[string]distributed.Task{
		// Task.Files — the shuffle task's implicit parquet scan.
		"task-files": {Files: []string{marked, clean}},
		// Task.Inputs — the gather task reading a pass-through leaf scan,
		// and every compute stage's alias map.
		"task-inputs": {Inputs: map[string][]string{"t": {marked}}},
		// Task.InputFiles / BuildFiles — the legacy per-stage handlers.
		"task-input-files": {InputFiles: []string{marked}},
		"task-build-files": {BuildFiles: []string{marked}},
		// Probe-split / scan-split pipelines.
		"pre-scanned":      {PreScannedInputs: map[string][]string{"t": {marked}}},
		"scan-file-filter": {ScanFileFilter: map[string][]string{"t": {marked}}},
		// Fragment operators — the source and the join build read their own
		// file lists, which no Task-level field mirrors for every dispatcher.
		"op-input-files": {Operators: []distributed.OpSpec{{Type: distributed.OpScan, InputFiles: []string{marked}}}},
		"op-build-files": {Operators: []distributed.OpSpec{{Type: distributed.OpHashJoinProbe, BuildFiles: []string{marked}}}},
		// Chained-join builds from stage-chain fusion.
		"fused-join-builds": {FusedJoins: []distributed.FusedJoinSpec{{BuildFiles: []string{marked}}}},
	}

	for name, task := range carriers {
		t.Run(name, func(t *testing.T) {
			stampTaskDeleteMarkers(&task, deletes)
			if len(task.DeleteMarkers) != 1 {
				t.Fatalf("carrier %s got %d specs, want 1 — a task reading %s was dispatched "+
					"without its delete markers", name, len(task.DeleteMarkers), marked)
			}
			if task.DeleteMarkers[0].File != marked {
				t.Fatalf("stamped the wrong file: %s", task.DeleteMarkers[0].File)
			}
			set, err := scan.DecodeDeleteSet(task.DeleteMarkers[0].Runs)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if set.Rows() != 3 || !set.Contains(2) {
				t.Fatalf("round trip lost rows: %d rows, contains(2)=%v", set.Rows(), set.Contains(2))
			}
		})
	}

	// A file with no markers contributes nothing, and neither does a task
	// that reads none: the common case must stay off the wire entirely.
	clean1 := distributed.Task{Files: []string{clean}}
	stampTaskDeleteMarkers(&clean1, deletes)
	if clean1.DeleteMarkers != nil {
		t.Fatalf("a task over unmarked files carries %d specs", len(clean1.DeleteMarkers))
	}
	none := distributed.Task{Files: []string{marked}}
	stampTaskDeleteMarkers(&none, nil)
	if none.DeleteMarkers != nil {
		t.Fatal("a query with no deletes must stamp nothing")
	}

	// A self-join names the same file under two aliases; it must be stamped
	// ONCE, and both readers get the same set from the one entry.
	selfJoin := distributed.Task{
		Inputs: map[string][]string{"t": {marked}},
		Operators: []distributed.OpSpec{
			{Type: distributed.OpScan, InputFiles: []string{marked}},
			{Type: distributed.OpHashJoinProbe, BuildFiles: []string{marked}},
		},
	}
	stampTaskDeleteMarkers(&selfJoin, deletes)
	if len(selfJoin.DeleteMarkers) != 1 {
		t.Fatalf("a self-join stamped %d specs for one file", len(selfJoin.DeleteMarkers))
	}
}

// The union that feeds the stamp has to see every stage's markers — a query
// over two tables, or a self-join planning two scan stages, must not lose
// one of them.
func TestCollectStageDeletesUnionsEveryStage(t *testing.T) {
	stages := []physical.Stage{
		{ID: "scan-0", TableName: "a", ScanDeletes: map[string][]int64{"tables/a/f0.parquet": {1}}},
		{ID: "scan-1", TableName: "b", ScanDeletes: map[string][]int64{"tables/b/f0.parquet": {2, 3}}},
		{ID: "join-2"},
	}
	got := collectStageDeletes(stages)
	if len(got) != 2 {
		t.Fatalf("union has %d files, want 2: %v", len(got), got)
	}
	if len(got["tables/b/f0.parquet"]) != 2 {
		t.Fatalf("second stage's markers lost: %v", got)
	}
	if collectStageDeletes([]physical.Stage{{ID: "scan-0"}}) != nil {
		t.Fatal("a plan with no deletes must produce a nil map")
	}
}

// #491's perf question, answered on the real wire form: how big does a task
// spec get for a 1000-file table with sparse deletes, and does a
// bitmap-per-file blow the 8 MB NATS payload cap?
//
// It does not, for two reasons, and both are asserted here.
//
//  1. The stamp is per TASK, over the files that task actually reads. The
//     realistic shape is a scan stage fanning out one task per file, and
//     that spec is measured below in the hundreds of bytes to tens of KB.
//  2. The runs encoding is strictly smaller than the manifest's own JSON
//     for the same markers — contiguous deletes collapse to two varints
//     however many rows they cover, scattered ones cost ~2 B against ~8.
//     Since the manifest is a single NATS KV value under the same message
//     size limit, a marker set the CATALOG cannot hold is unreachable, and
//     every set it can hold encodes to less than it did. The catalog, not
//     the task spec, is the binding constraint — which is why this needs no
//     S3 side channel.
//
// The residual is a single task that reads a very large number of heavily
// marked files at once (the unfiltered pass-through gather over a whole
// table). That fails LOUDLY at nc.Publish with ErrMaxPayload — a stage
// error the client sees — never silently, and never with the deleted rows
// back in the answer.
func TestTaskSpecSizeWithDeleteMarkers(t *testing.T) {
	const natsCap = 8 << 20
	const files = 1000
	rng := rand.New(rand.NewSource(3))

	sparse := make([]int64, 8)
	for i := range sparse {
		sparse[i] = int64(rng.Intn(1_000_000))
	}
	contiguous := make([]int64, 500_000)
	for i := range contiguous {
		contiguous[i] = int64(i)
	}
	scattered := make([]int64, 10_000)
	for i := range scattered {
		scattered[i] = int64(i) * 100
	}

	shapes := []struct {
		name string
		rows []int64
	}{
		{"8 sparse deletes per file", sparse},
		{"one 500k contiguous run per file", contiguous},
		{"1% scattered (10k rows) per file", scattered},
	}

	for _, sh := range shapes {
		key := "tables/lineitem/chunk_00000000.parquet"
		// The per-task spec, marshalled exactly as it goes on the wire.
		perTask := distributed.Task{ID: "t", Files: []string{key}}
		stampTaskDeleteMarkers(&perTask, map[string][]int64{key: sh.rows})
		perTaskBytes, err := distributed.Marshal(perTask)
		if err != nil {
			t.Fatalf("%s: marshal: %v", sh.name, err)
		}
		bare, err := distributed.Marshal(distributed.Task{ID: "t", Files: []string{key}})
		if err != nil {
			t.Fatalf("%s: marshal bare: %v", sh.name, err)
		}
		perFileCost := len(perTaskBytes) - len(bare)

		// Like for like: the same file's markers as the CATALOG holds them,
		// one catalog.DeleteMarker marshalled as the manifest marshals it —
		// path, row indices, timestamp — against one DeleteSpec's cost on
		// the wire.
		manifestJSON, err := json.Marshal(catalog.DeleteMarker{
			FilePath: key, RowIndices: sh.rows, CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("%s: marshal manifest form: %v", sh.name, err)
		}

		t.Logf("%-34s per-file wire=%8d B  per-file manifest JSON=%10d B  spec(1 file)=%8d B  projected spec(%d files)=%10d B",
			sh.name, perFileCost, len(manifestJSON), len(perTaskBytes), files, len(bare)+files*perFileCost)

		if perFileCost >= len(manifestJSON) {
			t.Errorf("%s: the wire form costs %d B against the manifest's own %d B — the "+
				"catalog is supposed to be the binding constraint", sh.name, perFileCost, len(manifestJSON))
		}
		if len(perTaskBytes) >= natsCap {
			t.Errorf("%s: a one-file scan task's spec is %d B, at or over the %d B NATS cap",
				sh.name, len(perTaskBytes), natsCap)
		}
	}
}
