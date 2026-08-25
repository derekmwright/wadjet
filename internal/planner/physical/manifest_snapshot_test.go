package physical

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// countingKV wraps a catalog.MetaKV and counts Get calls per key, so a test
// can assert exactly how many catalog reads a statement paid — the "MetaKV
// Get hook pattern" #502 asks for, standing in for counting NATS KV reads
// without needing a real NATS server.
type countingKV struct {
	catalog.MetaKV
	mu   sync.Mutex
	gets map[string]int
}

func newCountingKV() *countingKV {
	return &countingKV{MetaKV: catalog.NewMemKV(), gets: make(map[string]int)}
}

func (c *countingKV) Get(key string) ([]byte, uint64, error) {
	c.mu.Lock()
	c.gets[key]++
	c.mu.Unlock()
	return c.MetaKV.Get(key)
}

// manifestGets sums Get calls across every key containing "manifest." —
// Catalog namespaces the key by cluster ID (c.key("manifest."+table)), so
// this does not need to reconstruct that prefix.
func (c *countingKV) manifestGets() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, v := range c.gets {
		if strings.Contains(k, "manifest.") {
			n += v
		}
	}
	return n
}

// setupSelfJoinCatalog builds a one-table catalog behind a countingKV, with
// enough files that a manifest read is not a degenerate empty case.
func setupSelfJoinCatalog(t *testing.T) (*catalog.Catalog, *countingKV, context.Context) {
	t.Helper()
	ctx := context.Background()
	kv := newCountingKV()
	store := objstore.NewMemStore()
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}}
	if err := cat.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	files := []catalog.FileEntry{
		{Path: "tables/t/chunk_0000.parquet", SizeBytes: 1024, NumRows: 100},
		{Path: "tables/t/chunk_0001.parquet", SizeBytes: 1024, NumRows: 100},
	}
	if err := cat.AddFiles(ctx, "t", map[string]string{}, "tables/t/", files); err != nil {
		t.Fatalf("add files: %v", err)
	}
	// The catalog init/create/add-files calls above are setup, not part of
	// the statement under test — reset the counters so the test measures
	// only what planning a QUERY costs.
	kv.mu.Lock()
	kv.gets = make(map[string]int)
	kv.mu.Unlock()
	return cat, kv, ctx
}

// selfJoinLogicalPlan parses and builds the logical plan for a self-join
// over "t" — two NodeScan nodes naming the SAME table, the shape a query
// with two subqueries or a self-join takes (#502's own motivating example).
func selfJoinLogicalPlan(t *testing.T) *logical.Node {
	t.Helper()
	const sql = "SELECT t1.id FROM t t1 JOIN t t2 ON t1.id = t2.id"
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return logicalPlan
}

// TestManifestSnapshotPinsReadsAcrossAStatement is the #502 regression: a
// self-join statement over one table — annotated, optimized (which
// re-annotates), planned and (via PlanDistributed's NodeScan handling)
// walked into stages — reads the catalog exactly TWICE for table "t": once
// for its manifest, once for its aggregated column stats (a separate
// Catalog operation with its own internal manifest read — see
// ManifestSnapshot's doc for why the floor is two, not one) — when every
// physical.Planner built for the statement shares one context-attached
// ManifestSnapshot (physical.NewPlannerForContext), reproducing the
// coordinator's own ExecuteSQL/SubmitSQL wiring. Without the pin (plain
// NewPlanner, no context) it reads the catalog many times over: at least
// twice per scan node per AnnotateScanColumns call, and AnnotateScanColumns
// runs again for every logical.Optimize pass — see
// TestManifestSnapshotUnpinnedReadsRepeatedly.
func TestManifestSnapshotPinsReadsAcrossAStatement(t *testing.T) {
	cat, kv, ctx := setupSelfJoinCatalog(t)
	ctx = WithManifestSnapshot(ctx, NewManifestSnapshot())

	logicalPlan := selfJoinLogicalPlan(t)
	scanAnnotator := func(plan *logical.Node) {
		NewPlannerForContext(ctx, cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlannerForContext(ctx, cat)
	planner.WorkerCount = 2
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	scanStages := 0
	for i := range stages {
		if stages[i].Type == StageScan && stages[i].TableName == "t" {
			scanStages++
			if len(stages[i].ScanFiles) != 2 {
				t.Errorf("stage %s: %d scan files, want 2", stages[i].ID, len(stages[i].ScanFiles))
			}
		}
	}
	if scanStages < 2 {
		t.Fatalf("only %d scan stages for table t — the self-join did not produce the two-scan-node shape this test needs", scanStages)
	}

	if got := kv.manifestGets(); got != 2 {
		t.Errorf("manifest Get calls = %d, want exactly 2 (one GetManifest, one AggregateColumnStats) "+
			"for a self-join over one table pinned to one statement", got)
	}
}

// TestManifestSnapshotUnpinnedReadsRepeatedly is the contrast case: the
// SAME self-join, planned with a fresh (unshared) Planner at each step —
// what every call site looked like before #502 — reads the manifest more
// than once. It is not asserting an exact count (that number is an
// implementation-count accident of how many annotation/estimate passes
// happen to run), only that the pin in the test above is doing real work:
// without it, the read count is NOT 1.
func TestManifestSnapshotUnpinnedReadsRepeatedly(t *testing.T) {
	cat, kv, ctx := setupSelfJoinCatalog(t)

	logicalPlan := selfJoinLogicalPlan(t)
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan) // fresh Planner, fresh (unshared) snapshot
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = 2
	if _, err := planner.PlanDistributed(ctx, logicalPlan); err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	if got := kv.manifestGets(); got <= 1 {
		t.Errorf("manifest Get calls = %d, want more than 1 without a shared ManifestSnapshot "+
			"(if this starts failing, the pin below may not be needed any more — or something else started caching)", got)
	}
}

// TestManifestSnapshotIgnoresAConcurrentWriteMidStatement is the
// correctness half of #502 (the #491 review's finding): once a statement
// has pinned a table's manifest, a write that lands DURING the same
// statement's planning must not be observed by any later reader in that
// statement — the collectStageDeletes first-wins race this closes.
func TestManifestSnapshotIgnoresAConcurrentWriteMidStatement(t *testing.T) {
	cat, kv, ctx := setupSelfJoinCatalog(t)
	ctx = WithManifestSnapshot(ctx, NewManifestSnapshot())

	logicalPlan := selfJoinLogicalPlan(t)
	scanAnnotator := func(plan *logical.Node) {
		NewPlannerForContext(ctx, cat).AnnotateScanColumns(ctx, plan)
	}
	// First read: pins the 2-file manifest (and its column stats) into
	// this statement's snapshot.
	scanAnnotator(logicalPlan)

	// A concurrent write lands mid-statement — a third file added to "t"
	// after this statement has already read its manifest once. AddFiles'
	// own compare-and-swap update reads the manifest key too (a real,
	// separate read this test does not attribute to the statement under
	// test), so the counter resets after it to isolate what the REST of
	// this statement's planning costs.
	if err := cat.AddFiles(context.Background(), "t", map[string]string{}, "tables/t/",
		[]catalog.FileEntry{{Path: "tables/t/chunk_0002.parquet", SizeBytes: 1024, NumRows: 100}}); err != nil {
		t.Fatalf("add files mid-statement: %v", err)
	}
	kv.mu.Lock()
	kv.gets = make(map[string]int)
	kv.mu.Unlock()

	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)
	planner := NewPlannerForContext(ctx, cat)
	planner.WorkerCount = 2
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	for i := range stages {
		if stages[i].Type == StageScan && stages[i].TableName == "t" {
			if len(stages[i].ScanFiles) != 2 {
				t.Errorf("stage %s: %d scan files, want 2 (the PRE-write file count — "+
					"the mid-statement AddFiles must not be observed)", stages[i].ID, len(stages[i].ScanFiles))
			}
		}
	}
	if got := kv.manifestGets(); got != 0 {
		t.Errorf("manifest Get calls after the concurrent write = %d, want exactly 0 — "+
			"the rest of this statement's planning must reuse what it already pinned, not re-read", got)
	}

	// A NEW statement (fresh context, fresh snapshot) DOES see the write —
	// the pin is per-statement, not a permanent staleness.
	ctx2 := WithManifestSnapshot(context.Background(), NewManifestSnapshot())
	logicalPlan2 := selfJoinLogicalPlan(t)
	NewPlannerForContext(ctx2, cat).AnnotateScanColumns(ctx2, logicalPlan2)
	logicalPlan2 = logical.Optimize(logicalPlan2, func(plan *logical.Node) {
		NewPlannerForContext(ctx2, cat).AnnotateScanColumns(ctx2, plan)
	})
	planner2 := NewPlannerForContext(ctx2, cat)
	planner2.WorkerCount = 2
	stages2, err := planner2.PlanDistributed(ctx2, logicalPlan2)
	if err != nil {
		t.Fatalf("PlanDistributed (second statement): %v", err)
	}
	for i := range stages2 {
		if stages2[i].Type == StageScan && stages2[i].TableName == "t" {
			if len(stages2[i].ScanFiles) != 3 {
				t.Errorf("second statement stage %s: %d scan files, want 3 (a NEW statement must see the write)",
					stages2[i].ID, len(stages2[i].ScanFiles))
			}
		}
	}
}

// TestManifestSnapshotClosesTheDeleteMarkerRace pins the #491-review half
// of #502: collectStageDeletes (internal/coordinator/delete_markers.go)
// unions every stage's ScanDeletes FIRST-WINS on a file both saw, which is
// only sound when every scan node of one table in one statement built its
// ScanDeletes from the SAME manifest snapshot. Before the pin, each of the
// self-join's two scan nodes called p.catalog.GetManifest independently, so
// a DELETE landing between those two reads could leave the two nodes
// disagreeing about which rows are deleted. With the pin, both scan nodes'
// walkStages calls read through the same ManifestSnapshot, so their
// Stage.ScanDeletes are built from the IDENTICAL manifest object — this
// asserts that directly, rather than only the derived file-list count
// TestManifestSnapshotIgnoresAConcurrentWriteMidStatement checks.
func TestManifestSnapshotClosesTheDeleteMarkerRace(t *testing.T) {
	cat, _, ctx := setupSelfJoinCatalog(t)
	if err := cat.AddDeleteMarkers(context.Background(), "t", []catalog.DeleteMarker{
		{FilePath: "tables/t/chunk_0000.parquet", RowIndices: []int64{3, 7}},
	}); err != nil {
		t.Fatalf("AddDeleteMarkers: %v", err)
	}
	ctx = WithManifestSnapshot(ctx, NewManifestSnapshot())

	logicalPlan := selfJoinLogicalPlan(t)
	scanAnnotator := func(plan *logical.Node) {
		NewPlannerForContext(ctx, cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)

	// A second DELETE lands between the manifest being pinned and stage
	// construction — must not be observed by either scan node.
	if err := cat.AddDeleteMarkers(context.Background(), "t", []catalog.DeleteMarker{
		{FilePath: "tables/t/chunk_0001.parquet", RowIndices: []int64{1}},
	}); err != nil {
		t.Fatalf("AddDeleteMarkers (mid-statement): %v", err)
	}

	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)
	planner := NewPlannerForContext(ctx, cat)
	planner.WorkerCount = 2
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	var scanDeletesSeen []map[string][]int64
	for i := range stages {
		if stages[i].Type == StageScan && stages[i].TableName == "t" {
			scanDeletesSeen = append(scanDeletesSeen, stages[i].ScanDeletes)
		}
	}
	if len(scanDeletesSeen) < 2 {
		t.Fatalf("only %d scan stages for table t — need at least 2 for this test", len(scanDeletesSeen))
	}
	for _, sd := range scanDeletesSeen {
		if len(sd) != 1 {
			t.Fatalf("ScanDeletes = %v, want exactly the pre-statement marker on chunk_0000 "+
				"(the mid-statement AddDeleteMarkers on chunk_0001 must not be observed)", sd)
		}
		if rows, ok := sd["tables/t/chunk_0000.parquet"]; !ok || len(rows) != 2 {
			t.Fatalf("ScanDeletes = %v, want {tables/t/chunk_0000.parquet: [3 7]}", sd)
		}
	}
	// Every scan node agrees — not merely by coincidence of content, but
	// because they read the identical manifest object.
	for i := 1; i < len(scanDeletesSeen); i++ {
		got, want := scanDeletesSeen[i], scanDeletesSeen[0]
		if len(got) != len(want) {
			t.Fatalf("scan node %d's ScanDeletes disagrees with scan node 0's: %v vs %v", i, got, want)
		}
	}
}
