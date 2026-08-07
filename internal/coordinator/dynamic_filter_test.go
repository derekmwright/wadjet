package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

func putPartial(t *testing.T, store objstore.Store, bucket, key string, keys []int64) distributed.DynamicFilterPartialRef {
	t.Helper()
	bloom := make([]uint64, 16)
	mask := uint64(len(bloom) - 1)
	art := &distributed.DynamicFilterArtifact{
		KeyType: "int64", RowCount: int64(len(keys)),
		Bloom: bloom, BloomMask: mask,
	}
	for _, k := range keys {
		h := hashForTest(k)
		bloom[h&mask] |= 1 << (h & 63)
		bloom[(h>>17)&mask] |= 1 << ((h >> 6) & 63)
	}
	var buf bytes.Buffer
	if err := distributed.EncodeDynamicFilterArtifact(&buf, art); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), bucket, key,
		bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	return distributed.DynamicFilterPartialRef{FilterID: "f1", Bucket: bucket, Key: key}
}

func hashForTest(k int64) uint64 {
	x := uint64(k)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return x
}

// mergeCompleteBuildStats must WITHHOLD a filter whose partial coverage is
// short of the task count: a bloom missing any task's keys falsely rejects
// rows at the consume side. Full coverage attaches; partial coverage drops.
func TestMergeCompleteBuildStats_CompletenessGate(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{catalog: cat, logger: slog.Default(), config: Config{ResultBucket: "test"}}

	var refs []distributed.DynamicFilterPartialRef
	for i := 0; i < 3; i++ {
		refs = append(refs, putPartial(t, store, "test",
			fmt.Sprintf("queries/q/dynfilter/s/task%d-f1.wdf", i), []int64{int64(i * 10)}))
	}

	// Full coverage: 3 partials, 3 tasks → attach.
	merged := c.mergeCompleteBuildStats(context.Background(), "q", "s", refs, 3, nil)
	if merged["f1"] == nil {
		t.Fatal("full coverage must attach the filter")
	}

	// Short coverage: 2 of 3 tasks → withhold.
	merged = c.mergeCompleteBuildStats(context.Background(), "q", "s", refs[:2], 3, nil)
	if merged["f1"] != nil {
		t.Fatal("incomplete coverage must withhold the filter (missing keys = false rejections)")
	}

	// Duplicate refs from a task retry (same key) must not fake coverage.
	dup := []distributed.DynamicFilterPartialRef{refs[0], refs[0], refs[1]}
	merged = c.mergeCompleteBuildStats(context.Background(), "q", "s", dup, 3, nil)
	if merged["f1"] != nil {
		t.Fatal("duplicate refs must dedupe by key; 2 distinct partials != 3 tasks")
	}
}

// LateAttach filters must be staged to the deterministic merged key even
// when inline-sized — attach-on-arrival consumers poll that key, so an
// inline-only merge would leave them permanently unfiltered.
func TestMergeCompleteBuildStats_ForceStageLateAttach(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{catalog: cat, logger: slog.Default(), config: Config{ResultBucket: "test"}}

	refs := []distributed.DynamicFilterPartialRef{
		putPartial(t, store, "test", "queries/q/dynfilter/s/task0-f1.wdf", []int64{7}),
	}

	// Without force: tiny bloom stays inline (no staged key).
	merged := c.mergeCompleteBuildStats(context.Background(), "q", "s", refs, 1, nil)
	if merged["f1"] == nil || merged["f1"].StagedKey != "" {
		t.Fatalf("tiny filter without force must stay inline: %+v", merged["f1"])
	}

	// With force: staged at the deterministic key and readable.
	merged = c.mergeCompleteBuildStats(context.Background(), "q", "s", refs, 1, map[string]bool{"f1": true})
	stats := merged["f1"]
	if stats == nil {
		t.Fatal("filter must attach")
	}
	wantKey := mergedDynamicFilterKey("q", "s", "f1")
	if stats.StagedKey != wantKey || stats.StagedBucket != "test" {
		t.Fatalf("force-stage key mismatch: got %q/%q want test/%q", stats.StagedBucket, stats.StagedKey, wantKey)
	}
	rc, _, err := store.Get(context.Background(), "test", wantKey)
	if err != nil {
		t.Fatalf("staged artifact must exist at the deterministic key: %v", err)
	}
	art, err := distributed.DecodeDynamicFilterArtifact(rc)
	rc.Close()
	if err != nil || len(art.Bloom) == 0 {
		t.Fatalf("staged artifact must decode with a bloom: %v", err)
	}
}

// Deferred spec construction: an attach-on-arrival consume whose emitter
// output is absent (the stat-dep edge no longer exists) must produce a
// Deferred spec pointing at the deterministic merged key; a wait-mode
// consume in the same state is silently omitted (existing degradation).
func TestDynamicFilterSpecsDeferred(t *testing.T) {
	consumes := []physical.DynamicFilterConsume{
		{FilterID: "fa", SourceStageID: "src-a", TargetColumn: "l_suppkey", KeyType: "int64", AttachOnArrival: true},
		{FilterID: "fb", SourceStageID: "src-b", TargetColumn: "l_orderkey", KeyType: "int64"},
	}
	specs := dynamicFilterSpecsFromBuildStats(consumes, map[string]StageOutput{}, "q", "bkt")
	if len(specs) != 1 {
		t.Fatalf("want 1 spec (deferred only), got %d", len(specs))
	}
	s := specs[0]
	if !s.Deferred || s.FilterID != "fa" || s.BloomBucket != "bkt" ||
		s.BloomKey != mergedDynamicFilterKey("q", "src-a", "fa") ||
		s.TargetColumn != "l_suppkey" {
		t.Fatalf("deferred spec malformed: %+v", s)
	}
	if s.PartialPrefix != dynamicFilterPartialPrefix("q", "src-a") {
		t.Fatalf("deferred spec must carry the partial prefix for incremental publication: %+v", s)
	}
}

// Kill switch WADJET_DF_INCREMENTAL_PARTIALS=0: deferred specs carry no
// PartialPrefix, so consumers poll the merged key only (pre-2026-08-07
// attach behavior).
func TestDynamicFilterSpecsDeferredIncrementalKillSwitch(t *testing.T) {
	old := dfIncrementalPartials
	dfIncrementalPartials = false
	defer func() { dfIncrementalPartials = old }()
	consumes := []physical.DynamicFilterConsume{
		{FilterID: "fa", SourceStageID: "src-a", TargetColumn: "l_suppkey", KeyType: "int64", AttachOnArrival: true},
	}
	specs := dynamicFilterSpecsFromBuildStats(consumes, map[string]StageOutput{}, "q", "bkt")
	if len(specs) != 1 || !specs[0].Deferred {
		t.Fatalf("want 1 deferred spec, got %+v", specs)
	}
	if specs[0].PartialPrefix != "" {
		t.Fatalf("kill switch must suppress PartialPrefix: %+v", specs[0])
	}
}

// The partial prefix must match where dispatchScanFilterStage's tasks
// actually upload: emitter scan tasks are published under the stage-scoped
// query ID ("st-<stageID>-<queryID>"), and the worker's
// finalizeDynamicFilterEmits keys partials under queries/<task.QueryID>/
// dynfilter/<stageID>/. This test pins the three-site agreement.
func TestDynamicFilterPartialPrefixMatchesEmitLayout(t *testing.T) {
	got := dynamicFilterPartialPrefix("qid-123", "scan-5")
	want := "queries/st-scan-5-qid-123/dynfilter/scan-5/"
	if got != want {
		t.Fatalf("partial prefix drifted from the emit-side key layout:\n got %q\nwant %q", got, want)
	}
}
