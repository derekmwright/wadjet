package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// lowerSkewThresholds drops the skew-split byte thresholds so small test
// fixtures cross them, restoring the defaults on cleanup.
func lowerSkewThresholds(t *testing.T, target, floor int64) {
	t.Helper()
	oldTarget, oldFloor := skewSplitTargetBytes, skewSplitMinGroupBytes
	skewSplitTargetBytes, skewSplitMinGroupBytes = target, floor
	t.Cleanup(func() {
		skewSplitTargetBytes, skewSplitMinGroupBytes = oldTarget, oldFloor
	})
}

func skewTestStage() physical.Stage {
	return physical.Stage{
		ID:              "join-1",
		Type:            physical.StageHashJoin,
		JoinType:        "inner",
		Dependencies:    []string{"probe-stage", "build-stage"},
		LeftDepStage:    "probe-stage",
		RightDepStage:   "build-stage",
		BuildTableAlias: "d",
		Distribution:    physical.Distribution{Kind: physical.DistHashPartitioned, Keys: []string{"k"}, Count: 3},
	}
}

// skewTestOutput builds an OutputPartitioned StageOutput with nFiles files
// per partition and the given per-partition bytes.
func skewTestOutput(prefix string, partBytes []int64, nFiles int) StageOutput {
	files := make([][]string, len(partBytes))
	for p := range partBytes {
		for f := 0; f < nFiles; f++ {
			files[p] = append(files[p], fmt.Sprintf("%s/partition=%04d/t%d.wshf", prefix, p, f))
		}
	}
	rows := make([]int64, len(partBytes))
	for p, b := range partBytes {
		rows[p] = b / 100
	}
	return StageOutput{
		Kind:           OutputPartitioned,
		NumPartitions:  len(partBytes),
		Files:          files,
		PartitionRows:  rows,
		PartitionBytes: partBytes,
	}
}

func TestPlanSkewSplitTasks_UniformNoSplit(t *testing.T) {
	lowerSkewThresholds(t, 100<<20, 100<<20)
	c := &Coordinator{logger: slog.Default()}
	inputs := map[string]StageOutput{
		"probe-stage": skewTestOutput("probe", []int64{10 << 20, 12 << 20, 11 << 20}, 2),
		"build-stage": skewTestOutput("build", []int64{1 << 20, 1 << 20, 1 << 20}, 2),
	}
	if got := c.planSkewSplitTasks(skewTestStage(), inputs, 3, 3); got != nil {
		t.Fatalf("uniform layout planned a split: %+v", got)
	}
}

func TestPlanSkewSplitTasks_HotGroupSplits(t *testing.T) {
	lowerSkewThresholds(t, 100<<20, 100<<20)
	c := &Coordinator{logger: slog.Default()}
	probe := skewTestOutput("probe", []int64{10 << 20, 400 << 20, 10 << 20}, 4)
	build := skewTestOutput("build", []int64{1 << 20, 2 << 20, 1 << 20}, 2)
	inputs := map[string]StageOutput{"probe-stage": probe, "build-stage": build}

	before := SkewSplitsPlanned.Load()
	got := c.planSkewSplitTasks(skewTestStage(), inputs, 3, 3)
	if got == nil {
		t.Fatal("hot group not split")
	}
	if SkewSplitsPlanned.Load() != before+1 {
		t.Errorf("SkewSplitsPlanned delta = %d, want 1", SkewSplitsPlanned.Load()-before)
	}
	// Group 1 is hot: k = ceil(400MB/100MB) = 4, capped at workerCount 3.
	// Layout: group 0 (1 task), group 1 (3 sub-tasks), group 2 (1 task).
	if len(got) != 5 {
		t.Fatalf("len(assignments) = %d, want 5 (%+v)", len(got), got)
	}
	wantGroups := []int{0, 1, 1, 1, 2}
	for i, a := range got {
		if a.group != wantGroups[i] {
			t.Errorf("assignment %d group = %d, want %d", i, a.group, wantGroups[i])
		}
	}
	// Every group-1 sub-task replicates the FULL build files of partition 1
	// and the probe slices are a disjoint cover of partition 1's files.
	var probeSlices []string
	for _, a := range got[1:4] {
		if len(a.inputs["d"]) != len(build.Files[1]) {
			t.Errorf("sub-task build files = %v, want full %v", a.inputs["d"], build.Files[1])
		}
		if a.estBytes != (2<<20)+(400<<20)/3 {
			t.Errorf("sub-task estBytes = %d, want %d", a.estBytes, (2<<20)+(400<<20)/3)
		}
		probeSlices = append(probeSlices, a.inputs["probe"]...)
	}
	sort.Strings(probeSlices)
	wantProbe := append([]string(nil), probe.Files[1]...)
	sort.Strings(wantProbe)
	if len(probeSlices) != len(wantProbe) {
		t.Fatalf("probe slices cover %d files, want %d", len(probeSlices), len(wantProbe))
	}
	for i := range wantProbe {
		if probeSlices[i] != wantProbe[i] {
			t.Errorf("probe slice file %d = %s, want %s", i, probeSlices[i], wantProbe[i])
		}
	}
	// Non-hot assignments must match the unsplit layout's binding exactly.
	for _, a := range []skewTaskAssignment{got[0], got[4]} {
		wantInputs, err := buildTaskInputsForStage(skewTestStage(), inputs, a.group, 3)
		if err != nil {
			t.Fatalf("buildTaskInputsForStage: %v", err)
		}
		for alias, want := range wantInputs {
			gotFiles := a.inputs[alias]
			if len(gotFiles) != len(want) {
				t.Fatalf("group %d alias %s: %v, want %v", a.group, alias, gotFiles, want)
			}
			for i := range want {
				if gotFiles[i] != want[i] {
					t.Errorf("group %d alias %s file %d: %s, want %s", a.group, alias, i, gotFiles[i], want[i])
				}
			}
		}
	}
}

// TestPlanSkewSplitTasks_UniformHeavyNoSplit encodes the TPC-H Q21 SF10
// profile from the 2026-07-11 no-harm A/B: every group's probe crosses the
// absolute floor (~374 MB vs the 256 MiB default) but the layout is
// perfectly uniform (mean_ratio ≈ 1.0). v1's floor-only trigger split every
// group k=2 and regressed Q21 +21% wall for zero skew relief; the ratio
// gate must keep the standard layout. Runs at REAL default thresholds.
func TestPlanSkewSplitTasks_UniformHeavyNoSplit(t *testing.T) {
	c := &Coordinator{logger: slog.Default()}
	inputs := map[string]StageOutput{
		"probe-stage": skewTestOutput("probe", []int64{374 << 20, 374 << 20, 374 << 20}, 6),
		"build-stage": skewTestOutput("build", []int64{24 << 20, 24 << 20, 24 << 20}, 2),
	}
	if got := c.planSkewSplitTasks(skewTestStage(), inputs, 3, 3); got != nil {
		t.Fatalf("uniform heavy layout planned a split: %+v", got)
	}
}

func TestPlanSkewSplitTasks_BuildSideHotNoSplit(t *testing.T) {
	lowerSkewThresholds(t, 100<<20, 100<<20)
	c := &Coordinator{logger: slog.Default()}
	inputs := map[string]StageOutput{
		"probe-stage": skewTestOutput("probe", []int64{10 << 20, 400 << 20, 10 << 20}, 4),
		// Build partition 1 exceeds skewSplitMaxBuildBytes (200 MiB):
		// replicating it would multiply the OOM, so the group must not split.
		"build-stage": skewTestOutput("build", []int64{1 << 20, 300 << 20, 1 << 20}, 2),
	}
	if got := c.planSkewSplitTasks(skewTestStage(), inputs, 3, 3); got != nil {
		t.Fatalf("build-side-hot group split anyway: %+v", got)
	}
}

func TestPlanSkewSplitTasks_IneligibleShapes(t *testing.T) {
	lowerSkewThresholds(t, 100<<20, 100<<20)
	c := &Coordinator{logger: slog.Default()}
	hotProbe := skewTestOutput("probe", []int64{10 << 20, 400 << 20, 10 << 20}, 4)
	build := skewTestOutput("build", []int64{1 << 20, 2 << 20, 1 << 20}, 2)

	// Right/full outer joins emit unmatched BUILD rows — replication would
	// duplicate them.
	for _, jt := range []string{"right", "full", "cross"} {
		stage := skewTestStage()
		stage.JoinType = jt
		if got := c.planSkewSplitTasks(stage, map[string]StageOutput{"probe-stage": hotProbe, "build-stage": build}, 3, 3); got != nil {
			t.Errorf("join type %q split: %+v", jt, got)
		}
	}
	// Missing vectors (legacy workers) → off.
	noVec := hotProbe
	noVec.PartitionBytes = nil
	if got := c.planSkewSplitTasks(skewTestStage(), map[string]StageOutput{"probe-stage": noVec, "build-stage": build}, 3, 3); got != nil {
		t.Errorf("nil probe vectors split: %+v", got)
	}
	noBuildVec := build
	noBuildVec.PartitionBytes = nil
	if got := c.planSkewSplitTasks(skewTestStage(), map[string]StageOutput{"probe-stage": hotProbe, "build-stage": noBuildVec}, 3, 3); got != nil {
		t.Errorf("nil build vectors split: %+v", got)
	}
	// Single worker → nothing to fan out to.
	if got := c.planSkewSplitTasks(skewTestStage(), map[string]StageOutput{"probe-stage": hotProbe, "build-stage": build}, 3, 1); got != nil {
		t.Errorf("single-worker split: %+v", got)
	}
	// SMJ excluded v1 (needs aligned sorted runs).
	smj := skewTestStage()
	smj.Type = physical.StageSortMergeJoin
	if got := c.planSkewSplitTasks(smj, map[string]StageOutput{"probe-stage": hotProbe, "build-stage": build}, 3, 3); got != nil {
		t.Errorf("SMJ split: %+v", got)
	}
}

// TestPlanSkewSplitTasks_RangeGrouping: numTasks < NumPartitions — groups
// bind contiguous partition ranges exactly as partitionFilesForWorker does,
// and hotness is judged per group (range sum), not per partition.
func TestPlanSkewSplitTasks_RangeGrouping(t *testing.T) {
	lowerSkewThresholds(t, 100<<20, 100<<20)
	c := &Coordinator{logger: slog.Default()}
	// 6 partitions, 3 tasks → ranges [0,2) [2,4) [4,6). Partition 3 is hot.
	probe := skewTestOutput("probe", []int64{5 << 20, 5 << 20, 10 << 20, 350 << 20, 5 << 20, 5 << 20}, 3)
	build := skewTestOutput("build", []int64{1 << 20, 1 << 20, 1 << 20, 1 << 20, 1 << 20, 1 << 20}, 1)
	inputs := map[string]StageOutput{"probe-stage": probe, "build-stage": build}
	got := c.planSkewSplitTasks(skewTestStage(), inputs, 3, 3)
	if got == nil {
		t.Fatal("hot range not split")
	}
	// Group 1 (partitions 2+3, 360 MB) → k = ceil(360/100) = 4 → cap 3.
	if len(got) != 5 {
		t.Fatalf("len(assignments) = %d, want 5", len(got))
	}
	// The group's probe files must cover BOTH partitions of the range.
	var hotFiles []string
	for _, a := range got[1:4] {
		hotFiles = append(hotFiles, a.inputs["probe"]...)
	}
	want := len(probe.Files[2]) + len(probe.Files[3])
	if len(hotFiles) != want {
		t.Fatalf("hot group covers %d probe files, want %d", len(hotFiles), want)
	}
}

// TestSkewSplitParity is the end-to-end dispatch gate: a 3-worker cluster
// over a manufactured hot-key dataset, the same join queries run with
// --skew-split off and on, rows must be identical and the on-arm must have
// actually planned splits (SkewSplitsPlanned counter — the mechanism
// marker). 1-worker in-process suites cannot catch dispatch bugs; this is
// the fixture the design memo requires before any deploy.
func TestSkewSplitParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	// Thresholds low enough that only the hot key's partition group
	// crosses: hot group carries ~90% of ~5 MB, others share the rest.
	lowerSkewThresholds(t, 512<<10, 512<<10)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connecting to NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("creating JetStream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setting up streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("creating NATS KV: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	// Fixture: events (probe, skewed — 90% of rows on one key) joins dims
	// (build, uniform). Some event keys are absent from dims so the LEFT
	// JOIN query exercises unmatched-probe emission under a replicated
	// build. events is written as 8 chunks so the shuffle fans out and the
	// hot partition accumulates multiple files (v1 splits at file
	// granularity).
	eventsSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString},
	}}
	dimsSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "events", eventsSchema, nil); err != nil {
		t.Fatalf("creating events: %v", err)
	}
	if err := cat.CreateTable(ctx, "dims", dimsSchema, nil); err != nil {
		t.Fatalf("creating dims: %v", err)
	}

	const (
		hotKey    = int64(7)
		numDims   = 48 // dims hold keys [0, 48); event keys [0, 64) — keys ≥ 48 miss
		numChunks = 8
		rowsPer   = 8192
	)
	pad := make([]byte, 56)
	for i := range pad {
		pad[i] = 'x'
	}
	writeChunk := func(table string, schema parquet.Schema, rows []map[string]any, ci int) {
		t.Helper()
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer %s/%d: %v", table, ci, err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatalf("writing %s/%d: %v", table, ci, err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("closing %s/%d: %v", table, ci, err)
		}
		filePath := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, ci)
		pdata := buf.Bytes()
		if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
			t.Fatalf("storing %s/%d: %v", table, ci, err)
		}
		if err := cat.AddFiles(ctx, table, map[string]string{}, "tables/"+table+"/", []catalog.FileEntry{{
			Path:      filePath,
			SizeBytes: int64(len(pdata)),
			NumRows:   int64(len(rows)),
			CreatedAt: time.Now(),
		}}); err != nil {
			t.Fatalf("manifest %s/%d: %v", table, ci, err)
		}
	}

	for ci := 0; ci < numChunks; ci++ {
		rows := make([]map[string]any, rowsPer)
		for i := range rows {
			k := hotKey
			if i%10 == 9 { // 90% hot, 10% spread over [0, 64)
				k = int64((ci*rowsPer + i) % 64)
			}
			rows[i] = map[string]any{"k": k, "v": string(pad)}
		}
		writeChunk("events", eventsSchema, rows, ci)
	}
	dimRows := make([]map[string]any, numDims)
	for i := range dimRows {
		dimRows[i] = map[string]any{"k": int64(i), "name": fmt.Sprintf("dim-%02d", i)}
	}
	writeChunk("dims", dimsSchema, dimRows, 0)

	for i := 0; i < 3; i++ {
		w := worker.New(worker.Config{
			NATSUrl:       embeddedNATS.ClientURL(),
			MaxConcurrent: 4,
			CacheBytes:    64 * 1024 * 1024,
		}, store, nc, js, logger)
		workerCtx, workerCancel := context.WithCancel(context.Background())
		t.Cleanup(workerCancel)
		if err := w.Start(workerCtx); err != nil {
			t.Fatalf("starting worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
	}

	newCoord := func(skew bool) *Coordinator {
		c := New(Config{
			NATSUrl:      embeddedNATS.ClientURL(),
			ResultBucket: "test",
			// Never broadcast: forces the shuffled hash-join path the
			// skew split targets (otherwise the tiny dims build would
			// ride broadcast_join + probe-split and never repartition).
			BroadcastBytesOverride: -1,
			SkewSplit:              skew,
		}, cat, nc, js, logger)
		for i := 0; i < 3; i++ {
			c.workers.record(distributed.WorkerHeartbeat{
				WorkerID:  fmt.Sprintf("fake-worker-%d", i),
				Timestamp: time.Now(),
			})
		}
		return c
	}

	// Both queries aggregate over e.v so the fat pad column legitimately
	// survives shuffle projection — before the 2026-07-21 projection fix
	// (exchange-reuse memo A1/A2) the fixture leaned on the projection
	// BUG to ship v: the queries never referenced it, but the polluted
	// column list reverted the scan to full width. With real projection,
	// k-only shuffles fall below the skew byte thresholds and the split
	// never engages.
	queries := []string{
		// Inner join + aggregate over the hot key's dimension.
		"SELECT d.name, count(*) AS cnt, min(e.v) AS mv FROM events e JOIN dims d ON e.k = d.k GROUP BY d.name ORDER BY d.name",
		// Left join: unmatched probe rows (keys >= 48) must survive the
		// split exactly once each.
		"SELECT count(*) AS total_rows, count(d.name) AS matched_rows, min(e.v) AS mv FROM events e LEFT JOIN dims d ON e.k = d.k",
	}

	run := func(c *Coordinator, sql string) string {
		t.Helper()
		result, err := c.ExecuteSQL(ctx, sql)
		if err != nil {
			t.Fatalf("ExecuteSQL(%q): %v", sql, err)
		}
		if result.Error != "" {
			t.Fatalf("query error (%q): %s", sql, result.Error)
		}
		rows := mustRows(t, result)
		rendered := make([]string, len(rows))
		for i, r := range rows {
			rendered[i] = fmt.Sprintf("%v", r)
		}
		sort.Strings(rendered)
		return fmt.Sprintf("%d rows\n%v", len(rows), rendered)
	}

	coordOff := newCoord(false)
	coordOn := newCoord(true)

	for qi, sql := range queries {
		beforeOff := SkewSplitsPlanned.Load()
		want := run(coordOff, sql)
		if d := SkewSplitsPlanned.Load() - beforeOff; d != 0 {
			t.Fatalf("q%d: flag-off arm planned %d splits", qi, d)
		}
		beforeOn := SkewSplitsPlanned.Load()
		got := run(coordOn, sql)
		if got != want {
			t.Errorf("q%d row mismatch under skew split\nsql: %s\n--- off ---\n%s\n--- on ---\n%s", qi, sql, want, got)
		}
		if d := SkewSplitsPlanned.Load() - beforeOn; d == 0 {
			t.Errorf("q%d: skew-split arm planned no splits — fixture did not engage the mechanism", qi)
		} else {
			t.Logf("q%d: %d skew splits planned, rows identical", qi, d)
		}
	}
}
