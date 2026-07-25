package coordinator

import (
	"fmt"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

func newEagerTestCoordinator(eager bool) *Coordinator {
	return New(Config{
		ResultBucket:      "test",
		EagerDispatch:     eager,
		StreamingExchange: eager,
	}, nil, nil, nil, nil)
}

func TestEagerFeedHandleGating(t *testing.T) {
	off := newEagerTestCoordinator(false)
	if f := off.eagerFeedHandle("q1", "stage-1"); f != nil {
		t.Fatal("flag off: want nil feed")
	}
	on := newEagerTestCoordinator(true)
	if f := on.eagerFeedHandle("", "stage-1"); f != nil {
		t.Fatal("empty root: want nil feed")
	}
	f1 := on.eagerFeedHandle("q1", "stage-1")
	if f1 == nil {
		t.Fatal("flag on: want feed")
	}
	if f2 := on.eagerFeedHandle("q1", "stage-1"); f2 != f1 {
		t.Fatal("get-or-create must return the same instance")
	}
	if f3 := on.eagerFeedHandle("q2", "stage-1"); f3 == f1 {
		t.Fatal("different query must get a different feed")
	}
}

func TestDropEagerFeedsClosesAndScopes(t *testing.T) {
	c := newEagerTestCoordinator(true)
	f1 := c.eagerFeedHandle("q1", "s1")
	f2 := c.eagerFeedHandle("q2", "s1")
	c.dropEagerFeeds("q1")
	if !f1.isClosed() {
		t.Error("q1 feed should be closed")
	}
	if f2.isClosed() {
		t.Error("q2 feed must be untouched")
	}
	if f := c.eagerFeedHandle("q1", "s1"); f == f1 {
		t.Error("dropped feed must not be returned again")
	}
}

func TestEagerFeedDispatchUnblocksAndSnapshot(t *testing.T) {
	f := newEagerFeed()
	select {
	case <-f.dispatched:
		t.Fatal("dispatched must block before dispatch()")
	default:
	}
	f.dispatch("root-1", "shuffle-stage-x", []string{"t1", "t2"}, 6, 3)
	select {
	case <-f.dispatched:
	default:
		t.Fatal("dispatched must be closed after dispatch()")
	}
	f.appendReplay(distributed.ProducerTaskManifest{TaskID: "t1", Attempt: 1})
	snap := f.replaySnapshot()
	f.appendReplay(distributed.ProducerTaskManifest{TaskID: "t2", Attempt: 1})
	if len(snap) != 1 {
		t.Fatalf("snapshot must be isolated from later appends: got %d", len(snap))
	}

	out := f.provisionalOutput()
	if out.Kind != OutputPartitioned || out.NumPartitions != 6 || len(out.Files) != 6 {
		t.Fatalf("provisional layout: %+v", out)
	}
	if out.Bytes != 0 || out.PartitionBytes != nil || out.BuildStats != nil {
		t.Fatalf("provisional must degrade to unknown: %+v", out)
	}
	if out.eager != f {
		t.Fatal("provisional must carry the feed")
	}
}

// TestEagerFeedActivate pins the clearance-driven activation contract
// (§13 precondition 2): inactive until a consumer clears, exactly one
// republisher start across repeated activations, and no start on a feed
// already closed by query teardown.
func TestEagerFeedActivate(t *testing.T) {
	f := newEagerFeed()
	if f.isActive() {
		t.Fatal("fresh feed must be inactive")
	}
	if !f.activate() {
		t.Fatal("first activation must request a republisher start")
	}
	if !f.isActive() {
		t.Fatal("feed must be active after activate()")
	}
	if f.activate() {
		t.Fatal("second activation must not request another republisher")
	}

	closed := newEagerFeed()
	closed.markClosed()
	if closed.activate() {
		t.Fatal("activation on a closed feed must not start a republisher")
	}
	if !closed.isActive() {
		t.Fatal("activate() still marks a closed feed active (publisher gate reads it)")
	}
}

// TestEagerInputRangesMatchFrozenBinding pins the eager partition ranges to
// partitionRangeForWorker — the contract that makes eager and barrier
// dispatch read identical row sets per task.
func TestEagerInputRangesMatchFrozenBinding(t *testing.T) {
	for _, tc := range []struct{ numParts, numTasks int }{
		{6, 3}, {3, 3}, {24, 3}, {5, 3}, {3, 5}, {1, 1},
	} {
		f := newEagerFeed()
		f.dispatch("root", "st", []string{"t1"}, tc.numParts, tc.numTasks)
		for w := 0; w < tc.numTasks; w++ {
			ei := f.eagerInputForTask(w, tc.numTasks)
			start, end := partitionRangeForWorker(tc.numParts, w, tc.numTasks)
			if ei.PartitionStart != start || ei.PartitionEnd != end-1 {
				t.Errorf("parts=%d tasks=%d w=%d: eager [%d,%d], frozen [%d,%d)",
					tc.numParts, tc.numTasks, w, ei.PartitionStart, ei.PartitionEnd, start, end)
			}
		}
	}
}

func TestRefreshEagerReplay(t *testing.T) {
	f := newEagerFeed()
	f.dispatch("root", "st", []string{"t1", "t2"}, 3, 1)
	inputs := map[string]StageOutput{"dep-1": f.provisionalOutput()}

	task := distributed.Task{
		EagerInputs: map[string]distributed.EagerInput{
			"dep-1": f.eagerInputForTask(0, 1),
		},
	}
	orig := task.EagerInputs
	f.appendReplay(distributed.ProducerTaskManifest{TaskID: "t1", Attempt: 1})

	refreshed := task
	refreshEagerReplay(&refreshed, inputs)
	if got := len(refreshed.EagerInputs["dep-1"].Replay); got != 1 {
		t.Fatalf("refreshed replay: got %d manifests, want 1", got)
	}
	// The retrier's stored copy shares the original map; refresh must not
	// mutate it (Observe and RetryStuck can republish concurrently).
	if got := len(orig["dep-1"].Replay); got != 0 {
		t.Fatalf("original task's replay mutated: %d", got)
	}
	// Aliases without a feed pass through unchanged.
	noFeed := distributed.Task{EagerInputs: map[string]distributed.EagerInput{
		"other": {StageID: "x"},
	}}
	refreshEagerReplay(&noFeed, inputs)
	if noFeed.EagerInputs["other"].StageID != "x" {
		t.Fatal("non-feed alias must pass through")
	}
}

func TestEagerEligibleConsumer(t *testing.T) {
	repart := physical.Stage{ID: "ex-1", Type: physical.StageExchangeRepartition,
		Exchange: &physical.ExchangeStage{Keys: []string{"k"}, Count: 3}}
	scan := physical.Stage{ID: "scan-1", Type: physical.StageScan}
	byID := map[string]physical.Stage{"ex-1": repart, "scan-1": scan}

	base := physical.Stage{
		ID:           "agg-1",
		Type:         "final_aggregate",
		Dependencies: []string{"ex-1"},
		Distribution: physical.Distribution{Kind: physical.DistHashPartitioned, Count: 3},
	}
	if !eagerEligibleConsumer(base, byID, "", 3) {
		t.Fatal("base final_aggregate over repartition must be eligible")
	}

	mutate := func(fn func(*physical.Stage)) physical.Stage {
		s := base
		fn(&s)
		return s
	}
	cases := []struct {
		name    string
		s       physical.Stage
		fuseID  string
		workers int
		want    bool
	}{
		{"aggregate type", mutate(func(s *physical.Stage) { s.Type = "aggregate" }), "", 3, true},
		{"merge_aggregate type", mutate(func(s *physical.Stage) { s.Type = "merge_aggregate" }), "", 3, true},
		{"sort with keys", mutate(func(s *physical.Stage) {
			s.Type = "sort"
			s.SortKeys = []physical.SortKeySpec{{Column: "a"}}
		}), "", 3, true},
		{"sort without keys (legacy path)", mutate(func(s *physical.Stage) { s.Type = "sort" }), "", 3, false},
		{"join is C2", mutate(func(s *physical.Stage) {
			s.Type = physical.StageHashJoin
			s.Dependencies = []string{"ex-1"}
		}), "", 3, false},
		{"window not migrated", mutate(func(s *physical.Stage) { s.Type = physical.StageWindow }), "", 3, false},
		{"scalar deps keep barrier", mutate(func(s *physical.Stage) {
			s.ScalarDependencies = map[string]string{":scalar_1": "prod-1"}
		}), "", 3, false},
		{"gather-fused excluded (retry unsafe)", base, "agg-1", 3, false},
		{"dep not a repartition", mutate(func(s *physical.Stage) { s.Dependencies = []string{"scan-1"} }), "", 3, false},
		{"dep unknown", mutate(func(s *physical.Stage) { s.Dependencies = []string{"ghost"} }), "", 3, false},
		{"two deps", mutate(func(s *physical.Stage) { s.Dependencies = []string{"ex-1", "scan-1"} }), "", 3, false},
		{"dynamic-filter consumer", mutate(func(s *physical.Stage) {
			s.ConsumeDynamicFilters = []physical.DynamicFilterConsume{{FilterID: "f"}}
		}), "", 3, false},
		{"task count above workers (lane reservation)", mutate(func(s *physical.Stage) {
			s.Distribution.Count = 24
		}), "", 3, false},
		{"singleton always one task", mutate(func(s *physical.Stage) {
			s.Distribution = physical.Distribution{Kind: physical.DistSingleton}
		}), "", 3, true},
	}
	for _, tc := range cases {
		if got := eagerEligibleConsumer(tc.s, byID, tc.fuseID, tc.workers); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestPickEagerWorkerFrom(t *testing.T) {
	cases := []struct {
		name      string
		connected []string
		maxConc   map[string]int
		counts    map[string]int
		want      string
		wantOK    bool
	}{
		{"none connected", nil, nil, nil, "", false},
		{"spreads to zero-count worker",
			[]string{"w1", "w2"}, map[string]int{"w1": 3, "w2": 3},
			map[string]int{"w1": 1}, "w2", true},
		{"reservation skips full worker",
			[]string{"w1", "w2"}, map[string]int{"w1": 2, "w2": 3},
			map[string]int{"w1": 1, "w2": 1}, "w2", true},
		{"all reserved: min count anyway",
			[]string{"w1", "w2"}, map[string]int{"w1": 2, "w2": 2},
			map[string]int{"w1": 2, "w2": 1}, "w2", true},
		{"no stats: min count fallback",
			[]string{"w1", "w2"}, nil,
			map[string]int{"w1": 1}, "w2", true},
	}
	for _, tc := range cases {
		got, ok := pickEagerWorkerFrom(tc.connected, tc.maxConc, tc.counts)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("%s: got (%q,%v), want (%q,%v)", tc.name, got, ok, tc.want, tc.wantOK)
		}
	}
}

// TestEagerFeedKeyScoping pins the key format so two queries whose plans
// both contain "exchange-repartition-3" cannot collide.
func TestEagerFeedKeyScoping(t *testing.T) {
	if eagerFeedKey("q1", "s") == eagerFeedKey("q2", "s") {
		t.Fatal("keys must be query-scoped")
	}
	if eagerFeedKey("q", "1s") == eagerFeedKey("q1", "s") {
		t.Fatal("separator must prevent prefix collisions")
	}
}

func TestEagerFeedDecisionThreshold(t *testing.T) {
	// threshold = min(total, max(workerCount, ceil(total/4))), floor 1.
	cases := []struct{ total, workers, want int }{
		{12, 3, 3},  // one wave dominates
		{24, 3, 6},  // quarter dominates
		{2, 3, 2},   // capped at total
		{1, 3, 1},   // single-task producer
		{100, 4, 25},
	}
	for _, tc := range cases {
		f := newEagerFeed()
		ids := make([]string, tc.total)
		for i := range ids {
			ids[i] = string(rune('a' + i%26))
		}
		f.dispatch("r", "s", ids, 6, tc.workers)
		if f.decisionThreshold != tc.want {
			t.Errorf("total=%d workers=%d: threshold=%d, want %d",
				tc.total, tc.workers, f.decisionThreshold, tc.want)
		}
	}

	// decisionReady closes exactly when the threshold is crossed, and the
	// projection scales observed bytes by total/completed.
	f := newEagerFeed()
	f.dispatch("r", "s", []string{"t1", "t2", "t3", "t4"}, 3, 2)
	if f.decisionThreshold != 2 {
		t.Fatalf("threshold = %d, want 2", f.decisionThreshold)
	}
	f.noteCompletion([]int64{10, 0, 0})
	select {
	case <-f.decisionReady:
		t.Fatal("decisionReady must not close below threshold")
	default:
	}
	f.noteCompletion([]int64{10, 20, 0})
	select {
	case <-f.decisionReady:
	default:
		t.Fatal("decisionReady must close at threshold")
	}
	// observed = {20,20,0} over 2 of 4 tasks → projected ×2.
	got := f.projectedPartitionBytes()
	want := []int64{40, 40, 0}
	for p := range want {
		if got[p] != want[p] {
			t.Fatalf("projected = %v, want %v", got, want)
		}
	}
	// Later completions keep accumulating without double-closing.
	f.noteCompletion(nil)
	f.noteCompletion([]int64{0, 0, 4})

	// No accounting reported → nil projection (decision degrades to
	// "would not split", matching planSkewSplitTasks on nil vectors).
	blind := newEagerFeed()
	blind.dispatch("r", "s", []string{"t1"}, 3, 1)
	blind.noteCompletion(nil)
	if blind.projectedPartitionBytes() != nil {
		t.Fatal("projection must be nil without reported accounting")
	}
}

func TestEagerJoinWouldSplit(t *testing.T) {
	origTarget, origFloor := skewSplitTargetBytes, skewSplitMinGroupBytes
	skewSplitTargetBytes, skewSplitMinGroupBytes = 100, 100
	t.Cleanup(func() { skewSplitTargetBytes, skewSplitMinGroupBytes = origTarget, origFloor })

	mkFeed := func(parts []int64, tasks int) *eagerFeed {
		f := newEagerFeed()
		ids := make([]string, tasks)
		for i := range ids {
			ids[i] = string(rune('a' + i))
		}
		f.dispatch("r", "s", ids, len(parts), 1)
		f.noteCompletion(parts) // completed=1 of tasks → projection ×tasks
		return f
	}
	join := physical.Stage{
		ID: "join-1", Type: physical.StageHashJoin, JoinType: "inner",
		Distribution: physical.Distribution{Kind: physical.DistHashPartitioned, Count: 3},
	}
	c := newEagerTestCoordinator(true)
	c.config.SkewSplit = true

	// Hot: partition 0 projects to 3000, others 30 — over floor, ratio ≫ 2.
	hotProbe := mkFeed([]int64{1000, 10, 10}, 3)
	coldBuild := mkFeed([]int64{1, 1, 1}, 3)
	if !c.eagerJoinWouldSplit(join, hotProbe, coldBuild, 3, 3) {
		t.Error("skewed projection must report would-split")
	}
	// Partitioned chained build: the dispatcher never skew-splits such
	// stages (dispatchComputeStage skips planSkewSplitTasks), so the
	// projection must not send the consumer to the barrier either.
	chained := join
	chained.ChainedJoins = []physical.ChainedJoinSpec{{Partitioned: true}}
	if c.eagerJoinWouldSplit(chained, hotProbe, coldBuild, 3, 3) {
		t.Error("partitioned-chain stage must never report would-split")
	}
	// Uniform: every group equal → ratio 1 < 2 → no split (the Q21 lesson).
	uniform := mkFeed([]int64{1000, 1000, 1000}, 3)
	if c.eagerJoinWouldSplit(join, uniform, coldBuild, 3, 3) {
		t.Error("uniform-heavy projection must not split (ratio gate)")
	}
	// Build side over the replication bound → no split.
	hotBuild := mkFeed([]int64{1 << 40, 0, 0}, 3)
	if c.eagerJoinWouldSplit(join, hotProbe, hotBuild, 3, 3) {
		t.Error("build-side-hot group must not split")
	}
	// Missing accounting on either side → false (full data would also
	// decline to split on nil vectors).
	blind := newEagerFeed()
	blind.dispatch("r", "s", []string{"a"}, 3, 1)
	blind.noteCompletion(nil)
	if c.eagerJoinWouldSplit(join, blind, coldBuild, 3, 3) {
		t.Error("missing probe accounting must not split")
	}
	// Flag off → false regardless.
	c.config.SkewSplit = false
	if c.eagerJoinWouldSplit(join, hotProbe, coldBuild, 3, 3) {
		t.Error("skew-split disabled must never report would-split")
	}
	c.config.SkewSplit = true
	// Right joins can't split (replicated build would duplicate unmatched
	// build rows).
	right := join
	right.JoinType = "right"
	if c.eagerJoinWouldSplit(right, hotProbe, coldBuild, 3, 3) {
		t.Error("right join must not split")
	}
}

func TestEagerEligibleJoinConsumer(t *testing.T) {
	repartA := physical.Stage{ID: "ex-a", Type: physical.StageExchangeRepartition,
		Exchange: &physical.ExchangeStage{Keys: []string{"k"}, Count: 24}}
	repartB := physical.Stage{ID: "ex-b", Type: physical.StageExchangeRepartition,
		Exchange: &physical.ExchangeStage{Keys: []string{"k"}, Count: 24}}
	scan := physical.Stage{ID: "scan-1", Type: physical.StageScan}
	joinUp := physical.Stage{ID: "join-up", Type: physical.StageHashJoin,
		Distribution: physical.Distribution{Kind: physical.DistHashPartitioned, Count: 24}}
	faggUp := physical.Stage{ID: "fagg-up", Type: "final_aggregate",
		Distribution: physical.Distribution{Kind: physical.DistHashPartitioned, Count: 24}}
	joinSingle := physical.Stage{ID: "join-single", Type: physical.StageHashJoin,
		Distribution: physical.Distribution{Kind: physical.DistSingleton}}
	joinExch := physical.Stage{ID: "join-exch", Type: physical.StageHashJoin,
		Distribution: physical.Distribution{Kind: physical.DistHashPartitioned, Count: 24},
		Exchange:     &physical.ExchangeStage{Keys: []string{"k"}, Count: 24}}
	byID := map[string]physical.Stage{
		"ex-a": repartA, "ex-b": repartB, "scan-1": scan,
		"join-up": joinUp, "fagg-up": faggUp,
		"join-single": joinSingle, "join-exch": joinExch,
	}

	base := physical.Stage{
		ID: "join-1", Type: physical.StageHashJoin, JoinType: "inner",
		Dependencies: []string{"ex-a", "ex-b"},
		LeftDepStage: "ex-a", RightDepStage: "ex-b",
		Distribution: physical.Distribution{Kind: physical.DistHashPartitioned, Count: 24},
	}
	if !eagerEligibleJoinConsumer(base, byID, "") {
		t.Fatal("shuffled hash join over two repartitions must be eligible")
	}
	mutate := func(fn func(*physical.Stage)) physical.Stage {
		s := base
		fn(&s)
		return s
	}
	cases := []struct {
		name   string
		s      physical.Stage
		fuseID string
		want   bool
	}{
		{"broadcast join out of scope", mutate(func(s *physical.Stage) { s.Type = physical.StageBroadcastJoin }), "", false},
		{"sort-merge join out of slice-1 scope", mutate(func(s *physical.Stage) { s.Type = physical.StageSortMergeJoin }), "", false},
		{"group-by join takes legacy path", mutate(func(s *physical.Stage) { s.GroupByCols = []string{"g"} }), "", false},
		{"gather-fused excluded", base, "join-1", false},
		{"scalar deps keep barrier", mutate(func(s *physical.Stage) {
			s.ScalarDependencies = map[string]string{":s": "p"}
		}), "", false},
		{"probe dep not a repartition", mutate(func(s *physical.Stage) {
			s.Dependencies = []string{"scan-1", "ex-b"}
			s.LeftDepStage = "scan-1"
		}), "", false},
		{"fused-join dep count mismatch", mutate(func(s *physical.Stage) {
			s.FusedJoins = []physical.FusedJoinSpec{{}}
		}), "", false},
		// Stage-chain fusion: Dependencies grow one per ChainedJoinSpec;
		// a matching count is eligible (chained builds keep the barrier),
		// a mismatch is not.
		{"chained joins with matching deps eligible", mutate(func(s *physical.Stage) {
			s.ChainedJoins = []physical.ChainedJoinSpec{{BuildDepStage: "scan-1"}}
			s.Dependencies = []string{"ex-a", "ex-b", "scan-1"}
		}), "", true},
		{"chained joins with absorbed partial aggregate eligible", mutate(func(s *physical.Stage) {
			s.ChainedJoins = []physical.ChainedJoinSpec{{BuildDepStage: "scan-1"}}
			s.Dependencies = []string{"ex-a", "ex-b", "scan-1"}
			s.ChainedAggGroupBy = []string{"g"}
			s.ChainedAggSpecs = []physical.AggSpec{{}}
		}), "", true},
		{"chained-join dep count mismatch", mutate(func(s *physical.Stage) {
			s.ChainedJoins = []physical.ChainedJoinSpec{{BuildDepStage: "scan-1"}}
		}), "", false},
		// A3: a hash-partitioned compute producer backs a feed like a
		// repartition; a Singleton or exchange-sink one does not.
		{"compute-producer probe eligible", mutate(func(s *physical.Stage) {
			s.Dependencies = []string{"join-up", "ex-b"}
			s.LeftDepStage = "join-up"
		}), "", true},
		{"final-aggregate probe eligible", mutate(func(s *physical.Stage) {
			s.Dependencies = []string{"fagg-up", "ex-b"}
			s.LeftDepStage = "fagg-up"
		}), "", true},
		{"singleton compute probe ineligible", mutate(func(s *physical.Stage) {
			s.Dependencies = []string{"join-single", "ex-b"}
			s.LeftDepStage = "join-single"
		}), "", false},
		{"exchange-sink compute probe ineligible", mutate(func(s *physical.Stage) {
			s.Dependencies = []string{"join-exch", "ex-b"}
			s.LeftDepStage = "join-exch"
		}), "", false},
	}
	for _, tc := range cases {
		if got := eagerEligibleJoinConsumer(tc.s, byID, tc.fuseID); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestEagerAliasForDep(t *testing.T) {
	join := physical.Stage{
		Type:            physical.StageHashJoin,
		Dependencies:    []string{"ex-probe", "ex-build"},
		LeftDepStage:    "ex-probe",
		RightDepStage:   "ex-build",
		BuildTableAlias: "orders",
	}
	if got := eagerAliasForDep(join, "ex-build"); got != "orders" {
		t.Errorf("build alias = %q, want orders", got)
	}
	if got := eagerAliasForDep(join, "ex-probe"); got != "probe" {
		t.Errorf("probe alias = %q, want probe", got)
	}
	// Alias collision: build named "probe" pushes probe to "probe_side"
	// (mirrors buildTaskInputsForStage).
	join.BuildTableAlias = "probe"
	if got := eagerAliasForDep(join, "ex-probe"); got != "probe_side" {
		t.Errorf("collision probe alias = %q, want probe_side", got)
	}
	agg := physical.Stage{Type: "final_aggregate", Dependencies: []string{"ex-1"}}
	if got := eagerAliasForDep(agg, "ex-1"); got != "ex-1" {
		t.Errorf("single-input alias = %q, want dep ID", got)
	}
}

func TestEagerPublishGovernor(t *testing.T) {
	mk := func(n int) []distributed.Task {
		ts := make([]distributed.Task, n)
		for i := range ts {
			ts[i] = distributed.Task{ID: fmt.Sprintf("t%02d", i)}
		}
		return ts
	}
	// Under the cap: everything in the first wave, no governor.
	first, g := newEagerPublishGovernor(mk(3), 6, nil)
	if len(first) != 3 || g != nil {
		t.Fatalf("under cap: first=%d governor=%v", len(first), g != nil)
	}
	// Over the cap: first wave = cap, remainder governed one-for-one on
	// terminal results; duplicate terminals for one task free one slot.
	var published []string
	first, g = newEagerPublishGovernor(mk(5), 2, func(t distributed.Task) {
		published = append(published, t.ID)
	})
	if len(first) != 2 || g == nil {
		t.Fatalf("over cap: first=%d governor=%v", len(first), g != nil)
	}
	g.noteTerminal("t00")
	g.noteTerminal("t00") // duplicate delivery: no extra slot
	g.noteTerminal("t01")
	g.noteTerminal("t02")
	if want := []string{"t02", "t03", "t04"}; fmt.Sprint(published) != fmt.Sprint(want) {
		t.Fatalf("published = %v, want %v", published, want)
	}
	g.noteTerminal("t03") // pending exhausted: no-op
	if len(published) != 3 {
		t.Fatalf("exhausted governor must not publish more, got %v", published)
	}
}
