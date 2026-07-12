package coordinator

import (
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
	f.dispatch("root-1", "shuffle-stage-x", []string{"t1", "t2"}, 6)
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

// TestEagerInputRangesMatchFrozenBinding pins the eager partition ranges to
// partitionRangeForWorker — the contract that makes eager and barrier
// dispatch read identical row sets per task.
func TestEagerInputRangesMatchFrozenBinding(t *testing.T) {
	for _, tc := range []struct{ numParts, numTasks int }{
		{6, 3}, {3, 3}, {24, 3}, {5, 3}, {3, 5}, {1, 1},
	} {
		f := newEagerFeed()
		f.dispatch("root", "st", []string{"t1"}, tc.numParts)
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
	f.dispatch("root", "st", []string{"t1", "t2"}, 3)
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
