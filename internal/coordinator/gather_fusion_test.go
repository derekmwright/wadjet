package coordinator

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestCanFuseGather_Eligibility table-tests every reject path in
// canFuseGather. Each row constructs a gather stage + a pending map and
// asserts whether fusion is allowed.
func TestCanFuseGather_Eligibility(t *testing.T) {
	mkAgg := func(typ string, sortKeys []physical.SortKeySpec, limit int, dist physical.DistKind) physical.Stage {
		return physical.Stage{
			ID:           "agg",
			Type:         typ,
			SortKeys:     sortKeys,
			Limit:        limit,
			Distribution: physical.Distribution{Kind: dist},
		}
	}
	tests := []struct {
		name    string
		gather  physical.Stage
		pending map[string]physical.Stage
		want    bool
	}{
		{
			name: "happy path: gather over final_aggregate",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("final_aggregate", nil, 0, physical.DistSingleton),
			},
			want: true,
		},
		{
			name: "happy path: gather over merge_aggregate",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("merge_aggregate", nil, 0, physical.DistSingleton),
			},
			want: true,
		},
		{
			name: "happy path: gather over Singleton final_aggregate with SortKeys (post-fuseSortIntoPredecessor)",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("final_aggregate", []physical.SortKeySpec{{Column: "x"}}, 0, physical.DistSingleton),
			},
			want: true,
		},
		{
			name: "happy path: gather over Singleton final_aggregate with Limit",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("final_aggregate", nil, 100, physical.DistSingleton),
			},
			want: true,
		},
		{
			name: "rejects: ordered gather (Exchange.Ordering set)",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
				Exchange: &physical.ExchangeStage{
					Ordering: []physical.SortKeySpec{{Column: "x"}},
				},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("final_aggregate", nil, 0, physical.DistSingleton),
			},
			want: false,
		},
		{
			name: "rejects: dep is hash_join, not aggregate",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"join"},
			},
			pending: map[string]physical.Stage{
				"join": {ID: "join", Type: physical.StageHashJoin},
			},
			want: false,
		},
		{
			name: "rejects: dep has SortKeys but is hash-partitioned (multi-task sort loses global order)",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("final_aggregate", []physical.SortKeySpec{{Column: "x"}}, 0, physical.DistHashPartitioned),
			},
			want: false,
		},
		{
			name: "rejects: dep has Limit but is hash-partitioned",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("final_aggregate", nil, 100, physical.DistHashPartitioned),
			},
			want: false,
		},
		{
			name: "rejects: gather has multiple deps",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg", "other"},
			},
			pending: map[string]physical.Stage{
				"agg":   mkAgg("final_aggregate", nil, 0, physical.DistSingleton),
				"other": mkAgg("final_aggregate", nil, 0, physical.DistSingleton),
			},
			want: false,
		},
		{
			name: "rejects: dep not in pending map",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"missing"},
			},
			pending: map[string]physical.Stage{},
			want:    false,
		},
		{
			name: "happy path: gather over Singleton sort, no Ordering",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"sort"},
			},
			pending: map[string]physical.Stage{
				"sort": {
					ID:           "sort",
					Type:         "sort",
					SortKeys:     []physical.SortKeySpec{{Column: "x", Desc: true}},
					Distribution: physical.Distribution{Kind: physical.DistSingleton},
				},
			},
			want: true,
		},
		{
			name: "happy path: gather over Singleton merge_sort, no Ordering",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"ms"},
			},
			pending: map[string]physical.Stage{
				"ms": {
					ID:           "ms",
					Type:         "merge_sort",
					SortKeys:     []physical.SortKeySpec{{Column: "v"}},
					Distribution: physical.Distribution{Kind: physical.DistSingleton},
				},
			},
			want: true,
		},
		{
			name: "happy path: gather over Singleton sort with matching Ordering",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"sort"},
				Exchange: &physical.ExchangeStage{
					Ordering: []physical.SortKeySpec{{Column: "x", Desc: true}, {Column: "id"}},
				},
			},
			pending: map[string]physical.Stage{
				"sort": {
					ID:           "sort",
					Type:         "sort",
					SortKeys:     []physical.SortKeySpec{{Column: "x", Desc: true}, {Column: "id"}},
					Distribution: physical.Distribution{Kind: physical.DistSingleton},
				},
			},
			want: true,
		},
		{
			name: "rejects: gather over sort with mismatched Ordering keys",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"sort"},
				Exchange: &physical.ExchangeStage{
					Ordering: []physical.SortKeySpec{{Column: "y"}},
				},
			},
			pending: map[string]physical.Stage{
				"sort": {
					ID:           "sort",
					Type:         "sort",
					SortKeys:     []physical.SortKeySpec{{Column: "x"}},
					Distribution: physical.Distribution{Kind: physical.DistSingleton},
				},
			},
			want: false,
		},
		{
			name: "rejects: gather over multi-task sort (would lose global order)",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"sort"},
			},
			pending: map[string]physical.Stage{
				"sort": {
					ID:           "sort",
					Type:         "sort",
					SortKeys:     []physical.SortKeySpec{{Column: "x"}},
					Distribution: physical.Distribution{Kind: physical.DistHashPartitioned},
				},
			},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, got := canFuseGather(tc.gather, tc.pending)
			if got != tc.want {
				t.Errorf("canFuseGather: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestBuildSortFragment verifies that buildSortFragment emits the canonical
// [OpShuffleSource, OpSort, OpUnpartitionedSink] shape and forwards
// SortKeys + Limit onto the OpSort spec. Caller-side preconditions
// (single input alias, non-empty SortKeys) surface as errors.
func TestBuildSortFragment(t *testing.T) {
	stage := physical.Stage{
		Type:  "sort",
		Limit: 5,
	}
	task := &distributed.Task{DataBucket: "buk"}
	taskInputs := map[string][]string{"upstream": {"f1.wshf", "f2.wshf"}}
	sorts := []distributed.SortKeySpec{
		{Column: "rev", Desc: true},
		{Column: "id", Desc: false},
	}

	t.Run("happy path: 3-op fragment with sort keys + limit", func(t *testing.T) {
		ops, err := buildSortFragment(stage, task, taskInputs, sorts, "")
		if err != nil {
			t.Fatalf("buildSortFragment: %v", err)
		}
		if len(ops) != 3 {
			t.Fatalf("ops length: got %d, want 3", len(ops))
		}
		if ops[0].Type != distributed.OpShuffleSource {
			t.Errorf("ops[0]: got %q, want %q", ops[0].Type, distributed.OpShuffleSource)
		}
		if ops[0].InputAlias != "upstream" {
			t.Errorf("ops[0].InputAlias: got %q, want %q", ops[0].InputAlias, "upstream")
		}
		if len(ops[0].InputFiles) != 2 {
			t.Errorf("ops[0].InputFiles: got %d files, want 2", len(ops[0].InputFiles))
		}
		if ops[1].Type != distributed.OpSort {
			t.Errorf("ops[1]: got %q, want %q", ops[1].Type, distributed.OpSort)
		}
		if len(ops[1].SortKeySpecs) != 2 {
			t.Fatalf("ops[1].SortKeySpecs: got %d, want 2", len(ops[1].SortKeySpecs))
		}
		if ops[1].SortKeySpecs[0].Column != "rev" || !ops[1].SortKeySpecs[0].Desc {
			t.Errorf("ops[1].SortKeySpecs[0]: got %+v, want {rev DESC}", ops[1].SortKeySpecs[0])
		}
		if ops[1].SortLimit != 5 {
			t.Errorf("ops[1].SortLimit: got %d, want 5", ops[1].SortLimit)
		}
		if ops[2].Type != distributed.OpUnpartitionedSink {
			t.Errorf("ops[2]: got %q, want %q", ops[2].Type, distributed.OpUnpartitionedSink)
		}
	})
	t.Run("rejects empty SortKeys", func(t *testing.T) {
		_, err := buildSortFragment(stage, task, taskInputs, nil, "")
		if err == nil {
			t.Fatal("expected error on empty SortKeys")
		}
	})
	t.Run("rejects multi-alias input", func(t *testing.T) {
		_, err := buildSortFragment(stage, task, map[string][]string{
			"a": {"x"},
			"b": {"y"},
		}, sorts, "")
		if err == nil {
			t.Fatal("expected error on multi-alias input")
		}
	})
	t.Run("emits OpGatherSink when gatherReplySubject is set", func(t *testing.T) {
		ops, err := buildSortFragment(stage, task, taskInputs, sorts, "wadjet.gather.q-fused")
		if err != nil {
			t.Fatalf("buildSortFragment: %v", err)
		}
		if len(ops) != 3 {
			t.Fatalf("ops length: got %d, want 3", len(ops))
		}
		if ops[2].Type != distributed.OpGatherSink {
			t.Errorf("ops[2].Type: got %q, want %q", ops[2].Type, distributed.OpGatherSink)
		}
		if ops[2].ReplySubject != "wadjet.gather.q-fused" {
			t.Errorf("ops[2].ReplySubject: got %q, want %q", ops[2].ReplySubject, "wadjet.gather.q-fused")
		}
	})
}

// TestBuildAggregateFragment_GatherSink verifies that buildAggregateFragment
// emits OpGatherSink (carrying the reply subject) when gatherReplySubject is
// non-empty and OpUnpartitionedSink otherwise.
func TestBuildAggregateFragment_GatherSink(t *testing.T) {
	stage := physical.Stage{
		Type:        "final_aggregate",
		GroupByCols: []string{"g"},
	}
	task := &distributed.Task{DataBucket: "buk"}
	taskInputs := map[string][]string{"upstream": {"f1.wshf"}}
	aggs := []distributed.AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}}

	t.Run("with reply subject emits OpGatherSink", func(t *testing.T) {
		ops, err := buildAggregateFragment(stage, task, taskInputs, aggs, nil, "wadjet.gather.q-foo", 0)
		if err != nil {
			t.Fatalf("buildAggregateFragment: %v", err)
		}
		sink := ops[len(ops)-1]
		if sink.Type != distributed.OpGatherSink {
			t.Fatalf("terminal op: got %q, want %q", sink.Type, distributed.OpGatherSink)
		}
		if sink.ReplySubject != "wadjet.gather.q-foo" {
			t.Errorf("reply subject: got %q, want %q", sink.ReplySubject, "wadjet.gather.q-foo")
		}
	})
	t.Run("empty reply subject keeps legacy OpUnpartitionedSink", func(t *testing.T) {
		ops, err := buildAggregateFragment(stage, task, taskInputs, aggs, nil, "", 0)
		if err != nil {
			t.Fatalf("buildAggregateFragment: %v", err)
		}
		sink := ops[len(ops)-1]
		if sink.Type != distributed.OpUnpartitionedSink {
			t.Fatalf("terminal op: got %q, want %q", sink.Type, distributed.OpUnpartitionedSink)
		}
		if sink.ReplySubject != "" {
			t.Errorf("reply subject should be unset on unpartitioned sink, got %q", sink.ReplySubject)
		}
	})
	t.Run("with sort keys appends OpSort before terminal sink", func(t *testing.T) {
		stageWithSort := physical.Stage{
			Type:        "final_aggregate",
			GroupByCols: []string{"g"},
			SortKeys:    []physical.SortKeySpec{{Column: "total", Desc: true}},
			Limit:       10,
		}
		sorts := []distributed.SortKeySpec{{Column: "total", Desc: true}}
		ops, err := buildAggregateFragment(stageWithSort, task, taskInputs, aggs, sorts, "wadjet.gather.q-sort", 0)
		if err != nil {
			t.Fatalf("buildAggregateFragment with sort: %v", err)
		}
		// Expect 4 ops: shuffle source, hash aggregate, sort, gather sink.
		if len(ops) != 4 {
			t.Fatalf("ops length: got %d, want 4 (source/agg/sort/sink); ops=%+v", len(ops), ops)
		}
		if ops[2].Type != distributed.OpSort {
			t.Fatalf("ops[2]: got %q, want OpSort", ops[2].Type)
		}
		if ops[2].SortLimit != 10 {
			t.Errorf("OpSort.SortLimit: got %d, want 10 (forwarded from stage.Limit)", ops[2].SortLimit)
		}
		if len(ops[2].SortKeySpecs) != 1 || ops[2].SortKeySpecs[0].Column != "total" {
			t.Errorf("OpSort.SortKeySpecs: got %+v, want [{total Desc}]", ops[2].SortKeySpecs)
		}
		if ops[3].Type != distributed.OpGatherSink {
			t.Errorf("ops[3]: got %q, want OpGatherSink", ops[3].Type)
		}
	})
}

// TestGatherReceiver_SetExpectedTerminalsRace covers the race where every
// fragment task publishes its terminal BEFORE the dispatcher returns from
// PublishTasks (and thus before SetExpectedTerminals is called). Without
// the post-set re-check, recv.wait would hang forever because done is only
// signaled inside handle(). With the check, SetExpectedTerminals(N) trips
// done as soon as the threshold is met retroactively.
func TestGatherReceiver_SetExpectedTerminalsRace(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	en, err := distributed.NewEmbeddedNATS(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if err != nil {
		t.Fatalf("embed NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	const subject = "test.gather.race"
	recv, err := subscribeGather(nc, subject, 0, nil, 0)
	if err != nil {
		t.Fatalf("subscribeGather: %v", err)
	}

	// Publish 3 terminals BEFORE setting expectedTerminals, simulating
	// fast workers that finished while the dispatcher is still in
	// PublishTasks.
	for i := 0; i < 3; i++ {
		data, err := distributed.Marshal(distributed.GatherBatchMsg{Terminal: true, WorkerID: "w"})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := nc.Publish(subject, data); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Spin until handler has observed all three. Avoids a fixed sleep.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && recv.msgCount.Load() < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := recv.msgCount.Load(); got < 3 {
		t.Fatalf("only %d/3 terminals seen by handler", got)
	}

	// Now set the threshold. wait() must return immediately (well within
	// the 2-second guard), proving the post-set re-arm path fires.
	recv.SetExpectedTerminals(3)
	type waitOut struct {
		res *gatherResult
		err error
	}
	out := make(chan waitOut, 1)
	go func() {
		res, err := recv.wait(context.Background(), 2*time.Second)
		out <- waitOut{res, err}
	}()
	select {
	case r := <-out:
		if r.err != nil {
			t.Fatalf("wait returned err: %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("wait did not unblock after SetExpectedTerminals; race not handled")
	}
	// Sanity: msgCount didn't double-count.
	if got := recv.msgCount.Load(); got != 3 {
		t.Errorf("msgCount: got %d, want 3", got)
	}
}
