package coordinator

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestCanFuseGather_Eligibility table-tests every reject path in
// canFuseGather. Each row constructs a gather stage + a pending map and
// asserts whether fusion is allowed.
func TestCanFuseGather_Eligibility(t *testing.T) {
	mkAgg := func(typ string, sortKeys []physical.SortKeySpec, limit int) physical.Stage {
		return physical.Stage{
			ID:       "agg",
			Type:     typ,
			SortKeys: sortKeys,
			Limit:    limit,
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
				"agg": mkAgg("final_aggregate", nil, 0),
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
				"agg": mkAgg("merge_aggregate", nil, 0),
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
				"agg": mkAgg("final_aggregate", nil, 0),
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
			name: "rejects: dep has SortKeys",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("final_aggregate", []physical.SortKeySpec{{Column: "x"}}, 0),
			},
			want: false,
		},
		{
			name: "rejects: dep has Limit",
			gather: physical.Stage{
				ID:           "gather",
				Type:         physical.StageExchangeGather,
				Dependencies: []string{"agg"},
			},
			pending: map[string]physical.Stage{
				"agg": mkAgg("final_aggregate", nil, 100),
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
				"agg":   mkAgg("final_aggregate", nil, 0),
				"other": mkAgg("final_aggregate", nil, 0),
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
		ops, err := buildAggregateFragment(stage, task, taskInputs, aggs, "wadjet.gather.q-foo")
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
		ops, err := buildAggregateFragment(stage, task, taskInputs, aggs, "")
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
	recv, err := subscribeGather(nc, subject, 0, nil)
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
