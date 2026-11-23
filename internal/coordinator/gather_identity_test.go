// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

// Receiver-level tests of identity-aware gather acceptance (#1709): every
// rule handleParsed applies, through each production entrance — the
// transport-neutral call, the NATS decoder and the gRPC data-plane adapter.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// testGatherStream builds the messages one execution of one task sends,
// numbered the way gatherReplySink numbers them.
type testGatherStream struct {
	task, exec, worker string
	seq                uint64
}

func (g *testGatherStream) batch(payload []byte, rows int) *distributed.GatherBatchMsg {
	g.seq++
	return &distributed.GatherBatchMsg{
		Payload: payload, RowCount: int32(rows), WorkerID: g.worker,
		TaskID: g.task, ExecutionID: g.exec, Seq: g.seq,
	}
}

func (g *testGatherStream) terminal() *distributed.GatherBatchMsg {
	return &distributed.GatherBatchMsg{
		Terminal: true, WorkerID: g.worker,
		TaskID: g.task, ExecutionID: g.exec, BatchCount: g.seq,
	}
}

func (g *testGatherStream) errTerminal(text string) *distributed.GatherBatchMsg {
	m := g.terminal()
	m.Err = text
	return m
}

// newTestGatherReceiver is a receiver with tasks registered (default: one
// task "t1") and the stream of t1's execution "e1".
func newTestGatherReceiver(budget int64, tasks ...string) (*gatherReceiver, *testGatherStream) {
	if len(tasks) == 0 {
		tasks = []string{"t1"}
	}
	r := &gatherReceiver{budget: budget, done: make(chan struct{}, 1)}
	r.expectTasks(tasks)
	return r, &testGatherStream{task: tasks[0], exec: "e1", worker: "w"}
}

// gatherEntrance delivers one message to a receiver the way one production
// transport does; flush waits until every message sent has been handled.
type gatherEntrance struct {
	name  string
	setup func(t *testing.T, r *gatherReceiver) (send func(*distributed.GatherBatchMsg), flush func())
}

func gatherEntrances() []gatherEntrance {
	return []gatherEntrance{
		{"direct", func(t *testing.T, r *gatherReceiver) (func(*distributed.GatherBatchMsg), func()) {
			return r.handleParsed, func() {}
		}},
		{"nats", func(t *testing.T, r *gatherReceiver) (func(*distributed.GatherBatchMsg), func()) {
			// The NATS subscription's own decoder: bytes as the worker
			// marshals them, through gatherReceiver.handle.
			return func(m *distributed.GatherBatchMsg) {
				data, err := distributed.Marshal(*m)
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				r.handle(&nats.Msg{Data: data})
			}, func() {}
		}},
		{"grpc", func(t *testing.T, r *gatherReceiver) (func(*distributed.GatherBatchMsg), func()) {
			const queryID = "q1709"
			srv := dataplane.NewServer(dataplane.ServerConfig{Addr: "127.0.0.1:0", ClusterID: "c1709"}, nil)
			if err := srv.Start(); err != nil {
				t.Fatalf("data-plane server: %v", err)
			}
			t.Cleanup(func() { srv.Stop(0) })
			unregister := r.registerWithDataPlane(srv, queryID)
			t.Cleanup(unregister)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			client := dataplane.NewClient(dataplane.ClientConfig{
				CoordAddr: srv.Addr(), WorkerID: "w", ReconnectBackoff: 20 * time.Millisecond,
			}, nil)
			client.Start(ctx)
			t.Cleanup(client.Stop)
			deadline := time.Now().Add(5 * time.Second)
			for !client.Connected() && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if !client.Connected() {
				t.Fatal("data-plane client never connected")
			}
			var sent int64
			base := r.msgCount.Load()
			return func(m *distributed.GatherBatchMsg) {
					if err := client.SendResultBatch(dataplane.ResultBatch{
						QueryID: queryID, WorkerID: m.WorkerID, Terminal: m.Terminal,
						RowCount: m.RowCount, Payload: m.Payload, Err: m.Err,
						TaskID: m.TaskID, ExecutionID: m.ExecutionID, Seq: m.Seq, BatchCount: m.BatchCount,
					}); err != nil {
						t.Fatalf("SendResultBatch: %v", err)
					}
					sent++
				}, func() {
					deadline := time.Now().Add(5 * time.Second)
					for r.msgCount.Load()-base < sent && time.Now().Before(deadline) {
						time.Sleep(2 * time.Millisecond)
					}
					if got := r.msgCount.Load() - base; got < sent {
						t.Fatalf("gRPC delivered %d/%d messages", got, sent)
					}
				}
		}},
	}
}

// gatherCase is one acceptance scenario: the messages, in order, and what
// the statement must see.
type gatherCase struct {
	name   string
	tasks  []string
	budget int64
	msgs   func(t *testing.T) []*distributed.GatherBatchMsg
	// wantErr: substrings the failure must contain ("" = success expected).
	wantErr []string
	// wantPrompt: the failure is an identity/stream anomaly, so it must be
	// delivered to onFail (the statement's cancellation path).
	wantPrompt bool
	wantRows   []int64 // success only: the values, in order
}

func stream(task, exec string) *testGatherStream {
	return &testGatherStream{task: task, exec: exec, worker: "w-" + exec}
}

func gatherCases() []gatherCase {
	p := func(t *testing.T, n int, base int64) []byte { return wshfInt64Payload(t, n, base) }
	return []gatherCase{
		{
			name: "single execution, several batches", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a := stream("t1", "e1")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 2, 0), 2), a.batch(p(t, 2, 2), 2), a.terminal()}
			},
			wantRows: []int64{0, 1, 2, 3},
		},
		{
			name: "legitimate duplicate rows are kept", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a := stream("t1", "e1")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 2, 5), 2), a.batch(p(t, 2, 5), 2), a.terminal()}
			},
			wantRows: []int64{5, 6, 5, 6},
		},
		{
			name: "two tasks interleaved", tasks: []string{"t1", "t2"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a, b := stream("t1", "e1"), stream("t2", "e9")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 1, 0), 1), b.batch(p(t, 1, 10), 1), a.terminal(), b.batch(p(t, 1, 11), 1), b.terminal()}
			},
			wantRows: []int64{0, 10, 11},
		},
		{
			name: "empty output", tasks: []string{"t1", "t2"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a, b := stream("t1", "e1"), stream("t2", "e2")
				return []*distributed.GatherBatchMsg{a.terminal(), b.terminal()}
			},
			wantRows: nil,
		},
		{
			name: "a silent first execution is replaced by the first one observed", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				// e1 died before sending anything; e2 is the only execution
				// the coordinator ever sees.
				b := stream("t1", "e2")
				return []*distributed.GatherBatchMsg{b.batch(p(t, 2, 0), 2), b.terminal()}
			},
			wantRows: []int64{0, 1},
		},
		{
			name: "second execution's batch while the first streams", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a, b := stream("t1", "e1"), stream("t1", "e2")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 2, 0), 2), b.batch(p(t, 2, 0), 2), a.terminal(), b.terminal()}
			},
			wantErr: []string{"task t1 executed more than once", "e1", "e2"}, wantPrompt: true,
		},
		{
			name: "partial first execution dies, a replacement streams", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a, b := stream("t1", "e1"), stream("t1", "e2")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 2, 0), 2), b.batch(p(t, 2, 0), 2), b.batch(p(t, 2, 2), 2), b.terminal()}
			},
			wantErr: []string{"task t1 executed more than once", "e1", "e2"}, wantPrompt: true,
		},
		{
			name: "a duplicate terminal does not stand in for a running task", tasks: []string{"t1", "t2"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				// The early-completion construction: t1 completes twice while
				// t2 still runs. Counting terminals would complete here,
				// before t2's rows, and lose them.
				a, a2 := stream("t1", "e1"), stream("t1", "e2")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 1, 0), 1), a.terminal(), a2.batch(p(t, 1, 0), 1), a2.terminal()}
			},
			wantErr: []string{"task t1 executed more than once"}, wantPrompt: true,
		},
		{
			name: "a second execution's bare terminal", tasks: []string{"t1", "t2"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a, a2 := stream("t1", "e1"), stream("t1", "e2")
				return []*distributed.GatherBatchMsg{a.terminal(), a2.terminal()}
			},
			wantErr: []string{"task t1 executed more than once", "terminal arrived from execution e2"}, wantPrompt: true,
		},
		{
			name: "a second execution's error terminal is still a conflict", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a, a2 := stream("t1", "e1"), stream("t1", "e2")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 1, 0), 1), a2.errTerminal("boom"), a.terminal()}
			},
			wantErr: []string{"task t1 executed more than once"}, wantPrompt: true,
		},
		{
			name: "missing task id", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				m := stream("t1", "e1").batch(p(t, 1, 0), 1)
				m.TaskID = ""
				return []*distributed.GatherBatchMsg{m}
			},
			wantErr: []string{"carries no task or execution identity"}, wantPrompt: true,
		},
		{
			name: "missing execution id on a terminal", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				m := stream("t1", "e1").terminal()
				m.ExecutionID = ""
				return []*distributed.GatherBatchMsg{m}
			},
			wantErr: []string{"carries no task or execution identity"}, wantPrompt: true,
		},
		{
			name: "unknown task", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a, x := stream("t1", "e1"), stream("tX", "e7")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 1, 0), 1), x.batch(p(t, 1, 0), 1), a.terminal()}
			},
			wantErr: []string{"task tX", "did not dispatch"}, wantPrompt: true,
		},
		{
			name: "a gap in one execution's stream", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a := stream("t1", "e1")
				first := a.batch(p(t, 1, 0), 1)
				a.batch(p(t, 1, 1), 1) // lost in transit
				return []*distributed.GatherBatchMsg{first, a.batch(p(t, 1, 2), 1), a.terminal()}
			},
			wantErr: []string{"result batch 3 arrived where batch 2 was expected"}, wantPrompt: true,
		},
		{
			name: "batches out of order", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a := stream("t1", "e1")
				b1 := a.batch(p(t, 1, 0), 1)
				b2 := a.batch(p(t, 1, 1), 1)
				return []*distributed.GatherBatchMsg{b2, b1, a.terminal()}
			},
			wantErr: []string{"result batch 2 arrived where batch 1 was expected"}, wantPrompt: true,
		},
		{
			name: "the last batch lost before the terminal", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a := stream("t1", "e1")
				first := a.batch(p(t, 1, 0), 1)
				a.batch(p(t, 1, 1), 1) // lost in transit
				return []*distributed.GatherBatchMsg{first, a.terminal()}
			},
			wantErr: []string{"after sending 2 result batches, but 1 arrived"}, wantPrompt: true,
		},
		{
			name: "a batch after the execution's own terminal", tasks: []string{"t1", "t2"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a := stream("t1", "e1")
				term := a.terminal()
				return []*distributed.GatherBatchMsg{term, a.batch(p(t, 1, 0), 1)}
			},
			wantErr: []string{"after its terminal"}, wantPrompt: true,
		},
		{
			name: "an error terminal fails the statement with the worker's error", tasks: []string{"t1", "t2"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				a, b := stream("t1", "e1"), stream("t2", "e2")
				return []*distributed.GatherBatchMsg{a.batch(p(t, 1, 0), 1), a.errTerminal("division by zero"), b.terminal()}
			},
			// The worker's own failure keeps today's path: it is reported
			// when every task has finished, and is NOT delivered as a
			// cancellation, so the task's coded result error (read by the
			// stage dispatcher) still decides what the client sees.
			wantErr: []string{"gather worker error: division by zero"}, wantPrompt: false,
		},
		{
			name: "a failed send's error terminal keeps the worker's error", tasks: []string{"t1"},
			msgs: func(t *testing.T) []*distributed.GatherBatchMsg {
				// The sink numbered batch 2, its send failed, and the
				// terminal reports that failure with batch_count=2.
				a := stream("t1", "e1")
				first := a.batch(p(t, 1, 0), 1)
				a.batch(p(t, 1, 1), 1)
				return []*distributed.GatherBatchMsg{first, a.errTerminal("gather: dataplane send: broken pipe")}
			},
			wantErr: []string{"gather worker error: gather: dataplane send: broken pipe"}, wantPrompt: false,
		},
	}
}

func TestGatherIdentity_Acceptance(t *testing.T) {
	for _, ent := range gatherEntrances() {
		for _, tc := range gatherCases() {
			t.Run(ent.name+"/"+tc.name, func(t *testing.T) {
				budget := tc.budget
				if budget == 0 {
					budget = 1 << 20
				}
				r := &gatherReceiver{budget: budget, done: make(chan struct{}, 1)}
				var prompt atomic.Value
				r.onFail = func(err error) { prompt.Store(err) }
				r.expectTasks(tc.tasks)
				send, flush := ent.setup(t, r)
				for _, m := range tc.msgs(t) {
					send(m)
				}
				flush()
				gr, err := r.wait(context.Background(), 2*time.Second)
				r.discard()
				if len(tc.wantErr) == 0 {
					if err != nil {
						t.Fatalf("wait: %v", err)
					}
					got := drainReplay(t, newGatherReplayStream(gr.batches, gr.spillPath, nil))
					if fmt.Sprint(got) != fmt.Sprint(tc.wantRows) && !(len(got) == 0 && len(tc.wantRows) == 0) {
						t.Fatalf("rows = %v, want %v", got, tc.wantRows)
					}
					if prompt.Load() != nil {
						t.Fatalf("onFail called on success: %v", prompt.Load())
					}
					return
				}
				if err == nil {
					t.Fatalf("wait succeeded with %d rows; want failure containing %q", gr.totalRows, tc.wantErr)
				}
				if gr != nil {
					t.Fatalf("failure returned a result as well: %+v", gr)
				}
				for _, w := range tc.wantErr {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("error %q does not contain %q", err, w)
					}
				}
				if tc.wantPrompt {
					pe, _ := prompt.Load().(error)
					if pe == nil {
						t.Fatal("anomaly not delivered to onFail (the statement's cancellation path)")
					}
					if pe.Error() != err.Error() {
						t.Errorf("onFail got %q, wait returned %q", pe, err)
					}
					if code := sqlerr.StateOf(err); code != "XX000" {
						t.Errorf("SQLSTATE = %q, want XX000", code)
					}
				} else if pe := prompt.Load(); pe != nil {
					t.Errorf("onFail called for a worker-reported error: %v", pe)
				}
			})
		}
	}
}

// A first execution that streamed rows and then died, with no replacement,
// must end in the bounded timeout — never success over its partial rows.
func TestGatherIdentity_PartialFirstNoReplacementTimesOut(t *testing.T) {
	r, a := newTestGatherReceiver(1 << 20)
	r.handleParsed(a.batch(wshfInt64Payload(t, 4, 0), 4))
	start := time.Now()
	gr, err := r.wait(context.Background(), 200*time.Millisecond)
	r.discard()
	if err == nil || gr != nil {
		t.Fatalf("wait = %v, %v; want the bounded timeout", gr, err)
	}
	if !strings.Contains(err.Error(), "timed out") || !strings.Contains(err.Error(), "0/1") {
		t.Fatalf("error = %v, want a timeout naming 0/1 tasks complete", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("timeout not bounded: %v", time.Since(start))
	}
}

// A failure is decided once, and promptly: wait() returns the anomaly while
// the other task is still streaming, without waiting for its terminal.
func TestGatherIdentity_FailureWakesWaitPromptly(t *testing.T) {
	r, a := newTestGatherReceiver(1<<20, "t1", "t2")
	r.handleParsed(a.batch(wshfInt64Payload(t, 1, 0), 1))
	out := make(chan error, 1)
	go func() {
		_, err := r.wait(context.Background(), 30*time.Second)
		out <- err
	}()
	time.Sleep(20 * time.Millisecond)
	r.handleParsed(stream("t1", "e2").batch(wshfInt64Payload(t, 1, 0), 1))
	select {
	case err := <-out:
		if err == nil || !strings.Contains(err.Error(), "executed more than once") {
			t.Fatalf("wait = %v, want the conflict", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not return on the conflict; it waited for the remaining task")
	}
}

// After a failure every late message is discarded: no rows retained, no
// scratch recreated, the decision unchanged.
func TestGatherIdentity_LateFramesAfterFailure(t *testing.T) {
	r, a := newTestGatherReceiver(1) // budget 1: the second payload spills
	r.handleParsed(a.batch(wshfInt64Payload(t, 64, 0), 64))
	r.handleParsed(a.batch(wshfInt64Payload(t, 64, 64), 64))
	r.mu.Lock()
	spill := r.spillPath
	r.mu.Unlock()
	if spill == "" {
		t.Fatal("setup: no spill before the conflict")
	}
	b := stream("t1", "e2")
	r.handleParsed(b.batch(wshfInt64Payload(t, 64, 0), 64))
	// Spill cleanup on failure: the scratch the receiver held is gone.
	if _, err := os.Stat(spill); !os.IsNotExist(err) {
		t.Fatalf("receiver scratch survived the failure: %v", err)
	}
	first := r.failure()
	if first == nil {
		t.Fatal("no failure recorded")
	}
	// Late frames from both executions, and an unknown task.
	r.handleParsed(a.batch(wshfInt64Payload(t, 64, 128), 64))
	r.handleParsed(a.terminal())
	r.handleParsed(b.batch(wshfInt64Payload(t, 64, 0), 64))
	r.handleParsed(stream("tX", "e3").terminal())
	r.mu.Lock()
	if r.spillPath != "" || r.spillFile != nil || len(r.batches) != 0 {
		r.mu.Unlock()
		t.Fatalf("late frame after failure retained data: spill=%q batches=%d", r.spillPath, len(r.batches))
	}
	r.mu.Unlock()
	if got := r.failure(); got != first {
		t.Fatalf("decision changed by a late frame: %v → %v", first, got)
	}
	if _, err := r.wait(context.Background(), time.Second); !errors.Is(err, first) && err.Error() != first.Error() {
		t.Fatalf("wait = %v, want %v", err, first)
	}
}

// After valid completion a duplicate execution's frames are discarded: they
// neither fail the statement retroactively nor change the result.
func TestGatherIdentity_LateFramesAfterSuccess(t *testing.T) {
	r, a := newTestGatherReceiver(1 << 20)
	var prompt atomic.Value
	r.onFail = func(err error) { prompt.Store(err) }
	r.handleParsed(a.batch(wshfInt64Payload(t, 3, 0), 3))
	r.handleParsed(a.terminal())
	// Duplicate execution arrives after completion, before and after wait().
	b := stream("t1", "e2")
	r.handleParsed(b.batch(wshfInt64Payload(t, 3, 0), 3))
	gr, err := r.wait(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	r.handleParsed(b.batch(wshfInt64Payload(t, 3, 0), 3))
	r.handleParsed(b.terminal())
	if got := drainReplay(t, newGatherReplayStream(gr.batches, gr.spillPath, nil)); fmt.Sprint(got) != "[0 1 2]" {
		t.Fatalf("rows = %v, want [0 1 2]", got)
	}
	if prompt.Load() != nil || r.failure() != nil {
		t.Fatalf("a duplicate after valid completion failed the statement: %v", prompt.Load())
	}
}

// Conflicting executions racing completion: the bound execution's terminal
// and a second execution's batch arrive concurrently. Either order is legal;
// the outcome must be the bound execution's rows exactly, or the conflict —
// never a result that includes the second execution's rows.
func TestGatherIdentity_ConflictRacingCompletion(t *testing.T) {
	var successes, failures int
	for i := 0; i < 300; i++ {
		r, a := newTestGatherReceiver(1 << 20)
		r.handleParsed(a.batch(wshfInt64Payload(t, 2, 0), 2))
		b := stream("t1", "e2")
		bmsg := b.batch(wshfInt64Payload(t, 2, 100), 2)
		term := a.terminal()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); r.handleParsed(term) }()
		go func() { defer wg.Done(); r.handleParsed(bmsg) }()
		wg.Wait()
		gr, err := r.wait(context.Background(), time.Second)
		r.discard()
		if err != nil {
			if !strings.Contains(err.Error(), "executed more than once") {
				t.Fatalf("iteration %d: unexpected error %v", i, err)
			}
			failures++
			continue
		}
		if got := drainReplay(t, newGatherReplayStream(gr.batches, gr.spillPath, nil)); fmt.Sprint(got) != "[0 1]" {
			t.Fatalf("iteration %d: success with rows %v, want [0 1]", i, got)
		}
		successes++
	}
	t.Logf("racing completion: %d successes (the bound execution's rows), %d conflicts", successes, failures)
}
