package coordinator

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
)

func testCoordinatorForPeers(staleTTL time.Duration) *Coordinator {
	return &Coordinator{
		config:    Config{StreamingExchange: true},
		peerFiles: newPeerFileRegistry(),
		workers: &WorkerRegistry{
			workers: make(map[string]*WorkerInfo),
			stale:   staleTTL,
			logger:  slog.Default(),
		},
		logger: slog.Default(),
	}
}

func TestClassifyFatalResult(t *testing.T) {
	const key = "queries/q1/stage-2/partition=0001/t7.wshf"
	fail := distributed.ResultNotification{TaskID: "t9", MissingInputKey: key}

	t.Run("no missing key", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Minute)
		if c.classifyFatalResult(distributed.ResultNotification{TaskID: "t9"}) {
			t.Fatal("classified fatal without a missing key")
		}
	})
	t.Run("unknown producer", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Minute)
		if c.classifyFatalResult(fail) {
			t.Fatal("classified fatal with unknown producer")
		}
	})
	t.Run("producer alive", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Minute)
		c.peerFiles.Record([]string{key}, "w1")
		c.workers.record(distributed.WorkerHeartbeat{WorkerID: "w1"})
		if c.classifyFatalResult(fail) {
			t.Fatal("classified fatal with live producer (upload may still land)")
		}
	})
	t.Run("durable key", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Millisecond)
		c.peerFiles.Record([]string{key}, "w1")
		c.peerFiles.MarkDurable([]string{key})
		// Producer dead, but the copy landed — retry reads S3.
		time.Sleep(5 * time.Millisecond)
		if c.classifyFatalResult(fail) {
			t.Fatal("classified fatal despite durable copy")
		}
	})
	t.Run("producer dead, not durable = input lost", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Millisecond)
		c.peerFiles.Record([]string{key}, "w1")
		c.workers.record(distributed.WorkerHeartbeat{WorkerID: "w1"})
		time.Sleep(5 * time.Millisecond) // heartbeat goes stale
		if !c.classifyFatalResult(fail) {
			t.Fatal("dead producer + non-durable key must classify as input lost")
		}
	})
}

func TestRetrierFatalSkipsRetries(t *testing.T) {
	tasks := []distributed.Task{{ID: "t1"}}
	republished := 0
	fatal := func(r distributed.ResultNotification) bool { return r.MissingInputKey != "" }
	tr := newTaskRetrier(tasks, true, func(distributed.Task) { republished++ }, slog.Default(), "s", fatal)

	done := tr.Observe(distributed.ResultNotification{
		TaskID: "t1", Success: false,
		Error:           "input queries/q1/s/x.wshf unavailable",
		MissingInputKey: "queries/q1/s/x.wshf",
	})
	if !done {
		t.Fatal("fatal failure must be terminal immediately")
	}
	if republished != 0 {
		t.Fatalf("fatal failure was republished %d times", republished)
	}
	_, errMsg, _, failed := tr.FirstError()
	if !failed || !strings.Contains(errMsg, inputLostMarker) {
		t.Fatalf("terminal error %q missing input-lost marker", errMsg)
	}
	if !IsInputLostErr(errors.New("native DAG: stage s: " + errMsg)) {
		t.Fatal("wrapped error not recognized by IsInputLostErr")
	}
}

func TestRetrierExhaustedMissingInputCarriesMarker(t *testing.T) {
	// Producer ALIVE (fatal=false) but the upload never lands: attempts
	// exhaust and the terminal error must still carry the marker so
	// ExecuteSQL reruns streaming-disabled.
	tasks := []distributed.Task{{ID: "t1"}}
	tr := newTaskRetrier(tasks, true, func(distributed.Task) {}, slog.Default(), "s",
		func(distributed.ResultNotification) bool { return false })
	fail := distributed.ResultNotification{
		TaskID: "t1", Success: false,
		Error: "input unavailable", MissingInputKey: "queries/q1/s/x.wshf",
	}
	for i := 0; i < maxTaskAttempts; i++ {
		tr.Observe(fail)
	}
	_, errMsg, _, failed := tr.FirstError()
	if !failed || !strings.Contains(errMsg, inputLostMarker) {
		t.Fatalf("exhausted-attempts error %q missing marker", errMsg)
	}
}

func TestAnnotatorAsyncUploadAndDisable(t *testing.T) {
	c := testCoordinatorForPeers(time.Minute)
	c.peerFiles.Record([]string{"queries/qr/stage-1/a.wshf"}, "w1")
	c.workers.record(distributed.WorkerHeartbeat{WorkerID: "w1", PeerAddr: "10.0.0.1:9095"})

	stageTask := distributed.Task{
		ID: "t1", QueryID: "st-join-2-qr", Type: distributed.TaskTypeStage,
		ResultPrefix: "queries/qr/stage-2/",
		InputFiles:   []string{"queries/qr/stage-1/a.wshf"},
	}
	c.annotateTaskPeerLocations(&stageTask)
	if !stageTask.AsyncUpload {
		t.Fatal("stage task not marked AsyncUpload")
	}
	if stageTask.FetchToken == "" {
		t.Fatal("stage task got no fetch token")
	}
	if stageTask.InputLocations["queries/qr/stage-1/a.wshf"] != "10.0.0.1:9095" {
		t.Fatalf("hints = %v", stageTask.InputLocations)
	}

	pipelineTask := distributed.Task{
		ID: "t2", QueryID: "qr", Type: distributed.TaskTypePipeline,
		ResultPrefix: "queries/qr/",
	}
	c.annotateTaskPeerLocations(&pipelineTask)
	if pipelineTask.AsyncUpload {
		t.Fatal("pipeline task must stay on synchronous upload (coordinator-side reads)")
	}

	// Streaming-disabled root: no hints, no token, no async — pure S3.
	c.streamingDisabled.Store("qr", struct{}{})
	disabledTask := stageTask
	disabledTask.AsyncUpload = false
	disabledTask.FetchToken = ""
	disabledTask.InputLocations = nil
	c.annotateTaskPeerLocations(&disabledTask)
	if disabledTask.AsyncUpload || disabledTask.FetchToken != "" || disabledTask.InputLocations != nil {
		t.Fatalf("disabled query still annotated: async=%v token=%q hints=%v",
			disabledTask.AsyncUpload, disabledTask.FetchToken, disabledTask.InputLocations)
	}
}

// TestAnnotatorHintsFragmentSourceInputs is the window-2 §7.1 regression.
// dispatchFinalAggregateFanout's merge tasks carry their input list ONLY in
// OpShuffleSource.InputFiles — never in Task.Inputs, which every other
// dispatcher mirrors — so the annotator walked past them and the whole
// gather-merge tail was dispatched with a fetch token and no peer hint. It
// then had no tier between itself and the S3 durable wait.
func TestAnnotatorHintsFragmentSourceInputs(t *testing.T) {
	const (
		interm = "queries/qr/final_aggregate-7/interm-0.wshf"
		table  = "tables/lineitem/part-0000.parquet"
	)
	newMergeTask := func() distributed.Task {
		return distributed.Task{
			ID:      "m0",
			QueryID: "st-final_aggregate-7-interm-qr",
			StageID: "final_aggregate-7-merge-0",
			Type:    distributed.TaskTypeStage,
			// The whole point: the input list lives here and nowhere else.
			ResultPrefix: "queries/qr/final_aggregate-7/",
			Operators: []distributed.OpSpec{
				{Type: distributed.OpShuffleSource, InputAlias: "repartition-11", InputFiles: []string{interm, table}},
				{Type: distributed.OpHashAggregate},
				{Type: distributed.OpUnpartitionedSink},
			},
		}
	}

	c := testCoordinatorForPeers(time.Minute)
	c.peerFiles.Record([]string{interm}, "w1")
	// A base-table key is never recorded, so it can never acquire a hint.
	c.peerFiles.Record([]string{table}, "w1")
	c.workers.record(distributed.WorkerHeartbeat{WorkerID: "w1", PeerAddr: "10.0.0.1:9095"})

	task := newMergeTask()
	c.annotateTaskPeerLocations(&task)
	if task.InputLocations[interm] != "10.0.0.1:9095" {
		t.Fatalf("merge task got no hint for its source input: %v", task.InputLocations)
	}
	if _, ok := task.InputLocations[table]; ok {
		t.Fatalf("base-table key acquired a query-scratch hint: %v", task.InputLocations)
	}

	// Kill switch: back to builds-only hints.
	prev := intermPeerHints.Set(false)
	t.Cleanup(func() { intermPeerHints.Set(prev) })
	off := newMergeTask()
	c.annotateTaskPeerLocations(&off)
	if len(off.InputLocations) != 0 {
		t.Fatalf("WADJET_INTERM_PEER_HINTS=0 still hinted: %v", off.InputLocations)
	}
	if off.FetchToken == "" {
		t.Fatal("the switch must only drop hints, not the fetch token")
	}
}

func TestAnnotatorShuffleDurabilityPolicy(t *testing.T) {
	newStageTask := func() distributed.Task {
		return distributed.Task{
			ID: "t1", QueryID: "st-join-2-qr", StageID: "stage-2",
			Type:         distributed.TaskTypeStage,
			ResultPrefix: "queries/qr/stage-2/",
		}
	}

	t.Run("eager default stamps no policy", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Minute)
		task := newStageTask()
		c.annotateTaskPeerLocations(&task)
		if task.UploadPolicy != distributed.UploadEager {
			t.Fatalf("policy = %q, want eager", task.UploadPolicy)
		}
	})

	for _, mode := range []distributed.UploadPolicy{distributed.UploadLazy, distributed.UploadOff} {
		t.Run(string(mode)+" stamps stage and shuffle tasks", func(t *testing.T) {
			c := testCoordinatorForPeers(time.Minute)
			c.config.ShuffleDurability = mode
			task := newStageTask()
			c.annotateTaskPeerLocations(&task)
			if !task.AsyncUpload || task.UploadPolicy != mode {
				t.Fatalf("async=%v policy=%q, want async + %q", task.AsyncUpload, task.UploadPolicy, mode)
			}
			shuffle := newStageTask()
			shuffle.Type = distributed.TaskTypeShuffle
			c.annotateTaskPeerLocations(&shuffle)
			if shuffle.UploadPolicy != mode {
				t.Fatalf("shuffle policy = %q, want %q", shuffle.UploadPolicy, mode)
			}
		})
	}

	t.Run("coordinator-read stages stay eager", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Minute)
		c.config.ShuffleDurability = distributed.UploadLazy
		c.coordReadStages.Store("qr", map[string]struct{}{"stage-2": {}})
		task := newStageTask()
		c.annotateTaskPeerLocations(&task)
		if task.UploadPolicy != distributed.UploadEager {
			t.Fatalf("scalar-producer stage stamped %q; the coordinator can only read S3", task.UploadPolicy)
		}
		// Sibling stage of the same query still defers.
		other := newStageTask()
		other.StageID = "stage-3"
		other.QueryID = "st-agg-3-qr"
		other.ResultPrefix = "queries/qr/stage-3/"
		c.annotateTaskPeerLocations(&other)
		if other.UploadPolicy != distributed.UploadLazy {
			t.Fatalf("non-scalar sibling policy = %q, want lazy", other.UploadPolicy)
		}
	})

	t.Run("pipeline tasks never get a policy", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Minute)
		c.config.ShuffleDurability = distributed.UploadOff
		task := distributed.Task{
			ID: "t2", QueryID: "qr", Type: distributed.TaskTypePipeline,
			ResultPrefix: "queries/qr/",
		}
		c.annotateTaskPeerLocations(&task)
		if task.AsyncUpload || task.UploadPolicy != distributed.UploadEager {
			t.Fatalf("pipeline task annotated async=%v policy=%q", task.AsyncUpload, task.UploadPolicy)
		}
	})

	t.Run("streaming-disabled rerun stays pure S3", func(t *testing.T) {
		c := testCoordinatorForPeers(time.Minute)
		c.config.ShuffleDurability = distributed.UploadOff
		c.streamingDisabled.Store("qr", struct{}{})
		task := newStageTask()
		c.annotateTaskPeerLocations(&task)
		if task.AsyncUpload || task.UploadPolicy != distributed.UploadEager {
			t.Fatalf("rerun task annotated async=%v policy=%q", task.AsyncUpload, task.UploadPolicy)
		}
	})
}

func TestStreamingDisabledCtxDepthCap(t *testing.T) {
	ctx := withStreamingExchangeDisabled(t.Context())
	if !streamingExchangeDisabled(ctx) {
		t.Fatal("ctx flag not readable")
	}
	if streamingExchangeDisabled(t.Context()) {
		t.Fatal("flag leaked into a fresh ctx")
	}
}

func TestPendingNonDurableFor(t *testing.T) {
	c := testCoordinatorForPeers(time.Minute)
	r := c.peerFiles
	files := []string{
		"queries/q1/stage-2/partition=0000/t1.wshf",
		"queries/q1/stage-2/partition=0001/t1.wshf",
	}
	r.Record(files, "w1")
	r.Record([]string{"queries/q1/stage-3/t2.wshf"}, "w2")

	// Only keys the worker reported as upload-in-flight count — sync
	// uploads (pipeline/gather, old workers) never grant grace.
	if n := r.PendingNonDurableFor("w1"); n != 0 {
		t.Fatalf("expected 0 pending before RecordPending, got %d", n)
	}
	r.RecordPending(files)
	if n := r.PendingNonDurableFor("w1"); n != 2 {
		t.Fatalf("expected 2 pending, got %d", n)
	}
	if n := r.PendingNonDurableFor("w2"); n != 0 {
		t.Fatalf("w2 reported nothing pending, got %d", n)
	}

	// UploadComplete flips keys durable one at a time.
	r.MarkDurable(files[:1])
	if n := r.PendingNonDurableFor("w1"); n != 1 {
		t.Fatalf("expected 1 pending after first MarkDurable, got %d", n)
	}
	r.MarkDurable(files[1:])
	if n := r.PendingNonDurableFor("w1"); n != 0 {
		t.Fatalf("expected 0 pending after all durable, got %d", n)
	}
}

func TestPendingNonDurableForClearedByCleanup(t *testing.T) {
	c := testCoordinatorForPeers(time.Minute)
	r := c.peerFiles
	key := "queries/q9/stage-1/t1.wshf"
	r.Record([]string{key}, "w1")
	r.RecordPending([]string{key})
	if n := r.PendingNonDurableFor("w1"); n != 1 {
		t.Fatalf("expected 1 pending, got %d", n)
	}
	// Query done: terminal roots elide their uploads (no UploadComplete
	// ever comes), so cleanup must stop the keys from granting grace.
	r.CleanupQuery("q9")
	if n := r.PendingNonDurableFor("w1"); n != 0 {
		t.Fatalf("expected 0 pending after CleanupQuery, got %d", n)
	}
}

func TestClassifyFatalResultGraceWindow(t *testing.T) {
	const key = "queries/q1/stage-2/partition=0001/t7.wshf"
	fail := distributed.ResultNotification{TaskID: "t9", MissingInputKey: key}

	// Producer past the stale TTL but inside the reap-grace window: the
	// failure must stay retryable — the reaper is holding the worker open
	// precisely so its uploads can land (docs/design/reap-grace.md).
	c := testCoordinatorForPeers(time.Millisecond)
	c.workers.grace = time.Minute
	c.peerFiles.Record([]string{key}, "w1")
	c.workers.record(distributed.WorkerHeartbeat{WorkerID: "w1"})
	time.Sleep(5 * time.Millisecond) // past stale, inside grace
	if c.classifyFatalResult(fail) {
		t.Fatal("grace-window producer must stay retryable, not input-lost")
	}

	// Same silence with zero grace (pre-grace semantics): input lost.
	c.workers.grace = 0
	if !c.classifyFatalResult(fail) {
		t.Fatal("with grace disabled, dead producer must classify input-lost")
	}
}
