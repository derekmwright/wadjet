package coordinator

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
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
	_, errMsg, failed := tr.FirstError()
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
	_, errMsg, failed := tr.FirstError()
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
