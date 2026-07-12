package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

func testEagerSpec(taskIDs []string, pStart, pEnd int) distributed.EagerInput {
	return distributed.EagerInput{
		RootQueryID:     "rootq",
		StageID:         "stage-3",
		ProducerTaskIDs: taskIDs,
		PartitionStart:  pStart,
		PartitionEnd:    pEnd,
	}
}

func key(task string, part int) string {
	return "queries/rootq/stage-3/partition=" + pad4(part) + "/" + task + ".wshf"
}

func pad4(n int) string {
	s := "000" + itoa(n)
	return s[len(s)-4:]
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func TestPartitionOfKey(t *testing.T) {
	cases := []struct {
		key  string
		want int
		ok   bool
	}{
		{"queries/q/s/partition=0007/t1.wshf", 7, true},
		{"queries/q/s/partition=0000/t1.wshf", 0, true},
		{"queries/q/s/partition=1234/t1.wshf", 1234, true},
		{"queries/q/s/t1.wshf", 0, false},
		{"queries/q/s/partition=xx/t1.wshf", 0, false},
	}
	for _, c := range cases {
		got, ok := partitionOfKey(c.key)
		if got != c.want || ok != c.ok {
			t.Errorf("partitionOfKey(%q) = (%d,%v), want (%d,%v)", c.key, got, ok, c.want, c.ok)
		}
	}
}

// EOF requires every candidate resolved AND all in-range files drained;
// manifests with no in-range files still resolve their task.
func TestManifestSource_EOFAfterAllResolved(t *testing.T) {
	e := &Executor{peers: newPeerExchange()}
	s := newManifestStreamSource(e, "q", "b", testEagerSpec([]string{"t1", "t2"}, 0, 3))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// t1 resolves with files only OUTSIDE our range; t2 with none at all.
	s.observe(distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "t1", Attempt: 1,
		Files: []string{key("t1", 9)}})
	s.observe(distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "t2", Attempt: 1})

	b, err := s.Next(ctx)
	if err != nil || b != nil {
		t.Fatalf("Next = (%v, %v), want EOF (nil, nil)", b, err)
	}
}

// Next blocks until a manifest arrives, then EOFs once all resolve.
func TestManifestSource_BlocksThenResolves(t *testing.T) {
	e := &Executor{peers: newPeerExchange()}
	s := newManifestStreamSource(e, "q", "b", testEagerSpec([]string{"t1"}, 0, 3))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		b, err := s.Next(ctx)
		if b != nil {
			done <- context.DeadlineExceeded // unexpected batch
			return
		}
		done <- err
	}()

	time.Sleep(50 * time.Millisecond) // let Next park on the arrival channel
	s.observe(distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "t1", Attempt: 1})

	if err := <-done; err != nil {
		t.Fatalf("Next after resolve: %v", err)
	}
}

// A higher attempt for a CONSUMED task poisons the source with the
// StaleInputAttempt marker (memo §5); a higher attempt for an UNCONSUMED
// task silently supersedes.
func TestManifestSource_AttemptFencing(t *testing.T) {
	e := &Executor{peers: newPeerExchange()}
	s := newManifestStreamSource(e, "q", "b", testEagerSpec([]string{"t1", "t2"}, 0, 3))

	// t1 resolves and is consumed (simulate the pin the read path takes).
	s.observe(distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "t1", Attempt: 1,
		Files: []string{key("t1", 1)}})
	s.mu.Lock()
	s.consumed["t1"] = 1
	s.queue = nil // pretend the file was drained
	s.mu.Unlock()

	// t2 supersedes an unconsumed attempt — allowed, no poison.
	s.observe(distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "t2", Attempt: 1,
		Files: []string{key("t2", 2)}})
	s.observe(distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "t2", Attempt: 2,
		Files: []string{key("t2", 2)}})
	s.mu.Lock()
	if s.poisoned != nil {
		s.mu.Unlock()
		t.Fatal("unconsumed supersession must not poison")
	}
	if len(s.queue) != 1 || s.queue[0].attempt != 2 {
		s.mu.Unlock()
		t.Fatalf("queue = %+v, want single t2 attempt-2 entry", s.queue)
	}
	s.mu.Unlock()

	// t1 attempt 2 arrives after t1 was consumed → poison.
	s.observe(distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "t1", Attempt: 2,
		Files: []string{key("t1", 1)}})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.Next(ctx)
	if err == nil || !strings.Contains(err.Error(), StaleInputAttemptMarker) {
		t.Fatalf("Next = %v, want StaleInputAttempt poison", err)
	}
}

// Duplicate manifests are idempotent; unknown task IDs are ignored;
// peer hints are registered for in-range files.
func TestManifestSource_DuplicatesUnknownsHints(t *testing.T) {
	e := &Executor{peers: newPeerExchange()}
	s := newManifestStreamSource(e, "q", "b", testEagerSpec([]string{"t1"}, 0, 3))

	m := distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "t1", Attempt: 1,
		Files: []string{key("t1", 2), key("t1", 8)}, PeerAddr: "10.0.0.5:9445"}
	s.observe(m)
	s.observe(m) // duplicate
	s.observe(distributed.ProducerTaskManifest{StageID: "stage-3", TaskID: "zz", Attempt: 1,
		Files: []string{key("zz", 2)}})

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) != 1 {
		t.Fatalf("queue len = %d, want 1 (duplicate must not re-queue)", len(s.queue))
	}
	if got := len(s.queue[0].files); got != 1 {
		t.Fatalf("in-range files = %d, want 1 (partition 8 outside range)", got)
	}
	if addr, _ := e.peers.hintFor(key("t1", 2)); addr != "10.0.0.5:9445" {
		t.Fatalf("peer hint not registered: %q", addr)
	}
	if addr, _ := e.peers.hintFor(key("t1", 8)); addr != "" {
		t.Fatal("out-of-range file must not be hinted")
	}
}
