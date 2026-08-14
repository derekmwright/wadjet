package coordinator

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// The 2026-08-14 SF100 Q22-R3 hang: a frozen-spin-wedged worker keeps its
// TCP connection ESTABLISHED while heartbeats stop, so after the registry
// reaps it the data plane still lists it as connected, placement keeps
// routing (re-)dispatched tasks into the corpse, and reapStuckOnce's
// Remove() made the silent re-dispatch invisible to the stuck sweep — the
// query hung to its 30m deadline. These tests pin the two halves of the
// fix: liveness-filtered placement candidates, and a re-armed stuck clock.

func TestFilterAliveWorkers(t *testing.T) {
	alive := map[string]bool{"w1": true, "w3": true}
	oracle := func(id string) bool { return alive[id] }

	got := filterAliveWorkers([]string{"w1", "w2", "w3"}, oracle)
	if len(got) != 2 || got[0] != "w1" || got[1] != "w3" {
		t.Fatalf("filtered = %v, want [w1 w3]", got)
	}
	// All-dead falls back to the unfiltered list (startup races: data-plane
	// connections can precede first heartbeats).
	got = filterAliveWorkers([]string{"w2", "w4"}, oracle)
	if len(got) != 2 || got[0] != "w2" {
		t.Fatalf("all-dead fallback = %v, want [w2 w4]", got)
	}
	if got := filterAliveWorkers(nil, oracle); len(got) != 0 {
		t.Fatalf("empty in = %v, want empty", got)
	}
}

func TestPickRoundRobinFrom(t *testing.T) {
	var cur atomic.Uint64
	var seq []string
	for i := 0; i < 4; i++ {
		id, ok := pickRoundRobinFrom([]string{"a", "b", "c"}, &cur)
		if !ok {
			t.Fatal("pick failed")
		}
		seq = append(seq, id)
	}
	want := []string{"a", "b", "c", "a"}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("rotation = %v, want %v", seq, want)
		}
	}
	if _, ok := pickRoundRobinFrom(nil, &cur); ok {
		t.Fatal("empty candidates must fail")
	}
}

// A re-dispatched-but-still-silent task must re-enter the stuck sweep one
// threshold later and burn its remaining attempts, instead of vanishing
// from tracking when its new target never reports it.
func TestReapStuckOnce_RearmsSilentRedispatch(t *testing.T) {
	rep := &collectingRepublisher{}
	tr := newTaskRetrier(retryTestTasks(1), true, rep.republish, slog.Default(), "s", nil)
	tl := NewTaskLiveness()
	tl.Update([]string{"a"}, time.Now().Add(-time.Hour))

	if n := reapStuckOnce(tl, tr, time.Minute); n != 1 {
		t.Fatalf("first sweep redispatched %d, want 1 (attempt 2)", n)
	}
	// The clock was reset, not removed: an immediate sweep must NOT burn
	// another attempt...
	if n := reapStuckOnce(tl, tr, time.Minute); n != 0 {
		t.Fatalf("immediate re-sweep redispatched %d, want 0 (fresh clock)", n)
	}
	// ...but continued silence past the threshold re-triggers (attempt 3).
	time.Sleep(5 * time.Millisecond)
	if n := reapStuckOnce(tl, tr, time.Millisecond); n != 1 {
		t.Fatalf("post-threshold sweep redispatched %d, want 1 (attempt 3)", n)
	}
	// Terminal tasks still leave tracking entirely.
	tr.Observe(okResult("a", "f-a"))
	tl.Update([]string{"a"}, time.Now().Add(-time.Hour))
	if n := reapStuckOnce(tl, tr, time.Minute); n != 0 {
		t.Fatalf("terminal sweep redispatched %d, want 0", n)
	}
	if stuck := tl.StuckTasks(time.Minute); len(stuck) != 0 {
		t.Fatalf("terminal task still tracked: %v", stuck)
	}
}
