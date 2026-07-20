package coordinator

import (
	"testing"
	"time"
)

// TestProjectedTailSeconds verifies the eager tail projection: mean
// inter-arrival of observed completions extrapolated over remaining tasks,
// with conservative zeros for unmeasurable feeds (fewer than two
// completions, nothing remaining) so the gate declines toward the barrier.
func TestProjectedTailSeconds(t *testing.T) {
	mk := func(total, completed int, first, last time.Time) *eagerFeed {
		f := newEagerFeed()
		f.producerTaskIDs = make([]string, total)
		f.completed = completed
		f.firstDone = first
		f.lastDone = last
		return f
	}
	t0 := time.Now()

	cases := []struct {
		name string
		f    *eagerFeed
		want float64
	}{
		{
			// 4 of 12 done over 6s → mean gap 2s → 8 remaining → 16s tail.
			name: "long_tail",
			f:    mk(12, 4, t0, t0.Add(6*time.Second)),
			want: 16.0,
		},
		{
			// all done → nothing to overlap.
			name: "complete",
			f:    mk(12, 12, t0, t0.Add(22*time.Second)),
			want: 0,
		},
		{
			// single completion → no measurable gap → conservative 0.
			name: "one_completion",
			f:    mk(3, 1, t0, t0),
			want: 0,
		},
		{
			// single-task producer, threshold==1: remaining 0 → 0.
			name: "single_task_producer",
			f:    mk(1, 1, t0, t0),
			want: 0,
		},
		{
			// two completions at the same instant (degenerate clock) → 0,
			// never NaN/negative.
			name: "zero_gap",
			f:    mk(6, 2, t0, t0),
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.f.projectedTailSeconds()
			if got < tc.want-0.01 || got > tc.want+0.01 {
				t.Fatalf("projectedTailSeconds() = %v, want %v", got, tc.want)
			}
		})
	}
}
