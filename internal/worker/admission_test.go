package worker

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// TestIsHeavyTask_Classification confirms which task types pass through
// the heavyTaskSem admission gate. Stage and Shuffle hold multi-GB
// working sets at SF100; lighter types (gather, control) skip the gate
// so a fully-saturated worker can still service them.
func TestIsHeavyTask_Classification(t *testing.T) {
	w := &Worker{}
	cases := []struct {
		taskType distributed.TaskType
		want     bool
	}{
		{distributed.TaskTypeStage, true},
		{distributed.TaskTypeShuffle, true},
		{distributed.TaskTypeGather, false},
		{distributed.TaskType("unknown"), false},
	}
	for _, c := range cases {
		t.Run(string(c.taskType), func(t *testing.T) {
			got := w.isHeavyTask(distributed.Task{Type: c.taskType})
			if got != c.want {
				t.Errorf("isHeavyTask(%s) = %v, want %v", c.taskType, got, c.want)
			}
		})
	}
}
