package worker

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// TestAdmissionReservation_StageTasksReserve confirms heavy task types
// (Stage, Shuffle) reserve the per-task budget; lightweight types skip
// admission so they don't add latency to small operations.
func TestAdmissionReservation_StageTasksReserve(t *testing.T) {
	w := &Worker{
		config: Config{MemoryBudget: 5 * 1024 * 1024 * 1024}, // 5 GB
	}

	cases := []struct {
		taskType distributed.TaskType
		want     int64
	}{
		{distributed.TaskTypeStage, 5 * 1024 * 1024 * 1024},
		{distributed.TaskTypeShuffle, 5 * 1024 * 1024 * 1024},
		// TaskTypeGather and unknown types skip admission — they don't
		// stage heavy intermediate state (gather streams to coord; pipeline
		// is local-only) so admission would only add latency.
		{distributed.TaskTypeGather, 0},
		{distributed.TaskType("unknown"), 0},
	}

	for _, c := range cases {
		t.Run(string(c.taskType), func(t *testing.T) {
			got := w.admissionReservation(distributed.Task{Type: c.taskType})
			if got != c.want {
				t.Errorf("admissionReservation(%s) = %d, want %d", c.taskType, got, c.want)
			}
		})
	}
}

// TestAdmissionReservation_NoBudgetSkips confirms that when MemoryBudget
// is unset (single-process embeds, tests), admission is bypassed entirely —
// returning 0 means the worker never calls Reserve, so the absence of a
// shared pool doesn't break anything.
func TestAdmissionReservation_NoBudgetSkips(t *testing.T) {
	w := &Worker{config: Config{MemoryBudget: 0}}
	for _, taskType := range []distributed.TaskType{
		distributed.TaskTypeStage,
		distributed.TaskTypeShuffle,
		distributed.TaskTypeGather,
	} {
		if got := w.admissionReservation(distributed.Task{Type: taskType}); got != 0 {
			t.Errorf("with MemoryBudget=0, admissionReservation(%s)=%d, want 0", taskType, got)
		}
	}
}
