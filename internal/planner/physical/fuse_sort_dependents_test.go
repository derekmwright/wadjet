package physical

import "testing"

// Regression for #390. fuseSortIntoPredecessor folds a Singleton sort's
// SortKeys+Limit onto its predecessor and drops the sort stage. That is
// sound for the shape it was written for — the sort is the plan's root, so
// the terminal gather re-imposes the ordering as a merge-sort fragment
// (#288) — and unsound the moment another stage reads the sort's output:
// broadcastJoinProbeSplit re-fans a Singleton broadcast_join out at
// dispatch, each task then sorts and limits its own slice of the probe
// files, and every consumer other than the ordered gather reads those
// task files concatenated. The rows past each shard's local top-N were
// never written, so it is the wrong ANSWER, not just the wrong order.
func TestFuseSortIntoPredecessorSkipsSortsWithDependents(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	// The sort is the plan root: the fuse must still happen, or #288's
	// ordered gather has nothing to re-impose and every join+ORDER BY
	// query grows a stage.
	t.Run("root sort still fuses", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx, `
			SELECT s_suppkey, n_comment
			FROM supplier JOIN nation ON s_nationkey = n_nationkey
			ORDER BY n_comment DESC
			LIMIT 41`, 3)
		if id := findStageOfType(stages, "sort"); id != "" {
			t.Fatalf("standalone sort stage %q survived on a root sort: the fold stopped happening", id)
		}
		if !anyStageCarriesFusedSort(stages) {
			t.Fatal("no stage carries the fused SortKeys+Limit")
		}
	})

	// The sort feeds an aggregate, so its output is read by a stage, not
	// by the gather. The fold must be declined and the single-task sort
	// stage must stay in the plan.
	t.Run("sort with a dependent does not fuse", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx, `
			SELECT COUNT(*) AS c FROM (
				SELECT s_suppkey, n_comment
				FROM supplier JOIN nation ON s_nationkey = n_nationkey
				ORDER BY n_comment DESC
				LIMIT 41) t`, 3)
		sortID := findStageOfType(stages, "sort")
		if sortID == "" {
			t.Fatal("the sort stage was folded away even though a stage reads its output (#390)")
		}
		deps := stageDependents(stages)
		if !deps[sortID] {
			t.Fatalf("sort %q has no dependent, so this case is not the one under test", sortID)
		}
		for _, s := range stages {
			if s.ID == sortID {
				continue
			}
			if len(s.SortKeys) > 0 || s.Limit > 0 {
				t.Fatalf("stage %s (%s) carries SortKeys=%v Limit=%d — the fold happened anyway",
					s.ID, s.Type, s.SortKeys, s.Limit)
			}
		}
	})
}

func findStageOfType(stages []Stage, typ string) string {
	for _, s := range stages {
		if s.Type == typ {
			return s.ID
		}
	}
	return ""
}

func anyStageCarriesFusedSort(stages []Stage) bool {
	for _, s := range stages {
		if len(s.SortKeys) > 0 && s.Limit > 0 {
			return true
		}
	}
	return false
}
