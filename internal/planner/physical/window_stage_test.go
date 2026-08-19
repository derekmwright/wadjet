package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestWindowStageSpecIsResolved pins what a window stage carries to the
// worker. The worker has no catalog and no logical plan, so anything the
// planner leaves unresolved here is either a second implementation on the
// far side or a wrong answer: an argument list read as a column name finds
// no input vector, and a mis-declared output type is silently dropped writes
// (#345, inside the window operator).
func TestWindowStageSpecIsResolved(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	t.Run("value function over a string column", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx,
			`SELECT n_nationkey, NTH_VALUE(n_name, 2) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) AS w
			 FROM nation`, 3)
		wc := onlyWindowCol(t, stages)
		// The column alone, not "n_name, 2": three consumers read this
		// spelling and the raw argument list satisfies none of them.
		if wc.InputCol != "n_name" {
			t.Errorf("InputCol = %q, want %q (the column, not the argument list)", wc.InputCol, "n_name")
		}
		if wc.NthValueN != 2 {
			t.Errorf("NthValueN = %d, want 2 — parsed out of the argument list at plan time", wc.NthValueN)
		}
		// NTH_VALUE returns a value taken FROM its input column, so its
		// output type is that column's type.
		if wc.OutputType != parquet.TypeString {
			t.Errorf("OutputType = %v, want String", wc.OutputType)
		}
		if len(wc.PartitionBy) != 1 || wc.PartitionBy[0] != "n_regionkey" {
			t.Errorf("PartitionBy = %v, want [n_regionkey]", wc.PartitionBy)
		}
	})

	t.Run("rank family keeps its input-independent type", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx,
			"SELECT n_nationkey, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn FROM nation", 3)
		wc := onlyWindowCol(t, stages)
		if wc.OutputType != parquet.TypeInt64 {
			t.Errorf("OutputType = %v, want Int64", wc.OutputType)
		}
		if len(wc.PartitionBy) != 0 {
			t.Errorf("PartitionBy = %v, want none — this is a global window", wc.PartitionBy)
		}
	})

	t.Run("lag carries its offset and default", func(t *testing.T) {
		stages := sqlToStages(t, cat, ctx,
			"SELECT LAG(n_name, 2, 'none') OVER (ORDER BY n_nationkey) AS w FROM nation", 3)
		wc := onlyWindowCol(t, stages)
		if wc.LagLeadOffset != 2 {
			t.Errorf("LagLeadOffset = %d, want 2", wc.LagLeadOffset)
		}
		// Unquoted: the default arrives as SQL source, and passing it
		// through wrote 'none' — quotes included — into the result.
		if wc.LagLeadDefault != "none" {
			t.Errorf("LagLeadDefault = %v, want \"none\" without its SQL quotes", wc.LagLeadDefault)
		}
	})
}

// TestWindowStageDistribution pins the grain a window stage runs at. A
// window over PARTITION BY k is computable one partition at a time, so it
// fans out when its input already arrives clustered on k; everything else
// runs Singleton — one task holding every row, which is what a global
// window (no PARTITION BY) genuinely needs.
func TestWindowStageDistribution(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	// A leaf scan is Singleton, which already satisfies "clustered on
	// anything", so no exchange is inserted and the stage runs one task
	// over the whole input. That is the fallback, and it is correct: the
	// single task reads every partition of its input.
	for _, sql := range []string{
		"SELECT n_nationkey, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn FROM nation",
		"SELECT n_nationkey, ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) AS rn FROM nation",
	} {
		stages := sqlToStages(t, cat, ctx, sql, 3)
		s := onlyWindowStage(t, stages)
		if s.Distribution.Kind != DistSingleton {
			t.Errorf("over a Singleton scan, window stage distribution = %v, want Singleton\n  SQL: %s",
				s.Distribution.Kind, sql)
		}
	}
}

// TestValidateNativeDAGShapeWindow: a window stage the dispatcher cannot
// build a fragment from must fail at plan time, not as three dispatch
// attempts against a task with no operators.
func TestValidateNativeDAGShapeWindow(t *testing.T) {
	ok := Stage{
		ID: "window-1", Type: StageWindow,
		Dependencies: []string{"scan-0"},
		WindowCols:   []WindowColSpec{{Func: "row_number", OutputCol: "rn"}},
	}
	if err := ValidateNativeDAGShape([]Stage{ok}); err != nil {
		t.Fatalf("well-formed window stage rejected: %v", err)
	}

	twoDeps := ok
	twoDeps.Dependencies = []string{"scan-0", "scan-1"}
	if err := ValidateNativeDAGShape([]Stage{twoDeps}); err == nil {
		t.Error("expected an error for a window stage with two dependencies — " +
			"buildWindowFragment reads exactly one input alias")
	}

	noCols := ok
	noCols.WindowCols = nil
	if err := ValidateNativeDAGShape([]Stage{noCols}); err == nil {
		t.Error("expected an error for a window stage carrying no window columns")
	}
}

func onlyWindowStage(t *testing.T, stages []Stage) Stage {
	t.Helper()
	var found []Stage
	for _, s := range stages {
		if s.Type == StageWindow {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly 1 window stage, got %d in %d stages", len(found), len(stages))
	}
	return found[0]
}

func onlyWindowCol(t *testing.T, stages []Stage) WindowColSpec {
	t.Helper()
	s := onlyWindowStage(t, stages)
	if len(s.WindowCols) != 1 {
		t.Fatalf("expected 1 window column, got %d", len(s.WindowCols))
	}
	return s.WindowCols[0]
}
