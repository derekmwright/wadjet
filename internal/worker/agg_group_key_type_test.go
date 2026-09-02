package worker

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #379: the pre-aggregate projection types a derived GROUP BY key from the
// plan-declared type when the spec carries one, not from schema-blind
// inference over the expression text. COALESCE(l_extendedprice, 0) is the
// motivating key: with no catalog the literal 0 decides Int64, the projection
// allocates an Int64 key vector, and every float price truncates on write —
// the distinct set silently loses a fifth of its groups, on the stage DAG
// only (the single-process planner resolves the column's catalog type and
// gets Float64).
func TestAggInputProjectionDeclaredGroupKeyType(t *testing.T) {
	const key = "coalesce(l_extendedprice, 0)"

	keyCol := func(t *testing.T, groupByTypes map[string]int,
		groupByDecimal map[string]distributed.DecimalMeta) exec.ProjectColumn {
		t.Helper()
		project, _, err := buildAggInputProjection([]string{key}, nil, nil, groupByTypes, groupByDecimal, nil)
		if err != nil {
			t.Fatalf("buildAggInputProjection: %v", err)
		}
		if project == nil {
			t.Fatalf("no projection built — %q must count as a derived key", key)
		}
		// The derived key is materialized into its HIDDEN SLOT, not under
		// its own text: a column named after the key would shadow, or be
		// shadowed by, an input column the query spells the same way, and
		// which one won differed between the two engines (ADR-0026). The
		// key is published under its own text by the aggregate's
		// GroupByOutNames, one operator later.
		slot := physical.SlotName(physical.SlotGroupKey, 0)
		for _, pc := range project.Projections {
			if pc.Name == slot {
				return pc
			}
		}
		t.Fatalf("projection has no column named %q (for key %q)", slot, key)
		return exec.ProjectColumn{}
	}
	keyType := func(t *testing.T, groupByTypes map[string]int) parquet.TypeID {
		t.Helper()
		return keyCol(t, groupByTypes, nil).Type
	}

	// Undeclared (older coordinator): the schema-blind inference stands.
	// This documents WHY the declaration exists — the literal alone types
	// the key Int64, which truncates float values.
	if got := keyType(t, nil); got != parquet.TypeInt64 {
		t.Errorf("undeclared key type = %v, want the blind inference's Int64 (has the inference changed? then this test's premise needs re-checking)", got)
	}

	// Declared: the plan-time type wins over the inference.
	declared := map[string]int{key: int(parquet.TypeFloat64)}
	if got := keyType(t, declared); got != parquet.TypeFloat64 {
		t.Errorf("declared key type = %v, want Float64 (OpSpec.GroupByTypes must override the blind inference)", got)
	}

	// A DECIMAL key carries its (p,s) too (ADR-0024 item 2). Without them
	// the vector comes out at scale 0 and #379's truncation returns for a
	// different type: 12.7500 and 12.7501 both become the group 12.
	decTypes := map[string]int{key: int(parquet.TypeDecimal)}
	decMeta := map[string]distributed.DecimalMeta{key: {Precision: 18, Scale: 4}}
	got := keyCol(t, decTypes, decMeta)
	if got.Type != parquet.TypeDecimal || got.Precision != 18 || got.Scale != 4 {
		t.Errorf("declared DECIMAL key = %v(%d,%d), want DECIMAL(18,4) — OpSpec.GroupByDecimal "+
			"must reach the key vector or every value truncates at scale 0",
			got.Type, got.Precision, got.Scale)
	}
}
