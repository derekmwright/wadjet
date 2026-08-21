package worker

import (
	"testing"

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

	keyType := func(t *testing.T, groupByTypes map[string]int) parquet.TypeID {
		t.Helper()
		project, _, err := buildAggInputProjection([]string{key}, nil, nil, groupByTypes)
		if err != nil {
			t.Fatalf("buildAggInputProjection: %v", err)
		}
		if project == nil {
			t.Fatalf("no projection built — %q must count as a derived key", key)
		}
		for _, pc := range project.Projections {
			if pc.Name == key {
				return pc.Type
			}
		}
		t.Fatalf("projection has no column named %q", key)
		return 0
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
}
