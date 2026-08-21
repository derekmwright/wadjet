package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// BOOL_AND and BOOL_OR answered false regardless of their input (#371).
//
// The accumulator was fine; its INPUT was not. An aggregate over a derived
// expression is fed by a pre-aggregate projection whose output type comes
// from nodeDeclaredType, and no boolean-valued node — a comparison, AND/OR/
// NOT, IS NULL, LIKE, BETWEEN, IN — declared anything, so the Float64
// fallback stood. The comparison kernel's boolean writes had nowhere to go
// in a Float64 vector, every row read back as 0, and the accumulator never
// saw a true value: BOOL_AND(always_true) = false, BOOL_OR(sometimes_true)
// = false, and the only agreeing answer was the one that is genuinely
// false. Found by the PostgreSQL differential oracle (BoolAggregates).
//
// The pinning detail: a PROJECTED comparison was already correct, because
// exec.Project re-types from the value it computes. The pre-aggregate
// projection has no such correction, so the declaration is final there —
// which is why this file drives every shape through an aggregate.
func TestBoolAggregatesOverPredicates(t *testing.T) {
	ctx := context.Background()
	db := openBoolAggDB(t, ctx)

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		// The oracle's shape: the always-true column and the sometimes-true
		// column both answered false before the fix.
		{"bool_and over an always-true comparison",
			"SELECT BOOL_AND(v >= 0) AS x FROM b", []any{true}},
		{"bool_or over a sometimes-true comparison",
			"SELECT BOOL_OR(v > 3) AS x FROM b", []any{true}},
		{"bool_and over a sometimes-true comparison is genuinely false",
			"SELECT BOOL_AND(v > 3) AS x FROM b", []any{false}},
		{"bool_or over a never-true comparison is genuinely false",
			"SELECT BOOL_OR(v > 100) AS x FROM b", []any{false}},
		{"every is bool_and",
			"SELECT EVERY(v >= 0) AS x FROM b", []any{true}},

		// Compound predicates: AND/OR/NOT nodes must declare boolean too.
		{"bool_and over an AND of comparisons",
			"SELECT BOOL_AND(v >= 0 AND v < 100) AS x FROM b", []any{true}},
		{"bool_or over a NOT",
			"SELECT BOOL_OR(NOT (v < 100)) AS x FROM b", []any{false}},

		// A bare BOOL column: no derived projection at all.
		{"bool_and over a bare bool column",
			"SELECT BOOL_AND(flag) AS x FROM b", []any{false}},
		{"bool_or over a bare bool column",
			"SELECT BOOL_OR(flag) AS x FROM b", []any{true}},

		// Grouped: per-group accumulators; group 0 is mixed under v >= 5,
		// groups 1 and 2 are all-true.
		{"grouped bool_and",
			"SELECT g, BOOL_AND(v >= 5) AS x FROM b GROUP BY g ORDER BY g",
			[]any{false, true, true}},
		{"grouped bool_or",
			"SELECT g, BOOL_OR(v >= 5) AS x FROM b GROUP BY g ORDER BY g",
			[]any{true, true, true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertColumn(t, ctx, db, tc.sql, "x", tc.want)
		})
	}
}

// SQL's null rule: NULL inputs are skipped, and an accumulator that saw no
// non-NULL input at all answers NULL — not its identity value. flag is NULL
// on the two rows of group 2, so BOOL_AND(flag) there must be NULL, where
// the eager `true` initialization answered true.
func TestBoolAggregatesSkipNulls(t *testing.T) {
	ctx := context.Background()
	db := openBoolAggDB(t, ctx)

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		{"grouped bool_and over a column with an all-NULL group",
			"SELECT g, BOOL_AND(flag) AS x FROM b GROUP BY g ORDER BY g",
			[]any{false, true, nil}},
		{"grouped bool_or over a column with an all-NULL group",
			"SELECT g, BOOL_OR(flag) AS x FROM b GROUP BY g ORDER BY g",
			[]any{true, true, nil}},
		{"scalar bool_and over only NULLs",
			"SELECT BOOL_AND(flag) AS x FROM b WHERE g = 2", []any{nil}},
		{"a NULL row does not decide bool_and",
			"SELECT BOOL_AND(flag) AS x FROM b WHERE g = 1 OR v = 60", []any{true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertColumn(t, ctx, db, tc.sql, "x", tc.want)
		})
	}
}

func openBoolAggDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
		{Name: "flag", Type: parquet.TypeBool, Nullable: true},
	}}
	// Group 0: v in {0, 5}, flags true/false (mixed). Group 1: v in
	// {10, 20}, flags all true. Group 2: flags all NULL.
	ingestRows(t, ctx, db, "b", schema, []map[string]any{
		{"k": int64(1), "g": int64(0), "v": int64(0), "flag": true},
		{"k": int64(2), "g": int64(0), "v": int64(5), "flag": false},
		{"k": int64(3), "g": int64(1), "v": int64(10), "flag": true},
		{"k": int64(4), "g": int64(1), "v": int64(20), "flag": true},
		{"k": int64(5), "g": int64(2), "v": int64(50), "flag": nil},
		{"k": int64(6), "g": int64(2), "v": int64(60), "flag": nil},
	})
	return db
}
