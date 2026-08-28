package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSetOpContainerKeyStaysInjective gates the #612 fix: UNION, INTERSECT and
// EXCEPT used to decide membership for a container column by
// `fmt.Sprintf("%v", …)`, which is not injective for any of them, so two
// DIFFERENT values became one member. keyValueText now keys a container column
// through exec.AppendBoxedGroupKey — the same injective boxed group key the
// aggregate path uses (#566/ADR-0023) — so distinct containers stay distinct
// and equal ones still match.
//
// This was a PIN in the ADR-0013 sense (it asserted the defect and its own
// failure was the fix's proof). The fix landed, so it is now a plain
// regression gate on the RIGHT answers, per its own instruction: the
// deletion of the pinned wrong-answer assertion is the proof.
//
// The evidence is a self-contradiction inside one engine, so no second engine
// is needed to call it a defect: over the same two rows, GROUP BY and DISTINCT
// answer TWO values, and the set operations must agree — they key through the
// same injective producer now. INTERSECT is the sharper half — two disjoint
// one-row sets must intersect to an EMPTY result.
//
// It lives here rather than as a two-path corpus entry because the corpus
// cannot reach it: over a 5000-row table a set operation lowers to a
// GroupByAll aggregate, which keys columnar and is RIGHT on both arms. Only
// the small-input rowHashKey path renders, so the gate needs its own two-row
// table. typematrix.arrayValue's ids 101/102 carry the same colliding pair
// for the day the corpus can use it.
func TestSetOpContainerKeyStaysInjective(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	}}
	if err := db.CreateTable(ctx, "setopc", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("setopc", schema, nil, ingest.Config{MaxBufferRows: 64, RowGroupSize: 64})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "a": []any{"a b"}},
		{"id": int64(2), "a": []any{"a", "b"}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	rows := func(sql string) int {
		t.Helper()
		res, err := tmRun(ctx, db, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return len(res.Rows)
	}

	// The half that is RIGHT, and must stay right — it is what makes the
	// other half a defect rather than a debatable definition of equality.
	t.Run("aggregate_keying_is_correct", func(t *testing.T) {
		for _, q := range []struct {
			name string
			sql  string
		}{
			{"distinct", `SELECT DISTINCT a FROM setopc`},
			{"group_by", `SELECT a, COUNT(*) AS n FROM setopc GROUP BY a`},
		} {
			if got := rows(q.sql); got != 2 {
				t.Errorf("%s answered %d rows, want 2 — ARRAY['a b'] and ARRAY['a','b'] are "+
					"different values and the columnar group key has always said so", q.name, got)
			}
		}
	})

	t.Run("set_operations_keep_them_distinct", func(t *testing.T) {
		for _, q := range []struct {
			name string
			sql  string
			ok   int // what SQL requires
		}{
			// UNION of the table with itself keeps BOTH distinct arrays.
			{"union", `SELECT a FROM setopc UNION SELECT a FROM setopc`, 2},
			// Two disjoint one-row sets intersect to nothing.
			{"intersect", `SELECT a FROM setopc WHERE id = 1 INTERSECT SELECT a FROM setopc WHERE id = 2`, 0},
			// The left row is not in the right set, so it survives EXCEPT.
			{"except", `SELECT a FROM setopc WHERE id = 1 EXCEPT SELECT a FROM setopc WHERE id = 2`, 1},
		} {
			if got := rows(q.sql); got != q.ok {
				t.Errorf("%s answered %d rows, want %d — ARRAY['a b'] and ARRAY['a','b'] are "+
					"different values and the set-op key must keep them apart (#612)", q.name, got, q.ok)
			}
		}
	})

	// Equal containers must still dedup — the fix must not make everything
	// distinct. Two rows carrying the identical array collapse to one member.
	t.Run("equal_containers_still_dedup", func(t *testing.T) {
		if err := ing.Ingest(ctx, []map[string]any{
			{"id": int64(3), "a": []any{"x", "y"}},
			{"id": int64(4), "a": []any{"x", "y"}},
		}); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		// id=3 and id=4 hold the same array, so a UNION over just those two
		// rows yields ONE member; the two id=1/id=2 arrays add two more.
		if got := rows(`SELECT a FROM setopc UNION SELECT a FROM setopc`); got != 3 {
			t.Errorf("union answered %d rows, want 3 — equal arrays must dedup to one member", got)
		}
		if got := rows(
			`SELECT a FROM setopc WHERE id = 3 INTERSECT SELECT a FROM setopc WHERE id = 4`,
		); got != 1 {
			t.Errorf("intersect of two equal arrays answered %d rows, want 1", got)
		}
	})
}
