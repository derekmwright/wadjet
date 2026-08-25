package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSetOpContainerKeyIsAPinnedDivergence pins #612: UNION, INTERSECT and
// EXCEPT decide membership for a container column by `fmt.Sprintf("%v", …)`,
// which is not injective for any of them, so two DIFFERENT values become one
// member.
//
// It is a PIN in the ADR-0013 sense: it asserts the defect is still present
// and FAILS when the queries start answering correctly. That failure is the
// fix's proof and the signal to delete this file — do not "repair" it by
// relaxing the assertion.
//
// The evidence is a self-contradiction inside one engine, so no second engine
// is needed to call it a defect: over the same two rows, GROUP BY and DISTINCT
// answer TWO values and UNION answers ONE, because they key through different
// producers. GROUP BY walks the child vector (exec.appendColumnValue);
// the set operation renders the boxed value (physical.keyValueText's
// fall-through). INTERSECT is the sharper half — two disjoint one-row sets
// intersect to a non-empty result.
//
// It lives here rather than as a two-path corpus entry because the corpus
// cannot reach it: over a 5000-row table a set operation lowers to a
// GroupByAll aggregate, which keys columnar and is RIGHT on both arms. Only
// the small-input rowHashKey path renders, so the pin needs its own two-row
// table. typematrix.arrayValue's ids 101/102 carry the same colliding pair
// for the day the corpus can use it.
func TestSetOpContainerKeyIsAPinnedDivergence(t *testing.T) {
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

	t.Run("set_operations_still_merge_them", func(t *testing.T) {
		for _, q := range []struct {
			name string
			sql  string
			want int // what the defect answers today
			ok   int // what SQL requires
		}{
			{"union", `SELECT a FROM setopc UNION SELECT a FROM setopc`, 1, 2},
			{"intersect", `SELECT a FROM setopc WHERE id = 1 INTERSECT SELECT a FROM setopc WHERE id = 2`, 1, 0},
			{"except", `SELECT a FROM setopc WHERE id = 1 EXCEPT SELECT a FROM setopc WHERE id = 2`, 0, 1},
		} {
			got := rows(q.sql)
			if got == q.ok {
				t.Errorf("%s now answers %d rows, which is CORRECT — #612 is fixed. "+
					"Delete this pin and gate the three shapes on their right answers instead; "+
					"that deletion is the fix's proof.", q.name, got)
				continue
			}
			if got != q.want {
				t.Errorf("#612 changed shape for %s: answered %d rows, the known-wrong answer is %d "+
					"and the right one is %d — re-read the issue before re-pinning", q.name, got, q.want, q.ok)
				continue
			}
			t.Logf("known divergence, tracked in #612 — NOT gated: %s answers %d rows, SQL requires %d",
				q.name, got, q.ok)
		}
	})
}
