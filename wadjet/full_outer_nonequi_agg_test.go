package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #622: BOOL_OR(t2.c2) over `t5, t1 FULL OUTER JOIN t2 ON t1.c7` answered NULL,
// and the same query under an always-true `WHERE t5.c2 IS NULL` answered FALSE
// — an always-true filter changing a scalar aggregate, which is a TLP-Aggregate
// self-consistency violation regardless of any external oracle.
//
// Mechanism: the single-process planner stripped the table qualifier off an
// aggregate's input column ("t2.c2" -> "c2") via the physical-package
// cleanExpr, and a bare name binds to the FIRST column of that name in the
// join output schema. t5, t1 and t2 all carry a bare "c2", so BOOL_OR read
// t5.c2 (never inserted, all NULL) -> NULL. The always-true WHERE reordered
// the cross join, flipping which wrong "c2" was read (t1.c2, FALSE). The
// stage-DAG AggSpec already carried the qualifier; the fix aligns the
// single-process path with it (see internal/coordinator's two-path gate).
func fonReproDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	t1s := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c2", Type: parquet.TypeBool, Nullable: true},
		{Name: "c7", Type: parquet.TypeBool, Nullable: true},
	}}
	t2s := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c2", Type: parquet.TypeBool, Nullable: true},
		{Name: "c3", Type: parquet.TypeInt64, Nullable: true},
	}}
	t5s := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeString, Nullable: true},
		{Name: "c1", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c2", Type: parquet.TypeBool, Nullable: true},
	}}
	ingestTbl := func(name string, s parquet.Schema, rows []map[string]any) {
		if err := db.CreateTable(ctx, name, s, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(name, s, nil, ingest.Config{MaxBufferRows: 100})
		if len(rows) > 0 {
			if err := ing.Ingest(ctx, rows); err != nil {
				t.Fatal(err)
			}
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	ingestTbl("t1", t1s, []map[string]any{
		{"c1": int64(1337655936), "c2": false, "c7": true},
	})
	ingestTbl("t2", t2s, []map[string]any{
		{"c0": int64(-1070270311), "c1": int64(47652028), "c2": true},
		{"c2": false},
	})
	ingestTbl("t5", t5s, []map[string]any{
		{"c0": ""}, {"c0": ""}, {"c0": "Cnd"},
	})
	return db
}

func TestFullOuterNonEquiAggregateQualifier(t *testing.T) {
	ctx := context.Background()
	db := fonReproDB(t, ctx)

	scalar := func(q string) any {
		res, err := db.Query(ctx, q)
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("%q: want 1 row, got %d", q, len(res.Rows))
		}
		for _, v := range res.Rows[0] {
			return v
		}
		return nil
	}

	// The verbatim soak repro: base and always-true-filtered must agree, and
	// the true answer is TRUE — t2.c2 over the 6-row cross is {T,F} repeated.
	base := `SELECT BOOL_OR(t2.c2) FROM t5, t1 FULL OUTER JOIN t2 ON t1.c7`
	filt := base + ` WHERE t5.c2 IS NULL`
	if got := scalar(base); got != true {
		t.Errorf("base BOOL_OR: got %v, want true", got)
	}
	if got := scalar(filt); got != true {
		t.Errorf("filtered BOOL_OR: got %v, want true (an always-true filter must not change it)", got)
	}

	// The qualifier must bind each aggregate input to its OWN table's column,
	// not the first bare match on the probe side.
	res, err := db.Query(ctx, `SELECT
		COUNT(t1.c0) AS t1c0, COUNT(t1.c2) AS t1c2, COUNT(t1.c7) AS t1c7,
		COUNT(t2.c0) AS t2c0, COUNT(t2.c1) AS t2c1, COUNT(t2.c2) AS t2c2,
		COUNT(t5.c0) AS t5c0
		FROM t5, t1 FULL OUTER JOIN t2 ON t1.c7`)
	if err != nil {
		t.Fatal(err)
	}
	r := res.Rows[0]
	// Over the 6-row result: t1.c0 NULL(0), t1.c2 FALSE(6), t1.c7 TRUE(6);
	// t2.c0 present in 1 of 2 t2 rows -> 3, t2.c1 -> 3, t2.c2 all present -> 6;
	// t5.c0 present -> 6.
	want := map[string]int64{
		"t1c0": 0, "t1c2": 6, "t1c7": 6,
		"t2c0": 3, "t2c1": 3, "t2c2": 6, "t5c0": 6,
	}
	for k, w := range want {
		if got := toI64(t, r[k]); got != w {
			t.Errorf("COUNT %s: got %v, want %d", k, r[k], w)
		}
	}
}
