package wadjet

import (
	"context"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #621: an aggregate over a COMPILE-TIME LITERAL argument — MIN(1),
// BOOL_AND(TRUE), SUM(0) — was handed to the executor with its argument as a
// column name ("1"), which no scan produces. Without a GROUP BY the aggregate
// errored ("aggregate input \"1\" is not a column of its input"); WITH one it
// silently dropped every group, so `HAVING MIN(1) > 0` — TRUE for every
// non-empty group — excluded them all. The single-process pre-aggregate
// projection skipped a literal because isSimpleColRef treated a *plansql.Lit
// as a bare column reference (the DAG spec builder did not, projecting it),
// so the constant was never materialized into a column to aggregate over.
//
// The gate holds the constant-argument aggregate to its real per-group value,
// in the SELECT list and used as a HAVING predicate, for each aggregate kind.
func TestConstArgAggregate(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "x", Type: parquet.TypeInt64}}}
	if err := db.CreateTable(ctx, "t0", schema, nil); err != nil {
		t.Fatal(err)
	}
	// 3 groups: x=1 (2 rows), x=2, x=3.
	rows := []map[string]any{{"x": int64(1)}, {"x": int64(1)}, {"x": int64(2)}, {"x": int64(3)}}
	ing := db.NewIngester("t0", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// --- 1. A constant-argument aggregate in the SELECT list carries its
	//        real per-group value, and the query returns every group. ------
	t.Run("SelectList", func(t *testing.T) {
		q := `SELECT x, MIN(1) AS mn, MAX(1) AS mx, SUM(0) AS s, AVG(2) AS a,
		             COUNT(1) AS c, BOOL_AND(TRUE) AS ba, BOOL_OR(FALSE) AS bo
		      FROM t0 GROUP BY x ORDER BY x`
		res, err := db.Query(ctx, q)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != 3 {
			t.Fatalf("want 3 groups, got %d: %v", len(res.Rows), res.Rows)
		}
		for _, r := range res.Rows {
			eq := func(name string, want float64) {
				if got, ok := toF(r[name]); !ok || got != want {
					t.Errorf("%s: got %v (%T), want %v", name, r[name], r[name], want)
				}
			}
			eq("mn", 1)
			eq("mx", 1)
			eq("s", 0)
			eq("a", 2)
			// COUNT(1) counts rows in the group like COUNT(*): x=1 has 2.
			wantC := float64(1)
			if xv, _ := toF(r["x"]); xv == 1 {
				wantC = 2
			}
			eq("c", wantC)
			if r["ba"] != true {
				t.Errorf("ba: got %v, want true", r["ba"])
			}
			if r["bo"] != false {
				t.Errorf("bo: got %v, want false", r["bo"])
			}
		}
	})

	// --- 2. A constant-argument aggregate as the HAVING predicate keeps
	//        every group whose real aggregate value passes, matching an
	//        aggregate over a real column and PostgreSQL. -------------------
	t.Run("Having", func(t *testing.T) {
		allGroups := []string{"1", "2", "3"}
		for _, c := range []struct {
			name string
			pred string
			want []string
		}{
			{"MinConst", "MIN(1) > 0", allGroups},
			{"MaxConst", "MAX(1) > 0", allGroups},
			{"SumConst", "SUM(0) = 0", allGroups},
			{"AvgConst", "AVG(2) = 2", allGroups},
			{"CountConst", "COUNT(1) > 0", allGroups},
			{"BoolAndConst", "BOOL_AND(TRUE)", allGroups},
			{"BoolOrConst", "BOOL_OR(TRUE)", allGroups},
			{"MinConstExcludesAll", "MIN(1) > 1", nil},
			{"BoolAndFalseExcludesAll", "BOOL_AND(FALSE)", nil},
		} {
			t.Run(c.name, func(t *testing.T) {
				q := `SELECT x FROM t0 GROUP BY x HAVING ` + c.pred + ` ORDER BY x`
				res, err := db.Query(ctx, q)
				if err != nil {
					t.Fatalf("query: %v\n  SQL: %s", err, q)
				}
				var got []string
				for _, r := range res.Rows {
					if v, ok := toF(r["x"]); ok {
						got = append(got, map[float64]string{1: "1", 2: "2", 3: "3"}[v])
					}
				}
				sort.Strings(got)
				sort.Strings(c.want)
				if len(got) != len(c.want) {
					t.Fatalf("HAVING %s: got groups %v, want %v", c.pred, got, c.want)
				}
				for i := range got {
					if got[i] != c.want[i] {
						t.Fatalf("HAVING %s: got groups %v, want %v", c.pred, got, c.want)
					}
				}
			})
		}
	})

	// --- 3. And the no-GROUP-BY whole-table case, which used to ERROR. ----
	t.Run("WholeTable", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT MIN(1) AS mn, COUNT(1) AS c, SUM(0) AS s FROM t0`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("want 1 row, got %d", len(res.Rows))
		}
		r := res.Rows[0]
		if v, _ := toF(r["mn"]); v != 1 {
			t.Errorf("MIN(1) = %v, want 1", r["mn"])
		}
		if v, _ := toF(r["c"]); v != 4 {
			t.Errorf("COUNT(1) = %v, want 4", r["c"])
		}
		if v, _ := toF(r["s"]); v != 0 {
			t.Errorf("SUM(0) = %v, want 0", r["s"])
		}
	})
}
