package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestMinByMaxByEveryType is #392's gate. For each of the 22 types it asserts
// both halves of the contract that a six-case switch broke:
//
//   - the DECLARED output type is the value argument's own type, and
//   - the VALUE is the one the projection returns for the winning row.
//
// The value arm is the definition of MIN_BY read literally — MIN_BY(v, k) is
// v at the row where k is least — with the engine's own projection as the
// reference, so no rendering is hard-coded and a wrongly-typed output column
// (PORT answering as a float) fails on the value too.
func TestMinByMaxByEveryType(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	for _, col := range mbTypeCols() {
		col := col
		t.Run(col.Name, func(t *testing.T) {
			// One projection pass: the reference for every assertion below.
			proj, err := db.Query(ctx, fmt.Sprintf("SELECT id, g, %s AS v FROM mbtypes ORDER BY id", col.Name))
			if err != nil {
				t.Fatalf("projection: %v", err)
			}
			if len(proj.Rows) != mbRows {
				t.Fatalf("projection returned %d rows, want %d", len(proj.Rows), mbRows)
			}
			// winners[g] = (value at min id, value at max id) over the rows
			// of group g whose value is non-NULL; the "" key is the whole
			// table (the scalar form).
			type winner struct{ lo, hi any }
			winners := map[string]*winner{}
			seen := map[string]bool{}
			for _, r := range proj.Rows {
				if r["v"] == nil {
					continue // MIN_BY skips a NULL value
				}
				keys := []string{""}
				if r["g"] != nil {
					keys = append(keys, fmt.Sprint(r["g"]))
				}
				for _, k := range keys {
					if !seen[k] {
						seen[k] = true
						winners[k] = &winner{lo: r["v"]}
					}
					winners[k].hi = r["v"] // rows arrive in id order
				}
			}

			scalar, err := db.Query(ctx, fmt.Sprintf(
				"SELECT MIN_BY(%s, id) AS lo, MAX_BY(%s, id) AS hi FROM mbtypes", col.Name, col.Name))
			if err != nil {
				t.Fatalf("scalar MIN_BY/MAX_BY: %v", err)
			}
			mbAssertTypes(t, scalar.ColumnMetas, col.Type, "lo", "hi")
			if len(scalar.Rows) != 1 {
				t.Fatalf("scalar aggregate returned %d rows, want 1", len(scalar.Rows))
			}
			mbAssertEqual(t, "scalar lo", scalar.Rows[0]["lo"], winners[""].lo)
			mbAssertEqual(t, "scalar hi", scalar.Rows[0]["hi"], winners[""].hi)

			// The grouped form is the one that took the process down: its
			// finalize runs on the parallel-emit goroutine.
			grouped, err := db.Query(ctx, fmt.Sprintf(
				"SELECT g, MIN_BY(%s, id) AS lo, MAX_BY(%s, id) AS hi FROM mbtypes GROUP BY g ORDER BY g",
				col.Name, col.Name))
			if err != nil {
				t.Fatalf("grouped MIN_BY/MAX_BY: %v", err)
			}
			mbAssertTypes(t, grouped.ColumnMetas, col.Type, "lo", "hi")
			for _, r := range grouped.Rows {
				if r["g"] == nil {
					continue
				}
				w, ok := winners[fmt.Sprint(r["g"])]
				if !ok {
					t.Fatalf("group %v has no non-NULL rows but the aggregate emitted one", r["g"])
				}
				mbAssertEqual(t, fmt.Sprintf("group %v lo", r["g"]), r["lo"], w.lo)
				mbAssertEqual(t, fmt.Sprintf("group %v hi", r["g"]), r["hi"], w.hi)
			}
		})
	}
}

func mbAssertTypes(t *testing.T, metas []ColumnMeta, want parquet.TypeID, cols ...string) {
	t.Helper()
	byName := make(map[string]ColumnMeta, len(metas))
	for _, m := range metas {
		byName[m.Name] = m
	}
	for _, c := range cols {
		m, ok := byName[c]
		if !ok {
			t.Fatalf("result carries no metadata for column %q (metas: %+v)", c, metas)
		}
		if m.TypeID != want {
			t.Errorf("column %q declared %v, want the value argument's own type %v", c, m.TypeID, want)
		}
	}
}
