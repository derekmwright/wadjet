package coordinator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/oracle/rowdecl"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

func TestFixedRowFieldRefusals(t *testing.T) {
	if testing.Short() {
		t.Skip("five execution arms")
	}
	expr.RegisterFunc("a3b_fixed", func(a []any) any { return rowdecl.Value(a[0].(int64)) }, expr.RetRow(rowdecl.Fields()))
	defer expr.DefaultRegistry.Unregister("a3b_fixed")
	for _, mode := range []string{"single", "spilled", "dag", "dag-shuffled", "dag-morsel"} {
		t.Run(mode, func(t *testing.T) {
			db, c := coordinator.FixedRowWireArm(t, mode)
			srv := pgwire.NewServer(db, pgwire.Config{}, nil)
			srv.SetCoordinator(c)
			if e := srv.Start("127.0.0.1:0"); e != nil {
				t.Fatal(e)
			}
			defer srv.Shutdown()
			ctx := context.Background()
			conn, e := pgconn.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/test?sslmode=disable")
			if e != nil {
				t.Fatal(e)
			}
			defer conn.Close(ctx)
			label := conn.ExecParams(ctx, `SELECT (a3b_fixed(id)).major FROM a3b_rows WHERE id=1`, nil, nil, nil, []int16{0}).Read()
			if label.Err != nil || len(label.FieldDescriptions) != 1 || string(label.FieldDescriptions[0].Name) != "major" {
				t.Errorf("field label %+v %v", label.FieldDescriptions, label.Err)
			}
			cells := []struct{ sql, state string }{
				{`SELECT (a3b_fixed(id)).missing FROM a3b_rows WHERE id<0`, "42703"},
				{`SELECT COUNT((a3b_fixed(id)).missing) OVER () FROM a3b_rows WHERE id<0`, "42703"},
				{`WITH unused AS (SELECT (a3b_fixed(id)).missing FROM a3b_rows) SELECT id FROM a3b_rows WHERE id<0`, "42703"},
				{`SELECT (length(v)).major FROM a3b_rows WHERE id<0`, "42809"},
				{`SELECT (coalesce(r,r)).missing FROM a3b_rows WHERE id<0`, "42703"},
				{`SELECT (nullif(r,NULL)).missing FROM a3b_rows WHERE id<0`, "42703"},
			}
			for _, parent := range []string{"coalesce(v,v)", "least(id,id)", "greatest(id,id)", "nullif(id,id)", "count(*)", "sum(id)", "avg(id)", "min(id)", "max(id)", "json_extract(v,'$.a')", "row_field(r,'major')"} {
				for _, predicate := range []string{"id=1", "id<0"} {
					cells = append(cells, struct{ sql, state string }{"SELECT (" + parent + ").major FROM a3b_rows WHERE " + predicate, "42809"})
				}
			}
			for _, q := range []string{
				"SELECT id FROM a3b_rows WHERE (least(id,id)).major=1",
				"SELECT COUNT(*) FROM a3b_rows WHERE (least(id,id)).major=1",
				"SELECT id FROM a3b_rows WHERE (coalesce(v,v)).major IS NULL",
				"SELECT COUNT((coalesce(v,v)).major) OVER () FROM a3b_rows WHERE id<0",
				"WITH unused AS (SELECT (coalesce(v,v)).major FROM a3b_rows) SELECT id FROM a3b_rows WHERE id<0",
				"SELECT id FROM a3b_rows ORDER BY (least(id,id)).major",
				"SELECT (coalesce(v,v)).major FROM (SELECT v FROM a3b_rows) d WHERE false",
				"SELECT (coalesce(v,v)).major FROM (SELECT json_extract(v,'$.a') AS v FROM a3b_rows) d WHERE false",
				"SELECT (coalesce(v,v)).major FROM (SELECT * FROM a3b_rows) d WHERE false",
				"WITH c AS (SELECT v FROM a3b_rows) SELECT (coalesce(v,v)).major FROM c WHERE false",
				"SELECT (coalesce(a.v,a.v)).major FROM a3b_rows a JOIN a3b_rows b ON a.id=b.id WHERE false",
				"SELECT (least(id,id)).major, COUNT(*) FROM a3b_rows GROUP BY 1",
				"SELECT COUNT(*) FROM a3b_rows HAVING (count(*)).major=1",
			} {
				cells = append(cells, struct{ sql, state string }{q, "42809"})
			}
			for _, tc := range cells {
				t.Run("embedded/"+tc.sql, func(t *testing.T) {
					check := coordinator.FixedRowRouteCheck(t, c, "refusal")
					defer check()
					var err error
					if c != nil {
						_, err = c.ExecuteSQL(ctx, tc.sql)
					} else {
						_, err = db.Query(ctx, tc.sql)
					}
					if sqlerr.StateOf(err) != tc.state {
						t.Errorf("got %v want %s", err, tc.state)
					}
				})
				for _, format := range []int16{0, 1} {
					t.Run(fmt.Sprintf("%s/%d", tc.sql, format), func(t *testing.T) {
						check := coordinator.FixedRowRouteCheck(t, c, "field-refusal")
						r := conn.ExecParams(ctx, tc.sql, nil, nil, nil, []int16{format}).Read()
						check()
						pe, ok := r.Err.(*pgconn.PgError)
						if !ok || pe.Code != tc.state {
							t.Errorf("refusal %v want %s", r.Err, tc.state)
						} else if tc.state == "42809" {
							typ := "bigint"
							if strings.Contains(tc.sql, "coalesce(") || strings.Contains(tc.sql, "json_extract(") {
								typ = "text"
							}
							if strings.Contains(tc.sql, "sum(") || strings.Contains(tc.sql, "avg(") {
								typ = "numeric"
							}
							if strings.Contains(tc.sql, "length(") {
								typ = "integer"
							}
							want := "column notation .major applied to type " + typ + ", which is not a composite type"
							if pe.Message != want {
								t.Errorf("message %q want %q", pe.Message, want)
							}
						}
					})
				}
			}
		})
	}
}
