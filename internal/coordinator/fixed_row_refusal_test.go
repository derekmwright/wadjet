package coordinator_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/oracle/rowdecl"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
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
			for _, tc := range []struct{ sql, state string }{
				{`SELECT (a3b_fixed(id)).missing FROM a3b_rows WHERE id<0`, "42703"},
				{`SELECT COUNT((a3b_fixed(id)).missing) OVER () FROM a3b_rows WHERE id<0`, "42703"},
				{`WITH unused AS (SELECT (a3b_fixed(id)).missing FROM a3b_rows) SELECT id FROM a3b_rows WHERE id<0`, "42703"},
				{`SELECT (length(v)).major FROM a3b_rows WHERE id<0`, "42809"},
			} {
				for _, format := range []int16{0, 1} {
					t.Run(fmt.Sprintf("%s/%d", tc.sql, format), func(t *testing.T) {
						check := coordinator.FixedRowRouteCheck(t, c, "field-refusal")
						r := conn.ExecParams(ctx, tc.sql, nil, nil, nil, []int16{format}).Read()
						check()
						pe, ok := r.Err.(*pgconn.PgError)
						if !ok || pe.Code != tc.state {
							t.Errorf("refusal %v want %s", r.Err, tc.state)
						}
					})
				}
			}
		})
	}
}
