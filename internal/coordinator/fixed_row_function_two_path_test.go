package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A registry declaration is sufficient to project a fixed-schema ROW. This
// fixture uses no semver implementation; it gates the declaration mechanism.
func TestFixedRowFunctionDeclaration(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster census")
	}
	fields := []parquet.Column{{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString}}
	expr.RegisterFunc("a3_fixed_row", func(_ []any) any { return map[string]any{"a": int64(1), "b": "x"} }, expr.RetRow(fields))
	t.Cleanup(func() { expr.DefaultRegistry.Unregister("a3_fixed_row") })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	for i, db := range []*wadjet.DB{single, spilled} {
		for _, cond := range []string{"id=1", "id<0"} {
			t.Run(fmt.Sprintf("single%d/%s", i, cond), func(t *testing.T) {
				r, e := db.Query(ctx, `SELECT a3_fixed_row(id) AS r FROM tcpflow WHERE `+cond)
				if e != nil {
					t.Fatal(e)
				}
				if len(r.ColumnMetas) != 1 || r.ColumnMetas[0].TypeID != parquet.TypeRow || len(r.ColumnMetas[0].Fields) != 2 {
					t.Fatalf("single %s declaration: %+v", cond, r.ColumnMetas)
				}
			})
		}
	}
	for _, mode := range []string{"dag", "shuffled", "morsel"} {
		t.Run(mode, func(t *testing.T) {
			infra := tmdInfra(t, ctx)
			tmdWriteTables(t, ctx, infra, nil)
			var c *Coordinator
			if mode == "morsel" {
				c = tmdCoordinatorWithWorkers(t, ctx, infra, func(w *worker.Config) { w.MorselWorkers = 4 })
			} else {
				c = tmdCoordinator(t, ctx, infra, func(c *Config) {
					if mode == "shuffled" {
						c.BroadcastBytesOverride = 1
					}
				})
			}
			for _, cond := range []string{"id=1", "id<0"} {
				q := `SELECT a3_fixed_row(id) AS r FROM tcpflow WHERE ` + cond
				before := a2fReadRoutes(c)
				r, e := c.ExecuteSQL(ctx, q)
				if e != nil {
					t.Fatal(e)
				}
				schema := r.OutputSchema()
				if len(schema) != 1 || schema[0].Type != parquet.TypeRow || len(schema[0].Fields) != 2 {
					t.Fatalf("%s declaration: %+v", cond, schema)
				}
				for i, f := range fields {
					if schema[0].Fields[i].Name != f.Name || schema[0].Fields[i].Type != f.Type {
						t.Fatalf("field %d: %+v", i, schema[0].Fields[i])
					}
				}
				a2fCheckRoutes(t, mode, c, before, q)
			}
		})
	}
}
