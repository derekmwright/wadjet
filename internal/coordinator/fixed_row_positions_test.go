package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/oracle/rowdecl"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

func TestFixedRowScalarInEveryPosition(t *testing.T) {
	if testing.Short() {
		t.Skip("five execution arms")
	}
	body := func(args []any) any { return rowdecl.Value(args[0].(int64)) }
	expr.RegisterFunc("a3b_fixed", body, expr.RetRow(rowdecl.Fields()))
	expr.RegisterFunc("a3b_untyped", body, expr.RetRow(nil))
	defer expr.DefaultRegistry.Unregister("a3b_fixed")
	defer expr.DefaultRegistry.Unregister("a3b_untyped")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, mode := range []string{"single", "spilled", "dag", "dag-shuffled", "dag-morsel"} {
		t.Run(mode, func(t *testing.T) {
			var db *wadjet.DB
			var coord *Coordinator
			if mode == "single" {
				db = tmdStandalone(t, ctx)
			} else if mode == "spilled" {
				db = na2Standalone(t, ctx, 512*1024)
			} else {
				infra := tmdInfra(t, ctx)
				tmdWriteTables(t, ctx, infra, nil)
				if mode == "dag-morsel" {
					coord = tmdCoordinatorWithWorkers(t, ctx, infra, func(w *worker.Config) { w.MorselWorkers = 4 })
				} else {
					coord = tmdCoordinator(t, ctx, infra, func(c *Config) {
						if mode == "dag-shuffled" {
							c.BroadcastBytesOverride = 1
						}
					})
				}
			}
			for _, producer := range []string{"r", "a3b_fixed(id)", "a3b_untyped(id)", "semver_parse(v)"} {
				for _, cell := range rowdecl.Cells(producer) {
					// The review's RetRow(nil) control covers whole values, whose TEXT
					// disposition can preserve a box but cannot publish typed child fields.
					if producer == "a3b_untyped(id)" && cell.Name != "projection" && cell.Name != "limit" && cell.Name != "distinct" && cell.Name != "group" && cell.Name != "scalar_argument_routed" {
						continue
					}
					t.Run(producer+"/"+cell.Name, func(t *testing.T) {
						if mode == "spilled" {
							defer FixedRowSpill(t, cell.Name)()
						}
						before := a2fReadRoutes(coord)
						var rows []map[string]any
						var schema []parquet.Column
						if coord != nil {
							r, e := coord.ExecuteSQL(ctx, cell.SQL)
							if e != nil {
								t.Fatal(e)
							}
							if r.Error != "" {
								t.Fatal(r.Error)
							}
							schema = r.OutputSchema()
							rows, e = r.Rows()
							if e != nil {
								t.Fatal(e)
							}
						} else {
							r, e := db.Query(ctx, cell.SQL)
							if e != nil {
								t.Fatal(e)
							}
							rows = r.Rows
							for _, m := range r.ColumnMetas {
								schema = append(schema, parquet.Column{Name: m.Name, Type: m.TypeID, Fields: m.Fields})
							}
						}
						a3bCheckRoutes(t, coord, before, cell.Name)
						if cell.Row && producer != "a3b_untyped(id)" {
							if len(schema) == 0 || schema[0].Type != parquet.TypeRow || len(schema[0].Fields) != 5 {
								t.Errorf("declaration: %+v; want ROW fields=5", schema)
							} else {
								for i, want := range rowdecl.Fields() {
									got := schema[0].Fields[i]
									if got.Name != want.Name || got.Type != want.Type {
										t.Errorf("field %d: %+v want %+v", i, got, want)
									}
								}
							}
						}
						var got []string
						for _, r := range rows {
							got = append(got, fmt.Sprint(r["p"]))
							if n, ok := r["n"]; ok {
								want := "1"
								if cell.Name == "window_over_distinct" {
									want = "3"
								}
								if cell.Name == "window_order" {
									if row, ok := r["p"].(map[string]any); ok {
										want = fmt.Sprint(row["major"])
									}
								}
								if fmt.Sprint(n) != want {
									t.Errorf("count=%v want %s", n, want)
								}
							}
						}
						want := append([]string(nil), cell.Want...)
						if !strings.Contains(cell.SQL, "ORDER BY") {
							sort.Strings(got)
							sort.Strings(want)
						}
						if strings.Join(got, "\n") != strings.Join(want, "\n") {
							t.Errorf("%s\ngot %v want %v", cell.SQL, got, want)
						}
					})
				}
			}
		})
	}
}

// FixedRowWireArm supplies the same fixture to the external-package pgwire
// gate without creating an import cycle between coordinator and pgwire.
func FixedRowWireArm(t *testing.T, mode string) (*wadjet.DB, *Coordinator) {
	t.Helper()
	ctx := context.Background()
	if mode == "spilled" {
		return na2Standalone(t, ctx, 512*1024), nil
	}
	db := tmdStandalone(t, ctx)
	if mode == "single" {
		return db, nil
	}
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	if mode == "dag-morsel" {
		return db, tmdCoordinatorWithWorkers(t, ctx, infra, func(w *worker.Config) { w.MorselWorkers = 4 })
	}
	return db, tmdCoordinator(t, ctx, infra, func(c *Config) {
		if mode == "dag-shuffled" {
			c.BroadcastBytesOverride = 1
		}
	})
}

// a3bCheckRoutes holds the base disposition: the existing scalar-subquery
// literal transport routes composite values locally. Every other cell must
// execute on the DAG, with all route counters unchanged.
func a3bCheckRoutes(t *testing.T, c *Coordinator, before a2fRoutes, cell string) {
	t.Helper()
	after := a2fReadRoutes(c)
	for i, name := range after.names {
		want := int64(0)
		if (cell == "scalar_subquery" || cell == "scalar_argument_routed") && name == "ScalarProjection" {
			want = 1
		}
		if delta := after.values[i] - before.values[i]; delta != want {
			t.Errorf("%s route %s moved %d, base disposition %d", cell, name, delta, want)
		}
	}
}

func FixedRowRouteCheck(t *testing.T, c *Coordinator, cell string) func() {
	before := a2fReadRoutes(c)
	return func() { a3bCheckRoutes(t, c, before, cell) }
}

func FixedRowSpill(t *testing.T, cell string) func() {
	before := exec.ForcedAggDrains.Load()
	old := exec.ForceAggDrainEvery(1)
	runs := exec.ForceSmallSpillRuns(1)
	return func() {
		runs()
		exec.ForceAggDrainEvery(old)
		if cell == "distinct" || cell == "group" {
			if exec.ForcedAggDrains.Load() == before {
				t.Error("aggregate spill did not engage")
			}
		}
	}
}
