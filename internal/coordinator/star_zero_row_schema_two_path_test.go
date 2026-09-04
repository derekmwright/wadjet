package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #846 ON THE DAG. A `SELECT *` that returns zero rows must declare the same
// columns the same statement declares when it returns rows — on the
// single-process path AND on the stage DAG, which are two engines (ADR-0018
// §3) and derive a result's schema from two different places.
//
// The single path takes it from exec.CollectSink.SchemaHint; the DAG takes it
// from the terminal gather stage's OutputSchema, which the physical planner
// fills from the same physical.declaredOutputSchema call. Before the fix that
// call answered nil for a bare star — logical.BuildFromSelect builds no
// Project node for one — so both arms handed the client a result with no
// columns at all, and pgwire's coordinator path had nothing to describe.
//
// The arms are the two-path harness's, with LocalFastPathBytes 0 on the DAG
// side so every query goes through the stages rather than the small-query
// local pipeline (tmdCoordinator), and a second coordinator with the
// broadcast threshold forced down so the shuffled arm is exercised too.
func TestStarZeroRowSchemaAgreesOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	// One %s: the predicate. `id < 0` matches nothing over the 22-type matrix
	// fixture; `id < 40` matches.
	for _, tc := range []struct{ name, tmpl string }{
		{"star", "SELECT * FROM " + typematrix.Table + " WHERE %s"},
		{"star_ordered", "SELECT * FROM " + typematrix.Table + " WHERE %s ORDER BY id"},
		{"star_limit", "SELECT * FROM " + typematrix.Table + " WHERE %s LIMIT 5"},
		{"star_union_all", "SELECT * FROM " + typematrix.Table + " WHERE %s UNION ALL " +
			"SELECT * FROM " + typematrix.Table + " WHERE %[1]s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full := fmt.Sprintf(tc.tmpl, "id < 40")
			empty := fmt.Sprintf(tc.tmpl, "id < 0")

			for _, arm := range []struct {
				name string
				run  func(string) (cols []string, schema []parquet.Column, rows int, err error)
			}{
				{"single", func(sql string) ([]string, []parquet.Column, int, error) {
					res, err := single.Query(ctx, sql)
					if err != nil {
						return nil, nil, 0, err
					}
					schema := make([]parquet.Column, len(res.ColumnMetas))
					for i, m := range res.ColumnMetas {
						schema[i] = parquet.Column{Name: m.Name, Type: m.TypeID,
							Precision: m.Precision, Scale: m.Scale}
					}
					return res.Columns, schema, len(res.Rows), nil
				}},
				{"dag", func(sql string) ([]string, []parquet.Column, int, error) {
					return starArmDAG(ctx, coord, sql)
				}},
				{"dag-shuffled", func(sql string) ([]string, []parquet.Column, int, error) {
					return starArmDAG(ctx, coordB, sql)
				}},
			} {
				t.Run(arm.name, func(t *testing.T) {
					wantCols, wantSchema, wantRows, err := arm.run(full)
					if err != nil {
						t.Fatalf("non-empty arm: %v", err)
					}
					if wantRows == 0 {
						t.Fatal("the non-empty arm returned no rows; the reference is meaningless")
					}
					gotCols, gotSchema, gotRows, err := arm.run(empty)
					if err != nil {
						t.Fatalf("empty arm: %v", err)
					}
					if gotRows != 0 {
						t.Fatalf("the empty arm returned %d rows", gotRows)
					}
					if len(gotCols) == 0 {
						t.Fatalf("the zero-row arm declared NO columns; through pgwire that is no "+
							"RowDescription at all (#846). The non-empty arm declares %v", wantCols)
					}
					if strings.Join(gotCols, ",") != strings.Join(wantCols, ",") {
						t.Errorf("column NAMES differ:\n empty %v\n full  %v", gotCols, wantCols)
					}
					if got, want := starDescribe(gotSchema), starDescribe(wantSchema); got != want {
						t.Errorf("column TYPES differ:\n empty %s\n full  %s", got, want)
					}
				})
			}
		})
	}
}

// starArmDAG runs one statement through a coordinator and returns what a
// client would be told about the result's shape: the columns, the DECLARED
// schema (SQLResult.OutputSchema, which is what pgwire's coord path turns
// into a RowDescription) and the row count.
func starArmDAG(ctx context.Context, coord *Coordinator, sql string) ([]string, []parquet.Column, int, error) {
	out, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, nil, 0, err
	}
	if out.Error != "" {
		return nil, nil, 0, fmt.Errorf("%s", out.Error)
	}
	schema := out.OutputSchema()
	rows, err := out.Rows()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("materializing distributed rows: %w", err)
	}
	return out.Columns, schema, len(rows), nil
}

// starDescribe renders a declared schema the way wadjet's own describeMetas
// does: name, type, and a DECIMAL's (p, s).
func starDescribe(cols []parquet.Column) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s:%s(%d,%d)", c.Name, c.Type, c.Precision, c.Scale)
	}
	return strings.Join(parts, ",")
}
