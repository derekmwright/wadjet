package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The mixed-case RELATION fixture. Its point is the NAME, so its columns are
// deliberately ordinary: a mixed-case name that no DDL quoted, which is what
// parquet and ingest produce and what PostgreSQL's own catalog cannot hold
// without quoting.
//
// It rides in the shared corpus (tmdTables) rather than in this gate's own
// setup so that every suite in this package plans against a catalog that
// contains one — the correctness protocol's method 10. Before it, no fixture
// anywhere in the two-path corpora had a table name that was not already its
// own folded form, so the resolution path this exercises was untested on the
// DAG and on the worker.
const tncTable = "TncMixed"

func tncSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func tncData() []map[string]any {
	return []map[string]any{
		{"k": int64(1), "v": int64(10)},
		{"k": int64(2), "v": int64(20)},
		{"k": int64(3), "v": int64(30)},
	}
}

// A mixed-case TABLE name resolves the same way on every arm.
//
// The concession is a planner-and-catalog decision, and the DAG re-plans on
// the coordinator and re-resolves on the worker — so a name canonicalized on
// one path and not the other is a table that reads on the single path and
// answers ZERO ROWS on the DAG, which is the silent shape this arc exists to
// remove. Four arms: single, spilled, stage DAG, and the DAG with every build
// forced through a shuffle.
func TestAMixedCaseTableNameResolvesOnEveryArm(t *testing.T) {
	ctx := context.Background()
	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"spilled", func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, spilled, sql) }},
		{"dag", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
		{"dag-shuffled", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, sql) }},
	}

	cases := []struct {
		name  string
		sql   string
		want  string
		state string
	}{
		{name: "the stored spelling",
			sql: `SELECT SUM(v) AS s FROM "TncMixed"`, want: "s=60"},
		{name: "the unquoted spelling as written",
			sql: "SELECT SUM(v) AS s FROM TncMixed", want: "s=60"},
		{name: "the folded spelling",
			sql: "SELECT SUM(v) AS s FROM tncmixed", want: "s=60"},
		{name: "an upper-case unquoted spelling",
			sql: "SELECT SUM(v) AS s FROM TNCMIXED", want: "s=60"},
		{name: "qualified, aliased and filtered",
			sql: "SELECT SUM(x.v) AS s FROM TncMixed x WHERE x.k > 1", want: "s=50"},
		{name: "joined to a lower-case relation",
			sql: "SELECT SUM(TncMixed.v) AS s FROM TncMixed, clt0 WHERE TncMixed.k = 1", want: "s=20"},
		{name: "through a derived table",
			sql: "SELECT SUM(w) AS s FROM (SELECT v AS w FROM tncmixed) q", want: "s=60"},
		{name: "through a CTE",
			sql: "WITH c AS (SELECT v FROM TNCMIXED) SELECT SUM(v) AS s FROM c", want: "s=60"},
		// The BOUNDARY: a delimited reference carrying an upper-case letter
		// takes no concession, on every arm.
		{name: "a delimited upper-case reference is refused",
			sql: `SELECT SUM(v) AS s FROM "TNCMIXED"`, state: "42P01"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(c.sql)
				if c.state != "" {
					if err == nil {
						t.Errorf("%s arm answered where %s is due\n  SQL: %s", arm.name, c.state, c.sql)
						continue
					}
					if got := sqlerr.StateOf(err); got != c.state {
						t.Errorf("%s arm: SQLSTATE %s, want %s: %v\n  SQL: %s",
							arm.name, got, c.state, err, c.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm refused: %v\n  SQL: %s", arm.name, err, c.sql)
					continue
				}
				got := tncRender(res)
				if got != c.want {
					t.Errorf("%s arm answered %q, want %q\n  SQL: %s", arm.name, got, c.want, c.sql)
				}
			}
		})
	}
}

func tncRender(res *oracle.Result) string {
	var rows []string
	for i := range res.Rows {
		var cells []string
		for _, c := range res.Columns {
			cells = append(cells, fmt.Sprintf("%s=%v", c, res.Rows[i][c]))
		}
		rows = append(rows, strings.Join(cells, " "))
	}
	return strings.Join(rows, " | ")
}
