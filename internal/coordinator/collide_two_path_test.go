package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/collide"
)

// Two relations that share a bare column name must stay two relations.
//
// Every other corpus in this repository prefixes its column names per table —
// TPC-H's `l_`/`o_`/`c_`/`n_`, the type matrix's `c_i32`, multikey's shared
// schema across four tables that MEAN the same thing — so no query in any gate
// ever had two DIFFERENT relations whose bare column names collide. SQLancer's
// schemas always do (`c0, c1, …` in every table), and that is where #843 lived
// for twelve releases: inside a derived table with two or more UNALIASED
// relations, every base scan was re-aliased to the derived table's alias, so
// `t0.c1` bound to whichever relation was planned last. The headline shape is
// not an aggregate and not a fuzzer shape —
//
//	SELECT c FROM (SELECT clt0.c1 AS c FROM clt0, clt2, clt1
//	               WHERE clt0.c1 IS NOT NULL) x
//
// — and it answered NULL for rows the query's own WHERE says are not null.
//
// Every Want in collide.Corpus() was measured on live postgres:17-alpine over
// these exact rows. Four arms: single-process, spilled, stage DAG, and the
// DAG with every build forced through a shuffle.
func TestCollidingBareNamesOnEveryArm(t *testing.T) {
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

	for _, c := range collide.Corpus() {
		t.Run(c.Name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(c.SQL)
				if err != nil {
					if c.KnownBug != "" {
						t.Logf("%s arm: pinned (%s %s): %v", arm.name, c.Issue, c.KnownBug, err)
						continue
					}
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, c.SQL)
				}
				got := collideRender(res, c.Ordered)
				want := c.Want
				if !c.Ordered {
					want = append([]string(nil), want...)
					sort.Strings(want)
				}
				if !collideEqual(got, want) {
					if c.KnownBug != "" {
						t.Logf("%s arm diverges as pinned (%s %s):\n  got  %v\n  want %v",
							arm.name, c.Issue, c.KnownBug, got, want)
						continue
					}
					t.Errorf("%s arm answered\n  %v\nPostgreSQL 17 answers\n  %v\n  SQL: %s",
						arm.name, got, want, c.SQL)
					continue
				}
				if c.KnownBug != "" {
					t.Errorf("%s arm now AGREES with PostgreSQL, so %s is fixed — delete the "+
						"KnownBug on %s in collide.Corpus()\n  SQL: %s",
						arm.name, c.Issue, c.Name, c.SQL)
				}
			}
		})
	}
}

func collideEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// collideRender is one string per row, `col=value` in the result's own column
// ORDER — which is what a corpus over colliding names needs: a map keyed by
// name cannot hold two columns called c0, and the entry that projects two of
// them is the #844 cell.
func collideRender(res *oracle.Result, ordered bool) []string {
	out := make([]string, 0, len(res.Rows))
	for i := range res.Rows {
		var cells []string
		for j, col := range res.Columns {
			var v any
			if j < len(res.Columns) {
				v = res.Rows[i][col]
			}
			cells = append(cells, fmt.Sprintf("%s=%v", col, v))
		}
		out = append(out, strings.Join(cells, " "))
	}
	if !ordered {
		sort.Strings(out)
	}
	return out
}
