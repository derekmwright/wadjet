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

// A duplicate output NAME must not collapse two columns, on ANY arm.
//
// `wadjet/collide_duplicate_names_test.go` reads these positionally through
// QueryResult.Cells, which is exact — but it is single-process only, and that
// is precisely the gap round 0's B4 lived in: the DAG's positional ORDER BY
// over a join sorted by the wrong column and no gate looked. The DAG hands the
// caller rows keyed by NAME, so this compares each duplicate spelling against
// its DISTINCT-ALIAS reference, which is positional by construction and is the
// same technique `benchmarks/tpch/duplicate_name_dag_test.go` uses.
func TestCollidingDuplicateNamesOnEveryArm(t *testing.T) {
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
	for _, c := range collide.DuplicateNameCorpus() {
		if c.Ref == "" {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			for _, arm := range arms {
				ref, err := arm.run(c.Ref)
				if err != nil {
					t.Fatalf("%s arm refused the reference spelling: %v\n  SQL: %s", arm.name, err, c.Ref)
				}
				dup, err := arm.run(c.SQL)
				if err != nil {
					t.Fatalf("%s arm refused the duplicate spelling: %v\n  SQL: %s", arm.name, err, c.SQL)
				}
				if len(dup.Rows) != len(ref.Rows) {
					t.Fatalf("%s arm: %d rows for the duplicate spelling, %d for the reference\n  SQL: %s",
						arm.name, len(dup.Rows), len(ref.Rows), c.SQL)
				}
				// One column POSITION whose name is unique on both sides
				// carries the ORDER. Comparing it row by row is what sees a
				// sort that bound the wrong column, which is invisible to any
				// unordered check. Read by the arm's OWN published name at
				// that position, because the DAG publishes a set operation's
				// columns under the join's qualified spellings where the
				// single path publishes bare ones.
				j := collideUniquePosition(dup.Columns, ref.Columns)
				if j < 0 {
					t.Fatalf("%s arm: neither spelling has a uniquely-named column to read the "+
						"ORDER from (%v / %v)", arm.name, dup.Columns, ref.Columns)
				}
				for i := range dup.Rows {
					got, want := dup.Rows[i][dup.Columns[j]], ref.Rows[i][ref.Columns[j]]
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("%s arm row %d: the duplicate spelling has %s=%v where the "+
							"reference has %s=%v — same multiset, different ORDER\n  SQL: %s",
							arm.name, i, dup.Columns[j], got, ref.Columns[j], want, c.SQL)
						break
					}
				}
			}
		})
	}
}

// collideUniquePosition is the first column POSITION whose name is unique in
// both lists, or -1.
func collideUniquePosition(dup, ref []string) int {
	if len(dup) != len(ref) {
		return -1
	}
	count := func(list []string, name string) int {
		n := 0
		for _, c := range list {
			if c == name {
				n++
			}
		}
		return n
	}
	for j := range dup {
		if count(dup, dup[j]) == 1 && count(ref, ref[j]) == 1 {
			return j
		}
	}
	return -1
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
// ORDER.
//
// It reads res.Rows, a map keyed by NAME, and is therefore exact only while
// the result's names are unique — which every collide.Corpus() entry's are, by
// construction. The entries whose names collide live in
// collide.DuplicateNameCorpus() and are gated two other ways: positionally in
// wadjet/collide_duplicate_names_test.go, and against a distinct-alias
// reference on all four arms in TestCollidingDuplicateNamesOnEveryArm above.
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
