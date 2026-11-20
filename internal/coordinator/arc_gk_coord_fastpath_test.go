// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// gkFastPathApart lists the group-key cells on which the coordinator with
// its fast path on answers differently from the single-process engine, by the
// three classes ADR-0050 §1.4 names (Acceptance 1). Every entry but a
// nondeterministic one is checked to still differ, so a cell that starts
// agreeing FAILS here and is deleted; a nondeterministic cell is compared by
// the table's skip rule (not at all), never by "must differ".
const (
	gkApartUnionTypes   = "union-types: the fast path's physical planning of the bound plan refuses 42804 (the single arm's error), the statement falls through to the stage DAG and D's validation refuses 42803; PostgreSQL answers rows"
	gkApartSizeDeclined = "routing gap (#1678): the size estimate declines the bound plan (a residual subquery is unestimable), so the stage DAG plans D, which refuses 42803 where only L answers PostgreSQL's rows"
	gkApartNondet       = "nondeterministic: the 0A000 message quotes a per-row outer value that differs from run to run on the single arm too (ADR-0013)"
)

var gkFastPathApart = map[string]string{
	"pair/date/keyQual/union":      gkApartUnionTypes,
	"pair/timestamp/keyQual/union": gkApartUnionTypes,
	"pair/array/itemQual/union":    gkApartUnionTypes,
	"pair/array/keyQual/union":     gkApartUnionTypes,
	"p/inSubqJoin/ord":             gkApartSizeDeclined,
	"p/scalarDirectJoin/sel":       gkApartSizeDeclined,
	"c738/outerOrderBy":            gkApartNondet,
}

// TestArcGKCoordFastPathEqualsSingle is ADR-0050 Acceptance 1 and 2: a
// coord-fastpath arm — the coordinator with its fast path on, so the local
// paths run the BOUND plan — answers every cell of the group-key table
// exactly as the single-process engine does (answer, declared type classes
// and full error text), except the cells gkFastPathApart names with their
// cause. PostgreSQL agreement is counted and logged separately: agreeing with
// the single arm is not agreeing with PostgreSQL.
//
// GKFP_DUMP=<path> writes name<TAB>arm<TAB>answer for both arms.
func TestArcGKCoordFastPathEqualsSingle(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: two arms over the group-key spelling table")
	}
	answers := gkPGAnswers(t)
	for name, a := range gkReadTSV(t, "testdata/arc_gk_group_key_spelling_more_pg17.tsv", 2) {
		answers[name] = a
	}
	cells := append(gkCells(), gkMoreCells(t)...)
	skipped := map[string]bool{}
	for name, ks := range gkKeptLines(t) {
		for _, k := range ks {
			if k.arms == "skip" {
				skipped[name] = true
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)

	single := ssStandalone(t, ctx, 0)
	infra := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infra, nil, ssTables())
	fast := tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = DefaultLocalFastPathBytes })
	armDumpName(fast, "coord-fastpath")

	var dump *os.File
	if p := os.Getenv("GKFP_DUMP"); p != "" {
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		dump = f
	}
	render := func(out string, err error) string {
		if err != nil {
			return "ERR " + sqlerr.StateOf(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
		}
		return out
	}
	h0 := fast.LocalFastPathHits()
	equal, pgSingle, pgFast := 0, 0, 0
	var apart []string
	for _, tc := range cells {
		s := render(ssRunSingle(ctx, single, tc.sql, tc.ordered))
		f := render(ssRunDAG(ctx, fast, tc.sql, tc.ordered))
		if dump != nil {
			fmt.Fprintf(dump, "%s\tsingle\t%s\n%s\tcoord-fastpath\t%s\n", tc.name, s, tc.name, f)
		}
		pg := answers[tc.name]
		if ssMatches(s, pg) {
			pgSingle++
		}
		if ssMatches(f, pg) {
			pgFast++
		}
		class, listed := gkFastPathApart[tc.name]
		switch {
		case skipped[tc.name]:
			// The table's skip rule: a legal nondeterminism (ADR-0013) is not
			// compared.
			if s == f {
				equal++
			}
		case listed && class == gkApartNondet:
			// The same refusal on both arms, its message carrying a per-row
			// value: compared by SQLSTATE.
			if gkSQLState(s) != gkSQLState(f) {
				t.Errorf("%s: SQLSTATE %s on single, %s on coord-fastpath", tc.name, gkSQLState(s), gkSQLState(f))
			}
			if s == f {
				equal++
			}
		case s == f && listed:
			t.Errorf("%s: listed as apart, but the arms now agree — delete the entry", tc.name)
		case s == f:
			equal++
		case !listed:
			apart = append(apart, tc.name)
			t.Errorf("%s\n  single         %s\n  coord-fastpath %s", tc.sql, s, f)
		}
	}
	sort.Strings(apart)
	t.Logf("group-key table: %d cells; coord-fastpath equals single on %d; PostgreSQL's text: single %d, coord-fastpath %d; fast-path hits %d",
		len(cells), equal, pgSingle, pgFast, fast.LocalFastPathHits()-h0)
	if len(apart) > 0 {
		t.Logf("unlisted cells apart: %s", strings.Join(apart, ", "))
	}
}
