// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// A RESIDUAL CROSSES THE STAGE BOUNDARY WITH ITS IDENTITY (arc JR round 2, B1).
//
// Every cell of the arc's 156-cell table joins two BASE TABLES, so none of them
// reaches a stage that publishes a column under another name. Round 1's
// reviewer varied the RELATION instead of the residual's spelling and found the
// hole: on the three DAG arms, a LEFT JOIN of two RENAMING derived arms
// (`SELECT id AS a, s AS ss FROM jr_l` against `… AS ss2 FROM jr_r`, ON
// `x.kk = y.kk2 AND LOWER(y.ss2) = x.ss`) answered six all-padded rows where
// PostgreSQL answers seven. A Project emits no stage, so the fragment's sides
// publish the base names while the residual text still spells the aliases;
// neither reference resolved, the unbound slot is SQL NULL, the residual was
// UNKNOWN for every candidate pair, and the LEFT join padded its whole probe
// side with only a slog.Warn. Sixteen (cell, arm) results went from a LOUD
// REFUSAL at `563aa517` to a SILENT WRONG ROW SET.

// The join's equi-KEYS already made this trip re-spelled (`resolveShuffleKey`)
// and the residual's leaves take the same path now
// (`dagplan.residualWithStageSpellings`). The SIDE travels with the name,
// because both arms here re-spell to `s`. The FLOOR under it is `worker`'s own
// refusal: a residual reference the fragment's two DECLARED schemas do not
// publish is refused there, naming the references, so a leaf the rewrite cannot
// reach is never a NULL slot at run time.
//
// Every answer below is PostgreSQL 17.11's, measured live over jrProbeData /
// jrBuildData in a --locale=C container with COLLATE "C" text columns
// (jr_author/b1/pg.tsv); at `0ecb2348` this gate FAILS
// (jr_author/b1/g_b1_at_base.log).
func TestJRBAResidualKeepsItsIdentityAcrossTheStage(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over renamed relations")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := jrArms(t, ctx)

	cells := jrRenamedCells()
	names := make([]string, 0, len(cells))
	for n := range cells {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		c := cells[name]
		t.Run(name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(c.sql)
				if err != nil {
					got = "ERR " + err.Error()
				} else {
					got = jrStripCols(got)
				}
				want := c.want
				if p, ok := c.pin[arm.name]; ok {
					want = p
				}
				ok := got == want
				if !ok && strings.HasPrefix(want, "ERR ~") {
					// A SUBSTRING pin: a DAG task id is in the message and
					// changes every run.
					ok = strings.Contains(got, strings.TrimPrefix(want, "ERR ~"))
				} else if !ok && strings.HasPrefix(want, "ERR ") {
					ok = strings.Contains(got, strings.TrimPrefix(want, "ERR "))
				}
				if !ok {
					why := ""
					if c.why != "" {
						why = "\n  pinned: " + c.why
					}
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s%s", c.sql, arm.name, got, want, why)
				}
			}
		})
	}
}

// jrStripCols drops f1Render*'s declaration prefix so a cell's expected string
// is PostgreSQL's own rows, byte for byte. The DECLARATION of a padded row is
// pgwire.TestJRThePaddedRowDeclaresItsBuildSideType's subject, not this one's.
func jrStripCols(s string) string {
	if i := strings.Index(s, "] "); i >= 0 && strings.HasPrefix(s, "cols=[") {
		return s[i+2:]
	}
	return s
}

type jrRenCell struct {
	sql  string
	want string
	pin  map[string]string
	why  string
}

// jrRenamedCells is the reviewer's own corpus plus the two one-sided spellings
// and the no-residual control. The relations are always RENAMED — a derived
// table or a CTE — which is the dimension the 156-cell table holds fixed.
func jrRenamedCells() map[string]jrRenCell {
	const (
		px = "(SELECT id AS a, k AS kk, s AS ss, n AS nn FROM jr_l) x"
		by = "(SELECT id AS b, k AS kk2, s AS ss2, n AS nn2 FROM jr_r) y"
	)
	sel := func(kind, on string) string {
		return "SELECT x.a AS a, y.b AS b FROM " + px + " " + kind + " JOIN " + by +
			" ON x.kk = y.kk2 AND " + on + " ORDER BY 1, 2"
	}
	return map[string]jrRenCell{
		// ---- the shapes this arc UNLOCKED: a loud refusal at 563aa517 on all
		// five arms, and a silent wrong row set on the three DAG arms until
		// the residual kept its identity.
		"ren/left/fn": {sql: sel("LEFT", "LOWER(y.ss2) = x.ss"),
			want: "rows=7 | 1,101 | 1,102 | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL"},
		"ren/left/cast": {sql: sel("LEFT", "CAST(x.nn AS VARCHAR) = CAST(y.nn2 AS VARCHAR)"),
			want: "rows=6 | 1,101 | 2,NULL | 3,103 | 4,NULL | 5,NULL | 6,NULL"},
		"ren/left/between": {sql: sel("LEFT", "x.nn BETWEEN y.nn2 - 5 AND y.nn2 + 5"),
			want: "rows=7 | 1,101 | 1,102 | 2,102 | 3,103 | 4,NULL | 5,NULL | 6,104"},
		"ren/left/distinct": {sql: sel("LEFT", "x.ss IS DISTINCT FROM y.ss2"),
			want: "rows=7 | 1,102 | 2,101 | 2,102 | 3,103 | 4,NULL | 5,NULL | 6,104"},
		"ren/right/fn": {sql: sel("RIGHT", "LOWER(y.ss2) = x.ss"),
			want: "rows=6 | 1,101 | 1,102 | NULL,103 | NULL,104 | NULL,105 | NULL,106"},
		"ren/full/fn": {sql: sel("FULL", "LOWER(y.ss2) = x.ss"),
			want: "rows=11 | 1,101 | 1,102 | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL | " +
				"NULL,103 | NULL,104 | NULL,105 | NULL,106"},
		// An INNER join does not take the residual path at all: its ON
		// non-equality is LIFTED into a filter above the join, and that filter
		// loses its identity at the stage boundary the same way the residual
		// did — a different operator, and one dagplan's carrier check does not
		// model ("Joins … not modelled", carrier_schema.go).
		"ren/inner/fn": {sql: sel("INNER", "LOWER(y.ss2) = x.ss"),
			want: "rows=2 | 1,101 | 1,102",
			pin: map[string]string{
				"dag":          jrInnerSortKeyErr,
				"dag-morsel4":  jrInnerSortKeyErr,
				"dag-shuffled": "rows=5 | 1,101 | 1,102 | 2,101 | 2,102 | 3,103",
			}, why: jrLiftedFilterWhy},
		"ren/inner/between": {sql: sel("INNER", "x.nn BETWEEN y.nn2 - 5 AND y.nn2 + 5"),
			want: "rows=5 | 1,101 | 1,102 | 2,102 | 3,103 | 6,104",
			pin: map[string]string{
				"dag":          jrInnerSortKeyErr,
				"dag-morsel4":  jrInnerSortKeyErr,
				"dag-shuffled": "rows=6 | 1,101 | 1,102 | 2,101 | 2,102 | 3,103 | 6,104",
			}, why: jrLiftedFilterWhy},
		// ---- the same join spelled with two CTEs instead of two derived
		// tables: a different route to the same stage boundary.
		"cte/left/fn": {sql: "WITH cx AS (SELECT id AS a, k AS kk, s AS ss FROM jr_l), " +
			"cy AS (SELECT id AS b, k AS kk2, s AS ss2 FROM jr_r) " +
			"SELECT x.a AS a, y.b AS b FROM cx x LEFT JOIN cy y " +
			"ON x.kk = y.kk2 AND LOWER(y.ss2) = x.ss ORDER BY 1, 2",
			want: "rows=7 | 1,101 | 1,102 | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL"},
		// ---- ONE side renamed. The side that is NOT renamed spells its
		// columns the way the stage publishes them already, so these two
		// isolate which arm's re-spelling the rewrite is doing.
		"ren/left/proberenonly": {sql: "SELECT x.a AS a, r.id AS b FROM " +
			"(SELECT id AS a, k AS kk, s AS ss FROM jr_l) x LEFT JOIN jr_r r " +
			"ON x.kk = r.k AND LOWER(r.s) = x.ss ORDER BY 1, 2",
			want: "rows=7 | 1,101 | 1,102 | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL"},
		"ren/left/buildrenonly": {sql: "SELECT l.id AS a, y.b AS b FROM jr_l l LEFT JOIN " +
			"(SELECT id AS b, k AS kk2, s AS ss2 FROM jr_r) y " +
			"ON l.k = y.kk2 AND LOWER(y.ss2) = l.s ORDER BY 1, 2",
			want: "rows=7 | 1,101 | 1,102 | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
			pin:  jrRenamedOutputPin("rows=7 | 1,1 | 1,1 | 2,2 | 3,3 | 4,4 | 5,5 | 6,6"),
			why:  jrRenamedOutputWhy},
		// ---- N1: residuals the OLD interpreter could already evaluate, so
		// these were WRONG on the three DAG arms at 563aa517 too. The same
		// re-spelling closes them; they are cells and not pins for that reason.
		"ren/left/colres": {sql: sel("LEFT", "y.ss2 > x.ss"),
			want: "rows=6 | 1,NULL | 2,NULL | 3,103 | 4,NULL | 5,NULL | 6,NULL"},
		"ren/left/arithres": {sql: sel("LEFT", "x.nn < y.nn2"),
			want: "rows=6 | 1,102 | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,104"},
		"ren/full/colres": {sql: sel("FULL", "y.ss2 > x.ss"),
			want: "rows=11 | 1,NULL | 2,NULL | 3,103 | 4,NULL | 5,NULL | 6,NULL | " +
				"NULL,101 | NULL,102 | NULL,104 | NULL,105 | NULL,106"},
		"ren/left/derivedbuild": {sql: "SELECT l.id AS a, y.b AS b FROM jr_l l LEFT JOIN " +
			"(SELECT id AS b, k AS kk, s AS ls FROM jr_r) y " +
			"ON l.k = y.kk AND y.ls = l.s ORDER BY 1, 2",
			want: "rows=6 | 1,101 | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
			pin:  jrRenamedOutputPin("rows=6 | 1,1 | 2,2 | 3,3 | 4,4 | 5,5 | 6,6"),
			why:  jrRenamedOutputWhy},
		// ---- the CONTROLS. A residual naming only the BUILD side has a
		// better home — pushdownPredicates moves it into that side's scan,
		// below the rename — and the no-residual join was right on all five
		// arms at the base, so both say the rewrite changed nothing it should
		// not have.
		"ren/left/like": {sql: sel("LEFT", "y.ss2 LIKE 'a%'"),
			want: "rows=6 | 1,101 | 2,101 | 3,NULL | 4,NULL | 5,NULL | 6,NULL"},
		"ren/left/inlist": {sql: sel("LEFT", "y.ss2 IN ('alpha', 'zeta')"),
			want: "rows=6 | 1,101 | 2,101 | 3,NULL | 4,NULL | 5,NULL | 6,104"},
		"ren/nores": {sql: "SELECT x.a AS a, y.b AS b FROM " +
			"(SELECT id AS a, k AS kk, s AS ss FROM jr_l) x LEFT JOIN " +
			"(SELECT id AS b, k AS kk2, s AS ss2 FROM jr_r) y " +
			"ON x.kk = y.kk2 ORDER BY 1, 2",
			want: "rows=8 | 1,101 | 1,102 | 2,101 | 2,102 | 3,103 | 4,NULL | 5,NULL | 6,104"},
	}
}

// jrRenamedOutputPin is the three DAG arms' answer where the RESIDUAL is right
// and the OUTPUT COLUMN is not.
//
// Both pinned cells rename only the BUILD arm, and on the DAG the enclosing
// `y.b` reads the PROBE's `id` — every row of the pin is `a,a`. The residual's
// own disposition is correct there, and the row COUNT and the match pattern
// say so: `ren/left/buildrenonly` answers seven rows with probe 1 matched
// TWICE, which is PostgreSQL's shape, and `ren/left/derivedbuild` answers six
// with probe 1 matched once. What is wrong is which column the join PUBLISHES
// for the renamed build arm, one operator above this seam.
//
// PRE-EXISTING and `distributed`: `ren/left/derivedbuild`'s residual is a bare
// column comparison the OLD interpreter could already evaluate, and it answers
// `1,1 | 2,2 | …` at 563aa517 too (jr_author/b1/base.tsv). Not chased here
// (engine-first, Derek 2026-09-16); recorded as a filing candidate.
//
// A pin that starts agreeing FAILS.
func jrRenamedOutputPin(got string) map[string]string {
	return map[string]string{"dag": got, "dag-shuffled": got, "dag-morsel4": got}
}

const jrRenamedOutputWhy = "the residual is right — the row count and the match pattern are " +
	"PostgreSQL's — and the join publishes the PROBE's column for the renamed build arm's " +
	"`y.b`; pre-existing at 563aa517 on the cell whose residual the old interpreter could " +
	"already evaluate"

// jrInnerSortKeyErr is the loud failure the two non-shuffled DAG arms answer
// for an INNER join over two renamed relations. Loud, so a refusal and not a
// wrong value; pinned as a SUBSTRING because the task id changes every run.
const jrInnerSortKeyErr = `ERR ~sort consume: sink consume: sort: key column "a" does not exist in the input schema`

const jrLiftedFilterWhy = "an INNER join LIFTS its ON non-equality into a filter ABOVE the " +
	"join, so it never reaches the residual path; that filter loses its identity at the stage " +
	"boundary the same way, and dagplan's carrier check does not model a join stage's output. " +
	"ren/inner/fn is WRONG at 563aa517 on dag-shuffled too"
