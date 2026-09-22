// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import "testing"

// Arc DC round 4: the Codex closure review's two findings, as row sets against
// live PostgreSQL 17.11 on five arms (CX/ and CXF/ cells carry the review's own
// names from binding_corpus.tsv and followup_corpus.tsv).

// A CORRELATED INEQUALITY WHOSE TWO SIDES SHARE A BARE NAME, OVER A BODY THAT
// RENAMES (review B2). `EXISTS (SELECT 1 FROM (SELECT k, amt AS total FROM
// dc_in) b WHERE b.k = o.id AND total > o.total)` rendered its join residual
// as `total < total`, and the stage DAG's re-spelling moved BOTH leaves to the
// build's source column (`b.amt < b.amt`): EXISTS answered no rows and NOT
// EXISTS every row on the three DAG arms, where PostgreSQL and the single arm
// answer `1 | 2`. Such a condition now declines to the per-row rerun, which
// answers on every arm — and the rerun now scopes the body's OWN WITH items
// over its FROM, so the CTE spelling (and the review's N4, `P-CTEi/*`) reads
// `total` as the body's instead of substituting the enclosing value. The
// B2V/V-base* and V-derivedPass cells are the same names over relations that
// do not rename, which keep lowering. At 18e9af62: 24 + 12 five-arm cells of
// the review's corpora, plus V-cteRename, and the P-CTEi cells, fail.
func TestArcDCASameNameResidualAcrossARenamingBodyAnswersOnEveryArm(t *testing.T) {
	dcRunRound3(t, []dcRound3Case{
		{name: "CX/derived/mixed/exists",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k, amt AS total FROM dc_in) b WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "CX/derived/mixed/notexists",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM (SELECT k, amt AS total FROM dc_in) b WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "CX/derived_alias/mixed/exists",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k, amt FROM dc_in) b(k,total) WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "CX/derived_alias/mixed/notexists",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM (SELECT k, amt FROM dc_in) b(k,total) WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "CX/cte/mixed/exists",
			sql:  "WITH d AS (SELECT k, amt AS total FROM dc_in) SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM d b WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "CX/cte/mixed/notexists",
			sql:  "WITH d AS (SELECT k, amt AS total FROM dc_in) SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM d b WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "CX/catalog_alias/mixed/exists",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b(k,tag,total) WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "CX/catalog_alias/mixed/notexists",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM dc_in b(k,tag,total) WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "CXF/derived/total > o.total",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k,amt AS total FROM dc_in) b WHERE b.k=o.id AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "CXF/derived/b.total > o.total",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k,amt AS total FROM dc_in) b WHERE b.k=o.id AND b.total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "CXF/derived/total > o.total AND b.k=1",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k,amt AS total FROM dc_in) b WHERE b.k=o.id AND total > o.total AND b.k=1) ORDER BY a",
			want: "rows=1 1"},
		{name: "CXF/cte/total > o.total",
			sql:  "WITH d AS (SELECT k,amt AS total FROM dc_in) SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "CXF/cte/b.total > o.total",
			sql:  "WITH d AS (SELECT k,amt AS total FROM dc_in) SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND b.total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "CXF/cte/total > o.total AND b.k=1",
			sql:  "WITH d AS (SELECT k,amt AS total FROM dc_in) SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND total > o.total AND b.k=1) ORDER BY a",
			want: "rows=1 1"},
		{name: "B2V/V-derivedRename/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k, amt AS total FROM dc_in) b WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/V-derivedRenameQual/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k, amt AS total FROM dc_in) b WHERE b.k = o.id AND b.total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/V-derivedPass/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT id, total FROM dc_out) b WHERE b.id = o.id AND b.total > o.total - 150) ORDER BY a",
			want: "rows=4 1 | 2 | 3 | 9"},
		{name: "B2V/V-basePlainQual/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_out b WHERE b.grp = o.grp AND b.total > o.total) ORDER BY a",
			want: "rows=2 1 | 3"},
		{name: "B2V/V-basePlainBare/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_out b WHERE b.grp = o.grp AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 3"},
		{name: "B2V/V-baseDiffName/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM dc_in b WHERE b.k = o.id AND amt > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/V-cteRename/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (WITH d AS (SELECT k, amt AS total FROM dc_in) SELECT 1 FROM d b WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/V-derivedRename/NOTEXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM (SELECT k, amt AS total FROM dc_in) b WHERE b.k = o.id AND total > o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "B2V/V-derivedRenameOther/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k, amt AS grp FROM dc_in) b WHERE b.k = o.id AND grp > o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/V-derivedRenameAgainstOther/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k, amt AS total FROM dc_in) b WHERE b.k = o.id AND total > o.grp) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/C-CTEi-q/ON/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (WITH d AS (SELECT k, tag, amt AS total FROM dc_in) SELECT b.k FROM d b JOIN dc_side c ON c.j = b.k AND b.total > 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/C-CTEi-x/ON/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (WITH d AS (SELECT k, tag, amt AS x FROM dc_in) SELECT b.k FROM d b JOIN dc_side c ON c.j = b.k AND b.x > 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/P-CTEi/W/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (WITH d AS (SELECT k, tag, amt AS total FROM dc_in) SELECT 1 FROM d b WHERE b.k = o.id AND total > 100) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/P-CTEi/ON/IN",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (WITH d AS (SELECT k, tag, amt AS total FROM dc_in) SELECT b.k FROM d b JOIN dc_side c ON c.j = b.k AND total > 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "B2V/P-CTEi/HAV/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (WITH d AS (SELECT k, tag, amt AS total FROM dc_in) SELECT 1 FROM d b WHERE b.k = o.id GROUP BY b.k HAVING MAX(total) > 120) ORDER BY a",
			want: "rows=2 1 | 2"},
	})
}

// A COMPUTED OUTPUT COLUMN OF AN ENCLOSING DERIVED TABLE IS AN OUTER NAME
// (review B1 / N5). `(SELECT id, grp, total * 1 AS t2 FROM dc_out) o` publishes
// `t2`, but the enclosing column maps — the logical decorrelations' and the
// physical re-run's — were built from SCANS and CTE outputs only, so a bare
// `t2` inside a correlated body was not an outer reference: `HAVING SUM(b.amt)
// > t2` was dropped (every row), the WHERE spelling refused, the ON spelling
// answered wrong on the DAG. Both maps now carry a derived table's output
// names. At 18e9af62 the three bare cells fail on five arms.
func TestArcDCAComputedEnclosingDerivedColumnIsAnOuterNameOnEveryArm(t *testing.T) {
	dcRunRound3(t, []dcRound3Case{
		{name: "N5/E-DTx/ON-q/IN",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE o.id IN (SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.t2 > 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=1 2"},
		{name: "N5/E-DTx/ON-q/EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.t2 > 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=1 2"},
		{name: "N5/E-DTx/ON-q/SCAL",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE (SELECT MAX(b.amt) FROM dc_in b JOIN dc_side c ON c.j = b.k AND o.t2 > 100 WHERE b.k = o.id) > 0 ORDER BY a",
			want: "rows=1 2"},
		{name: "N5/E-DTx/ON-corr/IN",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE o.id IN (SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "N5/E-DTx/ON-corrNE/EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE EXISTS (SELECT 1 FROM dc_in b JOIN dc_side c ON c.j = b.k AND b.amt > o.t2 WHERE b.k = o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "N5/E-DTx/W-q/IN",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE o.id IN (SELECT b.k FROM dc_in b WHERE o.t2 > 100) ORDER BY a",
			want: "rows=1 2"},
		{name: "N5/E-DTx/W-q/EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE EXISTS (SELECT 1 FROM dc_in b WHERE b.k = o.id AND o.t2 > 100) ORDER BY a",
			want: "rows=1 2"},
		{name: "N5/E-DTx/W-bare/IN",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE o.id IN (SELECT b.k FROM dc_in b WHERE t2 > 100) ORDER BY a",
			want: "rows=1 2"},
		{name: "N5/E-DTx/ON-bare/IN",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE o.id IN (SELECT b.k FROM dc_in b JOIN dc_side c ON c.j = b.k AND t2 > 100 WHERE b.k = o.id) ORDER BY a",
			want: "rows=1 2"},
		{name: "N5/E-DTx/HAV-bare/EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id, grp, total * 1 AS t2 FROM dc_out) o WHERE EXISTS (SELECT 1 FROM dc_in b WHERE b.tag = o.grp GROUP BY b.k HAVING SUM(b.amt) > t2) ORDER BY a",
			want: "rows=3 1 | 3 | 9"},
	})
}
