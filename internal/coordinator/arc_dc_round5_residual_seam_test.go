// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// Arc DC round 5: the Codex round-4 review's two findings (cell names are
// its own, from focused_corpus.tsv and extra_corpus.tsv), against live
// PostgreSQL 17.11 on five arms.

// A RESIDUAL WHOSE TWO SIDES RE-SPELL TO ONE STAGE COLUMN RUNS SINGLE-PROCESS
// (review B1). Pass-through layers over a renaming body (`(SELECT k, total
// FROM (SELECT k, amt AS total FROM dc_in) x) b`, star or explicit, derived or
// CTE) and a rename on the ENCLOSING side (`FROM (SELECT id, total AS amt FROM
// dc_out) o … amt > o.amt`) reached the stage DAG with the residual collapsed —
// logical `total < total`, stage `x.amt < x.amt` — and EXISTS answered no
// rows, NOT EXISTS every row, on the three DAG arms. The guard is now at the
// seam: dagplan refuses a residual whose respelling merges two leaves
// (ErrResidualSidesMergedDistributed) and the coordinator runs the plan on the
// single-process pipeline. withshadow/catalog/* are controls (a body WITH that
// shadows a CATALOG table, which answers). At f20d5bd3 the 14 B1 statements
// fail on the DAG arms.
func TestArcDCAResidualWhoseSidesMergeRunsSingleProcessOnEveryArm(t *testing.T) {
	dcRunRound3(t, []dcRound3Case{
		{name: "wrapped/derived/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k,total FROM (SELECT k,amt AS total FROM dc_in) x) b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "wrapped/derived/NOT EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM (SELECT k,total FROM (SELECT k,amt AS total FROM dc_in) x) b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "wrapped/star/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT * FROM (SELECT k,amt AS total FROM dc_in) x) b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "wrapped/star/NOT EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM (SELECT k,amt AS total FROM dc_in) x) b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "wrapped/cte/EXISTS",
			sql:  "WITH x AS (SELECT k,amt AS total FROM dc_in), d AS (SELECT k,total FROM x) SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "wrapped/cte/NOT EXISTS",
			sql:  "WITH x AS (SELECT k,amt AS total FROM dc_in), d AS (SELECT k,total FROM x) SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "wrapped/cte_star/EXISTS",
			sql:  "WITH x AS (SELECT k,amt AS total FROM dc_in), d AS (SELECT * FROM x) SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "wrapped/cte_star/NOT EXISTS",
			sql:  "WITH x AS (SELECT k,amt AS total FROM dc_in), d AS (SELECT * FROM x) SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "probe_rename/derived/EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id,total AS amt FROM dc_out) o WHERE EXISTS (SELECT 1 FROM (SELECT k,amt FROM dc_in) b WHERE b.k=o.id AND amt>o.amt) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "probe_rename/derived/NOT EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id,total AS amt FROM dc_out) o WHERE NOT EXISTS (SELECT 1 FROM (SELECT k,amt FROM dc_in) b WHERE b.k=o.id AND amt>o.amt) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "probe_rename/cte/EXISTS",
			sql:  "WITH d AS (SELECT k,amt FROM dc_in) SELECT o.id AS a FROM (SELECT id,total AS amt FROM dc_out) o WHERE EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND amt>o.amt) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "probe_rename/cte/NOT EXISTS",
			sql:  "WITH d AS (SELECT k,amt FROM dc_in) SELECT o.id AS a FROM (SELECT id,total AS amt FROM dc_out) o WHERE NOT EXISTS (SELECT 1 FROM d b WHERE b.k=o.id AND amt>o.amt) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "probe_rename/catalog/EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id,total AS amt FROM dc_out) o WHERE EXISTS (SELECT 1 FROM dc_in b WHERE b.k=o.id AND amt>o.amt) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "probe_rename/catalog/NOT EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id,total AS amt FROM dc_out) o WHERE NOT EXISTS (SELECT 1 FROM dc_in b WHERE b.k=o.id AND amt>o.amt) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
		{name: "withshadow/catalog/in",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE o.id IN (WITH dc_in AS (SELECT k,amt AS total FROM public.dc_in) SELECT b.k FROM dc_in b JOIN dc_side c ON c.j=b.k AND total>100 WHERE b.k=o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "withshadow/catalog/exists",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (WITH dc_in AS (SELECT k,amt AS total FROM public.dc_in) SELECT 1 FROM dc_in b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
	})
}

// A CORRELATED SUBQUERY WHOSE OWN WITH SHADOWS AN ENCLOSING WITH ITEM READS
// ITS OWN ITEM (review B2; ADR-0047 stage 3, #1606). PostgreSQL reads the
// subquery's own item. The per-row re-run planned the body with the enclosing
// item's definition (the builder's first-match walk, the physical CTE cache
// keyed by name), so these were refused 0A000 by name from arc DC until the
// CTE identity became scope-aware: the builder binds the innermost item of a
// name and a materialization answers by identity. Each cell asserts
// PostgreSQL 17.11's row set on every arm; they were pinned to the refusal
// until then.
func TestArcDCAShadowingBodyWithReadsItsOwnItemOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := dcArms(t, ctx)
	for _, tc := range []dcRound3Case{
		{name: "withshadow/outer/in",
			sql:  "WITH d AS (SELECT k,amt FROM dc_in) SELECT o.id AS a FROM dc_out o WHERE o.id IN (WITH d AS (SELECT k,amt AS total FROM dc_in) SELECT b.k FROM d b JOIN dc_side c ON c.j=b.k AND total>100 WHERE b.k=o.id) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "withshadow/outer/exists",
			sql:  "WITH d AS (SELECT k,amt FROM dc_in) SELECT o.id AS a FROM dc_out o WHERE EXISTS (WITH d AS (SELECT k,amt AS total FROM dc_in) SELECT 1 FROM d b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "shadow_nested_cte/EXISTS",
			sql:  "WITH d AS (SELECT k,amt AS total FROM dc_nul) SELECT o.id AS a FROM dc_out o WHERE EXISTS (WITH d AS (SELECT k,amt AS total FROM dc_in) SELECT 1 FROM d b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "shadow_nested_cte/NOT EXISTS",
			sql:  "WITH d AS (SELECT k,amt AS total FROM dc_nul) SELECT o.id AS a FROM dc_out o WHERE NOT EXISTS (WITH d AS (SELECT k,amt AS total FROM dc_in) SELECT 1 FROM d b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=3 3 | 9 | NULL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				if got := arm.run(tc.sql); got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, tc.want)
				}
			}
		})
	}
}
