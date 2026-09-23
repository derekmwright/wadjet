// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
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

// A CORRELATED SUBQUERY WHOSE OWN WITH SHADOWS AN ENCLOSING WITH ITEM IS
// REFUSED BY NAME (review B2). PostgreSQL reads the subquery's own item; the
// per-row re-run planned the body with the enclosing item's definition (the
// builder's first-match walk over scopeCTEs, the physical CTE cache keyed by
// name — docs/internals/nested-with-scope-precedence.md), so the round-4 decline
// reached a missing-column refusal, and the same-schema spelling answered ZERO
// rows / every row at base and at f20d5bd3. It is now 0A000 with the construct
// named on every arm; PostgreSQL's row set is recorded beside each pin. A pin
// that starts agreeing FAILS: assert the row set and delete the pin.
func TestArcDCAShadowingBodyWithIsRefusedByNameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := dcArms(t, ctx)
	const refusal = "a WITH item inside a correlated subquery that shadows an outer WITH item is not supported"
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
				got := arm.run(tc.sql)
				if got == tc.want {
					t.Errorf("%s\n  arm  %s now AGREES with PostgreSQL (%s): delete this pin", tc.sql, arm.name, tc.want)
					continue
				}
				if !strings.Contains(got, refusal) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want the refusal %q (PostgreSQL 17.11: %s)",
						tc.sql, arm.name, got, refusal, tc.want)
				}
			}
		})
	}
}
