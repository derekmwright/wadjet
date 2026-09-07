package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TWO CONSUMERS OF ONE CTE BODY EACH KEEP THEIR OWN COLUMNS — #876, four arms.
//
//	WITH c AS (SELECT id, c_i64 AS v FROM typemx)
//	SELECT COUNT(*) FROM typemx
//	WHERE c_i64 < (SELECT MAX(v) FROM c) AND c_i64 > (SELECT MIN(v) FROM c)
//
// failed on BOTH DAG arms, after three task attempts:
//
//	hash aggregate: aggregate input "c_i64" is not a column of its input
//	(input has: max(v))
//
// The scan-aggregate FUSION is the one optimization that changes what an
// ALREADY-EMITTED stage emits: it stamps FusedAggSpecs onto the scan and
// prunes its output columns. `MAX(v)`'s producer fused into the CTE body's
// scan; `MIN(v)`'s producer then took walkStages' CTE dedup, which points a
// later reference at that same stage — and asked a relation that had become
// `max(v)` for `c_i64`.
//
// The reference COUNT could not see it: `cteRefCounts` is computed from the
// statement's own logical plan, and a CTE named only inside a scalar
// subquery's TEXT appears there ZERO times.
//
// Every Want is live PostgreSQL 17 over the same rows.
func TestArcH1TwoConsumersOfOneCTEKeepTheirOwnColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	const cte = `WITH c AS (SELECT id, c_i64 AS v FROM typemx) `
	for _, tc := range []struct{ name, sql, want string }{
		// The filing's own shape, and then the same shape with three and four
		// producers — a bound that has to be attempted from both sides, since
		// the defect is "the SECOND consumer", not "the second one only".
		{"two-producers", cte + `SELECT COUNT(*) AS n FROM typemx ` +
			`WHERE c_i64 < (SELECT MAX(v) FROM c) AND c_i64 > (SELECT MIN(v) FROM c)`,
			`n | 4837`},
		{"three-producers", cte + `SELECT COUNT(*) AS n FROM typemx ` +
			`WHERE c_i64 < (SELECT MAX(v) FROM c) AND c_i64 > (SELECT MIN(v) FROM c) ` +
			`AND c_i64 <> (SELECT SUM(v) FROM c)`,
			`n | 4837`},
		{"four-producers", cte + `SELECT COUNT(*) AS n FROM typemx ` +
			`WHERE c_i64 < (SELECT MAX(v) FROM c) AND c_i64 > (SELECT MIN(v) FROM c) ` +
			`AND c_i64 <> (SELECT SUM(v) FROM c) AND c_i64 <> (SELECT COUNT(v) FROM c)`,
			`n | 4837`},
		{"two-producers-over-a-filtered-body",
			`WITH c AS (SELECT id, c_i64 AS v FROM typemx WHERE id < 100) ` +
				`SELECT COUNT(*) AS n FROM typemx ` +
				`WHERE c_i64 < (SELECT MAX(v) FROM c) AND c_i64 > (SELECT MIN(v) FROM c)`,
			`n | 95`},

		// NO SUBQUERY AT ALL. The mutation is in the fusion, not in the
		// producer lowering, so two ORDINARY consumers of one CTE body — each
		// with its own aggregate — reach it too. These are the shapes that
		// say the fix is not about scalar subqueries.
		{"two-select-list-scalars-over-one-cte", cte +
			`SELECT (SELECT MAX(v) FROM c) AS mx, (SELECT MIN(v) FROM c) AS mn FROM decpair WHERE id < 2`,
			`mx,mn | 4999014997,0`},
		{"join-of-two-aggregates-over-one-cte", cte +
			`SELECT a.m AS mx, b.n AS mn FROM (SELECT MAX(v) AS m FROM c) a, ` +
			`(SELECT MIN(v) AS n FROM c) b`,
			`mx,mn | 4999014997,0`},

		// THE CONTROLS. One producer keeps the fusion; two producers over TWO
		// CTE bodies never shared a stage; two producers over no CTE at all
		// were always right. A fix that declined the fusion everywhere would
		// pass a rows-only check and cost every aggregate its scan fusion,
		// which is why the TPC-H stage-dump golden is unchanged by this
		// commit.
		{"ctl-one-producer", cte + `SELECT COUNT(*) AS n FROM typemx ` +
			`WHERE c_i64 < (SELECT MAX(v) FROM c)`, `n | 4838`},
		{"ctl-two-producers-two-ctes",
			`WITH c AS (SELECT id, c_i64 AS v FROM typemx), d AS (SELECT id, c_i64 AS w FROM typemx) ` +
				`SELECT COUNT(*) AS n FROM typemx ` +
				`WHERE c_i64 < (SELECT MAX(v) FROM c) AND c_i64 > (SELECT MIN(w) FROM d)`,
			`n | 4837`},
		{"ctl-two-producers-no-cte",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 < (SELECT MAX(c_i64) FROM typemx) ` +
				`AND c_i64 > (SELECT MIN(c_i64) FROM typemx)`,
			`n | 4837`},
		{"ctl-one-consumer-of-a-cte-keeps-its-answer", cte +
			`SELECT MAX(v) AS m FROM c`, `m | 4999014997`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, tc.want, tc.sql)
				}
				if got := e3SortedRender(cols, rows); got != e3SortLines(tc.want) {
					t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// TWO PRODUCERS THAT EACH FILTER ONE SHARED CTE REFERENCE ARE REFUSED, LOUDLY.
//
// It is the residual of #876 and it is stated here rather than left to a
// reader's surprise. Each reference carries its OWN `WHERE`, so the predicate
// belongs to ONE consumer and must not land on the stage the other one reads —
// the rule #656 settled. `filterCarrierIndex` attaches it anyway while the
// stage is still single-consumer (the reference COUNT cannot see a CTE named
// only inside a scalar subquery's TEXT), and
// `assertNoConsumerScopedFilterOnSharedStage` then refuses the plan once the
// second producer is pointed at that stage.
//
// PostgreSQL 17 answers 3869. The refusal is not that answer, and it is not
// pinned as if it were: it is LOUD, it names the stage and the rule, and the
// single-process arms answer PostgreSQL's number — so a client gets a number
// or an error, never a wrong number.
//
// What it would take to answer is in this arc's REPORT: the consumer needs its
// own StageProject, which was tried in this commit and WITHDRAWN because the
// project that then carries an outer WHERE over a shared CTE answered ZERO for
// PostgreSQL's 4838 on Q15's own shape.
//
// The reference is QUALIFIED (`c.id`) deliberately: the BARE spelling binds to
// the ENCLOSING query and drops the predicate, which is a separate defect this
// arc found, measured and recorded rather than widened into.
func TestArcH1TwoFilteredProducersOverOneCTEAreRefusedLoudly(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	const sql = `WITH c AS (SELECT id, c_i64 AS v FROM typemx) SELECT COUNT(*) AS n FROM typemx ` +
		`WHERE c_i64 < (SELECT MAX(v) FROM c WHERE c.id < 4000) ` +
		`AND c_i64 > (SELECT MIN(v) FROM c WHERE c.id < 4000)`
	for _, arm := range arms {
		cols, rows, err := arm.run(sql)
		if arm.coord == nil {
			// The single-process arms answer PostgreSQL's number.
			if err != nil {
				t.Errorf("%s arm refused a shape it can answer: %v", arm.name, err)
				continue
			}
			if got := e3Render(cols, rows); got != `n | 3869` {
				t.Errorf("%s arm: %s, want n | 3869 (live PostgreSQL 17)", arm.name, got)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s arm ANSWERED %s — each producer's WHERE belongs to ONE consumer and "+
				"the CTE body's stage is shared; answering means one of them read the other's "+
				"filtered stream", arm.name, e3Render(cols, rows))
			continue
		}
		if !strings.Contains(err.Error(), "scoped to ONE of them") {
			t.Errorf("%s arm refused with %q, want the shared-producer refusal (#656)",
				arm.name, err.Error())
		}
	}
}
