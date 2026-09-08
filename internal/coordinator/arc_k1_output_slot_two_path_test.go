package coordinator

import (
	"context"
	"testing"
	"time"
)

// AN OUTPUT SLOT HAS ONE IDENTITY — #968, on FOUR arms against live
// PostgreSQL 17 over rows identical to `decpair`.
//
// `SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY a
// LIMIT 3` puts TWO columns called `a` into the aggregate's ONE output schema:
// the group key, whose relation qualifier `exec.PublishedGroupKeyNames`
// strips, and the aggregate, whose output name is the user's alias. Every
// consumer that resolves a name through `batch.RecordBatch.ColumnIndex` gets
// the FIRST of them, and the planner never saw the collision because it models
// that key as `x.a` — the spelling the query wrote — while the operator
// publishes `a`.
//
// Two arms, two different wrong answers, both silent:
//
//   - single / spilled published the GROUP KEY's value under the aggregate's
//     alias, with the KEY's declared type (DECIMAL(9,2) for a SUM that is
//     DECIMAL(38,4)): `12.75` where PostgreSQL answers `38.2500`. The #575
//     slot pinning exists for exactly this and never fired.
//   - dag / dagshuf computed the right values and SORTED on the other column:
//     `ORDER BY a LIMIT 3` returned the three smallest GROUP KEYS where
//     PostgreSQL returns the three smallest SUMS.
//
// The declared TYPE is asserted beside the rows: on the single-process arms
// the wrong value arrived under the wrong declaration too, and a value oracle
// cannot see a right value under a wrong OID (ADR-0012). PostgreSQL declares
// the SUM `numeric` (unconstrained); wadjet declares the width its exact
// arithmetic needs (ADR-0024), which is the recorded divergence and not this
// arc's subject — what matters here is that all four arms declare the SAME
// thing and that it is the AGGREGATE's declaration, not the key's.
func TestArcK1AnOutputSlotHasOneIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const cols = "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4)]"
	const five = cols + " rows=5 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000 | " +
		"2.00,10.0000 | 12.75,38.2500"

	f1Run(t, arms, []f1Case{
		{
			// The filing's exact SQL. LIMIT is the instrument: a row COUNT
			// cannot tell "sorted on the aggregate" from "sorted on the key",
			// and the top three differ under the two rules.
			name: "968 the filing: an output alias equal to a group key's source name",
			sql:  `SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY a LIMIT 3`,
			want: cols + " rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
		},
		{
			name: "968 the same list with no LIMIT",
			sql:  `SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY a`,
			want: five,
		},
		{
			// ORDER BY the OTHER alias, over the same collision: the key's
			// order, and the aggregate's value still has to be the
			// aggregate's. Right on the DAG arms at base and wrong on the
			// single-process ones, so it separates the two defects.
			name: "968 ordering by the other output name",
			sql:  `SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY b`,
			want: cols + " rows=5 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000 | " +
				"12.75,38.2500 | NULL,1.0000",
		},
		{
			// The HAVING spelling. `aggOutputNameIsShared` decides whether the
			// predicate may REUSE the SELECT list's aggregate or needs its own
			// `__having_N` slot, and it compared the key under the spelling the
			// query wrote — so a QUALIFIED key never looked shared and the
			// filter compared the KEY. It kept two groups of PostgreSQL's
			// three (ADR-0026 §3a, the qualified spelling).
			name: "968 a HAVING over the aggregate under the shared name",
			sql: `SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ` +
				`HAVING SUM(x.b) > 0 ORDER BY a`,
			want: cols + " rows=3 | NULL,1.0000 | 2.00,10.0000 | 12.75,38.2500",
		},
		{
			name: "968 DISTINCT over the collision",
			sql:  `SELECT DISTINCT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ORDER BY a`,
			want: five,
		},
		{
			name: "968 LIMIT with an OFFSET",
			sql: `SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ` +
				`ORDER BY a LIMIT 3 OFFSET 1`,
			want: cols + " rows=3 | 0.00,0.0000 | NULL,1.0000 | 2.00,10.0000",
		},
		{
			// THE MIRROR, and the control that says the fix is about the
			// COLLISION and not about aggregates in general: the two aliases
			// swapped, so nothing shares a name. Right on all four arms at
			// base and here. The DAG arms answer on the coordinator's local
			// pipeline — a disposition, asserted as one, because rows alone
			// cannot tell it from an executed plan.
			name: "968 ctl the mirror, where no name is shared",
			sql:  `SELECT x.a AS a, SUM(x.b) AS b FROM decpair x GROUP BY x.a ORDER BY b`,
			want: "cols=[a:DECIMAL(9,2) b:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | " +
				"0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,38.2500",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1",
			},
		},
		{
			// The BARE spelling of the key, which has no qualifier to strip:
			// right at base on every arm, and the other side of the boundary
			// the fix claims — the emitted-name model changed what the
			// planner believes about a QUALIFIED key and must not have moved
			// this one.
			name: "968 ctl the bare group-key spelling of the same collision",
			sql:  `SELECT a AS b, SUM(b) AS a FROM decpair GROUP BY a ORDER BY a`,
			want: five,
		},
		{
			// TWO aggregates under one name beside the key, so the per-class
			// cursor has to hand out the FIRST and the SECOND aggregate slot
			// rather than the same one twice. PostgreSQL answers it; a cursor
			// that reset per name would publish the same value in both.
			name: "968 two aggregate outputs sharing the key's published name",
			sql: `SELECT x.a AS b, SUM(x.b) AS a, MIN(x.b) AS a2 FROM decpair x ` +
				`GROUP BY x.a ORDER BY a`,
			want: "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) a2:DECIMAL(18,4)] rows=5 | " +
				"-0.01,-0.0100,-0.0100 | 0.00,0.0000,0.0000 | NULL,1.0000,1.0000 | " +
				"2.00,10.0000,10.0000 | 12.75,38.2500,12.7499",
		},
	})
}
