package coordinator

import (
	"context"
	"testing"
	"time"
)

// TWO JOIN ARMS PUBLISHING ONE ALIAS — #770, DEFERRED, censused on FOUR arms.
//
// This file asserts a DEFECT, not a fix. Every `pin` below is what wadjet
// answers at 2e386378 and still answers here; every `want` beside it is live
// PostgreSQL 17.11's over rows identical to the `decpair` fixture. A pin that
// starts AGREEING fails, so closing #770 means deleting cells from this table.
//
// # The mechanism
//
// A join stage's `Columns` is an OutputFilter and its exchanges' are payload
// manifests, both built from the join node's `NeededColumns` at stage emission
// (ADR-0025). `NeededColumns` spells the name the QUERY wrote. Four consumers
// spell something else, and where the two differ the column is dropped from the
// payload and the consumer reads NULL — or fails:
//
//   - a GROUP BY key has a PUBLISHED name and a RESOLUTION spelling (ADR-0026
//     §2): `y.w` publishes as `y.w` and resolves as `w`;
//   - a UNION ARM forwarding a derived table's COMPUTED column is rewritten
//     into the EXPRESSION that builds it (#554), so it reads `b`;
//   - an aggregate ARGUMENT naming the alias is re-spelled to the source;
//   - a WINDOW argument naming it is re-spelled the same way, and there the
//     DECLARATION goes with the value.
//
// # Why it is deferred rather than fixed
//
// A pass that carries the resolution spelling into the narrowing stages below
// was built and WITHDRAWN in this branch. It closed the filed query at exactly
// THREE relations and reopened at four: it could only start where the consuming
// stage ALREADY named the spelling, which is a MODEL of where the value is
// needed rather than a fact about it (rule 11). W2 and W3 below are that model's
// boundary, measured. Carrying every such reference unconditionally does close
// them and is a payload widening — it put `n_name` / `n1.n_name` / `n2.n_name`
// onto eight TPC-H joins and exchanges across Q05/Q07/Q08/Q09/Q10 and
// `c_name` / `l_quantity` onto two Q18 joins, each a second carry of a value the
// chained link's own `Columns` already supplies.
//
// The STRUCTURAL fix is the consumer resolving through the link's PUBLISHED
// IDENTITY — ADR-0026's two names applied to join and exchange consumers, so a
// consumer asks the producer what it calls the value instead of guessing a
// spelling and hoping the payload carries it — or a general carry whose payload
// cost is MEASURED on TPC-H rather than assumed prohibitive (bytes on the wire
// is a metric, not a veto). Either is an arc with its own brief, and this table
// is its census. See ADR-0025.
func TestH2TwoJoinArmsPublishingOneAliasIsDeferred(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const arm3 = "FROM (SELECT id, a AS w FROM decpair) x " +
		"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
		"JOIN decpair u ON x.id = u.id WHERE x.w > 1"
	const five = "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=5 | 2.00,1000.0000 | " +
		"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL"
	// The two loud refusals differ only in the task id and the input list, so
	// they are pinned by PREFIX — what the pin claims is WHICH refusal.
	const shufFail = "ERR native DAG: stage join-8 (hash_join)"
	const groupKeyWhy = "a GROUP BY key's RESOLUTION spelling (`w`) is a second name the join " +
		"below the aggregate never carried; ADR-0026 §2 + ADR-0025's not-settled list"
	const armWhy = "a UNION arm forwarding a COMPUTED alias is rewritten into the expression " +
		"that builds it (#554), so it reads `b`, which the join's payload never carried"

	f1Run(t, arms, []f1Case{
		{
			// The UNION spelling #770 filed as its correct control. It is the
			// SILENT one, on BOTH DAG arms, and the dedup then collapses five
			// distinct pairs into two.
			name: "770 PINNED: the UNION spelling answers 2 rows with yw NULL",
			sql: "SELECT x.w AS xw, y.w AS yw " + arm3 + " UNION " +
				"SELECT x.w AS xw, y.w AS yw " + arm3 + " ORDER BY xw, yw",
			want: five,
			pin: map[string]string{
				"dag":     "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=2 | 2.00,NULL | 12.75,NULL",
				"dagshuf": "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=2 | 2.00,NULL | 12.75,NULL",
			},
			why: armWhy,
		},
		{
			// UNION ALL, where no dedup hides the NULL behind a smaller row
			// count: ten rows, the second column gone on both DAG arms.
			name: "770 PINNED: the UNION ALL spelling keeps ten rows and loses the column",
			sql: "SELECT x.w AS xw, y.w AS yw " + arm3 + " UNION ALL " +
				"SELECT x.w AS xw, y.w AS yw " + arm3 + " ORDER BY xw, yw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=10 | 2.00,1000.0000 | " +
				"2.00,1000.0000 | 12.75,1274.9900 | 12.75,1274.9900 | 12.75,1275.0000 | " +
				"12.75,1275.0000 | 12.75,1275.0100 | 12.75,1275.0100 | 12.75,NULL | 12.75,NULL",
			pin: map[string]string{
				"dag": "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=10 | 2.00,NULL | 2.00,NULL | " +
					"12.75,NULL | 12.75,NULL | 12.75,NULL | 12.75,NULL | 12.75,NULL | 12.75,NULL | " +
					"12.75,NULL | 12.75,NULL",
				"dagshuf": "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=10 | 2.00,NULL | 2.00,NULL | " +
					"12.75,NULL | 12.75,NULL | 12.75,NULL | 12.75,NULL | 12.75,NULL | 12.75,NULL | " +
					"12.75,NULL | 12.75,NULL",
			},
			why: armWhy,
		},
		{
			// The arms SWAPPED in the SELECT list, so the loss is not an
			// artefact of which item is first.
			name: "770 PINNED: the UNION spelling with the two arms swapped",
			sql: "SELECT y.w AS yw, x.w AS xw " + arm3 + " UNION " +
				"SELECT y.w AS yw, x.w AS xw " + arm3 + " ORDER BY yw, xw",
			want: "cols=[yw:DECIMAL(22,4) xw:DECIMAL(9,2)] rows=5 | 1000.0000,2.00 | " +
				"1274.9900,12.75 | 1275.0000,12.75 | 1275.0100,12.75 | NULL,12.75",
			pin: map[string]string{
				"dag":     "cols=[yw:DECIMAL(22,4) xw:DECIMAL(9,2)] rows=2 | NULL,2.00 | NULL,12.75",
				"dagshuf": "cols=[yw:DECIMAL(22,4) xw:DECIMAL(9,2)] rows=2 | NULL,2.00 | NULL,12.75",
			},
			why: armWhy,
		},
		{
			// B3: a WINDOW over the contested alias — the value AND the
			// declaration are gone on the shuffled arm. A right value under a
			// wrong OID is what ADR-0012 says a value oracle cannot see; here
			// the value is gone too.
			name: "770 PINNED: a WINDOW over the contested alias loses its value and its type",
			sql:  "SELECT x.w AS xw, SUM(y.w) OVER () AS s " + arm3 + " ORDER BY xw",
			want: "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,4825.0000 | " +
				"12.75,4825.0000 | 12.75,4825.0000 | 12.75,4825.0000 | 12.75,4825.0000",
			pin: map[string]string{
				"dagshuf": "cols=[xw:DECIMAL(9,2) s:FLOAT64] rows=5 | 2.00,NULL | 12.75,NULL | " +
					"12.75,NULL | 12.75,NULL | 12.75,NULL",
			},
			why: "the window's argument is the fourth consumer of the same payload gap, and it " +
				"takes the DECLARATION with it: FLOAT64 where PostgreSQL and every other arm " +
				"say numeric",
		},
	})
}
