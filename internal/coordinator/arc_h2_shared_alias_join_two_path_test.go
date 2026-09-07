package coordinator

import (
	"context"
	"testing"
	"time"
)

// TWO JOIN ARMS PUBLISHING ONE ALIAS — #770, on FOUR arms.
//
// A join stage's `Columns` is an OutputFilter and its exchanges' are payload
// manifests: both NARROW what arrives, and both are built from the join node's
// `NeededColumns` at stage emission (ADR-0025). `NeededColumns` spells the
// name the QUERY wrote. Two later decisions spell something else, and where
// the two differ the column is dropped from the payload and read as NULL — or
// not read at all:
//
//   - a GROUP BY key has a PUBLISHED name and a RESOLUTION spelling
//     (ADR-0026 §2). `y.w` publishes as `y.w` and resolves as `w`, which the y
//     arm's fragment materializes; the join UNDER the aggregate's join dropped
//     it, and the shuffled arm failed the task outright with `GROUP BY key "w"
//     is not a column of its input`;
//   - a UNION ARM forwarding a derived table's COMPUTED column is rewritten
//     into the EXPRESSION that builds it (#554), so it reads `b`, which
//     `NeededColumns` never mentions. Both DAG arms answered 2 rows with `yw`
//     NULL where PostgreSQL has 5 — a silent wrong answer, and the dedup then
//     collapsed the five distinct pairs into two.
//
// Every Want below is PostgreSQL 17.11's over rows identical to the `decpair`
// fixture. The DISTINCT spelling and the UNION spelling sit beside each other
// deliberately: the two took DIFFERENT paths through the planner and were
// wrong in different ways at this arc's base — one loud on one arm, one silent
// on both — and the issue was filed on the belief that the UNION spelling was
// the correct control.
func TestH2TwoJoinArmsPublishingOneAliasKeepBothColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const five = "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=5 | 2.00,1000.0000 | " +
		"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL"
	const armSQL = "SELECT x.w AS xw, y.w AS yw FROM (SELECT id, a AS w FROM decpair) x " +
		"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
		"JOIN decpair u ON x.id = u.id WHERE x.w > 1"

	f1Run(t, arms, []f1Case{
		{
			// #770's exact SQL.
			name: "770 DISTINCT over a join whose arms share an output alias",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yw",
			want: five,
		},
		{
			// The GROUP BY the DISTINCT lowers to, written by hand: the same
			// key pair reaching the same stage by the other spelling.
			name: "770 the GROUP BY spelling of the same query",
			sql: "SELECT x.w AS xw, y.w AS yw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 GROUP BY x.w, y.w ORDER BY xw, yw",
			want: five,
		},
		{
			// The UNION spelling the issue named as its control. It reaches
			// the join through a set-operation arm rather than a group key,
			// and it was the SILENT one at this arc's base.
			name: "770 the UNION spelling of the same query",
			sql:  armSQL + " UNION " + armSQL + " ORDER BY xw, yw",
			want: five,
		},
		{
			// UNION ALL, so no dedup can hide a NULL column behind a smaller
			// row count: ten rows, both columns populated.
			name: "770 the UNION ALL spelling (no dedup to hide a NULL)",
			sql:  armSQL + " UNION ALL " + armSQL + " ORDER BY xw, yw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=10 | 2.00,1000.0000 | " +
				"2.00,1000.0000 | 12.75,1274.9900 | 12.75,1274.9900 | 12.75,1275.0000 | " +
				"12.75,1275.0000 | 12.75,1275.0100 | 12.75,1275.0100 | 12.75,NULL | 12.75,NULL",
		},
		{
			// BOTH arms computed, so neither side's `w` is a plain rename the
			// resolver can fall back to.
			name: "770 both arms COMPUTE the shared alias",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw FROM (SELECT id, a*3 AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yw",
			want: "cols=[xw:DECIMAL(11,2) yw:DECIMAL(22,4)] rows=5 | 6.00,1000.0000 | " +
				"38.25,1274.9900 | 38.25,1275.0000 | 38.25,1275.0100 | 38.25,NULL",
		},
		{
			// The arms SWAPPED in the SELECT list, so a fix that happened to
			// bind the first-listed arm cannot pass.
			name: "770 the UNION spelling with the two arms swapped",
			sql: "SELECT y.w AS yw, x.w AS xw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 UNION " +
				"SELECT y.w AS yw, x.w AS xw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY yw, xw",
			want: "cols=[yw:DECIMAL(22,4) xw:DECIMAL(9,2)] rows=5 | 1000.0000,2.00 | " +
				"1274.9900,12.75 | 1275.0000,12.75 | 1275.0100,12.75 | NULL,12.75",
		},
		{
			// TWO relations rather than three: the broadcast arm fuses all
			// three into ONE join, which is why only the shuffled arm failed
			// the three-relation shape. This one has no join to cross at all
			// and was right before — the control that says the widening did
			// not become the reason it works.
			name: "770 control: the same alias collision over TWO relations",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"WHERE x.w > 1 ORDER BY xw, yw",
			want: five,
		},
		{
			name: "770 control: DISTINCT aliases, nothing contested",
			sql: "SELECT DISTINCT x.w AS xw, y.z AS yz FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS z FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yz",
			want: "cols=[xw:DECIMAL(9,2) yz:DECIMAL(22,4)] rows=5 | 2.00,1000.0000 | " +
				"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL",
		},
		{
			// PINNED, and the sibling this arc does NOT close: an aggregate
			// ARGUMENT naming the contested alias is the third consumer of
			// the same payload gap, and the shuffled arm fails the task for
			// it. Carrying an aggregate's whole argument reference through
			// the joins below fixes it and costs bytes on the wire on every
			// row of every task everywhere else — it put `n_name`,
			// `n1.n_name` and `n2.n_name` onto eight TPC-H joins and
			// exchanges across Q05/Q07/Q08/Q09/Q10 and `c_name`/`l_quantity`
			// onto two Q18 joins, each a second carry of a value the chained
			// link's own list already supplies. The group key's version of
			// the same question has a spelling test that separates the two
			// (a key's published and resolution names differ exactly where
			// the payload cannot already name it); an argument carries no
			// second spelling to test. DEFERRED with that mechanism.
			// Fail-on-agree: the day the shuffled arm answers, delete the pin.
			name: "770 PINNED: an aggregate ARGUMENT over the contested alias",
			sql: "SELECT SUM(y.w) AS s FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1",
			want: "cols=[s:DECIMAL(38,4)] rows=1 | 4825.0000",
			pin: map[string]string{
				"dagshuf": `ERR native DAG: stage join-8 (hash_join): stage join-8: task `,
			},
			why: "the aggregate's argument is the third consumer of the payload gap " +
				"#770 names, and the only fix found for it widens eight TPC-H exchanges. " +
				"DEFERRED with mechanism.",
		},
	})
}
