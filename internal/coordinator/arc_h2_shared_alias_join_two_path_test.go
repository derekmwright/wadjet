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
			// #770's exact SQL. The DISTINCT lowers to a GROUP BY whose key
			// resolves to `w`.
			name:   "770 PINNED: DISTINCT over a join whose arms share an output alias",
			sql:    "SELECT DISTINCT x.w AS xw, y.w AS yw " + arm3 + " ORDER BY xw, yw",
			want:   five,
			pin:    map[string]string{"dagshuf": shufFail},
			why:    groupKeyWhy,
			routed: map[string]string{},
		},
		{
			name: "770 PINNED: the GROUP BY spelling of the same query",
			sql:  "SELECT x.w AS xw, y.w AS yw " + arm3 + " GROUP BY x.w, y.w ORDER BY xw, yw",
			want: five,
			pin:  map[string]string{"dagshuf": shufFail},
			why:  groupKeyWhy,
		},
		{
			// B1: the model's boundary. ONE MORE relation on the same key, and
			// the stage the key is computed on no longer names `w` itself —
			// which is exactly the condition the withdrawn pass needed.
			name: "770 PINNED: the same DISTINCT with one MORE join",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id JOIN decpair v ON x.id = v.id " +
				"WHERE x.w > 1 ORDER BY xw, yw",
			want: five,
			pin:  map[string]string{"dagshuf": shufFail},
			why: groupKeyWhy + "; this is the shape a carry bounded by " +
				"\"the consuming stage already names it\" cannot reach",
		},
		{
			// The same boundary reached the other way: THREE derived arms, so
			// the dropped spelling is the SOURCE column `a` rather than `w`.
			name: "770 PINNED: three derived arms sharing the alias",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw, z.w AS zw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN (SELECT id, a*3 AS w FROM decpair) z ON x.id = z.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yw, zw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4) zw:DECIMAL(11,2)] rows=5 | " +
				"2.00,1000.0000,6.00 | 12.75,1274.9900,38.25 | 12.75,1275.0000,38.25 | " +
				"12.75,1275.0100,38.25 | 12.75,NULL,38.25",
			pin: map[string]string{"dagshuf": shufFail},
			why: groupKeyWhy + "; here the missing spelling is the SOURCE column `a`",
		},
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
			// An aggregate ARGUMENT that IS a bare reference: loud.
			name: "770 PINNED: an aggregate ARGUMENT naming the contested alias",
			sql:  "SELECT SUM(y.w) AS s " + arm3,
			want: "cols=[s:DECIMAL(38,4)] rows=1 | 4825.0000",
			pin:  map[string]string{"dagshuf": shufFail},
			why:  "the aggregate's argument is re-spelled to a source the join's payload never carried",
		},
		{
			// COUNT(DISTINCT y.w): the DISTINCT-inside-an-aggregate spelling
			// reaches the GROUP-KEY consumer rather than the argument one, so
			// it fails on `w` and not on `y.w` — the same payload gap through
			// a fourth SQL surface.
			name: "770 PINNED: COUNT(DISTINCT) over the contested alias",
			sql:  "SELECT COUNT(DISTINCT y.w) AS n " + arm3,
			want: "cols=[n:INT64] rows=1 | 4",
			pin:  map[string]string{"dagshuf": shufFail},
			why:  groupKeyWhy + "; the DISTINCT inside the aggregate lowers to that key",
		},
		{
			// A HAVING term naming the alias: the aggregate-argument consumer
			// again, reached from the predicate rather than the SELECT list,
			// and the GROUP BY key beside it is the arm that DOES resolve.
			name: "770 PINNED: a HAVING term over the contested alias",
			sql: "SELECT x.w AS xw " + arm3 +
				" GROUP BY x.w HAVING MAX(y.w) > 1000 ORDER BY xw",
			want: "cols=[xw:DECIMAL(9,2)] rows=1 | 12.75",
			pin:  map[string]string{"dagshuf": shufFail},
			why: "the HAVING aggregate's argument is re-spelled to a source the join's payload " +
				"never carried; the query's own GROUP BY key (`x.w`) resolves and this does not",
		},
		{
			// B2: the same consumer with an EXPRESSION argument, and here it is
			// SILENT — and differently silent on the two arms. 9650.0000 is
			// 2 x SUM(y.w): `x.w` bound the OTHER arm's column.
			name: "770 PINNED: an aggregate argument that is an EXPRESSION answers a NUMBER",
			sql:  "SELECT SUM(y.w + x.w) AS s " + arm3,
			want: "cols=[s:DECIMAL(38,4)] rows=1 | 4865.2500",
			pin: map[string]string{
				"dag":     "cols=[s:DECIMAL(38,4)] rows=1 | 9650.0000",
				"dagshuf": "cols=[s:DECIMAL(38,4)] rows=1 | NULL",
			},
			why: "a silent wrong answer, not the loud refusal the bare-reference form gives: " +
				"9650.0000 is 2 x SUM(y.w), so `x.w` bound the other arm's column",
		},
		{
			// B2's second witness, inside #877's own family: a WINDOW arm
			// joined to a plain rename of the same name. NULL on both DAG arms.
			name: "770 PINNED: a window arm joined to a rename of the same alias",
			sql: "SELECT SUM(x.w + y.w) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x " +
				"JOIN (SELECT id, a AS w FROM decpair) y ON x.id = y.id",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 423.92",
			pin: map[string]string{
				"dag":     "cols=[s:DECIMAL(38,2)] rows=1 | NULL",
				"dagshuf": "cols=[s:DECIMAL(38,2)] rows=1 | NULL",
			},
			why: "the contested alias is one arm's WINDOW slot and the other's rename; " +
				"the sibling shape with TWO window arms is the control below and answers",
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

		// The controls. Each of these answers PostgreSQL on all four arms at
		// this commit and at 2e386378, so they say the census is about a
		// CONTESTED alias crossing a join boundary and not about derived
		// aliases, unions or windows in general.
		{
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
			name: "770 control: both arms COMPUTE the shared alias",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw FROM (SELECT id, a*3 AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yw",
			want: "cols=[xw:DECIMAL(11,2) yw:DECIMAL(22,4)] rows=5 | 6.00,1000.0000 | " +
				"38.25,1274.9900 | 38.25,1275.0000 | 38.25,1275.0100 | 38.25,NULL",
		},
		{
			// The sibling of the window cell above, with BOTH arms windows —
			// which #877 fixed in this branch and which answers here.
			name: "770 control: an expression over TWO window arms",
			sql: "SELECT SUM(p.w + q.w) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) p " +
				"JOIN (SELECT id, MAX(a) OVER () AS w FROM decpair) q ON p.id = q.id",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 591.66",
		},
	})
}
