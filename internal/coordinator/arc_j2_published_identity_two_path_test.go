package coordinator

import (
	"context"
	"testing"
	"time"
)

// A CONSUMER BINDS THROUGH THE IDENTITY ITS PRODUCER PUBLISHED — #770, on
// FOUR arms against live PostgreSQL 17.11 over rows identical to `decpair`.
//
// ADR-0026 §2 gave a GROUP BY key two names and stopped at the aggregate. A
// JOIN publishes names of its own — `joinOutputSchemaWithMapping` qualifies
// every duplicate bare name by its owning alias — while a join's `Columns` and
// its exchanges' payload manifests are built from `NeededColumns`, which
// spells what the QUERY wrote. #770 is what falls in that gap, in two
// directions at once:
//
//   - the value IS on the stream under the join's own spelling. With three
//     derived arms, x's key resolves to the source column `a` and the join
//     publishes it as `x.a` because z's arm has an `a` too; the runtime finds
//     TWO columns ending `.a`, declines (it may not guess an arm, #742) and
//     the task fails.
//   - the value is on NO stream, because a join below the consumer dropped
//     it: y's key resolves to `w`, which y's fragment computes and the join
//     underneath filtered away.
//
// `bindConsumersToPublishedIdentity` answers the first by RESPELLING to the
// producer's name, which costs no bytes, and only then the second by CARRYING
// the value. `TestTPCHStageDumpGolden` is byte-identical: every TPC-H group
// key already binds, so no query there gains a column.
//
// The FOUR- and THREE-ARM cells are the ones that matter. Arc H2's carry was
// bounded by "the consuming stage already names the spelling", which closed
// the filed query at three relations and reopened at four; that bound is a
// MODEL of where a value can be needed and rule 11 does not ship one. Nothing
// here asks whether a stage already names anything — it asks the stream what
// it will ship.
func TestJ2AJoinConsumerBindsThePublishedIdentity(t *testing.T) {
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

	f1Run(t, arms, []f1Case{
		{
			// #770's exact SQL. The DISTINCT lowers to a GROUP BY whose key
			// resolves to `w`, which nothing on the shuffled arm's stream
			// carried: `GROUP BY key "w" is not a column of its input`.
			name: "770 DISTINCT over a join whose arms share an output alias",
			sql:  "SELECT DISTINCT x.w AS xw, y.w AS yw " + arm3 + " ORDER BY xw, yw",
			want: five,
		},
		{
			name: "770 the GROUP BY spelling of the same query",
			sql:  "SELECT x.w AS xw, y.w AS yw " + arm3 + " GROUP BY x.w, y.w ORDER BY xw, yw",
			want: five,
		},
		{
			// The shape a carry bounded by "the consuming stage already names
			// it" cannot reach: ONE MORE relation on the same key, and the
			// stage the key is computed on no longer names `w` itself.
			name: "770 the same DISTINCT with one MORE join",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id JOIN decpair v ON x.id = v.id " +
				"WHERE x.w > 1 ORDER BY xw, yw",
			want: five,
		},
		{
			// Two more joins, so the value crosses three narrowing stages.
			name: "770 the same DISTINCT with TWO more joins",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id JOIN decpair v ON x.id = v.id " +
				"JOIN decpair t2 ON x.id = t2.id WHERE x.w > 1 ORDER BY xw, yw",
			want: five,
		},
		{
			// The same boundary reached the other way: THREE derived arms, so
			// the contested SOURCE column `a` is published as `x.a` on one arm
			// and `z.a` on another, and the bare spelling is ambiguous rather
			// than absent. This is the RESPELL half — it carries nothing.
			name: "770 three derived arms sharing the alias",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw, z.w AS zw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN (SELECT id, a*3 AS w FROM decpair) z ON x.id = z.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yw, zw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4) zw:DECIMAL(11,2)] rows=5 | " +
				"2.00,1000.0000,6.00 | 12.75,1274.9900,38.25 | 12.75,1275.0000,38.25 | " +
				"12.75,1275.0100,38.25 | 12.75,NULL,38.25",
		},
		{
			// The GROUP BY spelling of the three-arm shape, so the fix is not
			// specific to the DISTINCT lowering.
			name: "770 three derived arms, the GROUP BY spelling",
			sql: "SELECT x.w AS xw, y.w AS yw, z.w AS zw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN (SELECT id, a*3 AS w FROM decpair) z ON x.id = z.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 " +
				"GROUP BY x.w, y.w, z.w ORDER BY xw, yw, zw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4) zw:DECIMAL(11,2)] rows=5 | " +
				"2.00,1000.0000,6.00 | 12.75,1274.9900,38.25 | 12.75,1275.0000,38.25 | " +
				"12.75,1275.0100,38.25 | 12.75,NULL,38.25",
		},
		{
			// COUNT(DISTINCT y.w): the DISTINCT inside the aggregate lowers to
			// the GROUP-KEY consumer, so it reaches the same gap through a
			// fourth SQL surface and fails on `w` rather than on `y.w`.
			name: "770 COUNT(DISTINCT) over the contested alias",
			sql:  "SELECT COUNT(DISTINCT y.w) AS n " + arm3,
			want: "cols=[n:INT64] rows=1 | 4",
		},
		{
			// The arms SWAPPED in the SELECT list, so nothing here depends on
			// which item is first.
			name: "770 the arms swapped in the SELECT list",
			sql:  "SELECT DISTINCT y.w AS yw, x.w AS xw " + arm3 + " ORDER BY yw, xw",
			want: "cols=[yw:DECIMAL(22,4) xw:DECIMAL(9,2)] rows=5 | 1000.0000,2.00 | " +
				"1274.9900,12.75 | 1275.0000,12.75 | 1275.0100,12.75 | NULL,12.75",
		},
		{
			// The contested alias as the ONLY output, so the key is the whole
			// payload question rather than one column of it.
			name: "770 the contested alias alone under DISTINCT",
			sql:  "SELECT DISTINCT y.w AS yw " + arm3 + " ORDER BY yw",
			want: "cols=[yw:DECIMAL(22,4)] rows=5 | 1000.0000 | 1274.9900 | 1275.0000 | " +
				"1275.0100 | NULL",
		},

		// The UNION ARM's projection, the consumer that arrives as an
		// EXPRESSION rather than a name. An arm forwarding a derived table's
		// COMPUTED column is rewritten into the expression that defines it
		// (#554), so the arm shipped `b * 100 AS yw` over a join stream that
		// carries the computed `w` and never carries `b`. #770 filed the
		// UNION spelling as its correct CONTROL; it was the silent one.
		{
			name: "770 the UNION spelling of the same query",
			sql: "SELECT x.w AS xw, y.w AS yw " + arm3 + " UNION " +
				"SELECT x.w AS xw, y.w AS yw " + arm3 + " ORDER BY xw, yw",
			want: five,
		},
		{
			// UNION ALL, where no dedup hides the loss behind a smaller row
			// count: ten rows with the second column gone.
			name: "770 the UNION ALL spelling keeps ten rows and the column",
			sql: "SELECT x.w AS xw, y.w AS yw " + arm3 + " UNION ALL " +
				"SELECT x.w AS xw, y.w AS yw " + arm3 + " ORDER BY xw, yw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=10 | 2.00,1000.0000 | " +
				"2.00,1000.0000 | 12.75,1274.9900 | 12.75,1274.9900 | 12.75,1275.0000 | " +
				"12.75,1275.0000 | 12.75,1275.0100 | 12.75,1275.0100 | 12.75,NULL | 12.75,NULL",
		},
		{
			name: "770 the UNION spelling with the two arms swapped",
			sql: "SELECT y.w AS yw, x.w AS xw " + arm3 + " UNION " +
				"SELECT y.w AS yw, x.w AS xw " + arm3 + " ORDER BY yw, xw",
			want: "cols=[yw:DECIMAL(22,4) xw:DECIMAL(9,2)] rows=5 | 1000.0000,2.00 | " +
				"1274.9900,12.75 | 1275.0000,12.75 | 1275.0100,12.75 | NULL,12.75",
		},

		// The AGGREGATE ARGUMENT, which is the same gap one consumer over: the
		// spec ships the alias as TEXT and the fragment compiles it against
		// the join's stream. `aggInputAliasIsMaterializedUnderItsName` says a
		// JOIN materializes a derived alias under its own name, and that is
		// false whenever the arm's SELECT list is a bare rename —
		// attachScanSelectProjections puts no projection there, so the join
		// publishes the SOURCE column and the alias names nothing.
		{
			// An argument that IS a bare reference. Loud on the shuffled arm:
			// `aggregate input "y.w" is not a column of its input`.
			name: "770 an aggregate ARGUMENT naming the contested alias",
			sql:  "SELECT SUM(y.w) AS s " + arm3,
			want: "cols=[s:DECIMAL(38,4)] rows=1 | 4825.0000",
		},
		{
			// The same argument reached from a HAVING rather than the SELECT
			// list, with the query's own GROUP BY key beside it.
			name: "770 a HAVING term over the contested alias",
			sql: "SELECT x.w AS xw " + arm3 +
				" GROUP BY x.w HAVING MAX(y.w) > 1000 ORDER BY xw",
			want: "cols=[xw:DECIMAL(9,2)] rows=1 | 12.75",
		},
		{
			// An argument that is an EXPRESSION over BOTH contested arms, and
			// this one was SILENT: 9650.0000 is 2 x SUM(y.w), because the
			// runtime's strip-the-qualifier step bound `x.w` to the probe's
			// `w` — a bind that succeeds and is still the wrong value, which
			// is why bindStreamColumnFromArm asks which ARM it landed on.
			name: "770 an aggregate argument that is an EXPRESSION over both arms",
			sql:  "SELECT SUM(y.w + x.w) AS s " + arm3,
			want: "cols=[s:DECIMAL(38,4)] rows=1 | 4865.2500",
		},
		{
			// The same expression argument one relation deeper and with a
			// coefficient, so the substitution is exercised inside a larger
			// term where a bare splice would re-associate.
			name: "770 an expression argument over four relations",
			sql: "SELECT SUM(y.w * 2 + x.w) AS s FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id JOIN decpair v ON x.id = v.id WHERE x.w > 1",
			want: "cols=[s:DECIMAL(38,4)] rows=1 | 9690.2500",
		},
		{
			// MIN, MAX and COUNT of the contested alias in one query: the
			// respell is per SPEC, so three of them have to agree.
			name: "770 three aggregates over the contested alias at once",
			sql:  "SELECT MIN(y.w) AS lo, MAX(y.w) AS hi, COUNT(y.w) AS n " + arm3,
			want: "cols=[lo:DECIMAL(22,4) hi:DECIMAL(22,4) n:INT64] rows=1 | " +
				"1000.0000,1275.0100,4",
		},
		{
			// A WINDOW arm joined to a plain rename of the same name — #877's
			// own family. `respellWindowSlotAliasRefs` re-spells x's alias to
			// its `__win_0` slot and left y's naming nothing, so the sum was
			// NULL on both DAG arms while the sibling with TWO window arms
			// (the control below) answered.
			name: "770 a window arm joined to a rename of the same alias",
			sql: "SELECT SUM(x.w + y.w) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x " +
				"JOIN (SELECT id, a AS w FROM decpair) y ON x.id = y.id",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 423.92",
		},
		{
			// The WINDOW argument, the fourth consumer, and it takes the
			// DECLARATION with it: FLOAT64 with every value NULL on the
			// shuffled arm, where PostgreSQL and every other arm say numeric.
			// A right value under a wrong OID is what ADR-0012 says a value
			// oracle cannot see; here the value was gone too.
			name: "770 a WINDOW over the contested alias keeps its value and its type",
			sql:  "SELECT x.w AS xw, SUM(y.w) OVER () AS s " + arm3 + " ORDER BY xw",
			want: "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,4825.0000 | " +
				"12.75,4825.0000 | 12.75,4825.0000 | 12.75,4825.0000 | 12.75,4825.0000",
		},
		{
			name: "770 a WINDOW over the contested alias, one relation deeper",
			sql: "SELECT x.w AS xw, SUM(y.w) OVER () AS s FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id JOIN decpair v ON x.id = v.id " +
				"WHERE x.w > 1 ORDER BY xw",
			want: "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,4825.0000 | " +
				"12.75,4825.0000 | 12.75,4825.0000 | 12.75,4825.0000 | 12.75,4825.0000",
		},
		{
			// MAX rather than SUM, so the declaration follows the INPUT's
			// (p,s) instead of the aggregate's widening rule.
			name: "770 a MAX window over the contested alias",
			sql:  "SELECT x.w AS xw, MAX(y.w) OVER () AS s " + arm3 + " ORDER BY xw",
			want: "cols=[xw:DECIMAL(9,2) s:DECIMAL(22,4)] rows=5 | 2.00,1275.0100 | " +
				"12.75,1275.0100 | 12.75,1275.0100 | 12.75,1275.0100 | 12.75,1275.0100",
		},
		{
			// PINNED, a SIXTH consumer this arc does not reach: the window's
			// own PARTITION BY key. `resolveWindowKeys` settles it at
			// emission, because the key is also the stage's DISTRIBUTION and
			// rewriting it after EnsureDistribution would leave the two
			// disagreeing — and it settles it ARM-BLIND, so `PARTITION BY x.w`
			// over two arms that both publish `w` binds the OTHER arm's
			// column. That column is distinct on every row, so the window
			// partitions each row on its own and answers its own value where
			// PostgreSQL answers the partition's total.
			//
			// Wrong on ALL FOUR arms at a3f9b664 and here, so it is a
			// wadjet-vs-PostgreSQL divergence rather than a two-path one, and
			// nothing in this arc moved a value. The shuffled arm's
			// DISPOSITION did move: at a3f9b664 it failed at
			// `exchange-repartition-window-9-9` because the payload was
			// incomplete, and now that the carry completes it, it computes the
			// same wrong partition as the other three. Fail-on-agree.
			name: "770 PINNED: a PARTITIONED window over the contested alias binds the other arm",
			sql: "SELECT x.w AS xw, SUM(y.w) OVER (PARTITION BY x.w) AS s " + arm3 +
				" ORDER BY xw, s",
			want: "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,1000.0000 | " +
				"12.75,3825.0000 | 12.75,3825.0000 | 12.75,3825.0000 | 12.75,3825.0000",
			pin: map[string]string{
				"single": "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,1000.0000 | " +
					"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL",
				spilledArm: "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,1000.0000 | " +
					"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL",
				"dag": "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,1000.0000 | " +
					"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL",
				"dagshuf": "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,1000.0000 | " +
					"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL",
			},
			why: "a window's PARTITION BY key is the sixth consumer of the published identity " +
				"and the one this arc does not reach: it is settled at emission because it is " +
				"also the stage's distribution, and it is settled arm-blind",
		},
		{
			// The same query with the two aliases DISTINCT: the partition is
			// right on the local path, and the DAG refuses it outright — a
			// second pre-existing defect in the same key, and the control that
			// says the cell above is about the COLLISION.
			name: "770 PINNED: the same PARTITIONED window with distinct aliases",
			sql: "SELECT x.w AS xw, SUM(y.z) OVER (PARTITION BY x.w) AS s " +
				"FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS z FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, s",
			want: "cols=[xw:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | 2.00,1000.0000 | " +
				"12.75,3825.0000 | 12.75,3825.0000 | 12.75,3825.0000 | 12.75,3825.0000",
			pin: map[string]string{
				"dag":     "ERR native DAG: stage window-5 (window)",
				"dagshuf": "ERR native DAG: stage exchange-repartition-window-9-9",
			},
			why: "`window: PARTITION BY \"w\" is not a column of its input` — the key was " +
				"re-spelled to the source `a` at emission and the window's input publishes " +
				"the alias; identical at a3f9b664",
		},
		{
			name: "770 control: an expression over TWO window arms",
			sql: "SELECT SUM(p.w + q.w) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) p " +
				"JOIN (SELECT id, MAX(a) OVER () AS w FROM decpair) q ON p.id = q.id",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 591.66",
		},

		// The controls. Each answers PostgreSQL on all four arms at
		// a3f9b664 as well, so they say the fix is about a CONTESTED alias
		// crossing a join boundary and not about derived aliases in general.
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
			// The BASE column of the contested name beside the alias: a
			// respell that ignored the arm would bind u's `a` here.
			name: "770 control: the base column beside the contested alias",
			sql: "SELECT DISTINCT x.w AS xw, u.a AS ua FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, ua",
			want: "cols=[xw:DECIMAL(9,2) ua:DECIMAL(9,2)] rows=2 | 2.00,2.00 | 12.75,12.75",
		},
		{
			// An UNCONTESTED derived alias over the same three relations,
			// which never needed either half.
			name: "770 control: one derived arm, no collision",
			sql: "SELECT DISTINCT x.w AS xw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw",
			want: "cols=[xw:DECIMAL(9,2)] rows=2 | 2.00 | 12.75",
		},
	})
}
