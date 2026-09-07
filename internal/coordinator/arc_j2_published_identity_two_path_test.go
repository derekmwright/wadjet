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
