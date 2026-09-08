package coordinator

import (
	"context"
	"testing"
	"time"
)

// A WINDOW'S PARTITION BY KEY BINDS ITS OWN ARM — #975, on FOUR arms against
// live PostgreSQL 17 over rows identical to `decpair`.
//
// It is ADR-0025's sixth consumer of the published identity and the one arc J2
// pinned rather than closed. `resolveWindowKeys` settles the key at EMISSION —
// it has to, because the key is also the stage's DISTRIBUTION and rewriting it
// after `EnsureDistribution` would leave the exchange and the operator keyed on
// different columns — and it settled it ARM-BLIND, in two steps that were each
// reasonable alone:
//
//   - the key was dropped to its BARE form, which is safe only while one
//     column answers to it;
//   - the bind that would have narrowed it further reads `inputColTypes`,
//     which declines a JOIN outright — so over the one input shape where two
//     columns DO answer to a bare name, nothing ran.
//
// `PARTITION BY x.w` over two arms that both publish `w` therefore bound the
// other arm's column. That column is distinct on every row, so every row
// became its own partition and the window answered its own value where
// PostgreSQL answers the partition's total: 1274.9900 for PostgreSQL's
// 3825.0000, on all four arms, silently.
//
// The two engines see different streams here and the fix is two rules, not
// one: the single-process join publishes the arm's ALIAS (its Project is a
// real operator) and qualifies the duplicate, so the qualified spelling binds
// exactly; the DAG's join publishes the arm's SOURCE column (that Project
// emits no stage), so the key is resolved INSIDE the arm its qualifier names,
// through `windowArgSourceInScope` — the arm-aware helper the window's
// ARGUMENT has used since #742 round 4. Neither is reachable until the
// qualifier survives, which is why J2's attempt to route the key through that
// helper moved nothing.
func TestArcK1AWindowPartitionKeyBindsItsOwnArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const arm3 = "FROM (SELECT id, a AS w FROM decpair) x " +
		"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
		"JOIN decpair u ON x.id = u.id WHERE x.w > 1"
	const cols = "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4) s:DECIMAL(38,4)]"

	f1Run(t, arms, []f1Case{
		{
			// The filing's shape with both contested columns in the SELECT
			// list, so the rows say WHICH column the partition used and not
			// merely that a number moved.
			name: "975 PARTITION BY the alias two join arms contest",
			sql: "SELECT x.w AS xw, y.w AS yw, SUM(y.w) OVER (PARTITION BY x.w) AS s " +
				arm3 + " ORDER BY xw, yw",
			want: cols + " rows=5 | 2.00,1000.0000,1000.0000 | " +
				"12.75,1274.9900,3825.0000 | 12.75,1275.0000,3825.0000 | " +
				"12.75,1275.0100,3825.0000 | 12.75,NULL,3825.0000",
		},
		{
			// PARTITION BY and ORDER BY in ONE window, over the same contested
			// name: the running sum is the instrument, because a per-row
			// partition and a correct partition agree on the FIRST row and
			// disagree on every later one.
			name: "975 PARTITION BY and ORDER BY in one window over the contested alias",
			sql: "SELECT x.w AS xw, y.w AS yw, SUM(y.w) OVER (PARTITION BY x.w ORDER BY y.w) AS s " +
				arm3 + " ORDER BY xw, yw",
			want: cols + " rows=5 | 2.00,1000.0000,1000.0000 | " +
				"12.75,1274.9900,1274.9900 | 12.75,1275.0000,2549.9900 | " +
				"12.75,1275.0100,3825.0000 | 12.75,NULL,3825.0000",
		},
		{
			// TWO windows partitioned by the two contested aliases, so a fix
			// that binds "the first arm" rather than "the named arm" answers
			// one of them and not the other.
			name: "975 two windows partitioned by the two contested aliases",
			sql: "SELECT x.w AS xw, y.w AS yw, SUM(y.w) OVER (PARTITION BY x.w) AS s1, " +
				"SUM(x.w) OVER (PARTITION BY y.w) AS s2 " + arm3 + " ORDER BY xw, yw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4) s1:DECIMAL(38,4) s2:DECIMAL(38,2)] " +
				"rows=5 | 2.00,1000.0000,1000.0000,2.00 | 12.75,1274.9900,3825.0000,12.75 | " +
				"12.75,1275.0000,3825.0000,12.75 | 12.75,1275.0100,3825.0000,12.75 | " +
				"12.75,NULL,3825.0000,12.75",
		},
		{
			// The DISTINCT-alias twin, arc J2's control: right on the local
			// path at a3f9b664 and REFUSED on both DAG arms
			// (`window: PARTITION BY "w" is not a column of its input`),
			// because the key was left as the bare `w` while the DAG's stream
			// carries x's source column. Answering it on every arm is the same
			// arm-scoped resolution, and it is what says the cell above is
			// about the COLLISION rather than about qualified keys in general.
			name: "975 the same window with the two arms' aliases DISTINCT",
			sql: "SELECT x.w AS xw, y.v AS yv, SUM(y.v) OVER (PARTITION BY x.w) AS s " +
				"FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS v FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yv",
			want: "cols=[xw:DECIMAL(9,2) yv:DECIMAL(22,4) s:DECIMAL(38,4)] rows=5 | " +
				"2.00,1000.0000,1000.0000 | 12.75,1274.9900,3825.0000 | " +
				"12.75,1275.0000,3825.0000 | 12.75,1275.0100,3825.0000 | 12.75,NULL,3825.0000",
		},
		{
			// THE OTHER DIRECTION, and one arm of it is NOT closed.
			//
			// `PARTITION BY y.w` names the arm whose `w` is COMPUTED, so that
			// arm publishes no SOURCE column to resolve to and the key stays
			// as written. Right on three arms; the SHUFFLED arm refuses,
			// because the exchange ahead of the window is keyed on the same
			// name and the join's payload does not carry it.
			//
			// PRE-EXISTING: at bb8635a4 this arm refused too, with `key "w"
			// not in schema` rather than `key "y.w" not in schema` — the same
			// stage, the same cause, a different spelling of the name that is
			// missing. Nothing here moved a value or a disposition.
			//
			// The repair — materializing the arm's computed alias from that
			// arm's own subtree — was BUILT and WITHDRAWN in this arc: it
			// moved this cell's `dag` arm, the ORDER BY cell's and the
			// two-window cell's from executed-and-right to a LOCAL ROUTE, and
			// a right-to-routed move is not a fix (correctness protocol rule
			// 11). Pinned fail-on-agree with the mechanism instead.
			name: "975 PARTITION BY the arm whose alias is COMPUTED",
			sql: "SELECT x.w AS xw, y.w AS yw, SUM(x.w) OVER (PARTITION BY y.w) AS s " +
				arm3 + " ORDER BY xw, yw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4) s:DECIMAL(38,2)] rows=5 | " +
				"2.00,1000.0000,2.00 | 12.75,1274.9900,12.75 | 12.75,1275.0000,12.75 | " +
				"12.75,1275.0100,12.75 | 12.75,NULL,12.75",
			pin: map[string]string{
				"dagshuf": "ERR native DAG: stage exchange-repartition-window-9-9",
			},
			why: "a key naming an arm's COMPUTED alias has no source column to resolve to, " +
				"so the shuffle ahead of the window is keyed on a name the join's payload " +
				"does not carry — `key \"y.w\" not in schema`. Pre-existing: the same stage " +
				"refused at bb8635a4 with `key \"w\" not in schema`",
		},
		{
			// The BARE spelling of a contested name, which no qualifier can
			// disambiguate. PostgreSQL 17 REFUSES it — 42702 `column reference
			// "w" is ambiguous`, measured — and wadjet answers by binding one
			// of the two: a superset, recorded in ADR-0012 with this cell as
			// its record of WHICH column it binds. It is the boundary of the
			// fix attempted from the side the fix does not act on: a key with
			// no qualifier has no arm to be scoped to.
			name: "975 ctl the BARE contested spelling PostgreSQL refuses",
			sql: "SELECT x.w AS xw, y.w AS yw, SUM(y.w) OVER (PARTITION BY w) AS s " +
				arm3 + " ORDER BY xw, yw",
			want: cols + " rows=5 | 2.00,1000.0000,1000.0000 | " +
				"12.75,1274.9900,1274.9900 | 12.75,1275.0000,1275.0000 | " +
				"12.75,1275.0100,1275.0100 | 12.75,NULL,NULL",
		},
		{
			// A QUALIFIED key over a single relation, where the bare name is
			// NOT contested: the other side of the boundary. `bindWindowColRef`
			// still resolves it against a non-empty column set and still
			// answers the bare name, so nothing about this shape moved.
			name: "975 ctl a qualified key over one relation",
			sql: "SELECT d.a AS xa, SUM(d.b) OVER (PARTITION BY d.a) AS s FROM decpair d " +
				"WHERE d.id < 6 ORDER BY xa, s",
			want: "cols=[xa:DECIMAL(9,2) s:DECIMAL(38,4)] rows=5 | -0.01,-0.0100 | " +
				"2.00,10.0000 | 12.75,38.2500 | 12.75,38.2500 | 12.75,38.2500",
		},
	})
}
