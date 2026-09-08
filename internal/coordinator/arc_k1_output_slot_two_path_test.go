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
			// A WINDOW's ORDER BY over the colliding aggregate. The term is
			// re-spelled to the aggregate's output name, which the aggregate
			// also publishes for its KEY, so the rank came back in the KEY's
			// order beside a correct sum — wrong on ALL FOUR arms, the only
			// consumer of this collision that was.
			//
			// The CLASS is recorded where respellOverAggregate rewrote the
			// term and the POSITION is read from `aggregateEmittedOutputNames`,
			// which is 99cd49ab's rule at a second consumer; `exec.SortKey`
			// already carried the slot and the window operator now asks for it.
			name: "968 a window ORDER BY over the colliding aggregate",
			sql: `SELECT x.a AS b, SUM(x.b) AS a, RANK() OVER (ORDER BY SUM(x.b)) AS rk ` +
				`FROM decpair x GROUP BY x.a ORDER BY a`,
			want: "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) rk:INT64] rows=5 | " +
				"-0.01,-0.0100,1 | 0.00,0.0000,2 | NULL,1.0000,3 | 2.00,10.0000,4 | " +
				"12.75,38.2500,5",
		},
		{
			// The same over 5000 rows, where both DAG arms really shuffle and
			// the sort above the window reads the window stage's output — the
			// step that has to see THROUGH the window to the aggregate's model.
			name: "968 a window ORDER BY over the collision, 5000 rows",
			sql: `SELECT x.g AS b, SUM(x.c_i64) AS g, RANK() OVER (ORDER BY SUM(x.c_i64)) AS rk ` +
				`FROM typemx x GROUP BY x.g ORDER BY g LIMIT 3`,
			want: "cols=[b:INT32 g:DECIMAL(38,0) rk:INT64] rows=3 | " +
				"NULL,929156787462,1 | 3,1591177773519,2 | 2,1592105776303,3",
		},
		{
			// The BOUNDARY of that fix, and PostgreSQL's own answer: a window
			// ORDER BY written as the output ALIAS is not an output reference
			// at all — PG binds it to the INPUT column, the group key, and
			// answers `1,2,5,3,4` (measured live). wadjet answers the same, so
			// the class recorded above is about the aggregate CALL spelling
			// and not about the name.
			name: "968 ctl a window ORDER BY written as the alias binds the input",
			sql: `SELECT x.a AS b, SUM(x.b) AS a, RANK() OVER (ORDER BY a) AS rk ` +
				`FROM decpair x GROUP BY x.a ORDER BY a`,
			want: "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) rk:INT64] rows=5 | " +
				"-0.01,-0.0100,1 | 0.00,0.0000,2 | NULL,1.0000,5 | 2.00,10.0000,3 | " +
				"12.75,38.2500,4",
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

		// ---- THE CONSUMERS THIS ARC DOES NOT REACH, pinned fail-on-agree ----
		//
		// Everything above is a consumer of the collision that reads the
		// aggregate's output THROUGH a spelling this arc could give a slot:
		// the projection immediately over it, the sort key on the stage the
		// aggregate produces, the window's ORDER BY term. The four below read
		// it through a consumer that has no slot to be given, and every one of
		// them is the DAG saying the group key where PostgreSQL says the sum.
		// They are wrong at `bb8635a4` too, on the same arms.
		//
		// WHY THEY ARE PINNED RATHER THAN FIXED. The producer-side repair —
		// making the aggregate stage PUBLISH its SELECT list, so the stream
		// carries `[b, a]` instead of `[a, a]` and every consumer above binds
		// unambiguously — was BUILT in round 2 and WITHDRAWN. It closes B1, B3
		// and their 5000-row twins, and it breaks the CTE-consumed-twice cell:
		// the join key had already been resolved to `a` meaning "the first
		// column of that name", and renaming the two apart silently changes
		// what that resolved name MEANS. The join then keyed on the SUM and
		// returned a row PostgreSQL excludes. Retargeting the prior consumers
		// needs each of them to carry its CLASS — a rename map has no class,
		// which is exactly the permutation ADR-0026 §3a's decline records —
		// and that is the same structural work §3a already deferred, not a
		// hunk in a names arc. Trading one silent wrong answer for another is
		// not a fix (rule 11).
		//
		// THE BOUNDARY IS A RULE, not this list. **Every consumer above the
		// aggregate that still resolves by NAME answers the group key on the
		// DAG**, and the cells below are witnesses to it rather than an
		// enumeration of it — a reader who takes them for the boundary will
		// find more shapes outside, because there are more consumers. The ones
		// witnessed here: a stage's GROUP BY key, a join key and a join
		// stage's projection, the coordinator's post-gather DISTINCT/LIMIT, a
		// window's ARGUMENT, a sort over a derived block's star, a SECOND
		// aggregate sharing the alias, and an explicit column read through a
		// derived table. The census in REPORT counts these cells, not the
		// rule's full extent.
		{
			name: "968 PINNED an outer GROUP BY over the collision",
			sql: `SELECT t.a AS ta, COUNT(*) AS n FROM (SELECT x.a AS b, SUM(x.b) AS a ` +
				`FROM decpair x GROUP BY x.a) t GROUP BY t.a ORDER BY ta`,
			want: "cols=[ta:DECIMAL(38,4) n:INT64] rows=5 | -0.0100,1 | 0.0000,1 | " +
				"1.0000,1 | 10.0000,1 | 38.2500,1",
			pin: map[string]string{
				"dag": "cols=[ta:DECIMAL(9,2) n:INT64] rows=5 | -0.01,1 | 0.00,1 | " +
					"2.00,1 | 12.75,1 | NULL,1",
				"dagshuf": "cols=[ta:DECIMAL(9,2) n:INT64] rows=5 | -0.01,1 | 0.00,1 | " +
					"2.00,1 | 12.75,1 | NULL,1",
			},
			why: "a stage's GROUP BY key is a NAME with no slot: the outer aggregate reads " +
				"`a` off a stream that publishes it twice and takes the first, the key — " +
				"with the key's declared type on the wire",
		},
		{
			name: "968 PINNED the same at 5000 rows, through a real shuffle",
			sql: `SELECT t.g AS tg, COUNT(*) AS n FROM (SELECT x.g AS b, SUM(x.c_i64) AS g ` +
				`FROM typemx x GROUP BY x.g) t GROUP BY t.g ORDER BY tg LIMIT 3`,
			want: "cols=[tg:DECIMAL(38,0) n:INT64] rows=3 | 929156787462,1 | " +
				"1591177773519,1 | 1592105776303,1",
			pin: map[string]string{
				"dag":     "cols=[tg:INT32 n:INT64] rows=3 | 0,1 | 1,1 | 2,1",
				"dagshuf": "cols=[tg:INT32 n:INT64] rows=3 | 0,1 | 1,1 | 2,1",
			},
			why: "same site as the cell above, with the partial→final re-aggregation and a " +
				"shuffle between; the declared type moves with the value",
		},
		{
			name: "968 PINNED a CTE of the collision consumed twice in one join",
			sql: `WITH g AS (SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a) ` +
				`SELECT g1.a AS a1, g2.b AS b2 FROM g g1 JOIN g g2 ON g1.b = g2.b ORDER BY a1`,
			want: "cols=[a1:DECIMAL(38,4) b2:DECIMAL(9,2)] rows=4 | -0.0100,-0.01 | " +
				"0.0000,0.00 | 10.0000,2.00 | 38.2500,12.75",
			pin: map[string]string{
				spilledArm: "ERR building physical plan: building hash table",
				"dag": "cols=[a1:DECIMAL(9,2) b2:DECIMAL(9,2)] rows=4 | -0.01,-0.01 | " +
					"0.00,0.00 | 2.00,2.00 | 12.75,12.75",
				"dagshuf": "cols=[a1:DECIMAL(9,2) b2:DECIMAL(9,2)] rows=4 | -0.01,-0.01 | " +
					"0.00,0.00 | 2.00,2.00 | 12.75,12.75",
			},
			why: "a join stage's projection reads `g1.a` off the two exchanges of one " +
				"aggregate and takes the first column of that name; the SPILLED arm's " +
				"refusal is the 512 KiB budget on a self-join, identical at bb8635a4",
		},
		{
			name: "968 ctl the same CTE consumed ONCE",
			sql: `WITH g AS (SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a) ` +
				`SELECT g1.a AS a1, g1.b AS b1 FROM g g1 ORDER BY a1`,
			want: "cols=[a1:DECIMAL(38,4) b1:DECIMAL(9,2)] rows=5 | -0.0100,-0.01 | " +
				"0.0000,0.00 | 1.0000,NULL | 10.0000,2.00 | 38.2500,12.75",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1",
			},
		},
		{
			name: "968 PINNED DISTINCT together with LIMIT",
			sql: `SELECT DISTINCT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a ` +
				`ORDER BY a LIMIT 3`,
			want: cols + " rows=3 | -0.01,-0.0100 | 0.00,0.0000 | NULL,1.0000",
			pin: map[string]string{
				"dag":     cols + " rows=3 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000",
				"dagshuf": cols + " rows=3 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000",
			},
			why: "the coordinator's post-gather DISTINCT and LIMIT run over the gathered " +
				"rows by NAME; DISTINCT alone and LIMIT alone are right (both are cells " +
				"above), and only the pair takes the top three in the KEY's order",
		},
		{
			name: "968 PINNED DISTINCT with LIMIT at 5000 rows",
			sql: `SELECT DISTINCT x.g AS b, SUM(x.c_i64) AS g FROM typemx x GROUP BY x.g ` +
				`ORDER BY g LIMIT 3`,
			want: "cols=[b:INT32 g:DECIMAL(38,0)] rows=3 | NULL,929156787462 | " +
				"3,1591177773519 | 2,1592105776303",
			pin: map[string]string{
				"dag": "cols=[b:INT32 g:DECIMAL(38,0)] rows=3 | 2,1592105776303 | " +
					"0,1593407780209 | 1,1597875793613",
				"dagshuf": "cols=[b:INT32 g:DECIMAL(38,0)] rows=3 | 2,1592105776303 | " +
					"0,1593407780209 | 1,1597875793613",
			},
			why: "same site; a DIFFERENT ROW SET, because the LIMIT is taken in the key's " +
				"order and the two smallest sums never reach the client",
		},
		{
			name: "968 PINNED a window ARGUMENT over the colliding aggregate",
			sql: `SELECT x.a AS b, SUM(x.b) AS a, SUM(SUM(x.b)) OVER () AS tot ` +
				`FROM decpair x GROUP BY x.a ORDER BY a`,
			want: "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) tot:DECIMAL(38,2)] rows=5 | " +
				"-0.01,-0.0100,49.2400 | 0.00,0.0000,49.2400 | NULL,1.0000,49.2400 | " +
				"2.00,10.0000,49.2400 | 12.75,38.2500,49.2400",
			pin: map[string]string{
				"single": "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) tot:DECIMAL(38,2)] rows=5 | " +
					"-0.01,-0.0100,14.74 | 0.00,0.0000,14.74 | NULL,1.0000,14.74 | " +
					"2.00,10.0000,14.74 | 12.75,38.2500,14.74",
				spilledArm: "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) tot:DECIMAL(38,2)] rows=5 | " +
					"-0.01,-0.0100,14.74 | 0.00,0.0000,14.74 | NULL,1.0000,14.74 | " +
					"2.00,10.0000,14.74 | 12.75,38.2500,14.74",
				"dag": "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) tot:DECIMAL(38,2)] rows=5 | " +
					"-0.01,-0.0100,14.74 | 0.00,0.0000,14.74 | NULL,1.0000,14.74 | " +
					"2.00,10.0000,14.74 | 12.75,38.2500,14.74",
				"dagshuf": "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) tot:DECIMAL(38,2)] rows=5 | " +
					"-0.01,-0.0100,14.74 | 0.00,0.0000,14.74 | NULL,1.0000,14.74 | " +
					"2.00,10.0000,14.74 | 12.75,38.2500,14.74",
			},
			why: "a window's ARGUMENT is a plain string on `exec.WindowColumn` with no slot " +
				"to carry a position, so it binds the first column of the name and sums " +
				"the KEYS: 14.74 for PostgreSQL's 49.2400, on all four arms",
		},
		{
			name: "968 PINNED a sort over a derived star of the collision",
			sql: `SELECT t.* FROM (SELECT x.a AS b, SUM(x.b) AS a FROM decpair x ` +
				`GROUP BY x.a) t ORDER BY t.a`,
			want: five,
			pin: map[string]string{
				"dag": cols + " rows=5 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000 | " +
					"12.75,38.2500 | NULL,1.0000",
				"dagshuf": cols + " rows=5 | -0.01,-0.0100 | 0.00,0.0000 | 2.00,10.0000 | " +
					"12.75,38.2500 | NULL,1.0000",
			},
			why: "the star expands to the block's two output names and the sort term `t.a` " +
				"reaches the aggregate's stream as `a`, where the slot pass has no SELECT " +
				"item to read a class from",
		},
		{
			// TWO AGGREGATES BOTH ALIASED `a`, beside the key that publishes
			// that name too — three columns of one name in the aggregate's
			// output. The single-process projection hands out the first and
			// the second AGGREGATE slot by class (the cursor cell above proves
			// it), so slot 2 is the SUM there; on the DAG the gather's pairing
			// takes the first column of the name and slot 2 carries the group
			// key, with the key's declared type.
			//
			// The window beside it ranks correctly on all four arms, which is
			// what says this is the deferred consumer and not the one round 2
			// closed.
			name: "968 PINNED two aggregates sharing the key's name, read on the DAG",
			sql: `SELECT x.a AS b, SUM(x.b) AS a, MIN(x.b) AS a, ` +
				`RANK() OVER (ORDER BY MIN(x.b)) AS rk FROM decpair x GROUP BY x.a ORDER BY 2`,
			want: "cols=[b:DECIMAL(9,2) a:DECIMAL(38,4) a:DECIMAL(18,4) rk:INT64] rows=5 | " +
				"-0.01,-0.0100,-0.0100,1 | 0.00,0.0000,0.0000,2 | NULL,1.0000,1.0000,3 | " +
				"2.00,10.0000,10.0000,4 | 12.75,38.2500,12.7499,5",
			pin: map[string]string{
				"dag": "cols=[b:DECIMAL(9,2) a:DECIMAL(9,2) a:DECIMAL(38,4) rk:INT64] rows=5 | " +
					"-0.01,-0.01,-0.0100,1 | 0.00,0.00,0.0000,2 | NULL,NULL,1.0000,3 | " +
					"2.00,2.00,10.0000,4 | 12.75,12.75,38.2500,5",
				"dagshuf": "cols=[b:DECIMAL(9,2) a:DECIMAL(9,2) a:DECIMAL(38,4) rk:INT64] rows=5 | " +
					"-0.01,-0.01,-0.0100,1 | 0.00,0.00,0.0000,2 | NULL,NULL,1.0000,3 | " +
					"2.00,2.00,10.0000,4 | 12.75,12.75,38.2500,5",
			},
			why: "the gather's rename pairing resolves each output name against the " +
				"aggregate's stream and takes the first of THREE columns called `a`; the " +
				"single-process projection hands out the two aggregate slots by class",
		},
		{
			// The collision read through a derived table by an EXPLICIT
			// column — no star, no outer aggregate, just `t.a`. It was wrong
			// on ALL FOUR arms at bb8635a4; the two single-process arms answer
			// now, because the projection over the aggregate binds by slot,
			// and the DAG's derived-block consumer is the same name path as
			// the cells above. `t.rk` beside it is right on every arm.
			name: "968 PINNED the collision read through a derived table by name",
			sql: `SELECT t.rk AS rk, t.a AS a FROM (SELECT x.a AS b, SUM(x.b) AS a, ` +
				`RANK() OVER (ORDER BY SUM(x.b)) AS rk FROM decpair x GROUP BY x.a) t ` +
				`ORDER BY rk`,
			want: "cols=[rk:INT64 a:DECIMAL(38,4)] rows=5 | 1,-0.0100 | 2,0.0000 | " +
				"3,1.0000 | 4,10.0000 | 5,38.2500",
			pin: map[string]string{
				"dag": "cols=[rk:INT64 a:DECIMAL(9,2)] rows=5 | 1,-0.01 | 2,0.00 | " +
					"3,NULL | 4,2.00 | 5,12.75",
				"dagshuf": "cols=[rk:INT64 a:DECIMAL(9,2)] rows=5 | 1,-0.01 | 2,0.00 | " +
					"3,NULL | 4,2.00 | 5,12.75",
			},
			why: "a derived block's consumer reads `a` off the aggregate's stream, which " +
				"publishes it twice, and takes the first; wrong on all four arms at " +
				"bb8635a4 and right on the single-process arms now",
		},
	})
}
