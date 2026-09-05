package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// spillRefusal is what the arc's 512 KiB spilled arm answers to every cross
// join over this corpus, and it is ADR-0006's routed-probe amendment (#832),
// not this arc's: a cross join's probe reads every build row, so its build
// cannot be grace-partitioned and must fit the budget. Asserted as itself
// rather than skipped.
const spillRefusal = "ERR building physical plan: building hash table: hash join build " +
	"(a cross join's probe reads every build row, so its build cannot be grace-partitioned " +
	"and cannot spill; the build must fit the budget)"

// TestF1AJoinArmPublishesTheColumnsItSelects is #780 and #770: what a join arm
// PUBLISHES is its own SELECT list, on every path.
//
// On the single-process pipeline the arm's Project is a real operator, so the
// build side the join receives IS the arm's output and nothing else can answer
// to its names. On the stage DAG a Project emits no stage (ADR-0025), so the
// stream was the arm's RAW inner columns — and the two defects here are the
// two ways that goes wrong: a COMPUTED column belongs to no scan, so the bare
// name bound the arm's raw column of that name (#780, a wrong VALUE in
// silence: 12.75 for PostgreSQL's 38.25), and a RENAME whose name the sibling
// arm also publishes has two sources, so the resolve-back-by-spelling
// convention named neither (#770, a GROUP BY key nothing emits).
//
// The controls are the other half of the claim, and they are what makes the
// fix's BOUNDARY a measurement rather than an assertion: an arm publishing a
// DISTINCT alias, an arm whose contested column is a plain RENAME with a
// source to resolve to, an UNCONTESTED rename, and an arm that is a bare
// relation must all answer exactly what they answered before.
func TestF1AJoinArmPublishesTheColumnsItSelects(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			name: "780 a computed column over an arm of bare scans",
			sql: "SELECT t.a AS ta, m.a AS ma FROM decpair t " +
				"JOIN (SELECT g.id AS id, g.a * 3 AS a FROM decpair g JOIN decpair h ON g.id = h.id) m " +
				"ON t.id = m.id ORDER BY t.id",
			want: "cols=[ta:DECIMAL(9,2) ma:DECIMAL(11,2)] rows=9 | 12.75,38.25 | 12.75,38.25 | " +
				"12.75,38.25 | -0.01,-0.03 | 2.00,6.00 | 0.00,0.00 | NULL,NULL | 12.75,38.25 | NULL,NULL",
		},
		{
			// The SAME cell with the arm on the PROBE side. #780's first fix
			// was build-side only and this shape stayed silently wrong on the
			// broadcast arm alone — the two DAG arms disagreeing with each
			// other, in value AND in declared type, which is a state that did
			// not exist before the fix. The arm's stage was ABSORBED by
			// `fuseJoinStages`, which has no field for a projection and drops
			// it, so the arm's column was un-computed one pass after it was
			// computed (round-1 review, B1).
			name: "780 the arm on the PROBE side",
			sql: "SELECT m.a AS ma, t.a AS ta FROM " +
				"(SELECT g.id AS id, g.a * 3 AS a FROM decpair g JOIN decpair h ON g.id = h.id) m " +
				"JOIN decpair t ON m.id = t.id ORDER BY m.id",
			want: "cols=[ma:DECIMAL(11,2) ta:DECIMAL(9,2)] rows=9 | 38.25,12.75 | 38.25,12.75 | " +
				"38.25,12.75 | -0.03,-0.01 | 6.00,2.00 | 0.00,0.00 | NULL,NULL | 38.25,12.75 | NULL,NULL",
		},
		{
			// The same face under an aggregate, where a wrong column is one
			// number rather than nine and nothing renders the difference.
			name: "780 the arm on the PROBE side, under SUM",
			sql: "SELECT SUM(m.a) AS s FROM " +
				"(SELECT g.id AS id, g.a * 3 AS a FROM decpair g JOIN decpair h ON g.id = h.id) m " +
				"JOIN decpair t ON m.id = t.id",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 158.97",
		},
		{
			// The probe-side distinct-alias control: nothing is contested, so
			// the arm's column is found by its own name whatever the fusion
			// does, and it was right before the fix too.
			name: "780 control: the PROBE-side arm publishing a DISTINCT alias",
			sql: "SELECT m.z AS mz, t.a AS ta FROM " +
				"(SELECT g.id AS id, g.a * 3 AS z FROM decpair g JOIN decpair h ON g.id = h.id) m " +
				"JOIN decpair t ON m.id = t.id ORDER BY m.id",
			want: "cols=[mz:DECIMAL(11,2) ta:DECIMAL(9,2)] rows=9 | 38.25,12.75 | 38.25,12.75 | " +
				"38.25,12.75 | -0.03,-0.01 | 6.00,2.00 | 0.00,0.00 | NULL,NULL | 38.25,12.75 | NULL,NULL",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1"},
		},
		{
			// PINNED, and pre-existing: `SELECT *` over a materialized arm
			// publishes the arm's INNER columns as well as the ones it
			// selects — 10 where PostgreSQL and the single path have 8. The
			// arm's stage passes its whole stream through and appends the
			// computed alias, deliberately: every DAG resolver reads those
			// source names, and narrowing the passthrough to the arm's SELECT
			// list would take them away from resolvers this arc does not
			// move. So the section's doctrine — one column per SELECT item —
			// is true of what the arm PUBLISHES and not of what its stage
			// SHIPS, and `SELECT *` is where the difference is visible.
			// Fail-on-agree: the day the DAG publishes 8, delete this.
			name: "780 PINNED: SELECT * over a materialized arm ships the arm's inner columns",
			sql: "SELECT * FROM decpair t JOIN " +
				"(SELECT g.id AS id, g.a * 3 AS a FROM decpair g JOIN decpair h ON g.id = h.id) m " +
				"ON t.id = m.id ORDER BY t.id",
			want: "cols=[id:INT64 a:DECIMAL(9,2) b:DECIMAL(18,4) s:STRING f:FLOAT64 r:FLOAT32 " +
				"m.id:INT64 m.a:DECIMAL(11,2)] rows=9 | 1,12.75,12.7500,1.50,1.5,1.5,1,38.25 | " +
				"2,12.75,12.7501,1.5,100,100,2,38.25 | 3,12.75,12.7499,abc,-3.5,-3.5,3,38.25 | " +
				"4,-0.01,-0.0100,10,0.5,0.5,4,-0.03 | 5,2.00,10.0000,9,9.5,9.5,5,6.00 | " +
				"6,0.00,0.0000,1.500,20,20,6,0.00 | 7,NULL,1.0000,0,7.25,7.25,7,NULL | " +
				"8,12.75,NULL,-1,NULL,NULL,8,38.25 | 9,NULL,NULL,1.5,3.5,3.5,9,NULL",
			pin: map[string]string{
				"dag": "cols=[id:INT64 a:DECIMAL(9,2) b:DECIMAL(18,4) s:STRING f:FLOAT64 r:FLOAT32 " +
					"m.id:INT64 h.a:DECIMAL(9,2) h.id:INT64 m.a:DECIMAL(11,2)] rows=9 | " +
					"1,12.75,12.7500,1.50,1.5,1.5,1,12.75,1,38.25 | " +
					"2,12.75,12.7501,1.5,100,100,2,12.75,2,38.25 | " +
					"3,12.75,12.7499,abc,-3.5,-3.5,3,12.75,3,38.25 | " +
					"4,-0.01,-0.0100,10,0.5,0.5,4,-0.01,4,-0.03 | " +
					"5,2.00,10.0000,9,9.5,9.5,5,2.00,5,6.00 | " +
					"6,0.00,0.0000,1.500,20,20,6,0.00,6,0.00 | " +
					"7,NULL,1.0000,0,7.25,7.25,7,NULL,7,NULL | " +
					"8,12.75,NULL,-1,NULL,NULL,8,12.75,8,38.25 | " +
					"9,NULL,NULL,1.5,3.5,3.5,9,NULL,9,NULL",
				"dagshuf": "cols=[id:INT64 a:DECIMAL(9,2) b:DECIMAL(18,4) s:STRING f:FLOAT64 r:FLOAT32 " +
					"m.id:INT64 h.a:DECIMAL(9,2) h.id:INT64 m.a:DECIMAL(11,2)] rows=9 | " +
					"1,12.75,12.7500,1.50,1.5,1.5,1,12.75,1,38.25 | " +
					"2,12.75,12.7501,1.5,100,100,2,12.75,2,38.25 | " +
					"3,12.75,12.7499,abc,-3.5,-3.5,3,12.75,3,38.25 | " +
					"4,-0.01,-0.0100,10,0.5,0.5,4,-0.01,4,-0.03 | " +
					"5,2.00,10.0000,9,9.5,9.5,5,2.00,5,6.00 | " +
					"6,0.00,0.0000,1.500,20,20,6,0.00,6,0.00 | " +
					"7,NULL,1.0000,0,7.25,7.25,7,NULL,7,NULL | " +
					"8,12.75,NULL,-1,NULL,NULL,8,12.75,8,38.25 | " +
					"9,NULL,NULL,1.5,3.5,3.5,9,NULL,9,NULL",
			},
			why: "the arm's stage ships its whole stream so every DAG resolver keeps the " +
				"source names it reads; pre-existing, base ships 10 too",
		},
		{
			name: "780 control: the same arm publishing a DISTINCT alias",
			sql: "SELECT t.a AS ta, m.z AS mz FROM decpair t " +
				"JOIN (SELECT g.id AS id, g.a * 3 AS z FROM decpair g JOIN decpair h ON g.id = h.id) m " +
				"ON t.id = m.id ORDER BY t.id",
			want: "cols=[ta:DECIMAL(9,2) mz:DECIMAL(11,2)] rows=9 | 12.75,38.25 | 12.75,38.25 | " +
				"12.75,38.25 | -0.01,-0.03 | 2.00,6.00 | 0.00,0.00 | NULL,NULL | 12.75,38.25 | NULL,NULL",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1"},
		},
		{
			name: "780 control: the contested column is a RENAME, not computed",
			sql: "SELECT t.a AS ta, m.a AS ma FROM decpair t " +
				"JOIN (SELECT g.id AS id, g.b AS a FROM decpair g JOIN decpair h ON g.id = h.id) m " +
				"ON t.id = m.id ORDER BY t.id",
			want: "cols=[ta:DECIMAL(9,2) ma:DECIMAL(18,4)] rows=9 | 12.75,12.7500 | 12.75,12.7501 | " +
				"12.75,12.7499 | -0.01,-0.0100 | 2.00,10.0000 | 0.00,0.0000 | NULL,1.0000 | " +
				"12.75,NULL | NULL,NULL",
		},
		{
			// #770, PINNED on the shuffled arm and DEFERRED. The filing says
			// both DAG arms return 2 rows with a NULL; on this base the
			// broadcast arm is RIGHT and the shuffled arm is LOUD, so the
			// tree moved and the shape is no longer silent.
			//
			// The mechanism is the one above — two arms publish `w`, so the
			// resolve-back-to-the-source-name convention has two sources and
			// names neither — and materializing a CONTESTED rename is the
			// repair that follows from it. It was built and WITHDRAWN: it
			// closes this cell and moves THREE shapes in
			// `TestAWindowBetweenTheSelectListAndItsJoinThreeArms` from right
			// to wrong (two answer NULL for a qualified reference resolved in
			// the other arm; one refuses with a sort key that no longer
			// exists), because the needed-column lists and the stream's
			// spelling stop agreeing the moment a resolver returns the
			// qualified name. Right → wrong is a blocker; ADR-0025 carries
			// the four-configuration census.
			//
			// Fail-on-agree: the day the shuffled arm answers, this pin is
			// stale and must be deleted rather than kept.
			name: "770 PINNED: DISTINCT over a join whose two arms publish one alias",
			sql: "SELECT DISTINCT x.w AS xw, y.w AS yw FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS w FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yw",
			want: "cols=[xw:DECIMAL(9,2) yw:DECIMAL(22,4)] rows=5 | 2.00,1000.0000 | " +
				"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL",
			pin: map[string]string{"dagshuf": `ERR native DAG: stage join-8 (hash_join)`},
			why: "two arms publish `w` and neither is materialized; see the arc report's " +
				"DEFERRED item for the repair that was built and withdrawn",
		},
		{
			name: "770 control: two arms whose aliases are DISTINCT",
			sql: "SELECT DISTINCT x.w AS xw, y.v AS yv FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN (SELECT id, b*100 AS v FROM decpair) y ON x.id = y.id " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, yv",
			want: "cols=[xw:DECIMAL(9,2) yv:DECIMAL(22,4)] rows=5 | 2.00,1000.0000 | " +
				"12.75,1274.9900 | 12.75,1275.0000 | 12.75,1275.0100 | 12.75,NULL",
		},
		{
			name: "770 control: one derived arm, one BARE relation — the rename is UNCONTESTED",
			sql: "SELECT DISTINCT x.w AS xw, u.a AS ua FROM (SELECT id, a AS w FROM decpair) x " +
				"JOIN decpair u ON x.id = u.id WHERE x.w > 1 ORDER BY xw, ua",
			want: "cols=[xw:DECIMAL(9,2) ua:DECIMAL(9,2)] rows=2 | 2.00,2.00 | 12.75,12.75",
		},
	})
}

// TestF1AWindowKeyGuardReadsProvenanceNotSpelling is #745: `__winkey_N` is the
// name the planner MINTS for a materialized window key, and a table written
// before that namespace was reserved can STORE a column of that name. The
// guard read the name and refused a bound reference on both DAG arms for a
// query the single-process path answers.
//
// The three cells are the guard's whole claim: a stored column whose name is
// in the reserved family is BOUND and must answer; an ordinary column is the
// control; and a genuinely MATERIALIZED key (`PARTITION BY id % 2`), which the
// fragment computes into a real `__winkey_N` slot, must still answer — that is
// the shape #585's guard was added for, and a fix that let it through would
// trade one loud failure for a window over one partition.
func TestF1AWindowKeyGuardReadsProvenanceNotSpelling(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			name: "745 PARTITION BY a STORED __winkey_1",
			sql:  "SELECT id, SUM(id) OVER (PARTITION BY __winkey_1) AS w FROM oldtab ORDER BY id",
			want: "cols=[id:INT64 w:FLOAT64] rows=4 | 1,1 | 2,2 | 3,3 | 4,4",
		},
		{
			name: "745 control: PARTITION BY an ordinary stored column",
			sql:  "SELECT id, SUM(id) OVER (PARTITION BY plain) AS w FROM oldtab ORDER BY id",
			want: "cols=[id:INT64 w:FLOAT64] rows=4 | 1,1 | 2,2 | 3,3 | 4,4",
		},
		{
			name: "745 control: a key the FRAGMENT really materializes still answers",
			sql:  "SELECT id, SUM(id) OVER (PARTITION BY id % 2) AS w FROM oldtab ORDER BY id",
			want: "cols=[id:INT64 w:FLOAT64] rows=4 | 1,4 | 2,6 | 3,4 | 4,6",
		},
		{
			name: "745 control: the same stored column as a GROUP BY key",
			sql:  "SELECT __winkey_1 AS k, COUNT(*) AS n FROM oldtab GROUP BY __winkey_1 ORDER BY k",
			want: "cols=[k:INT64 n:INT64] rows=4 | 10,1 | 20,1 | 30,1 | 40,1",
		},
	})
}

// TestF1AKeylessJoinAsksForWhatItNeeds is #480: a join with NO KEYS — a cross
// join, and every non-equi join, whose whole condition is lifted into a
// residual filter — was asked for a co-location it does not need and refused
// at plan time for a query PostgreSQL answers.
//
// Both halves of the repair are asserted here and the second is why the first
// is not enough. Dropping the probe's requirement alone lets the plan build
// and answers 0 for PostgreSQL's 12 and 1625 for its 38632, because each task
// then meets only its own slice of the build. What such a consumer needs is
// the BUILD replicated, which is the broadcast-join rule, and the CROSS JOIN
// cell is the one that measures it: its answer is the full product.
//
// The equi-join control is the boundary from the other side — a join WITH keys
// keeps the co-location requirement it had, and must not gain a replicate.
func TestF1AKeylessJoinAsksForWhatItNeeds(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			name: "480 a non-equi join fed by two GROUP BY aggregates",
			sql: "SELECT COUNT(*) AS c FROM (SELECT c_i32 FROM typemx GROUP BY c_i32) u " +
				"JOIN (SELECT k FROM typemx_dim GROUP BY k) v ON u.c_i32 < v.k",
			want: "cols=[c:INT64] rows=1 | 12",
		},
		{
			name: "480 the same shape spelled DISTINCT",
			sql: "SELECT COUNT(*) AS c FROM (SELECT DISTINCT c_i32 FROM typemx) u " +
				"JOIN (SELECT DISTINCT k FROM typemx_dim) v ON u.c_i32 < v.k",
			want: "cols=[c:INT64] rows=1 | 12",
			pin:  map[string]string{spilledArm: spillRefusal},
			why:  "ADR-0006's cross-join build budget (#832), not this arc's",
		},
		{
			name: "480 an explicit CROSS JOIN — the cell that measures the full product",
			sql: "SELECT COUNT(*) AS c FROM (SELECT c_i32 FROM typemx GROUP BY c_i32) u " +
				"CROSS JOIN (SELECT k FROM typemx_dim GROUP BY k) v",
			want: "cols=[c:INT64] rows=1 | 38632",
		},
		{
			// #480 with the rows PROJECTED and ORDERED. The COUNT(*) cells
			// above cannot see a wrong ORDER, and a wrong order is what the
			// first cut of this fix produced: a sort fused into the keyless
			// join was applied per PARTITION, because the join inherited its
			// probe's hash-partitioned label while running one task
			// (round-1 review, B2). `6,7` came back before `3,4`,
			// deterministically, on both DAG arms.
			name: "480 the same join with its rows ORDERED",
			sql: "SELECT u.c_i32 AS uc, v.k AS vk FROM (SELECT DISTINCT c_i32 FROM typemx) u " +
				"JOIN (SELECT DISTINCT k FROM typemx_dim) v ON u.c_i32 < v.k ORDER BY uc, vk",
			want: "cols=[uc:INT32 vk:INT32] rows=12 | 0,1 | 0,2 | 0,3 | 0,4 | 0,5 | 0,6 | 0,7 | " +
				"3,4 | 3,5 | 3,6 | 3,7 | 6,7",
			pin: map[string]string{spilledArm: spillRefusal},
			why: "ADR-0006's cross-join build budget (#832), not this arc's",
		},
		{
			name: "480 the same rows DESCENDING",
			sql: "SELECT u.c_i32 AS uc, v.k AS vk FROM (SELECT DISTINCT c_i32 FROM typemx) u " +
				"JOIN (SELECT DISTINCT k FROM typemx_dim) v ON u.c_i32 < v.k ORDER BY uc DESC, vk DESC",
			want: "cols=[uc:INT32 vk:INT32] rows=12 | 6,7 | 3,7 | 3,6 | 3,5 | 3,4 | 0,7 | 0,6 | " +
				"0,5 | 0,4 | 0,3 | 0,2 | 0,1",
			pin: map[string]string{spilledArm: spillRefusal},
			why: "ADR-0006's cross-join build budget (#832), not this arc's",
		},
		{
			// One key, because the wrong order the review found leads with
			// the `0` group and a single-key spelling still has to place the
			// `3` and `6` groups after it.
			name: "480 the same rows on a SINGLE ordering key",
			sql: "SELECT u.c_i32 AS uc, v.k AS vk FROM (SELECT DISTINCT c_i32 FROM typemx) u " +
				"JOIN (SELECT DISTINCT k FROM typemx_dim) v ON u.c_i32 < v.k ORDER BY uc, vk LIMIT 9",
			want: "cols=[uc:INT32 vk:INT32] rows=9 | 0,1 | 0,2 | 0,3 | 0,4 | 0,5 | 0,6 | 0,7 | 3,4 | 3,5",
			pin:  map[string]string{spilledArm: spillRefusal},
			why:  "ADR-0006's cross-join build budget (#832), not this arc's",
		},
		{
			name: "480 control: the EQUI-join twin keeps its co-location",
			sql: "SELECT COUNT(*) AS c FROM (SELECT DISTINCT c_i32 AS a FROM typemx) x " +
				"JOIN (SELECT DISTINCT k AS b FROM typemx_dim) y ON x.a = y.b",
			want: "cols=[c:INT64] rows=1 | 3",
			pin: map[string]string{spilledArm: "ERR building physical plan: building hash table: " +
				"hash join build: query: memory budget exceeded"},
			why: "the 512 KiB build budget, not this arc's",
		},
	})
}

// TestF1AWindowDeclaresTheSameTypeThroughADerivedTable is #796: the typing
// walk that resolves a window's input column stopped at a derived table's
// Project, so a window ONE nesting level above its scan declared float8 where
// the same window directly over the table declares numeric — and the aggregate
// reading it inherited the float box, on every arm, where PostgreSQL answers
// numeric.
//
// The last cell is #813's surviving half, PINNED rather than fixed: PostgreSQL
// declares `sum(int8) over ()` numeric and `sum(int4) over ()` bigint, and
// every arm here declares float8. It is a DECLARATION divergence with the
// right digits, it is uniform across the four arms, and closing it is an exact
// integer accumulator in the window operator (exec.windowAccOutputType) —
// recorded in this arc's report and in ADR-0012's divergence list. The pin
// fails the day the declaration moves, which is what makes it a proof.
func TestF1AWindowDeclaresTheSameTypeThroughADerivedTable(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			name: "796 a computed aggregate argument over a window whose INPUT is a derived table",
			sql: "SELECT SUM(w*2) AS s FROM (SELECT id, SUM(a) OVER () AS w " +
				"FROM (SELECT id, a FROM decpair) t) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 953.82",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1"},
		},
		{
			name: "796 control: the same window DIRECTLY over the scan",
			sql:  "SELECT SUM(w*2) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 953.82",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1"},
		},
		{
			name: "796 the derived table RENAMES the window's input column",
			sql: "SELECT SUM(w*2) AS s FROM (SELECT id, SUM(v) OVER () AS w " +
				"FROM (SELECT id, a AS v FROM decpair) t) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 953.82",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1"},
		},
		{
			name: "796 MIN over a derived table keeps the input's own declaration",
			sql:  "SELECT MIN(a) OVER () AS m FROM (SELECT id, a FROM decpair) t ORDER BY 1 LIMIT 1",
			want: "cols=[m:DECIMAL(9,2)] rows=1 | -0.01",
		},
		{
			name: "813 a CAST declares its target on every path",
			sql:  "SELECT CAST(SUM(id) OVER () AS BIGINT) AS c FROM decpair ORDER BY 1 LIMIT 1",
			want: "cols=[c:INT64] rows=1 | 45",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1"},
		},
		{
			// #813 pinned on the VALUE, which is what it actually is. The
			// first version of this pin, and ADR-0012's entry with it, said
			// "the digits are right"; over `numwidth`, whose `w_i64` carries
			// values past 2^53 on purpose, they are not. The window's float64
			// accumulator loses them where the GROUPED spelling below keeps
			// them exactly — one question, two spellings, two numbers.
			// Fail-on-agree: the day the accumulator is exact this reads
			// 1000800157666874.2222 and the cell must be deleted.
			name: "813 PINNED VALUE: AVG(int8) OVER () loses digits the grouped spelling keeps",
			sql:  "SELECT AVG(w_i64) OVER () AS w FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[w:FLOAT64] rows=1 | 1.0008001576668742e+15",
			why: "PostgreSQL 17 and the grouped spelling both answer " +
				"1000800157666874.2222 numeric; exec.windowAccOutputType gives an integer " +
				"input a float64 accumulator. DEFERRED with mechanism (ADR-0012).",
		},
		{
			// The control that makes the cell above a divergence rather than
			// a rendering: the same aggregate, not windowed, is exact.
			name: "813 control: the GROUPED spelling of the same aggregate is exact",
			sql:  "SELECT AVG(w_i64) AS w FROM numwidth",
			want: "cols=[w:DECIMAL(38,4)] rows=1 | 1000800157666874.2222",
		},
		{
			name: "813 control: the GROUPED integer SUM is exact",
			sql:  "SELECT SUM(w_i64) AS w FROM numwidth",
			want: "cols=[w:DECIMAL(38,0)] rows=1 | 9007201419001868",
		},
		{
			name: "813 PINNED: SUM(int8) OVER () declares float8 where PostgreSQL declares numeric",
			sql:  "SELECT SUM(id) OVER () AS w FROM decpair ORDER BY 1 LIMIT 1",
			want: "cols=[w:FLOAT64] rows=1 | 45",
			why: "the window accumulator is float64 for an integer input " +
				"(exec.windowAccOutputType); declaring the exact type without moving " +
				"the carrier is the #361 silent-write class. DEFERRED with mechanism.",
		},
	})
}

// TestF1JoinKeysThroughDerivedRenamesAndAggregates is the RATCHET for #681 and
// #730: a join key that is a computed aggregate alias, and a join key naming a
// derived table's rename reaching that arm's scan fragment. Both were loud
// stage-DAG failures for queries PostgreSQL answers; both answer on all four
// arms at this arc's base, and neither is re-fixed here.
//
// They are gated rather than closed silently because this arc rewrites the two
// resolvers they live in (`resolveShuffleKey`'s and `resolveAggInputName`'s
// join arms), and a shape that is right by accident today is one nothing would
// notice going wrong tomorrow.
func TestF1JoinKeysThroughDerivedRenamesAndAggregates(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			name: "681 the join key is a computed aggregate alias",
			sql: "SELECT COUNT(*) AS n FROM numwidth a " +
				"JOIN (SELECT w_key AS g, COUNT(*)+1 AS k FROM numwidth GROUP BY w_key) b " +
				"ON a.w_i64 = b.k",
			want: "cols=[n:INT64] rows=1 | 10",
		},
		{
			name: "730 the RIGHT arm's key is a derived rename",
			sql: "SELECT SUM(CASE WHEN x.s = '1.50' THEN y.w ELSE 0 END) AS v " +
				"FROM (SELECT s, a AS v FROM decpair) x " +
				"JOIN (SELECT s AS ss, b * 2 AS w FROM decpair) y ON x.s = y.ss",
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 25.5000",
		},
		{
			name: "730 the LEFT arm's key is a derived rename",
			sql: "SELECT COUNT(*) AS n FROM (SELECT s AS ss, a FROM decpair) x " +
				"JOIN decpair y ON x.ss = y.s",
			want: "cols=[n:INT64] rows=1 | 11",
		},
		{
			name: "730 BOTH arms' keys are derived renames",
			sql: "SELECT COUNT(*) AS n FROM (SELECT s AS l FROM decpair) x " +
				"JOIN (SELECT s AS r FROM decpair) y ON x.l = y.r",
			want: "cols=[n:INT64] rows=1 | 11",
		},
	})
}

// TestF1ADistributedOrderByOrdersEveryContainerType is #644: the gather's
// comparator had an arm for every scalar type after #548 and #642 and none for
// ARRAY, MAP, ROW or VECTOR, so those fell through to `extractFloat64`, which
// answers 0 for both rows — a TIE on every row. `slices.SortFunc` is not
// stable, so a merged `ORDER BY <container column>` came back in an ARBITRARY
// order where the single-process sort orders it element-wise. Silent.
//
// It drives `Coordinator.sortBatches` directly, which is what
// `TestSortBatchesOrdersMissingTypes` (#548) and `TestSortBatchesOrdersInt64Carriers`
// (#642) do for the same comparator and for the same reason: the SQL shapes
// that reach it are narrow — the DISTINCT gather (`dedupGatherResult`) and the
// async door's merge (`GetQueryResult`) — and a gate whose trigger is a plan
// SHAPE cannot be relied on to fire. Measured: every `SELECT DISTINCT <container>
// ... ORDER BY` shape tried on this base is lowered to an aggregate and a sort
// STAGE and never reaches this function at all, so a query-level cell would
// have passed with the arm deleted.
//
// The expected orders are the single-process engine's own
// (`kernel.CompareValuesAt` — element-wise, an empty container first, a NULL
// element last, a shorter prefix before its extension), which PostgreSQL 17's
// array order was re-measured against for the ARRAY cases. NULL placement is
// asserted with them and is absolute: NULLS LAST for ASC, NULLS FIRST for
// DESC, and it must not flip with the direction.
func TestF1ADistributedOrderByOrdersEveryContainerType(t *testing.T) {
	strElem := &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}
	mapElem := &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
		{Name: "key", Type: parquet.TypeString},
		{Name: "value", Type: parquet.TypeInt64, Nullable: true},
	}}

	for _, tc := range []struct {
		name    string
		col     parquet.Column
		rows    []map[string]any
		orderBy logical.OrderExpr
		want    []int64
	}{
		{
			name: "array asc",
			col:  parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true, ElementType: strElem},
			rows: []map[string]any{
				{"id": int64(1), "k": []any{"b"}},
				{"id": int64(2), "k": []any{}},
				{"id": int64(3), "k": nil},
				{"id": int64(4), "k": []any{"a", "z"}},
				{"id": int64(5), "k": []any{"a"}},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			// [] < [a] < [a z] < [b], NULL last.
			want: []int64{2, 5, 4, 1, 3},
		},
		{
			name: "array desc",
			col:  parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true, ElementType: strElem},
			rows: []map[string]any{
				{"id": int64(1), "k": []any{"b"}},
				{"id": int64(2), "k": []any{}},
				{"id": int64(3), "k": nil},
				{"id": int64(4), "k": []any{"a", "z"}},
				{"id": int64(5), "k": []any{"a"}},
			},
			orderBy: logical.OrderExpr{Column: "k", Desc: true},
			want:    []int64{3, 1, 4, 5, 2},
		},
		{
			// PostgreSQL 17, measured: `{a} < {a,b} < {a,NULL} < {NULL,a}`.
			// A NULL ELEMENT sorts after any non-NULL one and a shorter
			// prefix sorts before its extension — which is `compareElemAt`'s
			// in-container rule, and NOT the column-level NULL rule (that one
			// is absolute and resolved before this comparator's type switch).
			name: "array with NULL elements, PostgreSQL's in-container rule",
			col:  parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true, ElementType: strElem},
			rows: []map[string]any{
				{"id": int64(1), "k": []any{nil, "a"}},
				{"id": int64(2), "k": []any{"a", "b"}},
				{"id": int64(3), "k": []any{"a", nil}},
				{"id": int64(4), "k": []any{"a"}},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{4, 2, 3, 1},
		},
		{
			name: "row asc, field by field",
			col: parquet.Column{Name: "k", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "a", Type: parquet.TypeString, Nullable: true},
				{Name: "b", Type: parquet.TypeInt64, Nullable: true},
			}},
			rows: []map[string]any{
				{"id": int64(1), "k": map[string]any{"a": "m", "b": int64(2)}},
				{"id": int64(2), "k": map[string]any{"a": "m", "b": int64(1)}},
				{"id": int64(3), "k": map[string]any{"a": "a", "b": int64(9)}},
				{"id": int64(4), "k": nil},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{3, 2, 1, 4},
		},
		{
			name: "map asc",
			col:  parquet.Column{Name: "k", Type: parquet.TypeMap, Nullable: true, ElementType: mapElem},
			rows: []map[string]any{
				{"id": int64(1), "k": []any{map[string]any{"key": "k1", "value": int64(1)}}},
				{"id": int64(2), "k": []any{map[string]any{"key": "k0", "value": int64(9)}}},
				{"id": int64(3), "k": nil},
				{"id": int64(4), "k": []any{map[string]any{"key": "k0", "value": int64(1)}}},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{4, 2, 1, 3},
		},
		{
			name: "vector asc",
			col:  parquet.Column{Name: "k", Type: parquet.TypeVector, Nullable: true, Dimension: 2},
			rows: []map[string]any{
				{"id": int64(1), "k": []float32{1, 5}},
				{"id": int64(2), "k": []float32{1, 2}},
				{"id": int64(3), "k": nil},
				{"id": int64(4), "k": []float32{0, 9}},
			},
			orderBy: logical.OrderExpr{Column: "k"},
			want:    []int64{4, 2, 1, 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}, tc.col}
			b := batch.FromRows(schema, tc.rows)
			// The premise: without a container arm every pair ties, and a
			// comparator that ties is only VISIBLE when the input order is
			// not already the answer. These rows are deliberately shuffled.
			c := &Coordinator{}
			c.sortBatches([]*batch.RecordBatch{b}, []string{"id", "k"},
				map[string]int{"id": 0, "k": 1}, []logical.OrderExpr{tc.orderBy})
			var got []int64
			for i := 0; i < b.ActiveLen(); i++ {
				row := i
				if b.Sel != nil {
					row = int(b.Sel[i])
				}
				got = append(got, b.Columns[0].Int64Data[row])
			}
			if len(got) != len(tc.want) {
				t.Fatalf("sortBatches produced %d rows, want %d", len(got), len(tc.want))
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("id order = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestF1AContainerOrderAgreesOnEveryArm is the query-level RATCHET beside it:
// the same ORDER BY through the four execution arms, asserted as an ORDER and
// not as a row set. It does not reach `compareBatchRows` on this base — the
// DISTINCT is lowered to an aggregate and the ORDER BY to a sort stage — and
// that is exactly why it is a ratchet and not the gate: the day a plan change
// routes one of these through the gather's merge, this is what notices.
func TestF1AContainerOrderAgreesOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	for _, col := range []string{"c_arr", "c_row", "c_map", "c_vec"} {
		for _, dir := range []string{"", " DESC"} {
			t.Run(col+dir, func(t *testing.T) {
				sql := "SELECT DISTINCT " + col + " FROM typemx_nested WHERE id < 400 ORDER BY " + col + dir
				var want string
				for _, arm := range arms {
					got, err := arm.run(sql)
					if err != nil {
						t.Fatalf("%s\n  arm %s: %v", sql, arm.name, err)
					}
					if arm.name == "single" {
						want = got
						continue
					}
					if got != want {
						t.Errorf("%s\n  arm  %s disagrees with the single-process order\n"+
							"  got  %s\n  want %s", sql, arm.name, got, want)
					}
				}
				if want == "" {
					t.Fatalf("%s: the single arm produced no answer", sql)
				}
			})
		}
	}
}
