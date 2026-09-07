package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// THE HIDDEN SLOT — arc J1, on FOUR ARMS against live PostgreSQL 17.
//
// A decorrelated LATERAL promotes its correlated equality into the join
// condition, so the join can key on the inner value only if the subquery's
// output PUBLISHES it. Publishing it under the SOURCE COLUMN's own name is
// what this lowering did, and a name is not a handle:
//
//   - `SELECT MAX(t.id) AS g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k`
//     makes the aggregate emit [g(key), g(max), c] and every consumer above
//     resolves by name, so `s.g` read the KEY — `0,1,2,…` where PostgreSQL 17
//     answers `4998,4999,4993,…` (#956);
//   - `SELECT amount AS order_id FROM lat_item WHERE order_id = o.id` already
//     answers to the key's name while holding a different value, so no key was
//     materialized at all and the join keyed on nothing — ZERO rows for
//     PostgreSQL's four, and its LEFT twin every amount NULL (#767's mirror).
//
// The key is published under `__key_N` now: the reserved namespace, which no
// query can spell and no alias can shadow. For an AGGREGATED lateral the
// aggregate PUBLISHES the key under the slot while still RESOLVING it by the
// source column — ADR-0026 §2's pair of names, used in the direction §3a asks
// for.
//
// Every Want below is live PostgreSQL 17 over the same rows (`j1_fixture.sql`
// in the arc's scratch dir reproduces the fixtures).
//
// The SPILLED arm's `memory budget exceeded` is tolerated where marked: a
// LATERAL over the 5000-row fixture builds a hash join whose build side does
// not fit 512 KiB, and a CROSS-shaped build refuses rather than spilling
// (ADR-0006's routed-probe amendment). It is a CONDITION and it is loud.
func TestArcJ1ALateralKeyIsPublishedUnderAHiddenSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	const eightGroups = `k,g,c | 0,4998,660 | 1,4999,660 | 2,4993,659 | 3,4994,659 | ` +
		`4,4995,659 | 5,4996,659 | 6,4997,660 | 7,NULL,0`

	for _, tc := range []struct {
		name, sql, want string
		budgeted        bool
		// routes, when non-empty, is the local-route counter this shape MUST
		// move on both DAG arms. Rows alone cannot tell an executed query from
		// a refused-and-routed one.
		routes string
	}{
		// ---------------------------------------------------------------
		// #956 — an AGGREGATE aliased like the correlation key.
		//
		// The whole family, because a row count cannot tell "bound to the
		// key" from "bound to the aggregate": MAX/SUM/COUNT answer numbers
		// far from the key, and MIN over this fixture answers the key's own
		// value, which is why it is a CONTROL and not a cell.
		{name: "956/max-aliased-like-the-key", budgeted: true,
			sql: `SELECT d.k AS k, s.g AS g, s.c AS c FROM typemx_dim d JOIN LATERAL (` +
				`SELECT MAX(t.id) AS g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k) s ON true ` +
				`ORDER BY d.k`,
			want: eightGroups},
		{name: "956/sum-aliased-like-the-key", budgeted: true,
			sql: `SELECT d.k AS k, s.g AS g FROM typemx_dim d JOIN LATERAL (` +
				`SELECT SUM(t.id) AS g FROM typemx t WHERE t.g = d.k) s ON true ORDER BY d.k`,
			want: `k,g | 0,1647415 | 1,1648845 | 2,1645275 | 3,1646704 | 4,1648133 | ` +
				`5,1649562 | 6,1650990 | 7,NULL`},
		{name: "956/count-aliased-like-the-key", budgeted: true,
			sql: `SELECT d.k AS k, s.g AS g FROM typemx_dim d JOIN LATERAL (` +
				`SELECT COUNT(*) AS g FROM typemx t WHERE t.g = d.k) s ON true ORDER BY d.k`,
			want: `k,g | 0,660 | 1,660 | 2,659 | 3,659 | 4,659 | 5,659 | 6,660 | 7,0`},
		{name: "956/the-LEFT-spelling", budgeted: true,
			sql: `SELECT d.k AS k, s.g AS g, s.c AS c FROM typemx_dim d LEFT JOIN LATERAL (` +
				`SELECT MAX(t.id) AS g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k) s ON true ` +
				`ORDER BY d.k`,
			want: eightGroups},
		{name: "956/on-the-small-fixture",
			sql: `SELECT o.customer AS cu, s.order_id AS oi FROM lat_ord o JOIN LATERAL (` +
				`SELECT MAX(amount) AS order_id FROM lat_item WHERE order_id = o.id) s ON true ` +
				`ORDER BY o.customer`,
			want: `cu,oi | Alice,100 | Bob,125 | Carol,NULL`},
		{name: "956/ctl-MIN-answers-the-key-s-own-value", budgeted: true,
			sql: `SELECT d.k AS k, s.g AS g FROM typemx_dim d JOIN LATERAL (` +
				`SELECT MIN(t.id) AS g FROM typemx t WHERE t.g = d.k) s ON true ORDER BY d.k`,
			want: `k,g | 0,0 | 1,1 | 2,2 | 3,3 | 4,4 | 5,5 | 6,6 | 7,NULL`},
		{name: "956/ctl-aggregate-under-its-own-alias", budgeted: true,
			sql: `SELECT d.k AS k, s.mx AS mx FROM typemx_dim d JOIN LATERAL (` +
				`SELECT MAX(t.id) AS mx FROM typemx t WHERE t.g = d.k) s ON true ORDER BY d.k`,
			want: `k,mx | 0,4998 | 1,4999 | 2,4993 | 3,4994 | 4,4995 | 5,4996 | 6,4997 | 7,NULL`},

		// ---------------------------------------------------------------
		// #767's MIRROR — an inner ALIAS shadowing the key's name while
		// holding another column's value. Pinned in the D5 census as
		// `boundary_inner_alias_shadowing_the_key_answers_nothing` until now.
		{name: "767/mirror-inner-alias-shadows-the-key",
			sql: `SELECT o.customer AS c, li.order_id AS a FROM lat_ord o JOIN LATERAL (` +
				`SELECT amount AS order_id FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, a`,
			want: `c,a | Alice,50 | Alice,100 | Bob,75 | Bob,125`},
		{name: "767/mirror-the-LEFT-twin",
			sql: `SELECT o.customer AS c, li.order_id AS a FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT amount AS order_id FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, a`,
			want: `c,a | Alice,50 | Alice,100 | Bob,75 | Bob,125 | Carol,NULL`},
		{name: "767/ctl-alias-shadows-a-NON-key-column",
			sql: `SELECT o.customer AS cu, li.id AS i FROM lat_ord o JOIN LATERAL (` +
				`SELECT amount AS id FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, i`,
			want: `cu,i | Alice,50 | Alice,100 | Bob,75 | Bob,125`},

		// ---------------------------------------------------------------
		// #767's DAG HALF — the LATERAL join stage's files declare ONE
		// column set. Pinned as
		// `TestArcH1ALeftLateralOverAGroupedSubqueryStillFailsOnTheDAG` until
		// now: both DAG arms failed with `column "s.c" does not exist in the
		// input schema` (with an ORDER BY) or an ADR-0010 `.wshf` width /
		// type mismatch (without one), at `UnreachableOutputLocalRoutes +0`.
		{name: "767/dag-LEFT-aliased-key-ORDER-BY", budgeted: true,
			sql: `SELECT d.k AS k, s.gg AS gg, s.c AS c FROM typemx_dim d LEFT JOIN LATERAL (` +
				`SELECT t.g AS gg, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ` +
				`ON true ORDER BY d.k`,
			want: `k,gg,c | 0,0,660 | 1,1,660 | 2,2,659 | 3,3,659 | 4,4,659 | 5,5,659 | ` +
				`6,6,660 | 7,NULL,NULL`,
			// The DAG answers this on its LOCAL pipeline: the gather's rename
			// of the lateral's alias still names a column no stage emits, so
			// the plan is refused and the coordinator answers it (ADR-0021
			// §1c). It FAILED at base; the values are PostgreSQL's on every
			// arm now, and the counter says which engine produced them.
			routes: "unreachable output"},
		{name: "767/dag-LEFT-key-under-its-own-name-ORDER-BY", budgeted: true,
			sql: `SELECT d.k AS k, s.g AS g, s.c AS c FROM typemx_dim d LEFT JOIN LATERAL (` +
				`SELECT t.g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ` +
				`ON true ORDER BY d.k`,
			want: `k,g,c | 0,0,660 | 1,1,660 | 2,2,659 | 3,3,659 | 4,4,659 | 5,5,659 | ` +
				`6,6,660 | 7,NULL,NULL`},
		{name: "767/dag-INNER-key-under-its-own-name-ORDER-BY", budgeted: true,
			sql: `SELECT d.k AS k, s.g AS g, s.c AS c FROM typemx_dim d JOIN LATERAL (` +
				`SELECT t.g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ` +
				`ON true ORDER BY d.k`,
			want: `k,g,c | 0,0,660 | 1,1,660 | 2,2,659 | 3,3,659 | 4,4,659 | 5,5,659 | 6,6,660`},
		{name: "767/ctl-key-published-under-its-own-name", budgeted: true,
			sql: `SELECT COUNT(*) AS n FROM typemx_dim d JOIN LATERAL (` +
				`SELECT t.g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			want: `n | 7`},
		{name: "767/ctl-key-not-published-at-all", budgeted: true,
			sql: `SELECT COUNT(*) AS n FROM typemx_dim d JOIN LATERAL (` +
				`SELECT COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			want: `n | 7`},
		{name: "767/ctl-ungrouped-aggregate-keeps-the-unmatched-row",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o JOIN LATERAL (` +
				`SELECT COUNT(*) AS n FROM lat_item WHERE order_id = o.id) s ON true ` +
				`ORDER BY o.customer`,
			want:   `c,n | Alice,2 | Bob,2 | Carol,0`,
			routes: "unreachable output"},
		{name: "767/ctl-non-aggregated-lateral",
			sql: `SELECT o.customer AS c, li.amount AS a FROM lat_ord o JOIN LATERAL (` +
				`SELECT amount FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, li.amount`,
			want: `c,a | Alice,50 | Alice,100 | Bob,75 | Bob,125`},
		{name: "767/ctl-non-aggregated-lateral-publishing-its-key",
			sql: `SELECT o.customer AS c, li.amount AS a FROM lat_ord o JOIN LATERAL (` +
				`SELECT amount, order_id FROM lat_item WHERE order_id = o.id) li ON true ` +
				`ORDER BY o.customer, li.amount`,
			want: `c,a | Alice,50 | Alice,100 | Bob,75 | Bob,125`},
		{name: "767/ctl-aliased-key-H1-s-published-name-path", budgeted: true,
			sql: `SELECT d.k AS k, s.gg AS gg, s.c AS c FROM typemx_dim d JOIN LATERAL (` +
				`SELECT t.g AS gg, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ` +
				`ON true ORDER BY d.k`,
			want: `k,gg,c | 0,0,660 | 1,1,660 | 2,2,659 | 3,3,659 | 4,4,659 | 5,5,659 | 6,6,660`,
			// Same route as its LEFT twin above, and for the same reason: the
			// gather's rename of the lateral's ALIAS names a column no stage
			// emits. The values are PostgreSQL's on every arm.
			routes: "unreachable output"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := map[string]int64{}
				if arm.coord != nil {
					for _, rc := range e3RouteCounters {
						before[rc.name] = rc.fn(arm.coord)
					}
				}
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					if tc.budgeted && arm.name == spilledArm &&
						strings.Contains(err.Error(), "memory budget exceeded") {
						continue
					}
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, tc.want, tc.sql)
				}
				if got := e3Render(cols, rows); got != tc.want {
					t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
				if arm.coord == nil {
					continue
				}
				for _, rc := range e3RouteCounters {
					moved := rc.fn(arm.coord) - before[rc.name]
					want := int64(0)
					if rc.name == tc.routes {
						want = 1
					}
					if moved != want {
						t.Errorf("%s arm: %s local routes moved by %d, want %d — rows alone "+
							"cannot tell an executed query from a refused-and-routed one\n  SQL: %s",
							arm.name, rc.name, moved, want, tc.sql)
					}
				}
			}
		})
	}
}

// A `SELECT *` over an AGGREGATED LATERAL is LOUD on both DAG arms, and this
// pin says so with what the tree does.
//
// Two facts meet. The lateral's correlation key is a column of its output —
// `__key_0` now, `order_id` before — so a star over the join publishes a
// column PostgreSQL does not (`id, customer, total, amount` there); that
// divergence is older than this arc and is recorded in ADR-0012. And a star
// gives the join node no `NeededColumns`, so `joinSideSchemas` narrows the
// declared side schemas to the KEYS alone: a fragment whose build partition is
// empty then writes a file three columns wide where one with rows writes five,
// and the shuffle read refuses the pair (ADR-0010).
//
// It is LOUD, never silent, and it is the one lateral shape this arc does not
// close. Closing it is `joinSideSchemas` describing a star's width, which is
// the stage model's question and not this one's.
func TestArcJ1AStarOverAnAggregatedLateralIsLoudOnTheDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	const sql = `SELECT * FROM typemx_dim d JOIN LATERAL (` +
		`SELECT MAX(t.id) AS g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k) s ON true`

	for _, arm := range arms {
		cols, rows, err := arm.run(sql)
		switch {
		case arm.coord != nil:
			if err == nil {
				t.Errorf("%s arm ANSWERED %s — the star's width reaches the DAG now, so this "+
					"pin is spent: delete it and record the disposition\n  SQL: %s",
					arm.name, e3Render(cols, rows), sql)
				continue
			}
			if !strings.Contains(err.Error(), "one stage's files describe one relation") {
				t.Errorf("%s arm failed by a DIFFERENT sentence than the one pinned: %v", arm.name, err)
			}
		case arm.name == spilledArm:
			if err != nil && strings.Contains(err.Error(), "memory budget exceeded") {
				continue
			}
			fallthrough
		default:
			if err != nil {
				t.Errorf("%s arm refused a shape it answers: %v", arm.name, err)
				continue
			}
			// The single-process arms answer, with the planner's key column
			// beside the query's own — PostgreSQL publishes four columns here
			// and this engine publishes five (ADR-0012's recorded divergence,
			// which this arc RENAMES rather than removes).
			if !strings.HasPrefix(e3Render(cols, rows), "k,label,__key_0,g,c |") {
				t.Errorf("%s arm: %s\n  want the join's columns with the correlation slot among "+
					"them", arm.name, e3Render(cols, rows))
			}
		}
	}
}

// A ROW FIELD PATH inside an IN-subquery answers PostgreSQL's rows on FOUR
// ARMS — #866's positive answer.
//
// `d.b IN (SELECT c_row.b FROM typemx_nested)` names a FIELD of a ROW column.
// Two things were wrong with it and the second hid the first: the semi-join
// lowering keyed on a column its build side does not emit (silently answering
// `8, 9` — decpair's two NULL-keyed rows, which `NULL IN (…)` must EXCLUDE —
// where PostgreSQL answers `6`), and the DECLINE that closed that in v0.18.56
// left the IN a filter predicate whose subquery is then REFUSED by the
// dangling-reference guard: `plansql.DanglingTableRefs` reads a qualifier that
// names no FROM item as an OUTER TABLE, and a ROW field path is exactly that
// shape.
//
// The guard takes a relation resolver now, so it can tell a field path (whose
// qualifier is a COLUMN of a relation the subquery reads) from a lost
// correlation (whose qualifier is an outer relation). Both directions are
// cells: the field-path spellings ANSWER, and a genuinely dangling reference
// is still refused.
//
// PostgreSQL REFUSES the unqualified `c_row.b` spelling itself
// (`missing FROM-clause entry for table "c_row"`); the VALUES below are its
// answers to `(c_row).b`, and answering both spellings is ADR-0012's recorded
// superset.
func TestArcJ1AFieldPathInsideAnInSubqueryAnswers(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	for _, tc := range []struct{ name, sql, want string }{
		{"866/in-a-field-path", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
			`(SELECT c_row.b FROM typemx_nested) ORDER BY d.id`, `did | 6`},
		{"866/in-the-parenthesized-spelling", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
			`(SELECT (c_row).b FROM typemx_nested) ORDER BY d.id`, `did | 6`},
		{"866/in-under-an-alias", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
			`(SELECT c_row.b AS fb FROM typemx_nested) ORDER BY d.id`, `did | 6`},
		{"866/in-with-an-inner-filter", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
			`(SELECT c_row.b FROM typemx_nested WHERE id < 20) ORDER BY d.id`, `did | 6`},
		// NOT IN over a set that CONTAINS NULLs admits nothing — PostgreSQL's
		// three-valued rule. The engine answered all seven rows before #866's
		// first disposition and none after it; PostgreSQL answers none.
		{"866/not-in-a-field-path", `SELECT d.id AS did FROM decpair d WHERE d.b NOT IN ` +
			`(SELECT c_row.b FROM typemx_nested) ORDER BY d.id`, `did`},
		{"866/not-in-with-the-NULLs-excluded", `SELECT d.id AS did FROM decpair d WHERE d.b NOT IN ` +
			`(SELECT c_row.b FROM typemx_nested WHERE c_row.b IS NOT NULL) ORDER BY d.id`,
			`did | 1 | 2 | 3 | 4 | 5 | 7`},
		// The controls: an ordinary inner key, the OUTER-key field path E3
		// closed (#769), the path read on its own, and the path in a literal
		// list. None of them may move.
		{"866/ctl-ordinary-inner-key", `SELECT d.id AS did FROM decpair d WHERE d.b IN ` +
			`(SELECT id FROM typemx_nested WHERE id < 20) ORDER BY d.id`, `did | 5 | 6 | 7`},
		{"866/ctl-outer-key-field-path", `SELECT n.id AS nid FROM typemx_nested n WHERE c_row.b IN ` +
			`(SELECT b FROM decpair) ORDER BY n.id`, `nid | 0`},
		{"866/ctl-the-path-on-its-own", `SELECT COUNT(*) AS n, MIN(c_row.b) AS mn, ` +
			`MAX(c_row.b) AS mx FROM typemx_nested`, `n,mn,mx | 5000,0,54989`},
		{"866/ctl-the-path-in-a-literal-list", `SELECT n.id AS nid FROM typemx_nested n ` +
			`WHERE c_row.b IN (0, 11, 44) ORDER BY n.id`, `nid | 0 | 1 | 4`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, tc.want, tc.sql)
				}
				if got := e3Render(cols, rows); got != tc.want {
					t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// A RECURSIVE CTE with a DUPLICATE-NAME column list keeps both columns —
// #957, on the two single-process arms.
//
// `physical.materializeRecursiveCTE` works in `[]map[string]any`, and the CTE
// column list is exactly what makes a duplicate-name body legal and useful:
// `WITH RECURSIVE t(a, b) AS (SELECT 1 AS x, 10 AS x …)` renames the two
// positions apart. Reading both by the bare name `x` gave both aliases the
// FIRST column's value, so the working row collapsed and every iteration read
// it back: `1,10 | 2,100 | 3,10000` in PostgreSQL 17 came back `1,1 | 2,1 | 3,1`.
//
// The DAG arms are not asserted: a recursive CTE has no stage lowering at all
// (ADR-0021 §1b) and both arms fail with `stage scan-0 has no dependencies and
// no ScanFiles` — on the CONTROL too, which is what says it is not this defect.
func TestArcJ1ARecursiveCTEKeepsBothColumnsOfADuplicateName(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	for _, tc := range []struct{ name, sql, want string }{
		{"957/the-filing-shape",
			`WITH RECURSIVE t(a, b) AS (SELECT 1 AS x, 10 AS x UNION ALL ` +
				`SELECT a + 1, b * b FROM t WHERE a < 3) SELECT a, b FROM t ORDER BY a`,
			`a,b | 1,10 | 2,100 | 3,10000`},
		{"957/two-columns-of-one-name",
			`WITH RECURSIVE t(a, b) AS (SELECT 1 AS x, 10 AS x UNION ALL ` +
				`SELECT a + 1, b + 1 FROM t WHERE a < 3) SELECT a, b FROM t ORDER BY a`,
			`a,b | 1,10 | 2,11 | 3,12`},
		{"957/three-columns-one-rename",
			`WITH RECURSIVE t(a, b, c) AS (SELECT 1 AS x, 10 AS y, 100 AS x UNION ALL ` +
				`SELECT a + 1, b * 2, c * 3 FROM t WHERE a < 3) SELECT a, b, c FROM t ORDER BY a`,
			`a,b,c | 1,10,100 | 2,20,300 | 3,40,900`},
		{"957/ctl-distinct-names",
			`WITH RECURSIVE t(a, b) AS (SELECT 1 AS x, 10 AS y UNION ALL ` +
				`SELECT a + 1, b * b FROM t WHERE a < 3) SELECT a, b FROM t ORDER BY a`,
			`a,b | 1,10 | 2,100 | 3,10000`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				if arm.coord != nil {
					continue // no stage lowering for a recursive CTE (ADR-0021 §1b)
				}
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, tc.want, tc.sql)
				}
				if got := e3Render(cols, rows); got != tc.want {
					t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}

// #876's FILTERED-PRODUCER residual is still refused on the DAG, and the pin
// says so.
//
// Two scalar producers that EACH filter one shared CTE reference are refused
// with the #656 message where PostgreSQL and both single-process arms answer.
// Arc H1 wrote the consumer-scoped `StageProject` that would carry the filter,
// measured it, and WITHDREW it: Q15's own shape then answered ZERO for
// PostgreSQL's 4838. Nothing in the hidden-slot mechanism reaches a stage's
// consumer scoping, so this arc leaves it where H1 left it.
func TestArcJ1TwoFilteredProducersOverOneCTEAreStillRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	const filtered = `WITH c AS (SELECT id, c_i64 AS v FROM typemx) SELECT COUNT(*) AS n ` +
		`FROM typemx WHERE c_i64 < (SELECT MAX(v) FROM c WHERE id < 4000) ` +
		`AND c_i64 > (SELECT MIN(v) FROM c WHERE id > 10)`
	// Q15's own shape, the reason the repair was withdrawn. It answers on
	// every arm and must keep doing so.
	const q15 = `WITH c AS (SELECT id, c_i64 AS v FROM typemx) SELECT COUNT(*) AS n ` +
		`FROM c WHERE v < (SELECT MAX(v) FROM c)`

	for _, arm := range arms {
		cols, rows, err := arm.run(filtered)
		if arm.coord != nil {
			if err == nil {
				t.Errorf("%s arm ANSWERED %s — #876's filtered-producer residual is closed, so "+
					"this pin is spent: delete it and close the issue's residual",
					arm.name, e3Render(cols, rows))
			} else if !strings.Contains(err.Error(), "#656") {
				t.Errorf("%s arm failed by a different sentence than the one pinned: %v", arm.name, err)
			}
		} else if err != nil {
			t.Errorf("%s arm refused a shape it answers: %v", arm.name, err)
		} else if got := e3Render(cols, rows); got != "n | 3858" {
			t.Errorf("%s arm: %s, want n | 3858 (live PostgreSQL 17)", arm.name, got)
		}

		cols, rows, err = arm.run(q15)
		if err != nil {
			t.Errorf("%s arm refused Q15's shape: %v", arm.name, err)
			continue
		}
		if got := e3Render(cols, rows); got != "n | 4838" {
			t.Errorf("%s arm: %s, want n | 4838 (live PostgreSQL 17) for Q15's shape", arm.name, got)
		}
	}
}
