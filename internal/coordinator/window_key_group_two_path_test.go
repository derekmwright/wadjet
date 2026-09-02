package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// A WINDOW's output used as a GROUP BY key, on three arms against live
// PostgreSQL 17 (#777).
//
// #741's family — a window inside a derived table under a join — is all correct
// and gated next door. The composition with GROUP BY was the broken cell:
//
//	SELECT x.id, x.w, COUNT(*) FROM (SELECT id, SUM(a) OVER () + 0 AS w
//	  FROM decpair) x LEFT JOIN decpair z ON x.id = z.id GROUP BY x.id, x.w
//
// PostgreSQL 17 and the single-process path answer `w = 52.99` on all nine
// rows. Both DAG arms answered NULL on all nine, silently: `aggStageGroupKey`
// dispatches a computed alias as its DEFINING EXPRESSION, which for a wrapped
// window is `__win_0 + 0` — a slot the join does not carry, because the window
// arm's own projection already renamed it away to `w`.
//
// THE ANSWER IS A REFUSAL, and the history is why. The alternative — dispatch
// the ALIAS instead — needs to know whether any fragment actually publishes it,
// and that is decided AFTER stage emission by `attachScanSelectProjections` and
// `absorbWindowArmProjection`. Three successive attempts to infer it from NODE
// KINDS were each wrong in a different direction (ADR-0026 §4a; the last one
// answered three groups keyed by the SCAN's `g` where PostgreSQL answers one
// group of 240, silently, for a window alias that SHADOWS a base column).
// `refuseUnstageableGroupKey` condition (3) refuses the whole cell instead and
// routes it to the coordinator-local pipeline, where the derived table's
// Project is a real operator and the alias is a real column.
//
// So every entry here asserts TWO things: PostgreSQL's whole ordered result,
// and WHICH mechanism produced it. A gate that checked only the rows would pass
// just as happily if the DAG started answering these by luck — which is exactly
// what the shapes below the "answered right for the wrong reason" heading did
// before this arc.
func TestWindowOutputAsAGroupKeyMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	arms := []struct {
		name  string
		run   func(string) (*oracle.Result, error)
		coord *Coordinator
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }, nil},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }, coord},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }, coordB},
	}

	const tbl = dbpTable // decpair, nine rows; SUM(a) OVER () is 52.99

	// routed marks an entry whose DAG answer must come from the refusal.
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string // PostgreSQL 17, whole result, ordered
		routed    bool
	}{
		{
			name: "the-repro/left-join",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
			routed: true,
		},
		{
			// The INNER join twin. #777 named the LEFT join; the mechanism is
			// the key's spelling and has nothing to do with null-padding, so
			// both belong here — and if only one were gated, a fix that keyed on
			// the join type would pass.
			name: "inner-join",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
			routed: true,
		},
		{
			// The window output as the ONLY key: nine rows collapse to one, so a
			// NULL key here is the difference between one group of 9 and one
			// group of 9 — the row count is IDENTICAL either way, and only the
			// key's value says which happened.
			name: "the-window-output-as-the-only-key",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " +
				tbl + ") x LEFT JOIN " + tbl + " z ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols:   []string{"w", "n"},
			want:   "1 rows: 52.99|9;",
			routed: true,
		},
		{
			// The window arm on the NULL-SUPPLYING side, so the join type is
			// exercised in both directions.
			name: "the-window-arm-is-the-inner-side-of-the-left-join",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM " + tbl + " z LEFT JOIN (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x ON x.id = z.id " +
				"GROUP BY x.w ORDER BY w",
			cols:   []string{"w", "n"},
			want:   "1 rows: 52.99|9;",
			routed: true,
		},
		{
			name: "beside-a-having",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w HAVING COUNT(*) > 0 ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
			routed: true,
		},
		{
			// A PARTITIONED window, whose per-row values DIFFER — so a key that
			// resolved to one shared column rather than to nothing would still
			// fail here, which the constant-valued entries above cannot see.
			name: "a-partitioned-window-output-as-the-key",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER (PARTITION BY id) + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|12.75|1;2|12.75|1;3|12.75|1;4|-0.01|1;5|2.00|1;6|0.00|1;" +
				"7||1;8|12.75|1;9||1;",
			routed: true,
		},
		{
			// A RANKING function, and an aggregate over the OTHER arm beside it,
			// so the answer is not carried entirely by the key column.
			name: "a-ranking-window-output-as-the-key",
			sql: "SELECT x.id AS id, x.w AS w, SUM(z.a) AS s FROM (SELECT id, " +
				"ROW_NUMBER() OVER (ORDER BY id) + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "s"},
			want: "9 rows: 1|1|12.75;2|2|12.75;3|3|12.75;4|4|-0.01;5|5|2.00;6|6|0.00;" +
				"7|7|;8|8|12.75;9|9|;",
			routed: true,
		},
		{
			// Over typemx, grouping by a column of the OTHER arm beside the
			// window output — the shape where a NULL key would merge groups that
			// PostgreSQL keeps apart.
			name: "a-window-output-beside-a-key-from-the-other-arm",
			sql: "SELECT t.g AS g, x.w AS w, COUNT(*) AS n FROM (SELECT id, g, " +
				"SUM(c_i32) OVER () + 0 AS w FROM typemx WHERE id < 40) x LEFT JOIN typemx t " +
				"ON x.id = t.id GROUP BY t.g, x.w ORDER BY g",
			cols: []string{"g", "w", "n"},
			want: "8 rows: 0|2256|6;1|2256|6;2|2256|6;3|2256|5;4|2256|5;5|2256|4;6|2256|5;" +
				"|2256|3;",
			routed: true,
		},
		// --- the WRAPPER's shape does not matter, only that it wraps a window.
		{
			name: "wrapped-by-multiplication",
			sql: "SELECT x.id AS i, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () * 2 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"i", "w", "n"},
			want: "9 rows: 1|105.98|1;2|105.98|1;3|105.98|1;4|105.98|1;5|105.98|1;6|105.98|1;" +
				"7|105.98|1;8|105.98|1;9|105.98|1;",
			routed: true,
		},
		{
			name: "wrapped-by-a-case",
			sql: "SELECT x.id AS i, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"CASE WHEN SUM(a) OVER () > 0 THEN 1 ELSE 0 END AS w FROM " + tbl +
				") x LEFT JOIN " + tbl + " z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols:   []string{"i", "w", "n"},
			want:   "9 rows: 1|1|1;2|1|1;3|1|1;4|1|1;5|1|1;6|1|1;7|1|1;8|1|1;9|1|1;",
			routed: true,
		},
		// --- an intervening SORT or DISTINCT inside the arm. Both are shapes
		// `absorbWindowArmProjection` DECLINES, so a join above is not
		// sufficient either: the first was wrong-NULL on the DAG and the second
		// failed hard, and both are answered by the route.
		{
			name: "an-inner-order-by-between-the-window-and-the-alias",
			sql: "SELECT x.id AS i, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + " ORDER BY id) x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"i", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
			routed: true,
		},
		{
			name: "a-distinct-between-the-window-and-the-alias",
			sql: "SELECT x.id AS i, x.w AS w, COUNT(*) AS n FROM (SELECT DISTINCT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"i", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
			routed: true,
		},
		// --- ANSWERED RIGHT FOR THE WRONG REASON, and the whole reason the
		// refusal is unconditional.
		//
		// With NO join above the derived table, nothing attaches the window
		// arm's projection, so the DAG used to dispatch the DEFINING EXPRESSION
		// over `__win_N` — which the window stage really does emit — and got the
		// right answer. Dispatching the ALIAS instead, as round 1 of this arc
		// did, made `hash_aggregate` bind whatever the batch carried: LOUD where
		// the alias names nothing, and SILENTLY the SCAN's column where the
		// alias SHADOWS one. `g` here is both the window alias and a real
		// collslot column, which is the fixture the whole class was missing.
		{
			name: "shadowing/no-join-sum-over-id-aliased-g",
			sql: "SELECT g AS k, COUNT(*) AS n FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot) x GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: true,
		},
		{
			name: "shadowing/no-join-count-over-aliased-g",
			sql: "SELECT g AS k, COUNT(*) AS n FROM (SELECT id, COUNT(*) OVER () + 0 AS g " +
				"FROM collslot) x GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 240|240;", routed: true,
		},
		{
			// An aggregate over the shadowed column, so the answer moves if the
			// key binds the wrong one AND the aggregate reads the wrong one.
			name: "shadowing/no-join-with-an-aggregate-over-id",
			sql: "SELECT g AS k, SUM(id) AS s FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot) x GROUP BY g ORDER BY k",
			cols: []string{"k", "s"}, want: "1 rows: 28680|28680;", routed: true,
		},
		{
			name: "shadowing/no-join-with-an-inner-filter",
			sql: "SELECT g AS k, COUNT(*) AS n FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot WHERE id < 100) x GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 4950|100;", routed: true,
		},
		{
			name: "shadowing/no-join-through-a-cte",
			sql: "WITH x AS (SELECT id, SUM(id) OVER () + 0 AS g FROM collslot) " +
				"SELECT g AS k, COUNT(*) AS n FROM x GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: true,
		},
		{
			name: "shadowing/no-join-with-an-outer-filter",
			sql: "SELECT g AS k, COUNT(*) AS n FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot) x WHERE x.id >= 0 GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: true,
		},
		{
			// The alias does NOT shadow anything, but the base column it could
			// have shadowed is in scope beside it — the near-miss control for
			// the two above.
			name: "shadowing/no-join-alias-does-not-shadow",
			sql: "SELECT h AS k, COUNT(*) AS n FROM (SELECT id, g, SUM(id) OVER () + 0 AS h " +
				"FROM collslot) x GROUP BY h ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: true,
		},
		{
			name: "shadowing/with-a-join",
			sql: "SELECT x.g AS k, COUNT(*) AS n FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot) x LEFT JOIN collslot z ON x.id = z.id GROUP BY x.g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: true,
		},
		{
			// The window output is the ONLY column of the derived table, so the
			// alias names nothing at all below it — the LOUD face of the same
			// over-fire.
			name: "the-window-output-is-the-only-column",
			sql: "SELECT w, COUNT(*) AS n FROM (SELECT SUM(a) OVER () + 0 AS w FROM " + tbl +
				") x GROUP BY w ORDER BY w",
			cols: []string{"w", "n"}, want: "1 rows: 52.99|9;", routed: true,
		},
		// --- controls: shapes that must keep running ON THE DAG. Without these
		// a refusal widened by accident — every grouped query routed local —
		// would pass every assertion above.
		{
			name: "ctl/no-group-by",
			sql: "SELECT x.id AS id, x.w AS w FROM (SELECT id, SUM(a) OVER () + 0 AS w FROM " +
				tbl + ") x LEFT JOIN " + tbl + " z ON x.id = z.id ORDER BY x.id",
			cols: []string{"id", "w"},
			want: "9 rows: 1|52.99;2|52.99;3|52.99;4|52.99;5|52.99;6|52.99;7|52.99;8|52.99;9|52.99;",
		},
		{
			// The UNWRAPPED window, which is a plain rename of the slot and
			// therefore takes resolveAggInputName's rename arm, not the computed
			// one — so it is not in the cell and must stay distributed.
			name: "ctl/unwrapped-window-output",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
		},
		{
			// #781's no-join spelling, which used to fail LOUDLY — `cannot store
			// string into FLOAT64 vector` — and only because the key is DECIMAL:
			// the key is dispatched correctly as `a * 3` over a fused scan that
			// carries `a`, and ONLY the declared type was wrong.
			// `derivedGroupKeyDecl` types it in the scope that can NAME its
			// columns now (ADR-0026 §5): the emitted scope carries `id` and `w`
			// and no `a`, so its FLOAT64 was the float RULE and not an
			// observation, and `sourceColDeclsThroughRenames` stopped at the
			// computed projection item and answered nothing at all.
			name: "781/a-computed-decimal-alias-over-a-bare-scan",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a * 3 AS w FROM " + tbl +
				") x GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
		},
		{
			// #792: a bare-NAME key whose DEFINITION is DECIMAL arithmetic in
			// the derived Project. Dispatched as `c_dec + 1`, which the fused
			// scan can evaluate, and declared FLOAT64 with the (p,s) dropped —
			// the same site, digit for digit here.
			name: "792/a-decimal-expression-alias-as-a-key",
			sql: "SELECT k, COUNT(*) AS n FROM (SELECT c_dec + 1 AS k FROM typemx " +
				"WHERE id < 4) s GROUP BY k ORDER BY k",
			cols: []string{"k", "n"},
			want: "4 rows: 1.0000|1;2.0001|1;3.0002|1;4.0003|1;",
		},
		{
			// A computed alias BESIDE a window that has nothing to do with it.
			// `id % 7 AS k` is ordinary arithmetic over a scan column; round 1's
			// second predicate answered the alias here and turned a correct DAG
			// answer into `GROUP BY key "k" is not a column of its input`.
			name: "ctl/a-non-window-alias-beside-a-window",
			sql: "SELECT k, COUNT(*) AS n FROM (SELECT id % 7 AS k, " +
				"COUNT(*) OVER (PARTITION BY id % 7) AS w FROM typemx) u " +
				"GROUP BY k, w ORDER BY k",
			cols: []string{"k", "n"},
			want: "7 rows: 0|715;1|715;2|714;3|714;4|714;5|714;6|714;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := int64(0)
				if arm.coord != nil {
					before = arm.coord.GroupKeyLocalRoutes()
				}
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
				if arm.coord == nil {
					continue
				}
				routed := arm.coord.GroupKeyLocalRoutes() != before
				if routed != tc.routed {
					if tc.routed {
						t.Errorf("%s arm answered WITHOUT the group-key refusal firing. Either "+
							"walkStages grew a real lowering for a window-wrapped key — in which "+
							"case delete condition (3) and gate the lowering — or the refusal's "+
							"predicate narrowed and this shape is now answered by luck\n  SQL: %s",
							arm.name, tc.sql)
					} else {
						t.Errorf("%s arm ROUTED a shape the DAG executes correctly — "+
							"refuseUnstageableGroupKey condition (3) has widened\n  SQL: %s",
							arm.name, tc.sql)
					}
				}
			}
		})
	}

	// THE #781 BOUNDARY, pinned rather than described.
	//
	// A computed alias over a BARE SCAN — no window in it — used as a group key
	// through a join or a DISTINCT is still wrong on the DAG: one NULL group
	// over the whole table. It is the #736 family's one-field problem
	// (`Stage.GroupByCols` is both the RESOLUTION name and the PUBLISHED name)
	// in a shape neither of that arc's two refusal conditions covers: the key IS
	// a column of the aggregate's input by every logical-plan test, and only the
	// STAGE spelling is wrong. Byte-identical with the whole arc reverted.
	//
	// The AGGREGATE-wrapped spelling is here too, and it matters: it has the
	// identical NULL-key symptom and the #777 predicate that was discarded would
	// NOT have fixed it either — `COUNT(*) + 0 AS w` references no `__win_N`
	// slot, so no window-shaped condition can see it. It is the same site, and a
	// fix has to be about what a STAGE emits rather than about what kind of node
	// produced it.
	//
	// TODO(#781): delete these when a computed alias over a bare scan resolves.
	for _, c := range []struct {
		name, sql string
		cols      []string
		pg        string // PostgreSQL 17
		dagPinned string // what both DAG arms answer instead; "" = they FAIL
	}{
		{
			name: "pin781/a-computed-alias-over-a-bare-scan-under-a-join",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a * 3 AS w FROM " + tbl +
				") x JOIN " + tbl + " z ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols:      []string{"w", "n"},
			pg:        "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			dagPinned: "1 rows: |9;",
		},
		{
			name: "pin781/a-computed-alias-under-a-distinct",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT DISTINCT g * 3 AS w FROM typemx" +
				") x GROUP BY x.w ORDER BY w",
			cols:      []string{"w", "n"},
			pg:        "8 rows: 0|1;3|1;6|1;9|1;12|1;15|1;18|1;|1;",
			dagPinned: "1 rows: |8;",
		},
		{
			// The AGGREGATE-wrapped spelling: `COUNT(*) + 0 AS w` over a grouped
			// derived table, read as a key through a join. Same NULL key, and no
			// window anywhere in it.
			name: "pin781/an-aggregate-wrapped-alias-under-a-join",
			sql: "SELECT x.g AS g, x.w AS w FROM (SELECT g, COUNT(*) + 0 AS w FROM typemx " +
				"GROUP BY g) x LEFT JOIN typemx z ON x.g = z.g GROUP BY x.g, x.w ORDER BY x.g",
			cols:      []string{"g", "w"},
			pg:        "8 rows: 0|660;1|660;2|659;3|659;4|659;5|659;6|660;|384;",
			dagPinned: "8 rows: 0|;1|;2|;3|;4|;5|;6|;|;",
		},
		{
			// And through a DISTINCT rather than a join, so the pin covers both
			// producers the plain-arithmetic entries above cover.
			name: "pin781/an-aggregate-wrapped-alias-under-a-distinct",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT DISTINCT g, COUNT(*) + 0 AS w " +
				"FROM typemx GROUP BY g) x GROUP BY x.w ORDER BY w",
			cols:      []string{"w", "n"},
			pg:        "3 rows: 384|1;659|4;660|3;",
			dagPinned: "1 rows: |8;",
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(c.sql)
				if arm.coord == nil {
					if err != nil {
						t.Fatalf("single arm refused the query: %v\n  SQL: %s", err, c.sql)
					}
					if got := dajDigest(res, c.cols); got != c.pg {
						t.Errorf("single arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
							got, c.pg, c.sql)
					}
					continue
				}
				if c.dagPinned == "" {
					if err == nil {
						t.Fatalf("the %s arm now ANSWERS this, where this pin records a hard "+
							"failure and PostgreSQL answers %q. Re-measure #781 and assert or "+
							"update the pin\n  SQL: %s", arm.name, c.pg, c.sql)
					}
					continue
				}
				if err != nil {
					t.Fatalf("the %s arm now FAILS where this pin records %q; PostgreSQL answers "+
						"%q. Re-measure #781 and update or delete this pin: %v\n  SQL: %s",
						arm.name, c.dagPinned, c.pg, err, c.sql)
				}
				got := dajDigest(res, c.cols)
				if got == c.pg {
					t.Fatalf("the %s arm now answers PostgreSQL's rows — #781 is fixed for this "+
						"shape. Assert it and delete this pin\n  SQL: %s", arm.name, c.sql)
				}
				if got != c.dagPinned {
					t.Errorf("the %s arm answered\n  %s\nthis pin records\n  %s\nand PostgreSQL "+
						"answers\n  %s\nThe answer MOVED without becoming right, which is a "+
						"change #781 has to account for\n  SQL: %s", arm.name, got, c.dagPinned,
						c.pg, c.sql)
				}
			}
		})
	}
}
