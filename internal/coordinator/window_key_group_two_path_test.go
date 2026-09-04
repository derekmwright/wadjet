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
// THE ANSWER IS TWO NAMES. Dispatching the ALIAS needs to know whether any
// fragment actually publishes it, and that is decided AFTER stage emission by
// `attachScanSelectProjections` and `absorbWindowArmProjection`. Three
// successive attempts to infer it from NODE KINDS were each wrong in a
// different direction (ADR-0026 §4a; the last one answered three groups keyed
// by the SCAN's `g` where PostgreSQL answers one group of 240, silently, for a
// window alias that SHADOWS a base column), and the whole cell was REFUSED
// while a `Stage` had one field for both names.
//
// It has two now: `Stage.GroupByCols` is what the aggregate PUBLISHES and
// `Stage.GroupByResolve` is what the computing fragment RESOLVES the key by,
// and `resolveStageGroupKeys` settles the second at the END of planning
// against a model of what the producing fragment really SHIPS. The alias where
// a fragment materialized it, the window slot where none did — including the
// shadowing shapes, where the stream carries a base column of the alias's name
// that nothing computed and the definition is the only right answer.
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

	// routed marks an entry the DAG REFUSES, so its answer comes from the
	// coordinator-local pipeline. Since ADR-0026 §2's two names it is ONE
	// entry, and it carries its own mechanism at its own case:
	//
	//   - the DISTINCT between the window and the alias, whose lowering made
	//     a WINDOW CALL a group-key expression that no projection can
	//     evaluate — the DISTINCT rewrite's defect, not this carrier's.
	//
	// Every other cell RUNS on the DAG, and asserting routed=false is what
	// says so — the rows alone cannot tell "the DAG answered this" from "the
	// DAG refused it and the local pipeline did".
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
			routed: false,
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
			routed: false,
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
			routed: false,
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
			routed: false,
		},
		{
			name: "beside-a-having",
			sql: "SELECT x.id AS id, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w HAVING COUNT(*) > 0 ORDER BY x.id",
			cols: []string{"id", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
			routed: false,
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
			routed: false,
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
			routed: false,
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
			routed: false,
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
			routed: false,
		},
		{
			name: "wrapped-by-a-case",
			sql: "SELECT x.id AS i, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"CASE WHEN SUM(a) OVER () > 0 THEN 1 ELSE 0 END AS w FROM " + tbl +
				") x LEFT JOIN " + tbl + " z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols:   []string{"i", "w", "n"},
			want:   "9 rows: 1|1|1;2|1|1;3|1|1;4|1|1;5|1|1;6|1|1;7|1|1;8|1|1;9|1|1;",
			routed: false,
		},
		// --- an intervening SORT or DISTINCT inside the arm. Both are shapes
		// `absorbWindowArmProjection` DECLINES, so a join above is not
		// sufficient either: the first was wrong-NULL on the DAG and the second
		// failed hard, and both are answered by the route.
		{
			// An ORDER BY inside the derived table stops
			// `absorbWindowArmProjection` from putting `__win_0 + 0 AS w` on
			// the arm's fragment, so nothing in the plan materializes `w` and
			// the key is resolved by its DEFINITION instead — over the ARM's
			// own `__win_0`, re-spelled into the spelling the join gives it.
			// The payload that carries it is widened by
			// `ensureJoinCarriesEvaluatedColumns`, which reads the resolution
			// for exactly this reason: the manifest between the sort and the
			// join was built before the key had a spelling.
			//
			// It ROUTED in round 1, and the round-2 review's R1 is why that
			// was wrong: the same shape without the window ran on the DAG at
			// base, and refusing it was a right-to-routed regression the
			// widening closes for both.
			name: "an-inner-order-by-between-the-window-and-the-alias",
			sql: "SELECT x.id AS i, x.w AS w, COUNT(*) AS n FROM (SELECT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + " ORDER BY id) x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"i", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
		},
		{
			// It ROUTED, by the DISTINCT lowering's own defect rather than by
			// anything about the key's two names: making every SELECT item a
			// GROUP BY key turned `SUM(a) OVER () + 0` into a key EXPRESSION,
			// and a pre-aggregate projection cannot evaluate a window call.
			//
			// It runs on the DAG now. `rewriteDistinctAsGroupBy` records the
			// SLOT the operator below publishes — the AST the builder had
			// already re-spelled to `__win_0 + 0`, where the TEXT beside it
			// still said `sum(a) OVER (...) + 0` — so the key is an expression
			// over a column of the aggregate's input on both paths (#797).
			// Its aggregate twin is `781/an-aggregate-wrapped-alias-under-a-
			// distinct` below, and it moved for the same reason.
			name: "a-distinct-between-the-window-and-the-alias",
			sql: "SELECT x.id AS i, x.w AS w, COUNT(*) AS n FROM (SELECT DISTINCT id, " +
				"SUM(a) OVER () + 0 AS w FROM " + tbl + ") x LEFT JOIN " + tbl +
				" z ON x.id = z.id GROUP BY x.id, x.w ORDER BY x.id",
			cols: []string{"i", "w", "n"},
			want: "9 rows: 1|52.99|1;2|52.99|1;3|52.99|1;4|52.99|1;5|52.99|1;6|52.99|1;" +
				"7|52.99|1;8|52.99|1;9|52.99|1;",
			routed: false,
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
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: false,
		},
		{
			name: "shadowing/no-join-count-over-aliased-g",
			sql: "SELECT g AS k, COUNT(*) AS n FROM (SELECT id, COUNT(*) OVER () + 0 AS g " +
				"FROM collslot) x GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 240|240;", routed: false,
		},
		{
			// An aggregate over the shadowed column, so the answer moves if the
			// key binds the wrong one AND the aggregate reads the wrong one.
			name: "shadowing/no-join-with-an-aggregate-over-id",
			sql: "SELECT g AS k, SUM(id) AS s FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot) x GROUP BY g ORDER BY k",
			cols: []string{"k", "s"}, want: "1 rows: 28680|28680;", routed: false,
		},
		{
			name: "shadowing/no-join-with-an-inner-filter",
			sql: "SELECT g AS k, COUNT(*) AS n FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot WHERE id < 100) x GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 4950|100;", routed: false,
		},
		{
			name: "shadowing/no-join-through-a-cte",
			sql: "WITH x AS (SELECT id, SUM(id) OVER () + 0 AS g FROM collslot) " +
				"SELECT g AS k, COUNT(*) AS n FROM x GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: false,
		},
		{
			name: "shadowing/no-join-with-an-outer-filter",
			sql: "SELECT g AS k, COUNT(*) AS n FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot) x WHERE x.id >= 0 GROUP BY g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: false,
		},
		{
			// The alias does NOT shadow anything, but the base column it could
			// have shadowed is in scope beside it — the near-miss control for
			// the two above.
			name: "shadowing/no-join-alias-does-not-shadow",
			sql: "SELECT h AS k, COUNT(*) AS n FROM (SELECT id, g, SUM(id) OVER () + 0 AS h " +
				"FROM collslot) x GROUP BY h ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: false,
		},
		{
			name: "shadowing/with-a-join",
			sql: "SELECT x.g AS k, COUNT(*) AS n FROM (SELECT id, SUM(id) OVER () + 0 AS g " +
				"FROM collslot) x LEFT JOIN collslot z ON x.id = z.id GROUP BY x.g ORDER BY k",
			cols: []string{"k", "n"}, want: "1 rows: 28680|240;", routed: false,
		},
		{
			// The window output is the ONLY column of the derived table, so the
			// alias names nothing at all below it — the LOUD face of the same
			// over-fire.
			name: "the-window-output-is-the-only-column",
			sql: "SELECT w, COUNT(*) AS n FROM (SELECT SUM(a) OVER () + 0 AS w FROM " + tbl +
				") x GROUP BY w ORDER BY w",
			cols: []string{"w", "n"}, want: "1 rows: 52.99|9;", routed: false,
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
			// Method 10 for the typing walk's descent: a derived alias that
			// SHADOWS a base column. `typemx.g` is `i % 7` — seven groups plus
			// a NULL — so a scope walk that typed or bound the SCAN's `g`
			// instead of the derived `id % 3` would answer eight rows here,
			// silently, with the right total.
			name: "ctl/a-derived-alias-that-shadows-a-base-column",
			sql: "SELECT x.g AS g, COUNT(*) AS n FROM (SELECT id, id % 3 AS g FROM typemx) x " +
				"GROUP BY x.g ORDER BY g",
			cols: []string{"g", "n"},
			want: "3 rows: 0|1667;1|1667;2|1666;",
		},
		{
			// The two shapes that put a value-preserving WRAPPER between the
			// derived Project and the scan, which is what `groupKeyScopeDescends`
			// has to look through. Both are DECIMAL, so a scope that stopped at
			// the Project would type them FLOAT64 and die at the #361 store
			// guard rather than answering.
			name: "ctl/a-derived-table-with-an-order-by-inside",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a * 3 AS w FROM " + tbl +
				" ORDER BY id) x GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
		},
		{
			name: "ctl/a-derived-table-with-a-limit-inside",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a * 3 AS w FROM " + tbl +
				" ORDER BY id LIMIT 5) x GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "3 rows: -0.03|1;6.00|1;38.25|3;",
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

	// #781's CELL, asserted rather than pinned.
	//
	// A computed alias over a BARE SCAN — no window in it — used as a group
	// key through a join or a DISTINCT was ONE NULL group over the whole table
	// on both DAG arms, byte-identical with the whole naming arc reverted. It
	// is #736's family's one-field problem (`Stage.GroupByCols` was both the
	// RESOLUTION name and the PUBLISHED name) in a shape neither of that arc's
	// refusal conditions covered: the key IS a column of the aggregate's input
	// by every logical-plan test, and only the STAGE spelling was wrong.
	//
	// The AGGREGATE-wrapped spelling is here too, and it matters: it had the
	// identical NULL-key symptom and the #777 predicate that was discarded
	// would NOT have fixed it either — `COUNT(*) + 0 AS w` references no
	// `__win_N` slot, so no window-shaped condition could see it. It is the
	// same site, and the fix is about what a STAGE emits rather than about
	// what kind of node produced it.
	//
	// Every entry asserts the DISPOSITION beside the rows, because rows alone
	// cannot tell "the DAG executed this" from "the DAG refused it and the
	// local pipeline answered" — a right-to-routed move survived a whole
	// review round in this cell on a green row assertion.
	for _, c := range []struct {
		name, sql string
		cols      []string
		want      string // PostgreSQL 17
		routed    bool   // the DAG refuses this shape and the local pipeline answers
		why       string
	}{
		{
			name: "781/a-computed-alias-over-a-bare-scan-under-a-join",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a * 3 AS w FROM " + tbl +
				") x JOIN " + tbl + " z ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			why: "the arm's projection materializes `w` on the scan's fragment, so the join " +
				"stream carries it and the key resolves by that name",
		},
		{
			name: "781/a-computed-alias-under-a-distinct",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT DISTINCT g * 3 AS w FROM typemx" +
				") x GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "8 rows: 0|1;3|1;6|1;9|1;12|1;15|1;18|1;|1;",
			why:  "the DISTINCT's own projection publishes `w`, and the outer key reads it",
		},
		{
			// The AGGREGATE-wrapped spelling: `COUNT(*) + 0 AS w` over a
			// grouped derived table, read as a key through a join.
			name: "781/an-aggregate-wrapped-alias-under-a-join",
			sql: "SELECT x.g AS g, x.w AS w FROM (SELECT g, COUNT(*) + 0 AS w FROM typemx " +
				"GROUP BY g) x LEFT JOIN typemx z ON x.g = z.g GROUP BY x.g, x.w ORDER BY x.g",
			cols: []string{"g", "w"},
			want: "8 rows: 0|660;1|660;2|659;3|659;4|659;5|659;6|660;|384;",
			why:  "the inner aggregate's own projection publishes `w` over `__agg_0 + 0`",
		},
		{
			// TWO derived arms publishing the SAME alias from DIFFERENT
			// expressions, with the key naming one of them. This is the shape
			// that says what the structural fix had to carry, and ADR-0026
			// §4a's claim about it was FALSE about the engine: a join qualifies
			// a duplicate build column with its owning alias, so the stream
			// carries `w` AND `y.w`, and the key resolves per ARM.
			//
			// Both directions, because a repair that picks the first hit is
			// right on one of them by luck: the bare `w` a broadcast join
			// carries is the PROBE arm's, so `GROUP BY x.w` would look fixed
			// while `GROUP BY y.w` silently answered x's values.
			name: "781/ambiguous-alias-two-arms-key-names-the-build-arm",
			sql: "SELECT y.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl +
				") x JOIN (SELECT id, a*100 AS w FROM " + tbl + ") y ON x.id=y.id " +
				"GROUP BY y.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -1.00|1;0.00|1;200.00|1;1275.00|4;|2;",
			why:  "the join qualified the BUILD arm's duplicate to `y.w`, which the key names",
		},
		{
			name: "781/ambiguous-alias-two-arms-key-names-the-probe-arm",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl +
				") x JOIN (SELECT id, a*100 AS w FROM " + tbl + ") y ON x.id=y.id " +
				"GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			why:  "the PROBE arm's `w` stays bare, and it is the only bare one",
		},
		{
			// ROUTED, and by a DIFFERENT mechanism from every other entry
			// here. A DISTINCT over a SELECT list makes every item a GROUP BY
			// key, so `COUNT(*) + 0` becomes a key EXPRESSION and the stage
			// carries that text — as both names, which agree. The two-name
			// carrier has nothing to separate: what is wrong is that a
			// pre-aggregate PROJECTION cannot evaluate an aggregate call at
			// all, and the value it names was computed by the operator below
			// and published under `__agg_0`.
			//
			// `refuseUnevaluableGroupKey` said exactly that and the local
			// pipeline answered PostgreSQL's rows; before the refusal existed
			// this shape was a silent `1 rows: |8;`.
			//
			// The repair landed where that note said it belonged: the DISTINCT
			// lowering records the SLOT the operator below publishes. The
			// builder had ALREADY re-spelled this projection's AST to
			// `__agg_0 + 0` for the nested-aggregate rewrite, and the lowering
			// was reading the TEXT beside it — one projection, two names that
			// disagreed (#797, ADR-0026 §2c/§2d). It runs on the DAG now, with
			// the key an ordinary expression over a column of the aggregate's
			// input.
			name: "781/an-aggregate-wrapped-alias-under-a-distinct",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT DISTINCT g, COUNT(*) + 0 AS w " +
				"FROM typemx GROUP BY g) x GROUP BY x.w ORDER BY w",
			cols:   []string{"w", "n"},
			want:   "3 rows: 384|1;659|4;660|3;",
			routed: false,
		},
		{
			// A LIMIT inside the derived table stops
			// `attachScanSelectProjections` from materializing `w` on the
			// arm's fragment, so the key resolves by its DEFINITION over ARM
			// x's own `a` — and the payload that carries it is widened by
			// `ensureJoinCarriesEvaluatedColumns`, which reads the resolution.
			//
			// This ROUTED in round 1, when the resolution was decided against
			// what the join's OutputFilter SHIPPED rather than what its arms
			// could SUPPLY. The review's R1 is the same shape with a second
			// arm that also has an `a`, and it ran on the DAG at base — so
			// refusing was a right-to-routed regression, and the two are
			// closed by one change.
			name: "781/a-derived-table-with-a-limit-inside-under-a-join",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a * 3 AS w FROM " + tbl +
				" ORDER BY id LIMIT 5) x JOIN " + tbl + " z ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "3 rows: -0.03|1;6.00|1;38.25|3;",
			why: "the key resolves by its definition over arm x's own `a`, and the join's " +
				"payload is widened to carry it",
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				before := int64(0)
				if arm.coord != nil {
					before = arm.coord.GroupKeyLocalRoutes()
				}
				res, err := arm.run(c.sql)
				if err != nil {
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, c.sql)
				}
				if got := dajDigest(res, c.cols); got != c.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  (%s)\n  SQL: %s",
						arm.name, got, c.want, c.why, c.sql)
				}
				if arm.coord == nil {
					continue
				}
				routed := arm.coord.GroupKeyLocalRoutes() != before
				if routed == c.routed {
					continue
				}
				if c.routed {
					t.Errorf("the %s arm now EXECUTES this on the DAG where it is recorded as "+
						"refused. If the answer is still PostgreSQL's that is a fix — assert "+
						"routed=false and say what now carries the key\n  SQL: %s",
						arm.name, c.sql)
					continue
				}
				t.Errorf("the %s arm ROUTED this to the coordinator-local pipeline. The answer "+
					"is right either way, which is why the disposition is asserted: the DAG "+
					"resolves this key (%s), and a group-key refusal that starts firing on it "+
					"is a right-to-routed regression in kind\n  SQL: %s", arm.name, c.why, c.sql)
			}
		})
	}

	// THE ARM CELL (#794 round 2). A key names ONE arm of a join, and a column
	// of the same name on another arm is a different value. Nothing in the
	// first round's rules asked WHICH ARM: rule 2 took the only bare column of
	// the alias's name and rule 4 took the definition's columns from wherever
	// the stream had them, so with the key's own arm unable to supply the
	// value — its inner ORDER BY / LIMIT stops
	// `attachScanSelectProjections` — both bound the OTHER arm's column and
	// the fragment grouped by a different table's value under the key's name.
	//
	// `R6a` answered `38.25|38.25|3`, which is `x.a * 3` where the key is
	// `z.a * 3`: five plausible rows of the wrong column, `routed=false`, on
	// both DAG arms, and identical on base.
	//
	// Every entry here asserts PostgreSQL's rows AND the disposition, and the
	// three CONTROLS (`no-order-by`) are what say the trigger is "the key's
	// arm did not materialise its alias" rather than the join or the join key.
	for _, c := range []struct {
		name, sql string
		cols      []string
		want      string // PostgreSQL 17
		why       string
	}{
		{
			// The key names the BUILD arm's alias; that arm's inner ORDER BY
			// blocks materialisation; the PROBE carries a bare column of the
			// DEFINITION's name.
			name: "arm/def-binds-the-key's-arm-not-the-probe's-order-by",
			sql: "SELECT z.w AS w, SUM(x.a) AS s, COUNT(*) AS n FROM " + tbl + " x JOIN " +
				"(SELECT id, a*3 AS w FROM " + tbl + " ORDER BY id) z ON x.id = z.id + 1 " +
				"GROUP BY z.w ORDER BY w",
			cols: []string{"w", "s", "n"},
			want: "5 rows: -0.03|2.00|1;0.00||1;6.00|0.00|1;38.25|25.49|4;|12.75|1;",
			why:  "the definition is re-spelled into arm z's own `z.a`",
		},
		{
			name: "arm/def-binds-the-key's-arm-not-the-probe's-limit",
			sql: "SELECT z.w AS w, SUM(x.a) AS s, COUNT(*) AS n FROM " + tbl + " x JOIN " +
				"(SELECT id, a*3 AS w FROM " + tbl + " ORDER BY id LIMIT 8) z ON x.id = z.id + 1 " +
				"GROUP BY z.w ORDER BY w",
			cols: []string{"w", "s", "n"},
			want: "5 rows: -0.03|2.00|1;0.00||1;6.00|0.00|1;38.25|25.49|4;|12.75|1;",
			why:  "the same, with the LIMIT spelling of the block",
		},
		{
			name: "ctl/arm/def-with-nothing-blocking-the-arm's-projection",
			sql: "SELECT z.w AS w, SUM(x.a) AS s, COUNT(*) AS n FROM " + tbl + " x JOIN " +
				"(SELECT id, a*3 AS w FROM " + tbl + ") z ON x.id = z.id + 1 " +
				"GROUP BY z.w ORDER BY w",
			cols: []string{"w", "s", "n"},
			want: "5 rows: -0.03|2.00|1;0.00||1;6.00|0.00|1;38.25|25.49|4;|12.75|1;",
			why:  "arm z materialises `w`, so the key reads the column and never the definition",
		},
		{
			// Rule 2's half: the only BARE materialised column of the alias's
			// name in the stream belongs to the PROBE arm, and the key names
			// the build's.
			name: "arm/bare-hit-is-the-other-arm's-order-by",
			sql: "SELECT y.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a*100 AS w FROM " + tbl + " ORDER BY id) y ON x.id = y.id + 1 " +
				"GROUP BY y.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -1.00|1;0.00|1;200.00|1;1275.00|4;|1;",
			why:  "the bare `w` is x's; the key's arm supplies `y.a` and the definition is re-spelled",
		},
		{
			name: "arm/bare-hit-is-the-other-arm's-limit",
			sql: "SELECT y.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a*100 AS w FROM " + tbl + " ORDER BY id LIMIT 8) y " +
				"ON x.id = y.id + 1 GROUP BY y.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -1.00|1;0.00|1;200.00|1;1275.00|4;|1;",
			why:  "the same, with the LIMIT spelling",
		},
		{
			name: "ctl/arm/bare-hit-with-nothing-blocking-the-arm's-projection",
			sql: "SELECT y.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a*100 AS w FROM " + tbl + ") y ON x.id = y.id + 1 " +
				"GROUP BY y.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -1.00|1;0.00|1;200.00|1;1275.00|4;|1;",
			why:  "both arms materialise `w`, so the join qualifies y's and rule 1 names it",
		},
		{
			// THREE arms all publishing one alias, keyed by each in turn. Two
			// arms cannot tell "picks the right arm" from "picks the second".
			name: "arm/three-arms-key-names-the-middle",
			sql: "SELECT y.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a*100 AS w FROM " + tbl + ") y ON x.id=y.id " +
				"JOIN (SELECT id, a*7 AS w FROM " + tbl + ") v ON x.id=v.id GROUP BY y.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -1.00|1;0.00|1;200.00|1;1275.00|4;|2;",
			why:  "the join qualifies each contested arm with its own alias",
		},
		{
			name: "arm/three-arms-key-names-the-last",
			sql: "SELECT v.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a*100 AS w FROM " + tbl + ") y ON x.id=y.id " +
				"JOIN (SELECT id, a*7 AS w FROM " + tbl + ") v ON x.id=v.id GROUP BY v.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -0.07|1;0.00|1;14.00|1;89.25|4;|2;",
			why:  "the CHAINED link's own alias, which the model reads off that link",
		},
		{
			name: "arm/three-arms-key-names-the-probe",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl + ") x " +
				"JOIN (SELECT id, a*100 AS w FROM " + tbl + ") y ON x.id=y.id " +
				"JOIN (SELECT id, a*7 AS w FROM " + tbl + ") v ON x.id=v.id GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			why:  "the probe arm's copy is the bare one, and the key names no build alias",
		},
		{
			// A derived alias SHADOWING a base column of a DIFFERENT TYPE
			// (`decpair.s` is TEXT), on each side of the join: binding the
			// wrong one is a type error as well as a wrong value.
			name: "arm/alias-shadows-a-base-column-probe-side",
			sql: "SELECT x.s AS s, COUNT(*) AS n FROM (SELECT id, a*3 AS s FROM " + tbl + ") x " +
				"JOIN " + tbl + " z ON x.id=z.id GROUP BY x.s ORDER BY s",
			cols: []string{"s", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			why:  "the arm's `s` is MATERIALIZED and the table's is not",
		},
		{
			name: "arm/alias-shadows-a-base-column-build-side",
			sql: "SELECT y.s AS s, COUNT(*) AS n FROM " + tbl + " z JOIN (SELECT id, a*3 AS s " +
				"FROM " + tbl + ") y ON y.id=z.id GROUP BY y.s ORDER BY s",
			cols: []string{"s", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			why:  "the join qualified the contested build column to `y.s`",
		},
		{
			// The DEFINITION's column exists on BOTH arms and the key's arm
			// cannot materialise its alias — the shape whose payload
			// `ensureJoinCarriesEvaluatedColumns` widens from the resolution.
			// It ran on the DAG at base through the gather's rename, and
			// refusing it in round 1 was a right-to-routed regression.
			name: "arm/def-column-on-both-arms-limit",
			sql: "SELECT x.w AS w, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " + tbl +
				" ORDER BY id LIMIT 5) x JOIN (SELECT id + 1 AS id, a FROM " + tbl + ") z " +
				"ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "3 rows: -0.03|1;6.00|1;38.25|2;",
			why:  "the payload is widened with `x.a`, and the definition is re-spelled to it",
		},
		{
			name: "arm/def-column-on-both-arms-limit-with-a-sum-over-the-other",
			sql: "SELECT x.w AS w, SUM(z.a) AS s, COUNT(*) AS n FROM (SELECT id, a*3 AS w FROM " +
				tbl + " ORDER BY id LIMIT 5) x JOIN (SELECT id + 1 AS id, a FROM " + tbl + ") z " +
				"ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols: []string{"w", "s", "n"},
			want: "3 rows: -0.03|12.75|1;6.00|-0.01|1;38.25|25.50|2;",
			why:  "the aggregate over the OTHER arm's `a` is what makes the two visibly different",
		},
		{
			// A RawInputAggregate final: COUNT(DISTINCT) makes the grouped
			// final read RAW rows, so the clustering its input is asked for
			// has to be spelled in the RESOLUTION and not in the published
			// name (`clusteringKeysForAggregate`).
			name: "arm/distinct-aggregate-beside-a-derived-alias-key",
			sql: "SELECT x.w AS w, COUNT(DISTINCT z.id) AS n FROM (SELECT id, a*3 AS w FROM " +
				tbl + ") x JOIN " + tbl + " z ON x.id = z.id GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|2;",
			why:  "a raw-input final's child carries the resolution spelling",
		},
		{
			name: "arm/distinct-aggregate-with-the-arm's-projection-blocked",
			sql: "SELECT x.w AS w, COUNT(DISTINCT z.id) AS n FROM (SELECT id, a*3 AS w FROM " +
				tbl + " ORDER BY id LIMIT 8) x JOIN " + tbl + " z ON x.id = z.id " +
				"GROUP BY x.w ORDER BY w",
			cols: []string{"w", "n"},
			want: "5 rows: -0.03|1;0.00|1;6.00|1;38.25|4;|1;",
			why:  "the key is MATERIALIZED, so its input has no column to be clustered on at all",
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				before := int64(0)
				if arm.coord != nil {
					before = arm.coord.GroupKeyLocalRoutes()
				}
				res, err := arm.run(c.sql)
				if err != nil {
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, c.sql)
				}
				if got := dajDigest(res, c.cols); got != c.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  (%s)\n"+
						"  SQL: %s", arm.name, got, c.want, c.why, c.sql)
				}
				if arm.coord == nil {
					continue
				}
				if arm.coord.GroupKeyLocalRoutes() != before {
					t.Errorf("the %s arm ROUTED this to the coordinator-local pipeline. The "+
						"answer is right either way, which is why the disposition is asserted: "+
						"the DAG resolves this key (%s)\n  SQL: %s", arm.name, c.why, c.sql)
				}
			}
		})
	}
}
