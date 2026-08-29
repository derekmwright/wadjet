package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/wadjet"
)

// The renamed-CTE-column gate (#653).
//
// A WHERE above a CTE reference names the CTE's OUTPUT columns, and a CTE
// that renamed one (`SELECT c_i64 AS v`) is the shape that broke: on the
// stage DAG the query answered ZERO rows for every type while the
// single-process engine answered correctly.
//
// The mechanism is a hole between two layers, and the four shapes below are
// what localize it. `pushdownPredicates` normally swaps Filter-Project and
// SUBSTITUTES the alias away (`splitFilterForProjectPush`, #384), so a
// DERIVED table's rename never survives into the physical plan. A CTE's
// Project carries a CTEName tag — a materialization fence, because the
// single-process planner replays ONE cached result for every reference — so
// the swap is declined there and the predicate keeps the alias. walkStages
// then appends the filter's TEXT to the producing stage verbatim
// (`case logical.NodeFilter`), and a Project emits no stage on the DAG, so
// the predicate reached a scan fragment whose schema carries `c_i64` and not
// `v`. `expr.ColRef.Eval` answered nil for it, the predicate was UNKNOWN on
// every row, and a WHERE admits only TRUE.
//
// So the gate is the CROSS of the four shapes with a column per storage
// class: the rename through a CTE (qualified and bare), the same rename
// through a derived table, the same CTE with the filter INSIDE it, and the
// CTE with no rename at all. Three of the four were already right, which is
// what makes the matrix the diagnosis and not just the alarm.
type ctrCol struct {
	// name is the fixture column, lit the literal the corpus compares it
	// against, and want the row predicate SQL requires — written from the
	// fixture's generator, not from either engine's behaviour.
	name string
	lit  string
	want func(v any) bool
}

func ctrColumns() []ctrCol {
	return []ctrCol{
		{"c_i64", "0", func(v any) bool { x, ok := v.(int64); return ok && x > 0 }},
		{"c_f32", "1", func(v any) bool { x, ok := v.(float32); return ok && float64(x) > 1 }},
		{"c_str", "'a'", func(v any) bool { x, ok := v.(string); return ok && x > "a" }},
		{"c_dec", "100", func(v any) bool { x, ok := v.(float64); return ok && x > 100 }},
	}
}

// ctrWantIDs is the id set the predicate selects, computed in Go over the same
// deterministic fixture both arms read.
func ctrWantIDs(c ctrCol) []int64 {
	var out []int64
	for _, r := range typematrix.Data(typematrix.Rows) {
		if c.want(r[c.name]) {
			out = append(out, r["id"].(int64))
		}
	}
	return out
}

// ctrShapes is the four-shape matrix from #653 plus the filter-inside control,
// each spelled over one column. %[1]s is the column, %[2]s the literal,
// %[3]s the table.
func ctrShapes() []struct{ name, tmpl string } {
	return []struct{ name, tmpl string }{
		// The control: no rename, so the predicate names a column the scan
		// already emits. Correct before the fix.
		{"CTENoRename",
			`WITH c AS (SELECT id, %[1]s FROM %[3]s) SELECT id FROM c WHERE c.%[1]s > %[2]s ORDER BY id`},
		// The reported shape, qualified by the CTE's own name.
		{"CTERenameQualified",
			`WITH c AS (SELECT id, %[1]s AS v FROM %[3]s) SELECT id FROM c WHERE c.v > %[2]s ORDER BY id`},
		// The same rename referenced bare. Qualification is not the trigger.
		{"CTERenameBare",
			`WITH c AS (SELECT id, %[1]s AS v FROM %[3]s) SELECT id FROM c WHERE v > %[2]s ORDER BY id`},
		// The same rename through a DERIVED TABLE. Correct before the fix,
		// because the logical Filter-Project swap substitutes the alias away
		// where no CTE fence declines it.
		{"DerivedRename",
			`SELECT id FROM (SELECT id, %[1]s AS v FROM %[3]s) s WHERE s.v > %[2]s ORDER BY id`},
		// The filter INSIDE the CTE body, where it names a source column.
		// Correct before the fix.
		{"CTEFilterInside",
			`WITH c AS (SELECT id, %[1]s AS v FROM %[3]s WHERE %[1]s > %[2]s) SELECT id FROM c ORDER BY id`},
		// The rename over a COMPUTED output rather than a plain column: the
		// substitution has to carry the defining expression, not a name.
		{"CTEComputedRename",
			`WITH c AS (SELECT id, %[1]s AS v FROM %[3]s) SELECT id FROM c WHERE COALESCE(v, %[2]s) > %[2]s ORDER BY id`},
	}
}

func TestCTERenamedColumnTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the renamed-CTE-column gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	check := func(t *testing.T, sql string, wantIDs []int64) {
		t.Helper()
		for _, arm := range []struct {
			name string
			run  func() (*oracle.Result, error)
		}{
			{"single", func() (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
			{"dag", func() (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
		} {
			res, err := arm.run()
			if err != nil {
				t.Errorf("%s arm failed: %v\n  SQL: %s", arm.name, err, sql)
				continue
			}
			if diff := diffIDs(wantIDs, predIDs(t, res)); diff != "" {
				t.Errorf("%s arm answered the wrong rows\n  SQL: %s\n  %s", arm.name, sql, diff)
			}
		}
	}

	for _, c := range ctrColumns() {
		c := c
		wantIDs := ctrWantIDs(c)
		if len(wantIDs) == 0 {
			t.Fatalf("%s: the corpus predicate selects no row, so the gate would pass on an "+
				"engine that returns nothing", c.name)
		}
		for _, s := range ctrShapes() {
			s := s
			t.Run(c.name+"/"+s.name, func(t *testing.T) {
				check(t, fmt.Sprintf(s.tmpl, c.name, c.lit, typematrix.Table), wantIDs)
			})
		}
		// The issue's own spelling: COUNT(*) rather than the row set, so the
		// predicate rides an aggregate stage instead of the gather.
		t.Run(c.name+"/CTERenameCount", func(t *testing.T) {
			sql := fmt.Sprintf(
				`WITH c AS (SELECT id, %s AS v FROM %s) SELECT COUNT(*) AS n FROM c WHERE c.v > %s`,
				c.name, typematrix.Table, c.lit)
			ctrCheckCount(t, ctx, single, coord, sql, int64(len(wantIDs)))
		})
	}

	// A renamed CTE column feeding a JOIN: the predicate travels through join
	// planning, and on the DAG the filter lands on one arm's own stage.
	t.Run("CTEFeedsJoin", func(t *testing.T) {
		sql := fmt.Sprintf(
			`WITH c AS (SELECT g AS gk, c_i64 AS v FROM %s) `+
				`SELECT COUNT(*) AS n FROM c JOIN %s d ON c.gk = d.k WHERE c.v > 0`,
			typematrix.Table, typematrix.Dim)
		var want int64
		for _, r := range typematrix.Data(typematrix.Rows) {
			if _, ok := r["g"].(int32); !ok {
				continue
			}
			if x, ok := r["c_i64"].(int64); ok && x > 0 {
				want++
			}
		}
		ctrCheckCount(t, ctx, single, coord, sql, want)
	})

	// The CTE referenced TWICE, so the reference the predicate sits above is
	// also the one the DAG's CTE dedup may serve from a `cte-alias` pointing
	// at the other one's terminal stage. The predicate is applied above the
	// OUTER reference, which is the placement both paths agree on today —
	// a predicate above the INNER one is dropped on the DAG, a separate
	// pre-existing defect that reproduces without any rename (see the issue
	// thread), so gating it here would gate someone else's bug.
	t.Run("CTEReferencedTwice", func(t *testing.T) {
		const lit = 1_000_000_000
		sql := fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %[1]s) `+
				`SELECT COUNT(*) AS n FROM c JOIN (SELECT id AS j FROM c) x `+
				`ON c.id = x.j WHERE c.v > %[2]d`, typematrix.Table, lit)
		var want int64
		for _, r := range typematrix.Data(typematrix.Rows) {
			if x, ok := r["c_i64"].(int64); ok && x > lit {
				want++
			}
		}
		ctrCheckCount(t, ctx, single, coord, sql, want)
	})

	// TWO renamed CTEs joined to each other, so both sides of the join carry
	// a rename the DAG has to resolve — and the join key is one of them.
	t.Run("TwoRenamedCTEsJoined", func(t *testing.T) {
		const lit = 1_000_000_000
		sql := fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %[1]s), `+
				`c2 AS (SELECT id AS i2, c_i64 AS w FROM %[1]s) `+
				`SELECT COUNT(*) AS n FROM c JOIN c2 ON c.id = c2.i2 `+
				`WHERE c.v > %[2]d AND c2.w > 0`, typematrix.Table, lit)
		var want int64
		for _, r := range typematrix.Data(typematrix.Rows) {
			if x, ok := r["c_i64"].(int64); ok && x > lit {
				want++
			}
		}
		ctrCheckCount(t, ctx, single, coord, sql, want)
	})

	// A NESTED CTE: the rename chains, `v` → `u` → `c_i64`, and stopping one
	// level short leaves a name nothing emits.
	t.Run("NestedCTE", func(t *testing.T) {
		sql := fmt.Sprintf(
			`WITH a AS (SELECT id, c_i64 AS u FROM %s), b AS (SELECT id, u AS v FROM a) `+
				`SELECT COUNT(*) AS n FROM b WHERE b.v > 0`, typematrix.Table)
		var want int64
		for _, r := range typematrix.Data(typematrix.Rows) {
			if x, ok := r["c_i64"].(int64); ok && x > 0 {
				want++
			}
		}
		ctrCheckCount(t, ctx, single, coord, sql, want)
	})
}

// TestCTERenamedColumnSiblingShapesTwoPath asks the same rename through every
// OTHER consumer a CTE output can reach — GROUP BY, ORDER BY, a SELECT-list
// expression, a join's ON clause and a window's PARTITION BY. Each has its own
// resolver on the DAG (see docs/internals/native-dag-execution.md
// §Derived-table aliases), so a fix to the filter's is no evidence about
// theirs; this is the sweep that says whether #653's cause is shared.
func TestCTERenamedColumnSiblingShapesTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the renamed-CTE-column sibling gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, c := range []struct {
		name, sql string
		// dagRefuses names a shape whose DAG arm fails LOUDLY today for a
		// mechanism that is not this issue's. It is a pin, not an exemption:
		// a silently different ANSWER still fails, and the day the DAG
		// answers it the pin fails so it gets deleted.
		dagRefuses string
	}{
		{name: "GroupBy", sql: fmt.Sprintf(
			`WITH c AS (SELECT g AS gk, c_i64 AS v FROM %s) `+
				`SELECT gk, COUNT(*) AS n FROM c GROUP BY gk ORDER BY gk`, typematrix.Table)},
		{name: "GroupByRenamedMeasure", sql: fmt.Sprintf(
			`WITH c AS (SELECT g AS gk, c_i64 AS v FROM %s) `+
				`SELECT gk, SUM(v) AS s FROM c GROUP BY gk ORDER BY gk`, typematrix.Table)},
		{name: "OrderBy", sql: fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %s) `+
				`SELECT id FROM c ORDER BY v DESC, id LIMIT 20`, typematrix.Table)},
		{name: "SelectListExpr", sql: fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %s) `+
				`SELECT id, v + 1 AS w FROM c WHERE id < 40 ORDER BY id`, typematrix.Table)},
		{name: "JoinOn", sql: fmt.Sprintf(
			`WITH c AS (SELECT g AS gk, c_i64 AS v FROM %s) `+
				`SELECT d.label AS label, COUNT(*) AS n FROM c JOIN %s d ON c.gk = d.k `+
				`GROUP BY d.label ORDER BY label`, typematrix.Table, typematrix.Dim)},
		{name: "HavingRenamed", sql: fmt.Sprintf(
			`WITH c AS (SELECT g AS gk, c_i64 AS v FROM %s) `+
				`SELECT gk, COUNT(*) AS n FROM c GROUP BY gk HAVING COUNT(*) > 1 ORDER BY gk`,
			typematrix.Table)},
		// A window's PARTITION BY naming a RENAMED output is a DIFFERENT
		// mechanism, and this entry is here to say so rather than to gate it.
		// resolveWindowKeys binds a QUALIFIED reference to an input column and
		// materializes an EXPRESSION, but a bare reference to a Project's
		// alias is neither: the single-process window reads it off the
		// Project's output, and on the DAG the Project emits no stage, so the
		// worker refuses with `window: PARTITION BY "gk" is not a column of
		// its input`. It reproduces identically through a DERIVED TABLE, with
		// no CTE anywhere, so it is not #653's cause — and unlike #653 it is
		// LOUD. Fixing it needs a DAG-side repair pass of its own, the way
		// resolveDerivedAliasSortKeys is the sort keys': resolveWindowKeys is
		// deliberately shared by both paths and the two have different input
		// schemas at the window, so the binding cannot be made there.
		{name: "WindowPartitionBy", dagRefuses: `is not a column of its input`, sql: fmt.Sprintf(
			`WITH c AS (SELECT id, g AS gk, c_i64 AS v FROM %s WHERE id < 200) `+
				`SELECT id, ROW_NUMBER() OVER (PARTITION BY gk ORDER BY id) AS rn `+
				`FROM c ORDER BY id`, typematrix.Table)},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			aRes, aErr := tmdRunSingle(ctx, single, c.sql)
			bRes, bErr := tmdRunDAG(ctx, coord, c.sql)
			if c.dagRefuses != "" {
				switch {
				case bErr == nil:
					t.Errorf("the stage DAG now ANSWERS this shape, so the separate defect this "+
						"entry pins is gone. Compare the arms and delete dagRefuses.\n  SQL: %s", c.sql)
				case !strings.Contains(bErr.Error(), c.dagRefuses):
					t.Errorf("the stage DAG failed for some OTHER reason than the pinned one (%q): %v\n  SQL: %s",
						c.dagRefuses, bErr, c.sql)
				default:
					t.Logf("tracked separate defect, NOT gated here: the DAG refuses this shape "+
						"loudly (%v); the single-process engine answers %d rows", bErr, len(aRes.Rows))
				}
				return
			}
			switch {
			case aErr != nil && bErr != nil:
				t.Skipf("both arms refuse this shape: single=%v dag=%v", aErr, bErr)
			case aErr != nil:
				t.Errorf("the single-process arm FAILED on a query the DAG answered (%d rows): %v\n  SQL: %s",
					len(bRes.Rows), aErr, c.sql)
			case bErr != nil:
				t.Errorf("the stage DAG FAILED on a query the single-process engine answered (%d rows): %v\n  SQL: %s",
					len(aRes.Rows), bErr, c.sql)
			default:
				if diff := oracle.Compare(aRes, bRes, oracle.CompareSpec{Mode: oracle.CmpOrdered}); diff != "" {
					t.Errorf("TWO-PATH DIVERGENCE over a renamed CTE column\n  SQL: %s\n  %s\n"+
						"  single: %s\n  dag:    %s", c.sql, diff, tmdRender(aRes, 3), tmdRender(bRes, 3))
				}
				if len(aRes.Rows) == 0 {
					t.Errorf("both arms answered ZERO rows, so this shape proves nothing\n  SQL: %s", c.sql)
				}
			}
		})
	}
}

// TestUnresolvableFilterColumnErrorsOnBothPaths is the general half of #653.
//
// The rename was one way to hand a filter a column name its input does not
// carry; the failure it produced — nil from expr.ColRef.Eval, UNKNOWN on
// every row, and a WHERE that admits only TRUE — belongs to the NAME, not to
// the rename. That is the #147 failure mode, which the vectorized KernelFilter
// has refused since ("filter column %q does not exist in the input schema")
// and the row evaluator did not. Both arms must refuse rather than answer
// zero rows, and the refusal must be PostgreSQL's 42703.
func TestUnresolvableFilterColumnErrorsOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, c := range []struct{ name, sql string }{
		{"BareUnknownColumn",
			fmt.Sprintf(`SELECT id FROM %s WHERE nosuchcol > 0`, typematrix.Table)},
		{"QualifiedUnknownColumn",
			fmt.Sprintf(`SELECT id FROM %s t WHERE t.nosuchcol > 0`, typematrix.Table)},
		{"UnknownColumnAboveCTE", fmt.Sprintf(
			`WITH c AS (SELECT id, c_i64 AS v FROM %s) SELECT COUNT(*) AS n FROM c WHERE c.nosuchcol > 0`,
			typematrix.Table)},
		{"UnknownColumnAboveDerived", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT id, c_i64 AS v FROM %s) s WHERE s.nosuchcol > 0`,
			typematrix.Table)},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				run  func() (*oracle.Result, error)
			}{
				{"single", func() (*oracle.Result, error) { return tmdRunSingle(ctx, single, c.sql) }},
				{"dag", func() (*oracle.Result, error) { return tmdRunDAG(ctx, coord, c.sql) }},
			} {
				res, err := arm.run()
				switch {
				case err == nil:
					t.Errorf("%s arm ANSWERED (%d rows) a query naming a column nothing has; "+
						"PostgreSQL refuses it with 42703\n  SQL: %s", arm.name, len(res.Rows), c.sql)
				case strings.HasPrefix(err.Error(), "PANIC:"):
					t.Errorf("%s arm PANICKED instead of reporting the refusal: %v\n  SQL: %s",
						arm.name, err, c.sql)
				case !strings.Contains(strings.ToLower(err.Error()), "nosuchcol"):
					t.Errorf("%s arm failed for some other reason: %v\n  SQL: %s", arm.name, err, c.sql)
				}
			}
		})
	}
}

// ctrCheckCount runs one scalar-count query on both arms and holds each to the
// count computed in Go.
func ctrCheckCount(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator, sql string, want int64) {
	t.Helper()
	ctrCheckCountPinned(t, ctx, single, coord, sql, want, false, "")
}

// ctrCheckCountPinned is ctrCheckCount with an escape for a DAG divergence
// whose cause is a DIFFERENT defect, verified separately. It is a pin and not
// an exemption: the single-process arm is still held to the answer, and the
// day the DAG agrees the pin FAILS so it gets deleted.
func ctrCheckCountPinned(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator,
	sql string, want int64, pinned bool, reason string) {
	t.Helper()
	for _, arm := range []struct {
		name string
		run  func() (*oracle.Result, error)
	}{
		{"single", func() (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"dag", func() (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
	} {
		res, err := arm.run()
		if err != nil {
			t.Errorf("%s arm failed: %v\n  SQL: %s", arm.name, err, sql)
			continue
		}
		got := ctrCounts(t, res)
		if len(got) != 1 {
			t.Errorf("%s arm answered %d rows, want 1\n  SQL: %s", arm.name, len(res.Rows), sql)
			continue
		}
		if got[0] == want {
			if pinned && arm.name == "dag" {
				t.Errorf("the stage DAG now answers this shape, so the separate defect this "+
					"entry pins is gone (%s). Delete the pin.\n  SQL: %s", reason, sql)
			}
			continue
		}
		if pinned && arm.name == "dag" {
			t.Logf("tracked separate defect, NOT gated here (%s): the DAG answered %d, "+
				"want %d\n  SQL: %s", reason, got[0], want, sql)
			continue
		}
		t.Errorf("%s arm answered %d, want %d\n  SQL: %s", arm.name, got[0], want, sql)
	}
}

// ctrCounts pulls the single numeric column out of a COUNT/SUM result.
func ctrCounts(t *testing.T, res *oracle.Result) []int64 {
	t.Helper()
	out := make([]int64, 0, len(res.Rows))
	for _, r := range res.Rows {
		var v any
		for _, c := range res.Columns {
			v = r[c]
			break
		}
		switch x := v.(type) {
		case int64:
			out = append(out, x)
		case int32:
			out = append(out, int64(x))
		case float64:
			out = append(out, int64(x))
		default:
			t.Fatalf("count came back as %T (%v)", v, v)
		}
	}
	return out
}
