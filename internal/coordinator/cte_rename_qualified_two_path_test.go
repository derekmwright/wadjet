package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The QUALIFIER half of #653's resolver.
//
// Re-spelling a predicate through the Projects below it (#653) has to answer
// which RELATION each reference names, not just which column. The first cut
// did not: `substituteColRefs` matched on the bare column name and the walk
// applied each join arm's map to the whole predicate in turn, so a reference
// qualified to the OTHER arm was rewritten with this arm's definition —
// `d.k > 3 OR c.gg > 100` over `SELECT … g AS gg` became `… or g > 100` and,
// worse, `d.k` became `id`. That is 4612 rows where PostgreSQL 17 answers
// 1978: a silent WRONG answer, strictly worse than the obviously-wrong 0 it
// replaced. It reproduces through a DERIVED table with no CTE anywhere,
// because the Filter's child is the JOIN and the logical Filter-Project swap
// never applies to either spelling.
//
// The gate is that predicate and each of its conjuncts alone, over both
// spellings: the reference qualified to the sibling arm must not be rewritten
// at all, and the one qualified to the renamed arm must be.
//
// `gg` IS `g`, so `c.gg > 100` is FALSE on every row of this fixture. That is
// deliberate: it makes the mis-attributed rewrite visible as a row COUNT,
// because `id > 100` is true for almost all of them.
func ctrJoinedRows(t *testing.T) (matched, gGt3 int64) {
	t.Helper()
	for _, r := range typematrix.Data(typematrix.Rows) {
		g, ok := r["g"].(int32)
		if !ok {
			continue // a NULL group key joins to no dim row
		}
		matched++
		if g > 3 {
			gGt3++
		}
	}
	return matched, gGt3
}

func TestFilterQualifiedToOneJoinArmTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the qualified-filter gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	matched, gGt3 := ctrJoinedRows(t)

	// %[1]s is the fixture table, %[2]s the dim table, %[3]s the first select
	// item, %[4]s the WHERE clause.
	const cteFrom = `WITH c AS (SELECT %[3]s, g AS gg FROM %[1]s) ` +
		`SELECT COUNT(*) AS n FROM c JOIN %[2]s d ON c.gg = d.k WHERE %[4]s`
	const derivedFrom = `SELECT COUNT(*) AS n FROM (SELECT %[3]s, g AS gg FROM %[1]s) c ` +
		`JOIN %[2]s d ON c.gg = d.k WHERE %[4]s`

	cases := []struct {
		name  string
		where string
		want  int64
		// pinned names a shape whose remaining divergence has a DIFFERENT
		// cause, verified independently below.
		pinned string
	}{
		// The reported predicate.
		{name: "OrAcrossArms", where: "d.k > 3 OR c.gg > 100", want: gGt3},
		// Each conjunct alone: one names only the sibling arm, the other only
		// the renamed one.
		{name: "OtherArmOnly", where: "d.k > 3", want: gGt3},
		{name: "RenamedArmOnly", where: "c.gg > 100", want: 0},
		{name: "RenamedArmOnlyTrue", where: "c.gg >= 0", want: matched},
		// AND, where a mis-attributed operand narrows instead of widening.
		{name: "AndAcrossArms", where: "d.k > 3 AND c.gg >= 0", want: gGt3},
	}

	for _, sel := range []struct {
		name, item string
	}{
		{name: "NoAliasCollision", item: "id AS kk"},
		// The literal spelling from the issue: the CTE aliases `id` to `k`,
		// which is also typemx_dim's own column name. The FAILING reference
		// was `d.k`, which this resolver leaves exactly as written (it does
		// rewrite `c.gg` to `g` in the same predicate).
		//
		// It was pinned as a separate defect and it was one: the collision
		// bit in the JOIN's OutputFilter. The probe arm publishes an alias
		// `k`, so the join's output carries the dim's own `k` and the
		// filter — which names `d.k` — matched neither the bare column nor
		// any qualified spelling, dropped it, and left the predicate
		// UNKNOWN. `joinOutputSchemaWithMapping` now keeps a BARE column a
		// QUALIFIED filter entry names, which is the mirror of the rule it
		// already had, and that closed this shape.
		//
		// Attributed by reverting each candidate separately: with the
		// OutputFilter mirror alone the shape answers; with only the
		// resolver's ROW-guard repair it does not.
		// TestBuildSideRefWithCollidingProbeAliasIsASeparateDefect existed
		// only to isolate this and is deleted with the pin.
		{name: "AliasCollidesWithBuildColumn", item: "id AS k"},
	} {
		for _, shape := range []struct{ name, tmpl string }{
			{"CTE", cteFrom},
			{"DerivedTable", derivedFrom},
		} {
			for _, c := range cases {
				c, shape, sel := c, shape, sel
				t.Run(sel.name+"/"+shape.name+"/"+c.name, func(t *testing.T) {
					sql := fmt.Sprintf(shape.tmpl, typematrix.Table, typematrix.Dim, sel.item, c.where)
					ctrCheckCount(t, ctx, single, coord, sql, c.want)
				})
			}
		}
	}

	// R2's shape: the rename is TWO levels up from the join, and both arms of
	// the inner join rename a column to the same name (`x`). The qualifier is
	// what tells `q.w` from anything on p, and before the fix the walk
	// declined outright and answered 0.
	t.Run("RenamedThroughAJoinOfTwoRenamedArms", func(t *testing.T) {
		sql := fmt.Sprintf(
			`WITH c AS (SELECT p.x AS px, q.w AS qw FROM `+
				`(SELECT id AS x FROM %[1]s) p JOIN (SELECT g AS x, c_i64 AS w FROM %[1]s) q `+
				`ON p.x = q.x) SELECT COUNT(*) AS n FROM c WHERE qw > 0`, typematrix.Table)
		var want int64
		for _, r := range typematrix.Data(typematrix.Rows) {
			if _, ok := r["g"].(int32); !ok {
				continue // q's join key is NULL: no p row matches it
			}
			if v, ok := r["c_i64"].(int64); ok && v > 0 {
				want++
			}
		}
		ctrCheckCount(t, ctx, single, coord, sql, want)
	})

	// A BARE reference two relations in scope both carry is rejected by the
	// plan validator, which owns ambiguity for every query shape and both
	// paths (physical.validate, "column reference %q is ambiguous", 42702).
	// This is a gate on THAT, not on the rename resolver: the resolver never
	// sees such a predicate, and an earlier cut of it grew a duplicate
	// ambiguity check that no SQL could reach.
	for _, c := range []struct{ name, sql, col string }{
		{"BothArmsBaseColumn", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %[1]s a JOIN %[1]s b ON a.g = b.g WHERE c_i64 > 0`,
			typematrix.Table), "c_i64"},
		{"RenamedArmVersusBaseColumn", fmt.Sprintf(
			`WITH c AS (SELECT g AS k, c_i64 AS v FROM %[1]s) `+
				`SELECT COUNT(*) AS n FROM c JOIN %[2]s d ON c.k = d.k WHERE k > 3`,
			typematrix.Table, typematrix.Dim), "k"},
	} {
		c := c
		t.Run("AmbiguousBareNameIsRefused/"+c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				run  func() (*oracle.Result, error)
			}{
				{"single", func() (*oracle.Result, error) { return tmdRunSingle(ctx, single, c.sql) }},
				{"dag", func() (*oracle.Result, error) { return tmdRunDAG(ctx, coord, c.sql) }},
			} {
				res, err := arm.run()
				if err == nil {
					t.Errorf("%s arm ANSWERED (%d rows) a reference two relations carry; "+
						"PostgreSQL refuses it with 42702\n  SQL: %s", arm.name, len(res.Rows), c.sql)
					continue
				}
				if !strings.Contains(err.Error(), "ambiguous") ||
					!strings.Contains(err.Error(), `"`+c.col+`"`) {
					t.Errorf("%s arm's refusal does not name the ambiguous column: %v\n  SQL: %s",
						arm.name, err, c.sql)
				}
			}
		})
	}

	// The substitution can INTRODUCE a name two arms carry: `k` is
	// unambiguous as written (only c has it), and rewriting it to `id` hands
	// the join stage a name BOTH arms have. The answer must still be the
	// probe side's. Nothing checks this at plan time — the validator saw an
	// unambiguous query — so it is pinned here.
	t.Run("SubstitutionIntroducesASharedName", func(t *testing.T) {
		sql := fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT id AS k FROM %[1]s) c `+
				`JOIN %[1]s d ON c.k = d.g WHERE k > 3`, typematrix.Table)
		var want int64
		for _, r := range typematrix.Data(typematrix.Rows) {
			g, ok := r["g"].(int32)
			if !ok {
				continue // c.k = d.g cannot match a NULL g
			}
			if int64(g) > 3 {
				want++ // c.k IS d.g on a matched row, and k = id
			}
		}
		ctrCheckCount(t, ctx, single, coord, sql, want)
	})
}

// TestRowFieldPathThroughRenamedColumnTwoPath is ADR-0022 reaching the
// resolver: `rw.b` over `c_row AS rw` is a ROW FIELD PATH, not a
// table-qualified column, so the QUALIFIER is what gets substituted and the
// field is kept — `c_row.b`. Reading it as a qualified reference looks `b` up
// as a column, finds nothing, and leaves a name no stage emits: the COUNT
// spelling answered 0 on the DAG and the row spelling hit the scan-filter
// guard once #653's first cut added one.
func TestRowFieldPathThroughRenamedColumnTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the ROW-field-path gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	var wantIDs []int64
	for _, r := range typematrix.NestedData(typematrix.Rows) {
		row, ok := r["c_row"].(map[string]any)
		if !ok {
			continue
		}
		if b, ok := row["b"].(int64); ok && b > 100 {
			wantIDs = append(wantIDs, r["id"].(int64))
		}
	}
	if len(wantIDs) == 0 {
		t.Fatal("the corpus predicate selects no row, so the gate would pass on an engine " +
			"that returns nothing")
	}

	for _, c := range []struct{ name, tmpl string }{
		{"CTE", `WITH c AS (SELECT c_row AS rw, id FROM %s) SELECT id FROM c WHERE rw.b > 100 ORDER BY id`},
		{"DerivedTable", `SELECT id FROM (SELECT c_row AS rw, id FROM %s) s WHERE rw.b > 100 ORDER BY id`},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf(c.tmpl, typematrix.Nested)
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
		})
	}

	// The COUNT spelling, which has no scan-filter guard over it: it is the
	// one that answered 0 in silence.
	t.Run("Count", func(t *testing.T) {
		sql := fmt.Sprintf(
			`WITH c AS (SELECT c_row AS rw, id FROM %s) SELECT COUNT(*) AS n FROM c WHERE rw.b > 100`,
			typematrix.Nested)
		ctrCheckCount(t, ctx, single, coord, sql, int64(len(wantIDs)))
	})

	// The SAME spelling where the projection ALSO outputs a column named `b`.
	//
	// The FIELD wins, and it is PostgreSQL's only anchored reading. PG rejects
	// the unparenthesised form outright — `missing FROM-clause entry for table
	// "rw"`, 42P01, for every spelling here — and reads `(rw).b` as the FIELD;
	// measured over the same shape:
	//
	//	WITH c AS (SELECT c_row AS rw, id AS b FROM n)
	//	SELECT COUNT(*) FROM c WHERE (rw).b > 100   -- the field
	//	SELECT COUNT(*) FROM c WHERE b > 100        -- the column, a different number
	//
	// Answering the bare spelling at all is the superset ADR-0012 records;
	// WHICH answer is ADR-0022 §1's order, and this subtest asserted the
	// opposite of it until 2026-09-04. The old order — strip the qualifier
	// and take the bare column BEFORE reading the qualifier as a container —
	// is what made `c_row.b` beside a join arm publishing `b` answer THAT
	// ARM's column, on every arm and in silence (#769). The strip exists to
	// resolve `t.col` where the stream carries only `col`; it is a fallback
	// for a RELATION qualifier and never a reading of a container.
	//
	// So the order is: the whole dotted spelling as a column of its own, then
	// the qualifier as a ROW column THAT DECLARES the field, and only then the
	// strip.
	t.Run("FieldWinsOverColumnWhenBothExist", func(t *testing.T) {
		var wantColumn int64
		for _, r := range typematrix.NestedData(typematrix.Rows) {
			if r["id"].(int64) > 100 {
				wantColumn++
			}
		}
		want := int64(len(wantIDs)) // the FIELD's rows, computed by the caller
		if want == wantColumn {
			t.Fatalf("the column and the field select the same rows (%d), so this gate "+
				"cannot tell them apart", want)
		}
		for _, tmpl := range []string{
			`WITH c AS (SELECT c_row AS rw, id AS b FROM %s) SELECT COUNT(*) AS n FROM c WHERE rw.b > 100`,
			`SELECT COUNT(*) AS n FROM (SELECT c_row AS rw, id AS b FROM %s) s WHERE rw.b > 100`,
		} {
			ctrCheckCount(t, ctx, single, coord, fmt.Sprintf(tmpl, typematrix.Nested), want)
		}
	})
	// The CONTROL that keeps the reorder off an ordinary qualified reference:
	// `rw` here is NOT a container, so the strip is the only reading and the
	// column answers.
	t.Run("ctl/a-qualifier-that-is-not-a-container-still-strips", func(t *testing.T) {
		var want int64
		for _, r := range typematrix.NestedData(typematrix.Rows) {
			if r["id"].(int64) > 100 {
				want++
			}
		}
		sql := fmt.Sprintf(
			`WITH c AS (SELECT id AS rw, id AS b FROM %s) SELECT COUNT(*) AS n FROM c WHERE rw.b > 100`,
			typematrix.Nested)
		ctrCheckCount(t, ctx, single, coord, sql, want)
	})
}
