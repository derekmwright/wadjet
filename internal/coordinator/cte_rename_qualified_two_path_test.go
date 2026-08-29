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
		// pin marks the select-item spelling whose ALIAS collides with the
		// build side's real column name.
		pin string
	}{
		{name: "NoAliasCollision", item: "id AS kk"},
		// The reviewer's literal spelling: the CTE aliases `id` to `k`, which
		// is also typemx_dim's own column name. A build-side reference then
		// stops resolving on the join stage — for a predicate this resolver
		// does not touch at all, which is what
		// TestBuildSideRefWithCollidingProbeAliasIsASeparateDefect proves.
		{name: "AliasCollidesWithBuildColumn", item: "id AS k",
			pin: "a probe-side SELECT alias equal to the build column's name"},
	} {
		for _, shape := range []struct{ name, tmpl string }{
			{"CTE", cteFrom},
			{"DerivedTable", derivedFrom},
		} {
			for _, c := range cases {
				c, shape, sel := c, shape, sel
				t.Run(sel.name+"/"+shape.name+"/"+c.name, func(t *testing.T) {
					sql := fmt.Sprintf(shape.tmpl, typematrix.Table, typematrix.Dim, sel.item, c.where)
					// The collision only bites a predicate that has to read
					// the build column ON THE JOIN STAGE; the conjunct alone
					// is pushed to the dim scan and is unaffected.
					pinned := sel.pin != "" && strings.Contains(c.where, "OR")
					ctrCheckCountPinned(t, ctx, single, coord, sql, c.want, pinned, sel.pin)
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

	// A BARE reference both arms can emit has no attribution in the
	// predicate's text. PostgreSQL rejects it (42702) and so does the
	// resolver: declining silently would leave a spelling no stage resolves,
	// which is UNKNOWN on every row and zero rows in silence.
	t.Run("AmbiguousBareNameIsRefused", func(t *testing.T) {
		sql := fmt.Sprintf(
			`WITH c AS (SELECT g AS k, c_i64 AS v FROM %[1]s) `+
				`SELECT COUNT(*) AS n FROM c JOIN %[2]s d ON c.k = d.k WHERE k > 3`,
			typematrix.Table, typematrix.Dim)
		_, err := tmdRunDAG(ctx, coord, sql)
		if err == nil {
			t.Fatalf("the stage DAG answered a reference both arms emit; PostgreSQL "+
				"refuses it with 42702\n  SQL: %s", sql)
		}
		if !strings.Contains(err.Error(), "ambiguous") || !strings.Contains(err.Error(), `"k"`) {
			t.Fatalf("the refusal does not name the ambiguous column: %v\n  SQL: %s", err, sql)
		}
	})
}

// TestBuildSideRefWithCollidingProbeAliasIsASeparateDefect isolates the
// remaining divergence the pinned rows above carry, and proves it is not this
// resolver's.
//
// The predicate here needs NO rewriting — `c.g` is a passthrough and `d.k`
// names the sibling arm — so ResolveFilterThroughProjects ships it verbatim,
// exactly as the pre-#653 planner did. It still answers 0 on the DAG, and
// renaming the probe-side alias away from the build column's name is the only
// change that fixes it. So the cause is the alias/column COLLISION somewhere
// in the join stage's schema, not the filter's spelling.
func TestBuildSideRefWithCollidingProbeAliasIsASeparateDefect(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)
	_, gGt3 := ctrJoinedRows(t)

	tmpl := `SELECT COUNT(*) AS n FROM (SELECT id AS %[3]s, g FROM %[1]s) c ` +
		`JOIN %[2]s d ON c.g = d.k WHERE d.k > 3 OR c.g > 100`

	// The control: an alias that collides with nothing.
	t.Run("NoCollision", func(t *testing.T) {
		ctrCheckCount(t, ctx, single, coord,
			fmt.Sprintf(tmpl, typematrix.Table, typematrix.Dim, "kk"), gGt3)
	})

	// The pin: same query, alias renamed onto the build column's name.
	t.Run("Collision", func(t *testing.T) {
		sql := fmt.Sprintf(tmpl, typematrix.Table, typematrix.Dim, "k")
		res, err := tmdRunDAG(ctx, coord, sql)
		if err != nil {
			t.Fatalf("dag arm failed: %v\n  SQL: %s", err, sql)
		}
		got := ctrCounts(t, res)
		if len(got) == 1 && got[0] == gGt3 {
			t.Fatalf("the stage DAG now answers this shape, so the separate defect this test "+
				"pins is FIXED. Un-pin the OR rows of "+
				"TestFilterQualifiedToOneJoinArmTwoPath and delete this test.\n  SQL: %s", sql)
		}
		t.Logf("tracked separate defect, NOT gated: a build-side qualified reference stops "+
			"resolving on the join stage when the PROBE subtree carries a SELECT alias of the "+
			"same bare name — the DAG answers %v where the single-process engine and "+
			"PostgreSQL answer %d, for a predicate this resolver ships verbatim", got, gGt3)
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
}
