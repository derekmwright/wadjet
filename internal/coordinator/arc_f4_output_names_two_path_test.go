package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// PostgreSQL's name for an UNALIASED output column, on FOUR arms (#732).
//
// Every `want` below is the column NAME plus the rows, rendered by e3Render,
// and every name was read off postgres:17-alpine with `\gdesc` — which is the
// same string a client reads out of RowDescription.
//
// The name is asserted on all four arms because the two engines derive it
// separately and had always agreed on the WRONG answer: wadjet named an
// unaliased item after its own rendered TEXT (`g + 1`, `count(*)`,
// `case when … end`), so a BI tool keying a column by name saw a different one
// from the one PostgreSQL would have given it. The single-process path applies
// the published name at its collecting sink and the stage DAG at the gather's
// OutputRename target; nothing between them shares a line of code, so a cell
// that passes on one arm and not the other is the expected failure mode and is
// why every cell drives all four.
//
// This gate, `sql.TestOutputColumnNameMatchesPostgres` and the wire arm's
// `Unaliased*` entries are the EVIDENCE for this arc. The TPC-H stage-dump
// golden is not: it records a stage's ID, type, column lists, deps and op
// counts — not `OutputRename.To`, not `ProjectExprSpec.SourceSlot` — and every
// computed TPC-H item is aliased, so it is structurally blind to both
// mechanisms. That it is unchanged says no plan SHAPE moved, which is a
// different and smaller claim (round-1 review P5).
//
// The name is a SECOND name and not a rewrite of the engine's own spelling: an
// aggregate's output column IS `logical.AggExpr.OutputCol`, which GROUP BY,
// HAVING and ORDER BY resolve against, and `SELECT COUNT(*), COUNT(g)` legally
// publishes ONE name for two of them. `unaliased-aggregate-twice` and
// `unaliased-aggregate-grouped` are the cells that hold that apart.
func TestArcF4OutputColumnNamesTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	cases := []struct {
		name string
		sql  string
		want string
		// pin records what an arm answers today where that is not
		// PostgreSQL's answer. A pin that starts agreeing FAILS.
		pin map[string]string
		// wantRoute names the local-route counter a DAG arm MUST move for this
		// shape, or "" when the DAG must EXECUTE it. Declaring it is the point:
		// a routed cell's name was published by the coordinator's own
		// single-process pipeline, so it proves nothing about the gather's
		// OutputRename.To, and a cell that silently started routing would look
		// like coverage it is not (COMMON's routing rule; review P2).
		wantRoute string
	}{
		// --- no natural name: `?column?` ---------------------------------
		{name: "arithmetic",
			sql:  "SELECT g + 1 FROM typemx WHERE id < 3 ORDER BY 1",
			want: "?column? | 1 | 2 | 3"},
		{name: "arithmetic-twice",
			sql:  "SELECT g + 1, g + 2 FROM typemx WHERE id < 3 ORDER BY 1",
			want: "?column?,?column? | 1,2 | 2,3 | 3,4"},
		{name: "unary-minus",
			sql:  "SELECT -g FROM typemx WHERE id < 3 ORDER BY 1",
			want: "?column? | -2 | -1 | 0"},
		{name: "literal",
			sql:  "SELECT 1 FROM typemx WHERE id < 3",
			want: "?column? | 1 | 1 | 1"},
		{name: "predicate",
			sql:  "SELECT g IN (1,2) FROM typemx WHERE id < 3 ORDER BY 1",
			want: "?column? | false | true | true"},
		{name: "between",
			sql:  "SELECT g BETWEEN 1 AND 2 FROM typemx WHERE id < 3 ORDER BY 1",
			want: "?column? | false | true | true"},
		{name: "concat",
			sql:  "SELECT c_str || 'x' FROM typemx WHERE id < 2 ORDER BY 1",
			want: "?column? | s-000000x | s-000001x"},
		// A `?column?` beside an ALIASED item, which is the shape #732 filed.
		{name: "arithmetic-beside-an-alias",
			sql:  "SELECT g + 1, COUNT(*) AS n FROM typemx WHERE id < 3 GROUP BY g + 1 ORDER BY 1",
			want: "?column?,n | 1,1 | 2,1 | 3,1"},

		// --- a call takes the FUNCTION's name ----------------------------
		{name: "scalar-function",
			sql:  "SELECT ABS(g) FROM typemx WHERE id < 3 ORDER BY 1",
			want: "abs | 0 | 1 | 2"},
		{name: "coalesce",
			sql:  "SELECT COALESCE(g, 0) FROM typemx WHERE id < 3 ORDER BY 1",
			want: "coalesce | 0 | 1 | 2"},
		{name: "case",
			sql:  "SELECT CASE WHEN g > 1 THEN 1 ELSE 0 END FROM typemx WHERE id < 3 ORDER BY 1",
			want: "case | 0 | 0 | 1"},
		{name: "extract",
			sql:  "SELECT EXTRACT(YEAR FROM c_date) FROM typemx WHERE id < 2 ORDER BY 1",
			want: "extract | 2010 | 2011"},
		// PostgreSQL labels the column after the function it RESOLVED to, and
		// `TRIM(x)` resolves to `btrim` — measured, along with `ltrim` and
		// `rtrim` keeping their own names.
		{name: "trim-is-btrim",
			sql:  "SELECT TRIM(' a ') FROM typemx WHERE id < 2",
			want: "btrim | a | a"},
		// …and so does POSITION, which this parser lowers to `strpos`.
		{name: "position-is-position",
			sql:  "SELECT POSITION('-' IN c_str) FROM typemx WHERE id < 2 ORDER BY 1",
			want: "position | 2 | 2"},

		// --- an AGGREGATE takes the function's name too -------------------
		{name: "aggregate",
			sql:  "SELECT COUNT(*) FROM typemx",
			want: "count | 5000"},
		{name: "aggregate-sum",
			sql:  "SELECT SUM(g) FROM typemx",
			want: "sum | 13846"},
		// TWO unaliased aggregates publish ONE name. Inside the planner they
		// must not: each is its own Aggregate output column.
		{name: "unaliased-aggregate-twice",
			sql:  "SELECT COUNT(*), COUNT(g) FROM typemx",
			want: "count,count | 5000,4616"},
		// …and beside a GROUP BY key with a HAVING and an ORDER BY on it, so
		// the published name is compared where three consumers resolve against
		// the planner's own spelling for the same column.
		{name: "unaliased-aggregate-grouped",
			sql: "SELECT g, COUNT(*) FROM typemx WHERE id < 30 GROUP BY g " +
				"HAVING COUNT(*) > 0 ORDER BY g LIMIT 3",
			want: "g,count | 0,5 | 1,5 | 2,4"},
		{name: "window",
			sql:  "SELECT SUM(g) OVER () FROM typemx WHERE id < 3",
			want: "sum | 3 | 3 | 3"},

		// --- a CAST takes its ARGUMENT's name ----------------------------
		{name: "cast-of-a-column",
			sql:  "SELECT CAST(g AS BIGINT) FROM typemx WHERE id < 3 ORDER BY 1",
			want: "g | 0 | 1 | 2"},
		// …and the TYPE's own internal name only when the argument has none.
		{name: "cast-of-a-literal",
			sql:  "SELECT CAST(1 AS BIGINT) FROM typemx WHERE id < 2",
			want: "int8 | 1 | 1"},

		// --- a bare reference and a scalar subquery -----------------------
		{name: "qualified-column",
			sql:  "SELECT n.id FROM typemx_nested n WHERE n.id < 3 ORDER BY 1",
			want: "id | 0 | 1 | 2"},
		{name: "row-field-path",
			sql:  "SELECT (c_row).b FROM typemx_nested WHERE id < 2 ORDER BY 1",
			want: "b | 0 | 11"},
		// The DAG has no stage lowering for a SELECT-list scalar subquery and
		// answers it on the coordinator's local pipeline (#659's route), so
		// this cell's DAG arms are that pipeline. Declared, not hidden: what
		// it still proves is that the two engines' naming agrees.
		{name: "scalar-subquery",
			sql:       "SELECT (SELECT MAX(g) FROM typemx) FROM typemx WHERE id < 2",
			want:      "max | 6 | 6",
			wantRoute: "scalar projection"},

		// --- through a derived table -------------------------------------
		// `SELECT *` over a block whose item has no name publishes that
		// block's published name, which is what PostgreSQL does.
		{name: "star-over-an-unnamed-derived-column",
			sql:  "SELECT * FROM (SELECT g + 1 FROM typemx WHERE id < 3) s ORDER BY 1",
			want: "?column? | 1 | 2 | 3"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				// The routing counters, taken around the run. Rows alone
				// cannot tell "the DAG executed this" from "the DAG refused it
				// and the coordinator answered on its local pipeline", and a
				// routed cell proves NOTHING about the gather's
				// OutputRename.To — which is one of this arc's two application
				// sites (COMMON's rule; round-1 review P2).
				before := map[string]int64{}
				if arm.coord != nil {
					for _, rc := range e3RouteCounters {
						before[rc.name] = rc.fn(arm.coord)
					}
				}
				cols, rows, err := arm.run(c.sql)
				if err != nil {
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, c.sql)
				}
				if arm.coord != nil {
					for _, rc := range e3RouteCounters {
						d := rc.fn(arm.coord) - before[rc.name]
						if rc.name == c.wantRoute {
							if d == 0 {
								t.Fatalf("%s arm did NOT route this shape, which the cell "+
									"declares it does (%s): if the DAG now executes it, the "+
									"cell proves more than it claims — drop wantRoute\n"+
									"  SQL: %s", arm.name, rc.name, c.sql)
							}
							continue
						}
						if d != 0 {
							t.Fatalf("%s arm ROUTED this shape to the coordinator's local "+
								"pipeline (%s +%d): the name it published is the "+
								"single-process one, so this cell proves nothing about the "+
								"gather's rename\n  SQL: %s", arm.name, rc.name, d, c.sql)
						}
					}
				}
				got := e3SortLines(e3Render(cols, rows))
				want := e3SortLines(c.want)
				if pinned, ok := c.pin[arm.name]; ok {
					if got == want {
						t.Fatalf("the %s arm now answers PostgreSQL's result for a shape this "+
							"gate PINS as divergent. It is fixed: assert it and delete the pin\n"+
							"  %s\n  SQL: %s", arm.name, got, c.sql)
					}
					if got != e3SortLines(pinned) {
						t.Fatalf("%s arm: %s\n  pinned: %s\n  SQL: %s", arm.name, got, pinned, c.sql)
					}
					continue
				}
				if got != want {
					t.Fatalf("%s arm:\n  got  %s\n  want %s (PostgreSQL 17)\n  SQL: %s",
						arm.name, got, want, c.sql)
				}
			}
		})
	}
}

// TestAnUnnamedDerivedColumnCannotBeReferencedByItsPublishedName is the
// BOUNDARY of #732's fix, driven rather than described.
//
// PostgreSQL publishes an unnamed derived column as `?column?` and lets an
// enclosing query refer to it by that name, quoted. wadjet publishes the same
// name to the CLIENT and keeps the block's own resolution spelling INSIDE the
// plan, so `"?column?"` names nothing there and the reference is refused —
// loudly, naming the column that does exist.
//
// The published name is deliberately not the resolution spelling: inside the
// plan a name is a HANDLE that a sort key, a HAVING and an aggregate's
// OutputCol resolve against, and two unnamed items in one block would then
// answer to one handle. PostgreSQL has the same ambiguity and REFUSES it
// (42702); every resolver here would silently take the first. Making the two
// names one is that decision, and it is not this change.
func TestAnUnnamedDerivedColumnCannotBeReferencedByItsPublishedName(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	const sql = `SELECT "?column?" FROM (SELECT g + 1 FROM typemx WHERE id < 3) s`
	for _, arm := range arms {
		_, _, err := arm.run(sql)
		if err == nil {
			t.Fatalf("%s arm answered a reference to the published name; if the two names "+
				"were unified, this gate records the decision that they were not\n  SQL: %s",
				arm.name, sql)
		}
		if !strings.Contains(err.Error(), "?column?") {
			t.Fatalf("%s arm: the refusal does not name the column asked for: %v", arm.name, err)
		}
	}

	// The CONTROL: the block's own spelling resolves, which is the superset
	// this engine keeps (PostgreSQL refuses it — ADR-0012's divergence list).
	for _, arm := range arms {
		cols, rows, err := arm.run(`SELECT "g + 1" FROM (SELECT g + 1 FROM typemx WHERE id < 3) s ORDER BY 1`)
		if err != nil {
			t.Fatalf("%s arm refused the block's own spelling: %v", arm.name, err)
		}
		// The OUTER item is a delimited column REFERENCE, so it publishes that
		// column's own name — which is the block's resolution spelling.
		if got, want := e3Render(cols, rows), "g + 1 | 1 | 2 | 3"; got != want {
			t.Fatalf("%s arm: %s, want %s", arm.name, got, want)
		}
	}
}
