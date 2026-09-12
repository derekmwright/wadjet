package coordinator

import (
	"context"
	"testing"
	"time"
)

// A RECURSIVE CTE'S FORM IS DECIDED BEFORE ITS BODY IS PLANNED — five arms,
// every cell under a wall-clock bound.
//
// Materializing a recursive CTE PLANS its body, and the body's self-reference
// is a tagged scan whose cache lookup misses until the first iteration seeds
// the work table. `splitRecursiveUnion` recognises only `UNION ALL`, so a
// recursive CTE written with plain `UNION` — PostgreSQL's cycle-safe spelling,
// and standard SQL — fell to the columnar materialization, which planned the
// body, whose self-reference re-materialized THE SAME DEFINITION from inside
// its own materialization. Nothing terminated it: at the STATEMENT ROOT,
// reachable by any pgwire client, the query never returned and took 25 GB of
// RSS in 45 seconds (round-2 review, B3). At the base it answered one row where
// PostgreSQL answers three — wrong, but bounded.
//
// Two things close it, and both are asserted here:
//
//   - the name is marked IN PROGRESS for the whole materialization, so a
//     self-reference that reaches the planner from inside it refuses instead of
//     re-entering (`Planner.cteInProgress`);
//   - the FORM is decided from the parsed body before anything is planned, so
//     the spellings this engine cannot iterate are refused by name rather than
//     discovered by recursing into them.
//
// EVERY CELL HAS A DEADLINE. A gate for a non-termination defect that waits
// forever is the defect; `c1FormRun` fails the cell at 60s and says so.
func TestC1DARecursiveCTEFormIsDecidedBeforeTheBodyIsPlanned(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	// PostgreSQL 17.11's own class and sentence for a body that is not a valid
	// recursive form (42P19), and 0A000 for the one it ANSWERS and this engine
	// cannot — a feature gap is not a syntax error (ADR-0012).
	const notTheForm = "ERR recursive query \"r\" does not have the form " +
		"non-recursive-term UNION [ALL] recursive-term"
	const inNonRecursiveTerm = "ERR recursive reference to query \"r\" must not appear " +
		"within its non-recursive term"
	const unionDistinct = "ERR a recursive CTE written with UNION rather than UNION ALL is not supported"

	c1FormRun(t, arms, []c1Case{
		{
			// THE HEADLINE: at the STATEMENT ROOT, no nesting involved. The
			// tip before this fix never returned.
			name: "B3 UNION without ALL at the statement root",
			sql:  "WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want: unionDistinct,
			pin:  c1RecDAGPins(),
			why: "PostgreSQL answers 1,2,3; this engine has no dedup-per-step fixed point and says so. " +
				"On the DAG arms #1042 fires FIRST — a recursive CTE has no distributed stage at all — " +
				"so the form refusal is a single-process claim and the pin says so",
			routed: c1RecRoutes,
		},
		{
			name:   "B3 UNION without ALL nested in a derived table",
			sql:    "SELECT q.v FROM (WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT v+1 FROM r WHERE v<3) SELECT v FROM r) q ORDER BY 1",
			want:   unionDistinct,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "B3 UNION without ALL inside another CTE's body",
			sql:    "WITH o AS (WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT v+1 FROM r WHERE v<3) SELECT v FROM r) SELECT v FROM o ORDER BY 1",
			want:   unionDistinct,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			// A self-reference with NO set operation at all: PostgreSQL's
			// 42P19, its sentence.
			name:   "a self-reference with no UNION is 42P19",
			sql:    "WITH RECURSIVE r AS (SELECT v FROM r) SELECT v FROM r",
			want:   notTheForm,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			// The self-reference in the NON-RECURSIVE term, which is the
			// position the in-progress marker catches.
			name:   "a self-reference in the anchor is 42P19",
			sql:    "WITH RECURSIVE r AS (SELECT v FROM r UNION ALL SELECT 1 AS v) SELECT v FROM r",
			want:   inNonRecursiveTerm,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "a self-reference in the anchor, nested",
			sql:    "SELECT q.v FROM (WITH RECURSIVE r AS (SELECT v FROM r UNION ALL SELECT 1 AS v) SELECT v FROM r) q",
			want:   inNonRecursiveTerm,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},

		// ---- the shapes the form test must NOT touch.
		{
			// RECURSIVE with a UNION and NO self-reference is not recursive at
			// all, and PostgreSQL answers it. It must keep answering.
			name:   "control: UNION without ALL and no self-reference answers",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT 2) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 1 | 2",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			// A UNION ALL whose second arm names no CTE is an ordinary set
			// operation. PostgreSQL answers two rows; the iteration re-ran
			// that arm until `maxRecursiveIterations` and answered 1001.
			name: "a UNION ALL arm that names no CTE is not a recursive term",
			sql:  "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT 1) SELECT v FROM r ORDER BY 1",
			want: "cols=[v:INT64] rows=2 | 1 | 1",
			pin:  c1RecDAGPins(),
			why: "PostgreSQL 17.11: two rows; the fixed-point loop answered 1001 before the form test. " +
				"#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			// CONTROL: the ordinary UNION ALL recursion, at the root and
			// nested, still answers. This is the gate the form test could
			// break and does not.
			name:   "control: UNION ALL recursion at the root",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE",
			routed: c1RecRoutes,
		},
		{
			name:   "control: UNION ALL recursion nested in a derived table",
			sql:    "SELECT q.v FROM (WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r) q ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE",
			routed: c1RecRoutes,
		},

		// ---- THE ARM TABLE (round-2 review, B3). PostgreSQL parses
		// `A UNION ALL B UNION ALL C` LEFT-ASSOCIATIVELY, so the LAST arm is
		// the recursive term and a self-reference anywhere else is 42P19.
		// Splitting the body's TEXT at the FIRST top-level UNION ALL put an
		// arm that names the CTE and an arm that does not into one "recursive
		// term": the iteration re-ran the constant arm every round and
		// answered 1002 rows. Every PostgreSQL answer here was measured live
		// on 17.11 before the code changed.
		{
			name:   "multi-arm: 2 arms, self-reference LAST, UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 2 arms, self-reference FIRST, UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT v+1 AS v FROM r WHERE v<3 UNION ALL SELECT 1) SELECT v FROM r ORDER BY 1",
			want:   inNonRecursiveTerm,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 3 arms, self-reference LAST, UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=5 | 1 | 2 | 2 | 3 | 3",
			pin:    c1RecDAGPins(),
			why:    "the text split answered 1002 rows — one, then 1001 NULLs; #1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 3 arms, self-reference MIDDLE, UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v<3 UNION ALL SELECT 9) SELECT v FROM r ORDER BY 1",
			want:   inNonRecursiveTerm,
			pin:    c1RecDAGPins(),
			why:    "the text split answered 1002 rows; #1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 3 arms, self-reference FIRST, UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT v+1 AS v FROM r WHERE v<3 UNION ALL SELECT 1 UNION ALL SELECT 2) SELECT v FROM r ORDER BY 1",
			want:   inNonRecursiveTerm,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 3 arms, NO self-reference, UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 4 arms, self-reference LAST, UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=6 | 1 | 2 | 2 | 3 | 3 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 4 arms, self-reference THIRD, UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT v+1 FROM r WHERE v<3 UNION ALL SELECT 9) SELECT v FROM r ORDER BY 1",
			want:   inNonRecursiveTerm,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 2 arms, self-reference LAST, UNION distinct",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   unionDistinct,
			pin:    c1RecDAGPins(),
			why:    "PostgreSQL answers 1,2,3 by removing duplicates at every step; #1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 2 arms, self-reference FIRST, UNION distinct",
			sql:    "WITH RECURSIVE r AS (SELECT v+1 AS v FROM r WHERE v<3 UNION SELECT 1) SELECT v FROM r ORDER BY 1",
			want:   inNonRecursiveTerm,
			pin:    c1RecDAGPins(),
			why:    "PostgreSQL REFUSES this one — the anchor position is asked BEFORE the ALL-ness (round-2 review, P1); #1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 3 arms, self-reference LAST, UNION distinct",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT 2 UNION SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   unionDistinct,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 3 arms, self-reference MIDDLE, UNION distinct",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT v+1 FROM r WHERE v<3 UNION SELECT 9) SELECT v FROM r ORDER BY 1",
			want:   inNonRecursiveTerm,
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 3 arms, UNION ALL then UNION, self-reference LAST",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT 2 UNION SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   unionDistinct,
			pin:    c1RecDAGPins(),
			why:    "the TOP operator decides: PostgreSQL dedups every step and answers 1,2,3; #1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "multi-arm: 3 arms, UNION then UNION ALL, self-reference LAST",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT 2 UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=5 | 1 | 2 | 2 | 3 | 3",
			pin:    c1RecDAGPins(),
			why:    "the TOP operator is UNION ALL, so PostgreSQL keeps duplicates; #1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},

		// ---- THE SPLIT IS LEXICAL (round-3 review, B1). The arm text the
		// iteration re-plans comes from `plansql.SplitLastTopLevelUnionAll`,
		// which runs the LEXER, so it cannot match the letters `union all`
		// inside an identifier, a delimited name, a string literal or a
		// comment. A second scanner that disagrees with the parse can only
		// ever be wrong, and this one was: four cells right -> refused.
		{
			name:   "lexical split: the letters `unionall` in an identifier after the last operator",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 AS unionall FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "a hand-rolled text scan matched the letters and split there, so the halves disagreed with the parse and the body was refused 42P19 where both bases answer (round-3 review, B1); #1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "lexical split: a comment naming UNION ALL after the last operator",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v<3 /* UNION ALL */) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "the scan did not skip comments; #1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "lexical split: a string literal 'UNION ALL' in the recursive term",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v<3 AND 'UNION ALL' <> 'z') SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "lexical split: a delimited identifier \"union all\" in the recursive term",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 AS \"union all\" FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "lexical split: control: the same letters BEFORE the operator",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS unionall, 1 AS v UNION ALL SELECT v+1, v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},

		// ---- A PARENTHESISED ARM (round-4 review, B1). The lexer-driven
		// split CONSUMES the token after a top-level UNION as its lookahead,
		// and not counting a `(` there left the depth at 0 through the whole
		// parenthesised arm: every later top-level UNION ALL was invisible,
		// the halves disagreed with the parse, and five bodies that answer
		// PostgreSQL's rows were refused 42P19.
		{
			name:   "parenthesised arm: a parenthesised arm after UNION",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION (SELECT 2) UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=5 | 1 | 2 | 2 | 3 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "parenthesised arm: a parenthesised arm with its own LIMIT",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION (SELECT id FROM lat_ord LIMIT 1) UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "parenthesised arm: a parenthesised arm holding its own UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION (SELECT 2 UNION ALL SELECT 9) UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=6 | 1 | 2 | 2 | 3 | 3 | 9",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "parenthesised arm: a doubly parenthesised arm",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ((SELECT 2)) UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=5 | 1 | 2 | 2 | 3 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "parenthesised arm: two parenthesised arms",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION (SELECT 2) UNION (SELECT 3) UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=6 | 1 | 2 | 2 | 3 | 3 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
		{
			name:   "parenthesised arm: control: a parenthesised arm after UNION ALL",
			sql:    "WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL (SELECT 2) UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r ORDER BY 1",
			want:   "cols=[v:INT64] rows=5 | 1 | 2 | 2 | 3 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042 fires first on the DAG arms",
			routed: c1RecRoutes,
		},
	})
}

// c1FormRun is c1Run with a WALL-CLOCK BOUND per cell. The family it gates is a
// non-termination defect, and a gate that waits for it forever reproduces it
// rather than catching it: 60 seconds is four orders of magnitude above every
// cell's measured time (all ten answer in under 30 ms on the single arm).
func c1FormRun(t *testing.T, arms []c1Arm, cases []c1Case) {
	t.Helper()
	for _, tc := range cases {
		bounded := make([]c1Arm, len(arms))
		for i, arm := range arms {
			run := arm.run
			bounded[i] = c1Arm{name: arm.name, coord: arm.coord, run: func(sql string) (string, error) {
				type result struct {
					out string
					err error
				}
				done := make(chan result, 1)
				go func() {
					out, err := run(sql)
					done <- result{out, err}
				}()
				select {
				case r := <-done:
					return r.out, r.err
				case <-time.After(60 * time.Second):
					return "DID NOT TERMINATE within 60s", nil
				}
			}}
		}
		c1Run(t, bounded, []c1Case{tc})
	}
}
