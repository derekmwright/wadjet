// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc CW round 4. The round-3 closure review found the one container ordering
// (kernel.CompareValuesAt, ADR-0045 §4) had not reached two seams, and that the
// round-3 gate could not see either because its axes never held them:
//
//	B1/B4  a SUBQUERY operand reached the cast and the comparators with no
//	       declared element (physical.declaredShapeOf dropped it), so a
//	       TIMESTAMP array printed its epoch, a DATE array never equalled, a
//	       DECIMAL array ordered as text and `{}` equalled `{""}`
//	B3     two DECIMAL elements of different scales: `CAST(… AS
//	       DECIMAL(p,s)[])` declared text[], so the hash-join key, the IN / `=
//	       ANY` membership and the set operations compared element TEXT
//	       (`10.00` ≠ `10.0000`)
//	P2     a multi-dimensional array ordered nested-lexicographically, not by
//	       PostgreSQL's array_cmp (flattened leaves, count, dimensions)
//
// This gate is the one-ordering gate with every axis the review listed:
// OPERAND (how each side is produced) × COMPARATOR (every spelling that
// orders or equates two arrays) × ELEMENT (the pair's element type, with the
// discriminating values) on four arms, plus the join and the set operations on
// two sort-merge arms. Every comparator's answer is derived from ONE sign per
// pair — PostgreSQL 17.11's array_cmp of the pair (measured, cw_author4/) — so
// no comparator can disagree with another, with the sort, or with PostgreSQL.
//
// The pairs discriminate: the wrong reading answers differently from the right
// one in every cell (a two-digit number against a one-digit one, an empty array
// against one empty string, two DECIMAL scales of one value, an address whose
// text order is not its order, rectangular arrays whose inner lengths differ).

type cw4Pair struct {
	name, a, b string
	sign       int // PostgreSQL 17.11: sign of array_cmp(a, b)
}

func cw4Pairs() []cw4Pair {
	ip := func(s string) string { return "CAST('" + s + "' AS IPV4)" }
	return []cw4Pair{
		{"int", "ARRAY[CAST(2 AS INT)]", "ARRAY[CAST(10 AS INT)]", -1},
		{"text", "ARRAY['b']", "ARRAY['ab']", 1},
		{"text-empty", "CAST(ARRAY[] AS TEXT[])", "ARRAY['']", -1},
		{"date", "ARRAY[CAST('2024-01-10' AS DATE)]", "ARRAY[CAST('2024-01-09' AS DATE)]", 1},
		{"date-equal", "ARRAY[CAST('2024-01-10' AS DATE)]", "ARRAY[CAST('2024-01-10' AS DATE)]", 0},
		{"timestamp", "ARRAY[CAST('2024-01-01 09:00:00' AS TIMESTAMP)]", "ARRAY[CAST('2024-01-01 10:00:00' AS TIMESTAMP)]", -1},
		{"decimal-scales", "ARRAY[CAST(10 AS DECIMAL(5,2))]", "ARRAY[CAST(9.5 AS DECIMAL(9,4))]", 1},
		{"decimal-scales-equal", "ARRAY[CAST(10 AS DECIMAL(5,2))]", "ARRAY[CAST(10 AS DECIMAL(9,4))]", 0},
		{"bool", "ARRAY[true]", "ARRAY[false]", 1},
		{"ipv4", "ARRAY[" + ip("10.0.0.10") + "]", "ARRAY[" + ip("9.255.0.1") + "]", 1},
		{"nested", "ARRAY[ARRAY[1,2],ARRAY[3,4]]", "ARRAY[ARRAY[1,2,3]]", 1},
		// Round 5: the MIXED-ELEMENT axis. Two element TYPES meet at
		// batch.CommonContainerColumn's type (int ⊕ bigint = bigint, int ⊕
		// numeric = numeric, anything ⊕ float = double), and every comparator
		// — the key paths included — must answer that one ordering (review
		// B1: `=` said equal while the hash key, the set operations and the
		// FULL join keyed each side under its own element type, and the
		// sort-merge key raised XX000). Signs: PostgreSQL 17.11's
		// btarraycmp over the explicit coercion it needs (cw_author5/).
		{"mixed-int-bigint", "ARRAY[CAST(2 AS INT)]", "ARRAY[CAST(10 AS BIGINT)]", -1},
		{"mixed-int-numeric-equal", "ARRAY[CAST(2 AS INT)]", "ARRAY[CAST(2 AS DECIMAL(9,2))]", 0},
		{"mixed-int-float8", "ARRAY[CAST(2 AS INT)]", "ARRAY[CAST(2.5 AS DOUBLE)]", -1},
		{"mixed-int-float8-equal", "ARRAY[CAST(2 AS INT)]", "ARRAY[CAST(2 AS DOUBLE)]", 0},
		{"mixed-numeric-float8", "ARRAY[CAST(10 AS DECIMAL(5,2))]", "ARRAY[CAST(9.5 AS DOUBLE)]", 1},
		{"mixed-numeric-float8-equal", "ARRAY[CAST(2.5 AS DECIMAL(5,2))]", "ARRAY[CAST(2.5 AS DOUBLE)]", 0},
		{"mixed-float4-float8-equal", "ARRAY[CAST(0.5 AS REAL)]", "ARRAY[CAST(0.5 AS DOUBLE)]", 0},
	}
}

// cw5TextComparator names the comparators whose expectation compares a
// value's TEXT with one operand's own text (GREATEST / LEAST): for two
// element TYPES the chosen value is rendered at the pair's common type
// (`{10}` for a numeric(5,2) 10 beside a double), which PostgreSQL does too,
// and for two DECIMAL scales at the common scale, ADR-0024's recorded choice
// rule (`{10.0000}`; PostgreSQL's unconstrained numeric keeps `{10.00}`, the
// scalar GREATEST identically at main) — so the sign formula does not apply.
// The unification gate asserts those values directly
// (TestArcCW5ElementTypesUnifyAtEveryMeetingPoint).
func cw5TextComparator(pair, comparator string) bool {
	return (strings.HasPrefix(pair, "mixed-") || strings.HasPrefix(pair, "decimal-scales")) &&
		(comparator == "greatest" || comparator == "least")
}

// cw4Operand spells how each side of a pair is PRODUCED: ea/eb are the two
// operand expressions and from the FROM clause (with a leading space) they are
// read under — every non-constructor spelling reads a table, so no constant
// folding answers in the comparator's place.
type cw4Operand struct {
	name     string
	ea, eb   string
	from     string
	scalarOK bool // the operand may stand in a relational wrapper's arm
}

func cw4Operands(p cw4Pair) []cw4Operand {
	derived := " FROM (SELECT " + p.a + " AS a, " + p.b + " AS b FROM typemx WHERE id = 1) s"
	return []cw4Operand{
		{"constructor", p.a, p.b, "", true},
		{"column", "s.a", "s.b", derived, true},
		{"expression", "COALESCE(s.a, s.a)", "COALESCE(s.b, s.b)", derived, true},
		{"subquery", "(SELECT " + p.a + " FROM typemx WHERE id = 1)", "(SELECT " + p.b + " FROM typemx WHERE id = 1)", "", true},
		{"aggregate", "MIN(s.a)", "MAX(s.b)", derived, true},
		// FIRST_VALUE on both sides: LAST_VALUE(x) OVER () answers NULL on the
		// DAG for every type (a pre-existing distributed defect, filed; not
		// this gate's ordering question).
		{"window", "FIRST_VALUE(s.a) OVER ()", "FIRST_VALUE(s.b) OVER ()", derived, true},
		{"lateral", "t.a", "t.b", " FROM typemx o, LATERAL (SELECT " + p.a + " AS a, " + p.b + " AS b) t WHERE o.id = 1", true},
	}
}

// cw4Comparator is one comparator: sql builds the query from the operand, and
// expect is its answer under the pair's sign.
type cw4Comparator struct {
	name   string
	join   bool // runs on the sort-merge arms too
	sql    func(o cw4Operand) string
	expect func(sign int) string
}

func cw4Comparators() []cw4Comparator {
	b := func(v bool) string { return fmt.Sprint(v) }
	n := func(v int) string { return fmt.Sprint(v) }
	one := func(c func(o cw4Operand) string) func(o cw4Operand) string { return c }
	scalar := func(expr string) func(o cw4Operand) string {
		return func(o cw4Operand) string {
			return "SELECT " + strings.NewReplacer("{a}", o.ea, "{b}", o.eb).Replace(expr) + " AS r" + o.from
		}
	}
	// rows is the two-row relation (v, k): the pair's a with k = 1, b with 2.
	rows := func(o cw4Operand) string {
		return "(SELECT " + o.ea + " AS v, 1 AS k" + o.from + " UNION ALL SELECT " + o.eb + " AS v, 2 AS k" + o.from + ")"
	}
	txt := func(s string) string { return "CAST(" + s + " AS TEXT)" }
	return []cw4Comparator{
		{name: "lt", sql: scalar("{a} < {b}"), expect: func(c int) string { return b(c < 0) }},
		{name: "le", sql: scalar("{a} <= {b}"), expect: func(c int) string { return b(c <= 0) }},
		{name: "gt", sql: scalar("{a} > {b}"), expect: func(c int) string { return b(c > 0) }},
		{name: "ge", sql: scalar("{a} >= {b}"), expect: func(c int) string { return b(c >= 0) }},
		{name: "eq", sql: scalar("{a} = {b}"), expect: func(c int) string { return b(c == 0) }},
		{name: "ne", sql: scalar("{a} <> {b}"), expect: func(c int) string { return b(c != 0) }},
		{name: "greatest", sql: scalar(txt("GREATEST({a}, {b})") + " = " + txt("{a}")), expect: func(c int) string { return b(c >= 0) }},
		{name: "least", sql: scalar(txt("LEAST({a}, {b})") + " = " + txt("{a}")), expect: func(c int) string { return b(c <= 0) }},
		{name: "between", sql: scalar("{a} BETWEEN {b} AND {b}"), expect: func(c int) string { return b(c == 0) }},
		{name: "between-range", sql: scalar("{a} BETWEEN {b} AND {a}"), expect: func(c int) string { return b(c >= 0) }},
		{name: "in-list", sql: scalar("{a} IN ({b})"), expect: func(c int) string { return b(c == 0) }},
		{name: "case", sql: scalar("CASE {a} WHEN {b} THEN true ELSE false END"), expect: func(c int) string { return b(c == 0) }},
		{name: "nullif", sql: scalar("NULLIF({a}, {b}) IS NULL"), expect: func(c int) string { return b(c == 0) }},
		{name: "distinct-from", sql: scalar("{a} IS DISTINCT FROM {b}"), expect: func(c int) string { return b(c != 0) }},
		{name: "in-subquery", sql: one(func(o cw4Operand) string {
			return "SELECT " + o.ea + " IN (SELECT " + o.eb + " AS w" + o.from + ") AS r" + o.from
		}), expect: func(c int) string { return b(c == 0) }},
		{name: "any-subquery", sql: one(func(o cw4Operand) string {
			return "SELECT " + o.ea + " = ANY (SELECT " + o.eb + " AS w" + o.from + ") AS r" + o.from
		}), expect: func(c int) string { return b(c == 0) }},
		{name: "all-subquery", sql: one(func(o cw4Operand) string {
			return "SELECT " + o.ea + " <> ALL (SELECT " + o.eb + " AS w" + o.from + ") AS r" + o.from
		}), expect: func(c int) string { return b(c != 0) }},
		{name: "order-by", sql: one(func(o cw4Operand) string {
			return "SELECT k FROM " + rows(o) + " q ORDER BY v, k LIMIT 1"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 2}[c <= 0]) }},
		{name: "order-by-desc", sql: one(func(o cw4Operand) string {
			return "SELECT k FROM " + rows(o) + " q ORDER BY v DESC, k LIMIT 1"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 2}[c >= 0]) }},
		{name: "min", sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM " + rows(o) + " q WHERE k = 1 AND v = (SELECT MIN(v) FROM " + rows(o) + " q2)"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 0}[c <= 0]) }},
		{name: "max", sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM " + rows(o) + " q WHERE k = 1 AND v = (SELECT MAX(v) FROM " + rows(o) + " q2)"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 0}[c >= 0]) }},
		{name: "distinct", sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM (SELECT DISTINCT v FROM " + rows(o) + " q) z"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 2}[c == 0]) }},
		{name: "group-by", sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM (SELECT v FROM " + rows(o) + " q GROUP BY v) z"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 2}[c == 0]) }},
		{name: "window-order", sql: one(func(o cw4Operand) string {
			return "SELECT rn FROM (SELECT k, ROW_NUMBER() OVER (ORDER BY v, k) AS rn FROM " + rows(o) + " q) z WHERE k = 1"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 2}[c <= 0]) }},
		{name: "window-partition", sql: one(func(o cw4Operand) string {
			return "SELECT MAX(c) AS r FROM (SELECT COUNT(*) OVER (PARTITION BY v) AS c FROM " + rows(o) + " q) z"
		}), expect: func(c int) string { return n(map[bool]int{true: 2, false: 1}[c == 0]) }},
		{name: "join", join: true, sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM (SELECT " + o.ea + " AS v" + o.from + ") x JOIN (SELECT " + o.eb + " AS w" + o.from + ") y ON x.v = y.w"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 0}[c == 0]) }},
		{name: "union", join: true, sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM (SELECT " + o.ea + " AS v" + o.from + " UNION SELECT " + o.eb + " AS v" + o.from + ") z"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 2}[c == 0]) }},
		{name: "intersect", join: true, sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM (SELECT " + o.ea + " AS v" + o.from + " INTERSECT SELECT " + o.eb + " AS v" + o.from + ") z"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 0}[c == 0]) }},
		{name: "except", join: true, sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM (SELECT " + o.ea + " AS v" + o.from + " EXCEPT SELECT " + o.eb + " AS v" + o.from + ") z"
		}), expect: func(c int) string { return n(map[bool]int{true: 0, false: 1}[c == 0]) }},
		{name: "filter", sql: one(func(o cw4Operand) string {
			return "SELECT COUNT(*) AS r FROM (SELECT " + o.ea + " AS v, " + o.eb + " AS w" + o.from + ") q WHERE v > w"
		}), expect: func(c int) string { return n(map[bool]int{true: 1, false: 0}[c > 0]) }},
	}
}

// cw4ShapeRefusal names the (operand, comparator) shapes an arm refuses LOUDLY
// for a reason outside this seam, each refused for every element type and
// identically for scalar operands (cw_author4/gate_cw4_refusals.log): a
// refusal is not an ordering and is counted, never compared.
func cw4ShapeRefusal(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, m := range []string{
		// The DAG's set-operation arm cannot project a SELECT list over an
		// aggregate its own aggregate stage names (dagplan, a recorded
		// refusal for any type).
		"whose output the arm's aggregate stage names for itself",
		// A DAG join whose keys are EXPRESSIONS over a derived table's
		// columns (`COALESCE(s.a, s.a) AS v … ON x.v = y.w`): refused for
		// integers identically at main 6cbe2041.
		"resolves to no column on either side",
		// A DAG join whose key is a derived table's RENAMED column is
		// respelled to its source name, and the pair's common type is not
		// resolved from that spelling: the scalar twin (`CAST(2 AS INT) AS a`
		// … `CAST(2 AS DOUBLE) AS b`) refuses identically at main 6cbe2041
		// (#615's backstop); round 5 makes the array pair refuse the same way
		// rather than key its leaves apart (filed, cw_landing_notes (v)).
		"was not resolved at plan time",
		"so the condition would be dropped",
		"not in schema",
	} {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

// cw4KnownDAGDefect names the (operand, comparator) shapes the stage DAG
// answers WRONG for every type — integers included, identically at main
// 6cbe2041 — so no ordering can be read from them there. Each is a filed
// distributed defect (cw_landing_notes.md §ROUND 4 filing candidates), and the
// single-process arms still run every one of them:
//
//	(q) a window function's output inside an expression, or a second window
//	    function in one SELECT, answers NULL on the DAG
//	    (`FIRST_VALUE(s.a) OVER () < s.b`, `LAST_VALUE(x) OVER ()`)
//	(r) a set-operation arm whose SELECT item is an EXPRESSION over a derived
//	    table answers NULL rows / one merged row on the DAG
//	    (`SELECT s.a + 0 … UNION ALL SELECT s.b + 0 …`)
func cw4KnownDAGDefect(arm, operand, comparator string) bool {
	if !strings.HasPrefix(arm, "dag") {
		return false
	}
	switch operand {
	case "window":
		return true // (q)
	case "expression":
		switch comparator {
		case "order-by", "order-by-desc", "min", "max", "distinct", "group-by",
			"window-order", "window-partition", "union", "intersect", "except":
			return true // (r)
		}
	}
	return false
}

func TestArcCW4OneOrderingEveryOperandComparatorElement(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := cw4ArmsWithSortMerge(t, ctx)

	answered, want, refused := 0, 0, map[string]int{}
	for _, arm := range arms {
		for _, p := range cw4Pairs() {
			for _, o := range cw4Operands(p) {
				for _, cm := range cw4Comparators() {
					if arm.sortMerge && !cm.join {
						continue
					}
					if cw5TextComparator(p.name, cm.name) {
						continue
					}
					if cw4KnownDAGDefect(arm.name, o.name, cm.name) {
						refused["filed-dag/"+o.name]++
						continue
					}
					sql := cm.sql(o)
					want++
					_, rows, err := arm.run(sql)
					if err != nil {
						if cw4ShapeRefusal(err) {
							refused[o.name+"/"+cm.name]++
							continue
						}
						t.Errorf("%s / %s / %s / %s: %s refused: %v", arm.name, p.name, o.name, cm.name, sql, err)
						continue
					}
					if len(rows) != 1 || len(rows[0]) != 1 {
						t.Errorf("%s / %s / %s / %s: %s answered %v", arm.name, p.name, o.name, cm.name, sql, rows)
						continue
					}
					answered++
					if got, exp := fmt.Sprint(rows[0][0]), cm.expect(p.sign); got != exp {
						t.Errorf("%s / %s / %s / %s: %s\n  got %s, want %s (the pair orders %d)",
							arm.name, p.name, o.name, cm.name, sql, got, exp, p.sign)
					}
				}
			}
		}
	}
	t.Logf("one ordering: %d of %d (cell, arm) answered; loud shape refusals %v", answered, want, refused)
	// Non-vacuous: at least nine in ten cells answer on every arm.
	if answered*10 < want*9 {
		t.Errorf("only %d of %d (cell, arm) pairs answered", answered, want)
	}
}

// cw4Arm is a cw3Arm that may be a sort-merge arm (joins and set operations
// only).
type cw4Arm struct {
	cw3Arm
	sortMerge bool
}

// cw4ArmsWithSortMerge is cw3Arms' four arms plus the sort-merge join on the
// embedded path and on the DAG (every equi-join over a one-byte threshold).
func cw4ArmsWithSortMerge(t *testing.T, ctx context.Context) []cw4Arm {
	t.Helper()
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	dag := tmdCoordinator(t, ctx, infra)
	shuf := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	smjDAG := tmdCoordinator(t, ctx, infra, func(c *Config) { c.SortMergeJoinBytes = 1 })
	single := tmdStandalone(t, ctx)
	spilled := e3BudgetedStandalone(t, ctx)
	smj := cw4SortMergeStandalone(t, ctx)
	embedded := func(db *wadjet.DB) func(string) ([]parquet.Column, [][]any, error) {
		return func(sql string) ([]parquet.Column, [][]any, error) {
			res, err := db.Query(ctx, sql)
			if err != nil {
				return nil, nil, err
			}
			cells := make([][]any, len(res.Rows))
			for i := range res.Rows {
				cells[i] = res.Cells(i)
			}
			return res.OutputSchema, cells, nil
		}
	}
	distributed := func(c *Coordinator) func(string) ([]parquet.Column, [][]any, error) {
		return func(sql string) ([]parquet.Column, [][]any, error) {
			res, err := c.ExecuteSQL(ctx, sql)
			if err != nil {
				return nil, nil, err
			}
			if res.Error != "" {
				return nil, nil, fmt.Errorf("%s", res.Error)
			}
			schema := res.OutputSchema()
			var cells [][]any
			s := res.Stream()
			defer s.Close()
			for {
				b, err := s.Next(ctx)
				if err != nil {
					return nil, nil, err
				}
				if b == nil {
					return schema, cells, nil
				}
				cells = append(cells, b.ToRowValues()...)
			}
		}
	}
	return []cw4Arm{
		{cw3Arm: cw3Arm{"single", embedded(single)}}, {cw3Arm: cw3Arm{spilledArm, embedded(spilled)}},
		{cw3Arm: cw3Arm{"dag", distributed(dag)}}, {cw3Arm: cw3Arm{"dagshuf", distributed(shuf)}},
		{cw3Arm: cw3Arm{"smj", embedded(smj)}, sortMerge: true},
		{cw3Arm: cw3Arm{"dagsmj", distributed(smjDAG)}, sortMerge: true},
	}
}

// cw4SortMergeStandalone is the embedded arm with the sort-merge join forced
// (a one-byte threshold), over the type-matrix table the gate reads.
func cw4SortMergeStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test", SortMergeJoinBytes: 1})
	if err != nil {
		t.Fatalf("open sort-merge standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema, rows := typematrix.Schema(), typematrix.Data(typematrix.Rows)
	if err := db.CreateTable(ctx, typematrix.Table, schema, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.Table, err)
	}
	ing := db.NewIngester(typematrix.Table, schema, nil, ingest.Config{
		MaxBufferRows: len(rows) + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest %s: %v", typematrix.Table, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", typematrix.Table, err)
	}
	return db
}
