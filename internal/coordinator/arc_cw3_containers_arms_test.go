// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc CW round 3. Three seams the closure review of round 2 found the element
// declaration had not reached, each gated here on the four arms (single,
// spilled at the 512 KiB budget, dag, dag-shuffled):
//
//	B1  a CAST that renders or converts a container reads its operand's
//	    DECLARED element (the planner's walk), not the Go box: a DATE element
//	    built by an expression printed its day count, a TIMESTAMP one its
//	    epoch milliseconds, and `AS TEXT[]` converted the box
//	B2  the null-padded side of an OUTER join / LATERAL that produced no rows
//	    declares its container column as the same side with rows does
//	B3  every comparator orders two arrays through the one container
//	    comparator (kernel.CompareValuesAt): GREATEST/LEAST answered the wrong
//	    array, BETWEEN compared text

// cw3Arm is one arm answering a query with its EXECUTED declared output.
type cw3Arm struct {
	name string
	run  func(sql string) ([]parquet.Column, [][]any, error)
}

func cw3Arms(t *testing.T, ctx context.Context) []cw3Arm {
	t.Helper()
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	dag := tmdCoordinator(t, ctx, infra)
	shuf := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	single := tmdStandalone(t, ctx)
	spilled := e3BudgetedStandalone(t, ctx)
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
	return []cw3Arm{
		{"single", embedded(single)}, {spilledArm, embedded(spilled)},
		{"dag", distributed(dag)}, {"dagshuf", distributed(shuf)},
	}
}

// TestArcCW3ContainerCastsRenderTheDeclaredElementOnEveryArm is B1. Each cell
// casts a container whose element some PRODUCER built — a column, COALESCE,
// CASE, a scalar subquery, an aggregate, a literal — to TEXT, JSON, TEXT[] and
// VARCHAR, beside the same container PROJECTED: the projection is typed by the
// declared-output walk (right since round 1) and the cast must render exactly
// what the one renderer makes of it under that declaration. At the round-2 tip
// 2090d4df a DATE element printed its day count and every expression-built
// TIMESTAMP element its epoch milliseconds, on every arm.
//
// The pinned cells are PostgreSQL 17.11's own text for literal elements; the
// INTERVAL cell is this engine's refusal (it has no interval text form: an
// element that could only render as Go's struct text is loud, ADR-0045 §2).
func TestArcCW3ContainerCastsRenderTheDeclaredElementOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := cw3Arms(t, ctx)

	// One row where every element column is present.
	_, rows, err := arms[0].run(`SELECT MIN(id) AS k FROM typemx WHERE c_date IS NOT NULL AND c_ts IS NOT NULL
		AND c_dec IS NOT NULL AND c_bool IS NOT NULL AND c_ipv4 IS NOT NULL AND c_uuid IS NOT NULL`)
	if err != nil || len(rows) != 1 || rows[0][0] == nil {
		t.Fatalf("no row with every element present: %v %v", rows, err)
	}
	k := fmt.Sprint(rows[0][0])

	elems := []struct {
		col string
		agg bool
	}{{"c_date", true}, {"c_ts", true}, {"c_dec", true}, {"c_bool", false}, {"c_ipv4", false}, {"c_uuid", false}}
	type producer struct{ name, expr, from string }
	producers := func(c string, agg bool) []producer {
		ps := []producer{
			{"column", "ARRAY[" + c + "]", " FROM typemx WHERE id = " + k},
			{"coalesce", "ARRAY[COALESCE(" + c + ", " + c + ")]", " FROM typemx WHERE id = " + k},
			{"case", "ARRAY[CASE WHEN id >= 0 THEN " + c + " END]", " FROM typemx WHERE id = " + k},
			{"subquery", "ARRAY[(SELECT " + c + " FROM typemx WHERE id = " + k + ")]", ""},
		}
		if agg {
			ps = append(ps, producer{"aggregate", "ARRAY[MAX(" + c + ")]", " FROM typemx WHERE id = " + k})
		}
		return ps
	}
	targets := []string{"TEXT", "JSON", "TEXT[]", "VARCHAR(80)"}

	// render is what the cast must answer: the projected container under its
	// declared column, through the one renderer.
	render := func(target string, v any, col parquet.Column) any {
		switch target {
		case "JSON":
			return batch.FormatPGJSON(v, &col)
		case "TEXT[]":
			// Each element as its own cast to text: the output function for
			// every type here but BOOL, whose text cast is `true` where its
			// array_out is `t` (PostgreSQL 17.11: `ARRAY[true]::text[]` is
			// {true}).
			elems, _ := v.([]any)
			out := make([]any, len(elems))
			for i, e := range elems {
				if b, ok := e.(bool); ok {
					out[i] = fmt.Sprint(b)
				} else if e != nil {
					out[i] = batch.FormatPGText(e, col.ElementType)
				}
			}
			return out
		}
		return batch.FormatPGText(v, &col)
	}

	answered, want := 0, 0
	for _, arm := range arms {
		for _, e := range elems {
			for _, p := range producers(e.col, e.agg) {
				for _, tg := range targets {
					want++
					sql := "SELECT CAST(" + p.expr + " AS " + tg + ") AS a, " + p.expr + " AS p" + p.from
					schema, rows, err := arm.run(sql)
					if err != nil {
						t.Errorf("%s / %s / %s / %s: refused: %v", arm.name, e.col, p.name, tg, err)
						continue
					}
					if len(rows) != 1 || len(schema) != 2 || schema[1].ElementType == nil {
						t.Errorf("%s / %s / %s / %s: %d rows, declared %+v", arm.name, e.col, p.name, tg, len(rows), schema)
						continue
					}
					answered++
					exp := render(tg, rows[0][1], schema[1])
					if fmt.Sprint(rows[0][0]) != fmt.Sprint(exp) {
						t.Errorf("%s / %s / %s / %s:\n  %s\n  cast       %v\n  projected  %v", arm.name, e.col, p.name, tg, sql, rows[0][0], exp)
					}
				}
			}
		}
	}
	if answered != want {
		t.Errorf("%d of %d (cell, arm) pairs answered", answered, want)
	}

	// PostgreSQL 17.11's text for literal elements, and the INTERVAL refusal.
	pinned := []struct{ sql, want string }{
		{`SELECT CAST(ARRAY[CAST('2024-01-02' AS DATE)] AS TEXT) AS a`, "{2024-01-02}"},
		{`SELECT CAST(ARRAY[CAST('2024-01-02' AS DATE)] AS JSON) AS a`, `["2024-01-02"]`},
		{`SELECT CAST(ARRAY[COALESCE(CAST('2024-01-01 01:00:00' AS TIMESTAMP), CAST('2020-01-01' AS TIMESTAMP))] AS TEXT) AS a`, `{"2024-01-01 01:00:00"}`},
		{`SELECT CAST(ARRAY[CASE WHEN 1 > 0 THEN CAST('2024-01-01 01:00:00' AS TIMESTAMP) END] AS JSON) AS a`, `["2024-01-01T01:00:00"]`},
		{`SELECT CAST(ARRAY[CAST('2024-01-01 01:00:00' AS TIMESTAMP)] AS TEXT[]) AS a`, `[2024-01-01 01:00:00]`},
		{`SELECT CAST(ARRAY[CAST(1.50 AS DECIMAL(12,2))] AS JSON) AS a`, `[1.50]`},
		{`SELECT CAST(ARRAY[true, NULL] AS TEXT) AS a`, `{t,NULL}`},
		{`SELECT CAST(ARRAY[CAST('2024-01-02 03:04:05' AS TIMESTAMP)] AS DATE[]) AS a`, `[2024-01-02]`},
		{`SELECT CAST(ARRAY[1.5, 2.5] AS INT[]) AS a`, `[2 3]`},
	}
	for _, arm := range arms {
		for _, c := range pinned {
			_, rows, err := arm.run(c.sql)
			if err != nil {
				t.Errorf("%s: %s refused: %v", arm.name, c.sql, err)
				continue
			}
			if got := fmt.Sprint(rows[0][0]); len(rows) != 1 || got != c.want {
				t.Errorf("%s: %s\n  got  %v\n  want %s", arm.name, c.sql, rows, c.want)
			}
		}
		_, _, err := arm.run(`SELECT CAST(ARRAY[INTERVAL '1 hour'] AS TEXT) AS a`)
		if err == nil || !strings.Contains(err.Error(), "no text form") {
			t.Errorf("%s: an INTERVAL element's text: want the refusal, got %v", arm.name, err)
		}
	}

	// CREATE TABLE AS stores the cast's text, and reads it back: the text is
	// the rendering under the declaration, not the box (round 2 stored
	// `{19724}` and `{1704085200000}`). The coordinator's ExecuteSQL has no
	// CTAS door (the server runs it over the DAG's result); the embedded arms
	// run it here.
	for i, arm := range arms[:2] {
		tbl := fmt.Sprintf("cw3_ctas_%d", i)
		src := "ARRAY[COALESCE(c_ts, c_ts)] AS pt, ARRAY[c_date] AS pd FROM typemx WHERE id = " + k
		if _, _, err := arm.run("CREATE TABLE " + tbl + " AS SELECT CAST(ARRAY[COALESCE(c_ts, c_ts)] AS TEXT) AS t, CAST(ARRAY[c_date] AS TEXT) AS d FROM typemx WHERE id = " + k); err != nil {
			t.Errorf("%s: CTAS refused: %v", arm.name, err)
			continue
		}
		_, stored, err := arm.run("SELECT t, d FROM " + tbl)
		if err != nil || len(stored) != 1 {
			t.Errorf("%s: reading the CTAS back: %v %v", arm.name, stored, err)
			continue
		}
		schema, proj, err := arm.run("SELECT " + src)
		if err != nil || len(proj) != 1 {
			t.Errorf("%s: projecting the source: %v %v", arm.name, proj, err)
			continue
		}
		if st, want := fmt.Sprint(stored[0][0]), batch.FormatPGText(proj[0][0], &schema[0]); st != want {
			t.Errorf("%s: CTAS stored timestamp[] text %q, want %q", arm.name, st, want)
		}
		if sd, want := fmt.Sprint(stored[0][1]), batch.FormatPGText(proj[0][1], &schema[1]); sd != want {
			t.Errorf("%s: CTAS stored date[] text %q, want %q", arm.name, sd, want)
		}
	}
}

// TestArcCW3NullPaddedSideDeclaresItsContainerOnEveryArm is B2: the side of an
// OUTER join or a LATERAL that produced NO rows declares its container column
// with the element the same side declares when it has rows. The empty side's
// declaration is the only one a client sees for it — the values are NULL — and
// it came from the plan's declared join schema, which built a container column
// from its TypeID alone (an ARRAY with no element, sent as text); the single
// path's LATERAL aggregate had no declaration at all (its block declined an
// aggregate item) and dropped to the STRING fallback. At the round-2 tip every
// empty cell declared text on every arm.
func TestArcCW3NullPaddedSideDeclaresItsContainerOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := cw3Arms(t, ctx)

	type decl struct{ typ, elem parquet.TypeID }
	arr := func(el parquet.TypeID) decl { return decl{parquet.TypeArray, el} }
	// Each shape is run twice: over an EMPTY padded side and over a side with
	// rows. {empty} is the predicate the side is filtered by.
	shapes := []struct {
		name string
		sql  string // %s = the padded side's filter
		want decl   // the declaration of column 2
		// notOn names an arm the shape does not run on, with the reason.
		notOn string
	}{
		{"left-join-stored", `SELECT a.id, b.c_arr FROM typemx_nested a LEFT JOIN (SELECT id, c_arr FROM typemx_nested WHERE %s) b ON a.id = b.id WHERE a.id < 4`,
			arr(parquet.TypeString), ""},
		// A RIGHT or FULL join whose other side is empty intermittently
		// loses the PRESERVED side's values on the spilled arm (id NULL in
		// 3-5 of 30 runs, a scalar computed column too, the same at main
		// 6cbe2041) — a spill-path defect outside this arc, filed; the
		// shapes run on the three arms that answer deterministically.
		{"right-join-computed", `SELECT b.id, a.x FROM (SELECT id, ARRAY[c_ts] AS x FROM typemx WHERE %s) a RIGHT JOIN typemx b ON a.id = b.id WHERE b.id < 4`,
			arr(parquet.TypeTimestamp), spilledArm},
		// The computed alias SHADOWS a column of the scan below (`c_i64`):
		// the side publishes the array under that name, and the empty side
		// declared the scan's bigint instead (qualified `a.c_i64`).
		{"right-join-shadowing-alias", `SELECT b.id, a.c_i64 FROM (SELECT id, ARRAY[c_ts] AS c_i64 FROM typemx WHERE %s) a RIGHT JOIN typemx b ON a.id = b.id WHERE b.id < 4`,
			arr(parquet.TypeTimestamp), spilledArm},
		{"full-join-computed", `SELECT a.id, b.x FROM (SELECT id FROM typemx WHERE id < 4) a FULL JOIN (SELECT id, ARRAY[c_date] AS x FROM typemx WHERE %s) b ON a.id = b.id`,
			arr(parquet.TypeDate), spilledArm},
		// The aggregate build over the whole table refuses at the spilled
		// arm's 512 KiB budget (memory budget exceeded) — loud, not a
		// declaration.
		{"left-join-aggregate", `SELECT a.id, b.m FROM typemx a LEFT JOIN (SELECT g, MAX(ARRAY[c_dec]) AS m FROM typemx WHERE %s GROUP BY g) b ON a.g = b.g WHERE a.id < 4`,
			arr(parquet.TypeDecimal), spilledArm},
		// The decorrelated LATERAL's hash-join build refuses at the spilled
		// arm's 512 KiB budget (memory budget exceeded) — loud, and the same
		// at main; it is not a declaration.
		{"lateral-aggregate", `SELECT t.id, l.m FROM typemx t, LATERAL (SELECT MAX(ARRAY[u.c_ts]) AS m FROM typemx u WHERE u.g = t.g AND %s) l WHERE t.id < 4`,
			arr(parquet.TypeTimestamp), spilledArm},
		{"left-join-lateral", `SELECT t.id, l.m FROM typemx t LEFT JOIN LATERAL (SELECT MIN(u.c_arr) AS m FROM typemx_nested u WHERE u.id = t.id AND %s) l ON true WHERE t.id < 4`,
			arr(parquet.TypeString), spilledArm},
	}
	sides := []struct{ name, filter string }{{"empty", "id < 0"}, {"rows", "id >= 0"}}

	answered, want := 0, 0
	for _, s := range shapes {
		for _, side := range sides {
			sql := fmt.Sprintf(s.sql, side.filter)
			if s.name == "lateral-aggregate" || s.name == "left-join-lateral" {
				sql = fmt.Sprintf(s.sql, "u."+side.filter)
			}
			for _, arm := range arms {
				if arm.name == s.notOn {
					continue
				}
				want++
				schema, rows, err := arm.run(sql)
				if err != nil {
					t.Errorf("%s / %s / %s: %s refused: %v", s.name, side.name, arm.name, sql, err)
					continue
				}
				if len(schema) != 2 || len(rows) == 0 {
					t.Errorf("%s / %s / %s: %d columns, %d rows", s.name, side.name, arm.name, len(schema), len(rows))
					continue
				}
				answered++
				got := decl{typ: schema[1].Type}
				if schema[1].ElementType != nil {
					got.elem = schema[1].ElementType.Type
				}
				if got != s.want {
					t.Errorf("%s / %s / %s: %s\n  declared %v<%v>, want %v<%v>", s.name, side.name, arm.name, sql,
						got.typ, got.elem, s.want.typ, s.want.elem)
				}
				if side.name == "empty" {
					for _, r := range rows {
						if r[1] != nil {
							t.Errorf("%s / empty / %s: padded value %v, want NULL", s.name, arm.name, r[1])
						}
					}
				}
			}
		}
	}
	if answered != want {
		t.Errorf("%d of %d (cell, arm) pairs answered", answered, want)
	}
}

// TestArcCW3EveryComparatorOrdersArraysOneWay is B3 and P1: the same pairs of
// arrays go through every construct that compares two values — the six
// operators, GREATEST, LEAST, BETWEEN, IN, a simple CASE, NULLIF, IS DISTINCT
// FROM, ORDER BY, MIN/MAX, DISTINCT, GROUP BY, a window's ORDER BY and
// PARTITION BY, a join key, a filter — and every one must agree with the
// ordering PostgreSQL 17.11 gives the pair (`want`: -1, 0, +1 for a vs b).
// One ordering, whichever spelling asks: at the round-2 tip GREATEST/LEAST
// picked by the boxes' text (`GREATEST(ARRAY[2], ARRAY[10])` = {2}) and BETWEEN
// compared text, while `<` and ORDER BY answered element-wise.
//
// The pairs discriminate: a two-digit number against a one-digit one (text
// order disagrees), a prefix, a NULL element, an empty array, equal arrays, a
// DECIMAL element (boxed as its text — only its declaration orders it as a
// number), text, dates, timestamps, a nested array.
func TestArcCW3EveryComparatorOrdersArraysOneWay(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := cw3Arms(t, ctx)

	pairs := []struct {
		name, a, b string
		want       int // PostgreSQL 17.11: sign of (a <=> b)
	}{
		{"digits", "ARRAY[2]", "ARRAY[10]", -1},
		{"prefix", "ARRAY[1,2]", "ARRAY[1]", 1},
		{"null-element", "ARRAY[1,NULL]", "ARRAY[1,2]", 1},
		{"empty", "CAST(ARRAY[] AS INT[])", "ARRAY[1]", -1},
		{"equal", "ARRAY[3,4]", "ARRAY[3,4]", 0},
		{"decimal", "ARRAY[CAST(10 AS DECIMAL(5,2))]", "ARRAY[CAST(9.5 AS DECIMAL(5,2))]", 1},
		{"decimal-column", "ARRAY[c_dec]", "ARRAY[CAST(-100000 AS DECIMAL(18,4))]", 1},
		{"text", "ARRAY['b']", "ARRAY['ab']", 1},
		{"date", "ARRAY[CAST('2024-01-10' AS DATE)]", "ARRAY[CAST('2024-01-09' AS DATE)]", 1},
		{"timestamp", "ARRAY[CAST('2024-01-01 09:00:00' AS TIMESTAMP)]", "ARRAY[CAST('2024-01-01 10:00:00' AS TIMESTAMP)]", -1},
		{"nested", "ARRAY[ARRAY[1,10]]", "ARRAY[ARRAY[1,9]]", 1},
	}
	// Each comparator answers one value per pair; expect derives it from the
	// ordering, so every comparator is held to the SAME ordering.
	type cmpr struct {
		name   string
		sql    string // {a}, {b}
		expect func(c int, a, b string) string
	}
	b2s := func(v bool) string { return fmt.Sprint(v) }
	txt := func(s string) string { return "CAST(" + s + " AS TEXT)" }
	comparators := []cmpr{
		{"lt", "SELECT {a} < {b} AS r", func(c int, _, _ string) string { return b2s(c < 0) }},
		{"le", "SELECT {a} <= {b} AS r", func(c int, _, _ string) string { return b2s(c <= 0) }},
		{"gt", "SELECT {a} > {b} AS r", func(c int, _, _ string) string { return b2s(c > 0) }},
		{"ge", "SELECT {a} >= {b} AS r", func(c int, _, _ string) string { return b2s(c >= 0) }},
		{"eq", "SELECT {a} = {b} AS r", func(c int, _, _ string) string { return b2s(c == 0) }},
		{"ne", "SELECT {a} <> {b} AS r", func(c int, _, _ string) string { return b2s(c != 0) }},
		{"greatest", "SELECT " + txt("GREATEST({a}, {b})") + " = " + txt("{b}") + " AS r", func(c int, _, _ string) string { return b2s(c <= 0) }},
		{"least", "SELECT " + txt("LEAST({a}, {b})") + " = " + txt("{a}") + " AS r", func(c int, _, _ string) string { return b2s(c <= 0) }},
		{"between", "SELECT {a} BETWEEN {b} AND {b} AS r", func(c int, _, _ string) string { return b2s(c == 0) }},
		{"between-range", "SELECT {b} BETWEEN {a} AND {a} OR {a} BETWEEN {b} AND {a} AS r", func(c int, _, _ string) string { return b2s(c >= 0) }},
		{"in", "SELECT {a} IN ({b}) AS r", func(c int, _, _ string) string { return b2s(c == 0) }},
		{"case", "SELECT CASE {a} WHEN {b} THEN true ELSE false END AS r", func(c int, _, _ string) string { return b2s(c == 0) }},
		{"nullif", "SELECT NULLIF({a}, {b}) IS NULL AS r", func(c int, _, _ string) string { return b2s(c == 0) }},
		{"distinct-from", "SELECT {a} IS DISTINCT FROM {b} AS r", func(c int, _, _ string) string { return b2s(c != 0) }},
		{"order-by", "SELECT v = {a} AS r FROM (SELECT {a} AS v, 1 AS k UNION ALL SELECT {b}, 2) q ORDER BY v, k LIMIT 1",
			func(c int, _, _ string) string { return b2s(c <= 0) }},
		{"order-by-desc", "SELECT v = {a} AS r FROM (SELECT {a} AS v, 1 AS k UNION ALL SELECT {b}, 2) q ORDER BY v DESC, k LIMIT 1",
			func(c int, _, _ string) string { return b2s(c >= 0) }},
		{"min", "SELECT MIN(v) = {a} AS r FROM (SELECT {a} AS v UNION ALL SELECT {b}) q", func(c int, _, _ string) string { return b2s(c <= 0) }},
		{"max", "SELECT MAX(v) = {a} AS r FROM (SELECT {a} AS v UNION ALL SELECT {b}) q", func(c int, _, _ string) string { return b2s(c >= 0) }},
		{"distinct", "SELECT COUNT(*) = 1 AS r FROM (SELECT DISTINCT v FROM (SELECT {a} AS v UNION ALL SELECT {b}) q) z",
			func(c int, _, _ string) string { return b2s(c == 0) }},
		{"group-by", "SELECT COUNT(*) = 1 AS r FROM (SELECT v FROM (SELECT {a} AS v UNION ALL SELECT {b}) q GROUP BY v) z",
			func(c int, _, _ string) string { return b2s(c == 0) }},
		{"window-order", "SELECT rn = 1 AS r FROM (SELECT k, ROW_NUMBER() OVER (ORDER BY v, k) AS rn FROM (SELECT {a} AS v, 1 AS k UNION ALL SELECT {b}, 2) q) z WHERE k = 1",
			func(c int, _, _ string) string { return b2s(c <= 0) }},
		{"window-partition", "SELECT MAX(n) = 2 AS r FROM (SELECT COUNT(*) OVER (PARTITION BY v) AS n FROM (SELECT {a} AS v UNION ALL SELECT {b}) q) z",
			func(c int, _, _ string) string { return b2s(c == 0) }},
		{"join", "SELECT COUNT(*) = 1 AS r FROM (SELECT {a} AS v) x JOIN (SELECT {b} AS w) y ON x.v = y.w",
			func(c int, _, _ string) string { return b2s(c == 0) }},
		{"filter", "SELECT COUNT(*) = 1 AS r FROM (SELECT {a} AS v) q WHERE v > {b}", func(c int, _, _ string) string { return b2s(c > 0) }},
	}
	answered, want := 0, 0
	for _, arm := range arms {
		for _, p := range pairs {
			for _, cm := range comparators {
				sql := strings.NewReplacer("{a}", p.a, "{b}", p.b).Replace(cm.sql)
				if strings.Contains(p.a, "c_dec") {
					// A column operand: one row of typemx where c_dec is set.
					sql = "SELECT r FROM (" + strings.Replace(sql, " AS r", " AS r FROM typemx WHERE c_dec IS NOT NULL", 1) + ") o LIMIT 1"
					if strings.Contains(cm.sql, " FROM ") {
						continue // the relational shapes are covered by the literal pairs
					}
				}
				want++
				_, rows, err := arm.run(sql)
				if err != nil {
					t.Errorf("%s / %s / %s: %s refused: %v", arm.name, p.name, cm.name, sql, err)
					continue
				}
				if len(rows) != 1 {
					t.Errorf("%s / %s / %s: %s answered %d rows", arm.name, p.name, cm.name, sql, len(rows))
					continue
				}
				answered++
				if got, exp := fmt.Sprint(rows[0][0]), cm.expect(p.want, p.a, p.b); got != exp {
					t.Errorf("%s / %s / %s: %s\n  got %s, want %s (the pair orders %d)", arm.name, p.name, cm.name, sql, got, exp, p.want)
				}
			}
		}
	}
	if answered != want {
		t.Errorf("%d of %d (cell, arm) pairs answered", answered, want)
	}
}
