// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc CW round 2, B1: every operator that PUBLISHES a container column
// publishes its element, on a ZERO-ROW result, on every arm (single, spilled,
// dag, dagshuf) — the declaration a client's RowDescription is built from
// when no vector exists to read it off.
//
// Round 1 carried the element through the declared-output walk for a column,
// a constructor and a set operation, and the reviewer found the publishers it
// did not reach: an aggregate's empty identity row on the single path
// (AggColumn.OutputElementType was set by the DAG only), a GROUPED or HAVING
// aggregate, a window, a scalar subquery — each declared ARRAY with no element,
// which the wire sends as text. The mechanism is one walk: declaredProjectionDecl
// rebuilt a named column from its TypeID and ROW fields, dropping the element
// (ColDecls.namedDecl now answers the whole shape), and the shapes walk had no
// Window arm, shadowed an aggregate read through a renaming projection, and
// withheld a bare GROUP BY key. The gather's computed column was allocated
// from a bare TypeID (evalDeclaredColumn) and published without its shape.
//
// Each cell asserts the column's declared TYPE and its ELEMENT's type — what
// PostgreSQL 17.11 declares for the same spelling (the OIDs are in the notes'
// zero-row publisher table). At the round-1 tip 8b50409c every cell but the
// control failed on at least one arm; at main 6cbe2041 every one did (text).
func TestArcCW2ZeroRowPublishersDeclareTheElementOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	dag := tmdCoordinator(t, ctx, infra)
	shuf := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	single := tmdStandalone(t, ctx)
	spilled := e3BudgetedStandalone(t, ctx)

	type decl struct{ typ, elem parquet.TypeID }
	fromSchema := func(cols []parquet.Column) []decl {
		out := make([]decl, len(cols))
		for i, c := range cols {
			out[i].typ = c.Type
			if c.ElementType != nil {
				out[i].elem = c.ElementType.Type
			}
		}
		return out
	}
	embedded := func(db *wadjet.DB) func(string) ([]decl, int, error) {
		return func(sql string) ([]decl, int, error) {
			res, err := db.Query(ctx, sql)
			if err != nil {
				return nil, 0, err
			}
			out := make([]decl, len(res.ColumnMetas))
			for i, m := range res.ColumnMetas {
				out[i].typ = m.TypeID
				if m.ElementType != nil {
					out[i].elem = m.ElementType.Type
				}
			}
			return out, len(res.Rows), nil
		}
	}
	distributed := func(c *Coordinator) func(string) ([]decl, int, error) {
		return func(sql string) ([]decl, int, error) {
			res, err := c.ExecuteSQL(ctx, sql)
			if err != nil {
				return nil, 0, err
			}
			if res.Error != "" {
				return nil, 0, fmt.Errorf("%s", res.Error)
			}
			schema := res.OutputSchema()
			rows := 0
			s := res.Stream()
			defer s.Close()
			for {
				b, err := s.Next(ctx)
				if err != nil {
					return nil, 0, err
				}
				if b == nil {
					break
				}
				rows += b.Len
			}
			return fromSchema(schema), rows, nil
		}
	}
	arms := []struct {
		name string
		run  func(string) ([]decl, int, error)
	}{
		{"single", embedded(single)}, {spilledArm, embedded(spilled)},
		{"dag", distributed(dag)}, {"dagshuf", distributed(shuf)},
	}

	arr := func(el parquet.TypeID) decl { return decl{parquet.TypeArray, el} }
	i32 := decl{typ: parquet.TypeInt32}
	i64 := decl{typ: parquet.TypeInt64}
	cells := []struct {
		name string
		sql  string
		want []decl
		// notOn names an arm the cell does not run on, with the reason.
		notOn string
	}{
		{"scalar-min", `SELECT MIN(ARRAY[c_i64]) AS a FROM typemx WHERE id < 0`, []decl{arr(parquet.TypeInt64)}, ""},
		{"scalar-max-stored", `SELECT MAX(c_arr) AS a FROM typemx_nested WHERE id < 0`, []decl{arr(parquet.TypeString)}, ""},
		{"scalar-min-beside-count", `SELECT MIN(ARRAY[c_ts]) AS a, COUNT(*) AS n FROM typemx WHERE id < 0`,
			[]decl{arr(parquet.TypeTimestamp), i64}, ""},
		{"grouped", `SELECT g, MAX(ARRAY[c_ts]) AS a FROM typemx WHERE id < 0 GROUP BY g`, []decl{i32, arr(parquet.TypeTimestamp)}, ""},
		{"having", `SELECT g, MIN(ARRAY[c_str]) AS a FROM typemx GROUP BY g HAVING COUNT(*) > 1000000`,
			[]decl{i32, arr(parquet.TypeString)}, ""},
		{"window-min", `SELECT MIN(ARRAY[c_date]) OVER () AS a FROM typemx WHERE id < 0`, []decl{arr(parquet.TypeDate)}, ""},
		{"window-first-value", `SELECT FIRST_VALUE(ARRAY[c_i64]) OVER (ORDER BY id) AS a FROM typemx WHERE id < 0`,
			[]decl{arr(parquet.TypeInt64)}, ""},
		{"window-lag-stored", `SELECT LAG(c_arr) OVER (ORDER BY id) AS a FROM typemx_nested WHERE id < 0`, []decl{arr(parquet.TypeString)}, ""},
		{"scalar-subquery", `SELECT (SELECT MAX(ARRAY[c_ts]) FROM typemx WHERE id < 0) AS a`, []decl{arr(parquet.TypeTimestamp)}, ""},
		{"derived-aggregate", `SELECT q.a FROM (SELECT MIN(ARRAY[c_i64]) AS a FROM typemx WHERE id < 0) q`, []decl{arr(parquet.TypeInt64)}, ""},
		{"distinct-stored", `SELECT DISTINCT c_arr AS a FROM typemx_nested WHERE id < 0`, []decl{arr(parquet.TypeString)}, ""},
		{"group-by-key", `SELECT c_arr FROM typemx_nested WHERE id < 0 GROUP BY c_arr`, []decl{arr(parquet.TypeString)}, ""},
		{"distinct-derived-qualified", `SELECT DISTINCT q.a FROM (SELECT c_arr AS a, id FROM typemx_nested) q WHERE q.id < 0`,
			[]decl{arr(parquet.TypeString)}, ""},
		{"lateral-aggregate", `SELECT t.id, l.a FROM typemx t, LATERAL (SELECT MAX(ARRAY[u.c_date]) AS a FROM typemx u WHERE u.g = t.g) l WHERE t.id < 0`,
			// The decorrelated LATERAL's hash-join build refuses at the
			// spilled arm's 512 KiB budget (memory budget exceeded) — loud,
			// and the same at main; it is not a declaration.
			[]decl{i64, arr(parquet.TypeDate)}, spilledArm},
		{"coalesce-over-aggregate", `SELECT COALESCE(MIN(ARRAY[c_i64]), CAST(ARRAY[] AS BIGINT[])) AS a FROM typemx WHERE id < 0`,
			[]decl{arr(parquet.TypeInt64)}, ""},
		{"cte-aggregate", `WITH c AS (SELECT MAX(ARRAY[c_str]) AS a FROM typemx WHERE id < 0) SELECT a FROM c`, []decl{arr(parquet.TypeString)}, ""},
		// Control: a plain container column, right since round 1.
		{"control-column", `SELECT ARRAY[c_i64] AS a FROM typemx WHERE id < 0`, []decl{arr(parquet.TypeInt64)}, ""},
	}
	decided, skipped := 0, 0
	for _, c := range cells {
		if c.notOn != "" {
			skipped++
		}
	}
	for _, c := range cells {
		for _, arm := range arms {
			if arm.name == c.notOn {
				continue
			}
			got, _, err := arm.run(c.sql)
			if err != nil {
				t.Errorf("%s / %s: %s refused: %v", c.name, arm.name, c.sql, err)
				continue
			}
			decided++
			if fmt.Sprint(got) != fmt.Sprint(c.want) {
				t.Errorf("%s / %s: %s\n  declared %s\n  want     %s", c.name, arm.name, c.sql, cw2RenderDecls(got), cw2RenderDecls(c.want))
			}
		}
	}
	if want := len(cells)*len(arms) - skipped; decided != want {
		t.Errorf("%d of %d (cell, arm) pairs answered", decided, want)
	}
}

// cw2RenderDecls prints a declaration list as `ARRAY<TIMESTAMP>, INT64`.
func cw2RenderDecls[T any](ds []T) string {
	var out []string
	for _, d := range ds {
		out = append(out, strings.TrimSpace(fmt.Sprint(d)))
	}
	return strings.Join(out, ", ")
}

// TestArcCW2CorrelatedSubqueryOverAnArrayAnswersOnEveryArm is P2 of the
// round-1 review: a correlated scalar subquery whose MIN/MAX returns an ARRAY
// was refused on every arm with ContainerShapeError — the local aggregate the
// subquery is re-run as got no element for its output (the single-path half
// of B1) — while ADR-0045 §2 claimed no declared path reached the refusal.
// The oracle is the same subquery over the bare element: MAX(ARRAY[x]) over
// non-NULL x is ARRAY[MAX(x)], so its first element is MAX(x).
func TestArcCW2CorrelatedSubqueryOverAnArrayAnswersOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)
	pairs := []struct{ name, got, want string }{
		{"max-subscript",
			`SELECT id, (SELECT MAX(ARRAY[u.c_i64]) FROM typemx u WHERE u.g = t.g AND u.c_i64 IS NOT NULL)[1] AS m FROM typemx t WHERE t.id < 5 ORDER BY id`,
			`SELECT id, (SELECT MAX(u.c_i64) FROM typemx u WHERE u.g = t.g AND u.c_i64 IS NOT NULL) AS m FROM typemx t WHERE t.id < 5 ORDER BY id`},
		{"min-timestamp",
			`SELECT id, (SELECT MIN(ARRAY[u.c_ts]) FROM typemx u WHERE u.g = t.g AND u.c_ts IS NOT NULL)[1] AS m FROM typemx t WHERE t.id < 5 ORDER BY id`,
			`SELECT id, (SELECT MIN(u.c_ts) FROM typemx u WHERE u.g = t.g AND u.c_ts IS NOT NULL) AS m FROM typemx t WHERE t.id < 5 ORDER BY id`},
	}
	for _, p := range pairs {
		for _, arm := range arms {
			_, got, err := arm.run(p.got)
			if err != nil {
				t.Errorf("%s / %s: %s\n  refused: %v", p.name, arm.name, p.got, err)
				continue
			}
			_, want, err := arm.run(p.want)
			if err != nil {
				t.Errorf("%s / %s: the oracle refused: %v", p.name, arm.name, err)
				continue
			}
			if fmt.Sprint(got) != fmt.Sprint(want) || len(got) == 0 {
				t.Errorf("%s / %s:\n  got  %v\n  want %v", p.name, arm.name, got, want)
			}
		}
	}
	// The whole value is an ARRAY (a slice), not its text.
	for _, arm := range arms {
		_, rows, err := arm.run(`SELECT (SELECT MAX(ARRAY[u.c_i64]) FROM typemx u WHERE u.g = t.g) AS m FROM typemx t WHERE t.id = 1`)
		if err != nil || len(rows) != 1 {
			t.Errorf("whole-array / %s: %v %v", arm.name, rows, err)
			continue
		}
		if _, ok := rows[0][0].([]any); !ok {
			t.Errorf("whole-array / %s: %T %v, not an array", arm.name, rows[0][0], rows[0][0])
		}
	}
}
