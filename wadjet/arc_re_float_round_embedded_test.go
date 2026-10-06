// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// What a WRITE stores from a rounded float (#381, #1542): a value rounds by
// its declared type — a double precision or real half to even, a numeric half
// away from zero — and CREATE TABLE AS stores what the SELECT computes, so
// the table holds PostgreSQL's value. At 89cea148 CREATE TABLE AS over
// round(f) stored 3, 1, -3, -1 for 2.5, 0.5, -2.5, -0.5 (PostgreSQL 2, 0, -2,
// -0), over a float expression, a REAL column and a derived table's float
// column alike, and over CAST(ARRAY[f, 1.5] AS BIGINT[]) stored {3,2} for
// PostgreSQL's {2,2}; an INSERT … SELECT of round(f) into a BIGINT column
// stored the half-away value. Every want is PostgreSQL 17.11's over the same
// DDL and rows (re_author/emb_pg17.tsv).
func TestArcREEmbeddedWritesStoreTheDeclaredTypesRounding(t *testing.T) {
	ctx := context.Background()
	setup := []string{
		"CREATE TABLE e_v (id BIGINT, f DOUBLE PRECISION, r REAL, n DECIMAL(10,2))",
		"INSERT INTO e_v VALUES (1, 2.5, 2.5, 2.5), (2, 0.5, 0.5, 0.5), (3, -2.5, -2.5, -2.5), (4, -0.5, -0.5, -0.5), (5, 3.5, 3.5, 3.5)",
	}
	cases := []struct {
		name  string
		stmts []string
		want  string
	}{
		{"ctasRound", []string{"CREATE TABLE e_c AS SELECT id, round(f) AS a FROM e_v", "SELECT id, a FROM e_c ORDER BY id"}, "1,2; 2,0; 3,-2; 4,-0; 5,4"},
		{"ctasRoundReal", []string{"CREATE TABLE e_c AS SELECT id, round(r) AS a FROM e_v", "SELECT id, a FROM e_c ORDER BY id"}, "1,2; 2,0; 3,-2; 4,-0; 5,4"},
		{"ctasRoundNumeric", []string{"CREATE TABLE e_c AS SELECT id, round(n) AS a FROM e_v", "SELECT id, a FROM e_c ORDER BY id"}, "1,3; 2,1; 3,-3; 4,-1; 5,4"},
		{"ctasRoundExpr", []string{"CREATE TABLE e_c AS SELECT id, round(f * 1.0) AS a FROM e_v", "SELECT id, a FROM e_c ORDER BY id"}, "1,2; 2,0; 3,-2; 4,-0; 5,4"},
		{"ctasRoundDerivedFloat", []string{"CREATE TABLE e_c AS SELECT id, round(x) AS a FROM (SELECT DISTINCT id, f AS x FROM e_v) s", "SELECT id, a FROM e_c ORDER BY id"}, "1,2; 2,0; 3,-2; 4,-0; 5,4"},
		{"ctasRoundDerivedNumeric", []string{"CREATE TABLE e_c AS SELECT id, round(x) AS a FROM (SELECT DISTINCT id, 2.5 + id * 0 AS x FROM e_v) s", "SELECT id, a FROM e_c ORDER BY id"}, "1,3; 2,3; 3,3; 4,3; 5,3"},
		{"ctasCastBigint", []string{"CREATE TABLE e_c AS SELECT id, CAST(f AS BIGINT) AS a FROM e_v", "SELECT id, a FROM e_c ORDER BY id"}, "1,2; 2,0; 3,-2; 4,0; 5,4"},
		{"ctasCastDerivedNumeric", []string{"CREATE TABLE e_c AS SELECT id, CAST(x AS INTEGER) AS a FROM (SELECT DISTINCT id, 2.5 + id * 0 AS x FROM e_v) s", "SELECT id, a FROM e_c ORDER BY id"}, "1,3; 2,3; 3,3; 4,3; 5,3"},
		{"ctasArray", []string{"CREATE TABLE e_c AS SELECT id, CAST(ARRAY[f, 1.5] AS BIGINT[]) AS a FROM e_v", "SELECT id, a FROM e_c ORDER BY id"}, "1,{2,2}; 2,{0,2}; 3,{-2,2}; 4,{0,2}; 5,{4,2}"},
		{"insertSelectRound", []string{"CREATE TABLE e_t (id BIGINT, a BIGINT)", "INSERT INTO e_t SELECT id, round(f) FROM e_v", "SELECT id, a FROM e_t ORDER BY id"}, "1,2; 2,0; 3,-2; 4,0; 5,4"},
		{"insertSelectRoundNumeric", []string{"CREATE TABLE e_t (id BIGINT, a BIGINT)", "INSERT INTO e_t SELECT id, round(n) FROM e_v", "SELECT id, a FROM e_t ORDER BY id"}, "1,3; 2,1; 3,-3; 4,-1; 5,4"},
		{"insertSelectRoundIntoFloat", []string{"CREATE TABLE e_t (id BIGINT, a DOUBLE PRECISION)", "INSERT INTO e_t SELECT id, round(f) FROM e_v", "SELECT id, a FROM e_t ORDER BY id"}, "1,2; 2,0; 3,-2; 4,-0; 5,4"},
		{"insertSelectExpr", []string{"CREATE TABLE e_t (id BIGINT, a BIGINT)", "INSERT INTO e_t SELECT id, f * 1.0 FROM e_v", "SELECT id, a FROM e_t ORDER BY id"}, "1,2; 2,0; 3,-2; 4,0; 5,4"},
		{"insertSelectDerivedFloat", []string{"CREATE TABLE e_t (id BIGINT, a BIGINT)", "INSERT INTO e_t SELECT id, x FROM (SELECT DISTINCT id, f AS x FROM e_v) s", "SELECT id, a FROM e_t ORDER BY id"}, "1,2; 2,0; 3,-2; 4,0; 5,4"},
		{"insertSelectDerivedNumeric", []string{"CREATE TABLE e_t (id BIGINT, a BIGINT)", "INSERT INTO e_t SELECT id, x FROM (SELECT DISTINCT id, 2.5 + id * 0 AS x FROM e_v) s", "SELECT id, a FROM e_t ORDER BY id"}, "1,3; 2,3; 3,3; 4,3; 5,3"},
		{"insertSelectArray", []string{"CREATE TABLE e_t (id BIGINT, a ARRAY(BIGINT))", "INSERT INTO e_t SELECT id, CAST(ARRAY[f] AS BIGINT[]) FROM e_v", "SELECT id, a FROM e_t ORDER BY id"}, "1,{2}; 2,{0}; 3,{-2}; 4,{0}; 5,{4}"},
		{"insertSelectSubquery", []string{"CREATE TABLE e_t (id BIGINT, a DOUBLE PRECISION)", "INSERT INTO e_t SELECT 1, round((SELECT f FROM e_v WHERE id = 1))", "SELECT id, a FROM e_t ORDER BY id"}, "1,2"},
		{"ctasRoundFloatCarriedNumeric", []string{"CREATE TABLE e_c AS SELECT id, round(x) AS a FROM (SELECT DISTINCT id, 5 / 2.0 + id * 0 AS x FROM e_v) s", "SELECT id, a FROM e_c ORDER BY id"}, "1,3; 2,3; 3,3; 4,3; 5,3"},
		{"ctasCastFloatCarriedNumeric", []string{"CREATE TABLE e_c AS SELECT id, CAST(x AS INTEGER) AS a FROM (SELECT DISTINCT id, 5 / 2.0 + id * 0 AS x FROM e_v) s", "SELECT id, a FROM e_c ORDER BY id"}, "1,3; 2,3; 3,3; 4,3; 5,3"},
		{"updateRound", []string{"CREATE TABLE e_t (id BIGINT, a DOUBLE PRECISION, f DOUBLE PRECISION)", "INSERT INTO e_t SELECT id, 7, f FROM e_v", "UPDATE e_t SET a = round(f)", "SELECT id, a FROM e_t ORDER BY id"}, "1,2; 2,0; 3,-2; 4,-0; 5,4"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, s := range setup {
				if _, err := db.Query(ctx, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			var parts []string
			for _, s := range c.stmts {
				res, err := db.Query(ctx, s)
				if err != nil {
					parts = append(parts, "ERR "+sqlerr.StateOf(err))
					continue
				}
				if !strings.HasPrefix(s, "SELECT") {
					continue
				}
				var rows []string
				for i := range res.Rows {
					cells := res.Cells(i)
					f := make([]string, len(cells))
					for j, v := range cells {
						f[j] = reText(v)
					}
					rows = append(rows, strings.Join(f, ","))
				}
				if len(rows) == 0 {
					rows = []string{"(0 rows)"}
				}
				parts = append(parts, strings.Join(rows, "; "))
			}
			if got := strings.Join(parts, " => "); got != c.want {
				t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", strings.Join(c.stmts, " ;; "), got, c.want)
			}
		})
	}
}

// reText is PostgreSQL's text of a value: a double's shortest digits, an
// array in braces.
func reText(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case float64:
		if math.IsInf(t, 0) {
			return map[bool]string{true: "Infinity", false: "-Infinity"}[t > 0]
		}
		return strconv.FormatFloat(t, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32)
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = reText(e)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	return fmt.Sprint(v)
}

// round(x, n) over a DOUBLE PRECISION or REAL operand is a SUPERSET:
// PostgreSQL has no round(float8, integer) and refuses it (42883, measured
// on 17.11 for a column, a cast literal and n = 0), and this engine answers it
// by the operand's own rule — x·10ⁿ rounded half to even, then scaled back —
// the rule round(x) has (docs/adr/0012-divergences/numeric-decimal.md).
// At 89cea148 the same call rounded half away from zero over a column and
// half to even over a CAST literal. The NUMERIC operand is PostgreSQL's
// round(numeric, integer), half away from zero (0.3 for 0.25, measured).
// The values are exact binary fractions, so the scaled value is the tie
// itself.
func TestArcRERoundWithDigitsOverAFloatTakesTheFloatRule(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		"CREATE TABLE e_w (id BIGINT, f DOUBLE PRECISION, r REAL, n DECIMAL(10,3))",
		"INSERT INTO e_w VALUES (1, 0.25, 0.25, 0.25), (2, 0.75, 0.75, 0.75), (3, -0.25, -0.25, -0.25), (4, 2.5, 2.5, 2.5), (5, 0.125, 0.125, 0.125)",
	} {
		if _, err := db.Query(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	for _, c := range []struct{ sql, want string }{
		{"SELECT round(f, 1) FROM e_w WHERE id < 4 ORDER BY id", "0.2; 0.8; -0.2"},
		{"SELECT round(r, 1) FROM e_w WHERE id < 4 ORDER BY id", "0.2; 0.8; -0.2"},
		{"SELECT round(f, 0) FROM e_w WHERE id = 4", "2"},
		{"SELECT round(f, 2) FROM e_w WHERE id = 5", "0.12"},
		{"SELECT round(f * 1.0, 1) FROM e_w WHERE id = 1", "0.2"},
		{"SELECT round(CAST(0.25 AS DOUBLE PRECISION), 1)", "0.2"},
		// The numeric control, PostgreSQL's own answer.
		{"SELECT round(n, 1) FROM e_w WHERE id < 4 ORDER BY id", "0.3; 0.8; -0.3"},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		var rows []string
		for i := range res.Rows {
			rows = append(rows, reText(res.Cells(i)[0]))
		}
		if got := strings.Join(rows, "; "); got != c.want {
			t.Errorf("%s\n  got  %s\n  want %s", c.sql, got, c.want)
		}
	}
}

// The superset's BOUND (numeric-decimal r22): the rule needs 10ⁿ and x·10ⁿ to
// be finite doubles. Past that — n ≥ 309, n ≥ 308 − log10|x|, n ≤ −324 — a
// finite x has no answer under the rule, and the call refuses 22003 where
// 89cea148 answered NaN (n = 400) or Infinity (n = 308) for 2.5. A NaN or
// infinite x answers itself, and n = 307 or −301 inside the bound answers
// the rule.
func TestArcRERoundWithDigitsRefusesPastItsBound(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		"CREATE TABLE e_b (id BIGINT, f DOUBLE PRECISION)",
		"INSERT INTO e_b VALUES (1, 2.5), (2, 1e300), (3, 'NaN'), (4, 'Infinity')",
	} {
		if _, err := db.Query(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	for _, c := range []struct{ sql, want string }{
		{"SELECT round(f, 400) FROM e_b WHERE id = 1", "ERR 22003"},
		{"SELECT round(f, 308) FROM e_b WHERE id = 1", "ERR 22003"},
		{"SELECT round(f, -400) FROM e_b WHERE id = 1", "ERR 22003"},
		{"SELECT round(f, 10) FROM e_b WHERE id = 2", "ERR 22003"},
		{"SELECT round(CAST(2.5 AS DOUBLE PRECISION), 400)", "ERR 22003"},
		{"SELECT round(f, 307) FROM e_b WHERE id = 1", "2.5"},
		{"SELECT round(f, -301) FROM e_b WHERE id = 2", "0"},
		{"SELECT round(f, 400) FROM e_b WHERE id = 3", "NaN"},
		{"SELECT round(f, 400) FROM e_b WHERE id = 4", "Infinity"},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			got := "ERR " + sqlerr.StateOf(err)
			if got != c.want {
				t.Errorf("%s\n  got  %s (%v)\n  want %s", c.sql, got, err, c.want)
			}
			continue
		}
		var rows []string
		for i := range res.Rows {
			rows = append(rows, reText(res.Cells(i)[0]))
		}
		if got := strings.Join(rows, "; "); got != c.want {
			t.Errorf("%s\n  got  %s\n  want %s", c.sql, got, c.want)
		}
	}
}
