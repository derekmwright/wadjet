// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A SET OPERATION'S RESULT COLUMN IS ONE RULE (ADR-0024 §10). Nine arm kinds
// — an untyped NULL, a quoted literal, an integer literal, a numeric literal
// spelled with a trailing zero, a NULL cast to NUMERIC and to NUMERIC(10,2),
// an expression, a NUMERIC(10,2) column, a scalar subquery — each in every
// position of a two-, three- and four-arm UNION ALL / UNION / INTERSECT /
// EXCEPT whose other arms are a column created from an unconstrained NUMERIC,
// read six ways (the bare column, its text, a text equality count, a text
// LIKE count, SUM, GROUP BY): 1,944 statements, measured on PostgreSQL 17.11
// and on the base before the rule was one function.
//
// Each statement's row in testdata/arc_un_setop_matrix.tsv names the answer
// every arm that answers must give, how it stands to PostgreSQL's
// (`pg` equal; `fenced` equal to PostgreSQL's answer with the set operation
// fenced, below; `refused` a refusal the base gives too), PostgreSQL's own
// answer, and the arms that refuse
// (each one a
// refusal identical at the base: the stage arms' ORDER BY over the text of a
// UNION, the asynchronous door's rename, a quoted-first SUM, a scalar
// subquery arm). Before the rule was one function the stage planner and the
// single-process path each decided the column's mark, and three reviews
// found an input on which they differed; the agreed repair unmarked the
// result beside a NULL, integer or expression arm, so the text equality
// count answered 0 where PostgreSQL and the base answer 2.
//
// Since arc PS stage 1 (ADR-0024 §11, 2026-10-06) each value prints its own
// display scale: the 126 `r4r18` pins (the stored-scale text of an unmarked
// mixed result, 52 text reads and 74 counts, #1647's 28 among them) are
// deleted — 120 answer PostgreSQL's text on every arm, and the other six
// answer PostgreSQL's fenced answer below, as do twelve more count cells.
//
// FENCED (ADR-0013 item 11, 2026-10-06): PostgreSQL's answer to a predicate
// over the TEXT of a set operation's numeric output depends on its plan.
// Unfenced, its planner pushes the predicate into each arm, so it tests each
// arm's own text (`1` from un_x, `1.00` from the NUMERIC(10,2) rv_n) BEFORE
// the UNION or INTERSECT forms its output and chooses one representative of
// the equal values; fenced with `OFFSET 0` it tests the representative. This
// engine evaluates the predicate over the set operation's output and prints
// the representative's own display scale, so it answers the fenced
// statement. The fence is PER STATEMENT: exactly the 18 count statements
// whose answer it changes carry the disposition `fenced` and PostgreSQL's
// answer to `SELECT count(*) FROM (SELECT v FROM <body> OFFSET 0) q WHERE
// <predicate>` — u_fixed_{2_0,3_0,4_0}_one (unfenced 1, fenced 0),
// u_fixed_{2_1,3_1,3_2,4_1,4_2,4_3}_zero (5, 2), i_fixed_{2_0,3_0,4_0}_zero
// (0, 3), i_fixed_{2_1,3_1,3_2,4_1,4_2,4_3}_one (0, 1); the other 630 count
// statements answer alike either way and carry the statement as written
// (measured 17.11 over all 648).

// The default run is a deterministic 300-statement subset that covers every
// arm kind × position × operation × read; WADJET_UN_SETOP_MATRIX=full runs
// all 1,944.
func TestArcUNSetOperationMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: eleven arms over the set-operation matrix")
	}
	want := unSetOpMatrixExpected(t)
	cells := unSetOpMatrixCells()
	if os.Getenv("WADJET_UN_SETOP_MATRIX") != "full" {
		cells = unSetOpMatrixSubset(cells)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := unibArms(t, ctx)
	answered := 0
	for _, c := range cells {
		exp, ok := want[c.id]
		if !ok {
			t.Fatalf("%s: no row in testdata/arc_un_setop_matrix.tsv", c.id)
		}
		if exp.disposition != "pg" && exp.disposition != "refused" && exp.disposition != "fenced" && exp.answer == exp.pg {
			t.Errorf("%s: pinned as %s but the pinned answer is PostgreSQL's: delete the pin", c.id, exp.disposition)
		}
		if exp.disposition == "fenced" && (exp.answer != exp.pg || !(strings.HasSuffix(c.id, "_one") || strings.HasSuffix(c.id, "_zero"))) {
			t.Errorf("%s: `fenced` (ADR-0013 item 11) names a count read answering PostgreSQL's fenced answer", c.id)
		}
		var refused []string
		for _, arm := range arms {
			res, err := arm.run(c.sql)
			if err != nil {
				msg := strings.SplitN(err.Error(), "\n", 2)[0]
				if strings.Contains(msg, "in the file header") {
					t.Errorf("%s\n  arm %s: the writer's mark check refused: %s", c.sql, arm.name, msg)
				}
				refused = append(refused, arm.name)
				continue
			}
			answered++
			got := unSetOpMatrixCanon(c.id, res)
			if got != exp.answer {
				t.Errorf("%s (%s)\n  arm %s: %s\n  want (%s): %s\n  PostgreSQL: %s",
					c.sql, c.id, arm.name, got, exp.disposition, exp.answer, exp.pg)
			}
		}
		if r := strings.Join(refused, ","); r != exp.refused {
			t.Errorf("%s (%s)\n  refusing arms: %q\n  pinned: %q", c.sql, c.id, r, exp.refused)
		}
	}
	if answered == 0 {
		t.Fatal("no arm answered: the matrix measured nothing")
	}
	t.Logf("%d statements × %d arms, %d answers", len(cells), len(arms), answered)
}

type unSetOpMatrixCell struct{ id, sql string }

type unSetOpMatrixRow struct{ disposition, answer, pg, refused string }

// unSetOpMatrixCells is the review's statement generator, unchanged, so the
// IDs are the measured ones.
func unSetOpMatrixCells() []unSetOpMatrixCell {
	kinds := []struct{ name, sql string }{
		{"null", "SELECT NULL AS v FROM un_u"},
		{"quoted", "SELECT '1.5' AS v FROM un_u"},
		{"integer", "SELECT 2 AS v FROM un_u"},
		{"numeric", "SELECT 2.50 AS v FROM un_u"},
		{"typednull", "SELECT CAST(NULL AS NUMERIC) AS v FROM un_u"},
		{"fixednull", "SELECT CAST(NULL AS NUMERIC(10,2)) AS v FROM un_u"},
		{"expr", "SELECT v + 0 AS v FROM un_x"},
		{"fixed", "SELECT v FROM rv_n"},
		{"scalar", "SELECT (SELECT max(v) FROM un_x) AS v FROM un_u"},
	}
	ops := []struct{ name, sql string }{{"ua", "UNION ALL"}, {"u", "UNION"}, {"i", "INTERSECT"}, {"e", "EXCEPT"}}
	var cells []unSetOpMatrixCell
	for _, op := range ops {
		for _, kind := range kinds {
			for n := 2; n <= 4; n++ {
				for pos := 0; pos < n; pos++ {
					arms := make([]string, n)
					for j := range arms {
						arms[j] = "SELECT v FROM un_x"
						if j == pos {
							arms[j] = kind.sql
						}
					}
					body := "(" + strings.Join(arms, " "+op.sql+" ") + ") s"
					reads := []struct{ name, sql string }{
						{"bare", "SELECT v FROM " + body + " ORDER BY 1"},
						{"text", "SELECT CAST(v AS TEXT) FROM " + body + " ORDER BY 1"},
						{"one", "SELECT count(*) FROM " + body + " WHERE CAST(v AS TEXT) = '1'"},
						{"zero", "SELECT count(*) FROM " + body + " WHERE CAST(v AS TEXT) LIKE '%0'"},
						{"sum", "SELECT sum(v) FROM " + body},
						{"group", "SELECT v, count(*) FROM " + body + " GROUP BY v ORDER BY 1"},
					}
					for _, r := range reads {
						cells = append(cells, unSetOpMatrixCell{
							fmt.Sprintf("%s_%s_%d_%d_%s", op.name, kind.name, n, pos, r.name), r.sql})
					}
				}
			}
		}
	}
	return cells
}

// unSetOpMatrixSubset keeps, for every (operation, kind, read), one shape —
// the (arms, position) pair rotating with the three indices — and, for every
// (kind, arms, position), the text equality count under a rotating
// operation: every arm kind × position × operation × read is reached.
func unSetOpMatrixSubset(cells []unSetOpMatrixCell) []unSetOpMatrixCell {
	ops := []string{"ua", "u", "i", "e"}
	kinds := []string{"null", "quoted", "integer", "numeric", "typednull", "fixednull", "expr", "fixed", "scalar"}
	reads := []string{"bare", "text", "one", "zero", "sum", "group"}
	var shapes [][2]int
	for n := 2; n <= 4; n++ {
		for pos := 0; pos < n; pos++ {
			shapes = append(shapes, [2]int{n, pos})
		}
	}
	keep := map[string]bool{}
	for o, op := range ops {
		for k, kind := range kinds {
			for r, read := range reads {
				s := shapes[(o*7+k*3+r)%len(shapes)]
				keep[fmt.Sprintf("%s_%s_%d_%d_%s", op, kind, s[0], s[1], read)] = true
			}
		}
	}
	for k, kind := range kinds {
		for si, s := range shapes {
			keep[fmt.Sprintf("%s_%s_%d_%d_one", ops[(k+si)%len(ops)], kind, s[0], s[1])] = true
		}
	}
	var out []unSetOpMatrixCell
	for _, c := range cells {
		if keep[c.id] {
			out = append(out, c)
		}
	}
	return out
}

// unSetOpMatrixCanon renders an answer the way a client reads it (a marked
// column trimmed, as the doors print it) and, except for the text reads,
// with every number's trailing fraction zeros removed: those reads compare
// values, the text reads compare text.
func unSetOpMatrixCanon(id string, res wdResult) string {
	text := strings.HasSuffix(id, "_text")
	rows := make([][]string, 0, len(res.rows))
	for _, r := range res.rows {
		cells := make([]string, len(r))
		for j, v := range r {
			var c parquet.Column
			if j < len(res.schema) {
				c = res.schema[j]
			}
			cells[j] = wdFmt(c, v)
			if s, ok := v.(string); ok && c.Type == parquet.TypeDecimal && unMarked(c) {
				cells[j] = unTrim(s)
			}
			if !text {
				cells[j] = unSetOpMatrixNumber(cells[j])
			}
		}
		rows = append(rows, cells)
	}
	b, _ := json.Marshal(rows)
	return string(b)
}

func unSetOpMatrixNumber(s string) string {
	if strings.ContainsAny(s, "eE") || !strings.Contains(s, ".") {
		return s
	}
	for _, r := range s {
		if (r < '0' || r > '9') && r != '.' && r != '-' {
			return s
		}
	}
	return unTrim(s)
}

func unSetOpMatrixExpected(t *testing.T) map[string]unSetOpMatrixRow {
	t.Helper()
	f, err := os.Open("testdata/arc_un_setop_matrix.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]unSetOpMatrixRow{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.Split(line, "\t")
		if len(p) != 5 {
			t.Fatalf("arc_un_setop_matrix.tsv: malformed line %q", line)
		}
		out[p[0]] = unSetOpMatrixRow{disposition: p[1], answer: p[2], pg: p[3], refused: p[4]}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
