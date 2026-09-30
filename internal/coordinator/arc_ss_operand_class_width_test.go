// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

// A SCALAR SUBQUERY'S ANSWER CARRIES ITS OPERANDS' DECLARED CLASS AND WIDTH
// (#1422). The companion of TestArcSSScalarSubqueryTypedOperandEveryArm over
// the same fixture and five arms, for the cells where the operand's TYPE —
// not only its value — decides the answer:
//
//   - int4 arithmetic inside a scalar subquery is int4: a result past its
//     range is 22003, uncorrelated (`x.v * … * x.v`) and correlated over an
//     int4 outer column, a derived table's `CAST(… AS INT)` included;
//   - an operator PostgreSQL does not have for the operands' types is 42883
//     whatever spelling reaches it: `||` between two non-text operands and
//     `*`, `/`, `%` beside a DATE or a TIMESTAMP, outside a subquery, inside
//     one, and correlated through the outer column's typed spelling;
//   - a correlated outer value is typed as its COLUMN in DECIMAL
//     arithmetic — `x.m / o.i` answers `x.m / i`'s digits and `x.v / o.n`
//     `v / n`'s, the plain/* cells beside them — while an integer CAST the
//     user writes keeps its own rule, with no subquery anywhere
//     (`CAST(t.i AS INTEGER) / t.n`, castI/*: v0.25.3's digits, stored
//     value and row count); a NULL outer value is a NULL of its column's
//     type (`sum(x.v + o.i)` over a NULL is NULL); `(SELECT x.b - x.i …)`
//     is bigint, not the int4 of its last operand;
//   - a DATE answer past 9999-12-31 is read back (10000-01-01);
//   - a scalar subquery is declared as any SELECT-list expression is, at
//     the integer width its plan publishes (shared/*): a subscript of an
//     int4[], a day count and ascii() are int4 (`(SELECT o.a[1] +
//     2147483647 …)` is 22003, `(SELECT (o.d - DATE …) + x.v …)` integer),
//     `(SELECT x.b - x.i …)` stays bigint, and SUM over a derived or CTE
//     int4 column is bigint, with no subquery at all;
//   - the re-run's spelling of an outer value, `__column_value(…)`, is
//     42883 from a client on every arm (spelling/*), as on PostgreSQL.
//
// Every want is PostgreSQL 17.11's, measured over ssPGFixture
// (testdata/arc_ss_operand_class_width_pg17.tsv: name, ordered, sql,
// answer), except the cells in testdata/arc_ss_operand_class_width_kept.tsv,
// each a catalogued divergence or a recorded filing candidate asserted as it
// stands.

import (
	"bufio"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

type ssOCWCell struct {
	name, sql, pg string
	ordered       bool
}

func ssOCWCells(t *testing.T) []ssOCWCell {
	t.Helper()
	f, err := os.Open("testdata/arc_ss_operand_class_width_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []ssOCWCell
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 4)
		if len(p) != 4 {
			t.Fatalf("malformed cell line %q", line)
		}
		out = append(out, ssOCWCell{name: p[0], ordered: p[1] == "true", sql: p[2], pg: p[3]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func ssOCWKept(t *testing.T) map[string]ssKeep {
	t.Helper()
	f, err := os.Open("testdata/arc_ss_operand_class_width_kept.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]ssKeep{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 4)
		if len(p) != 4 || (p[1] != "all" && p[1] != "dag") {
			t.Fatalf("malformed kept line %q", line)
		}
		out[p[0]] = ssKeep{dagOnly: p[1] == "dag", want: p[2], why: p[3]}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestArcSSOperandClassAndWidthEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the operand class and width cells")
	}
	cells := ssOCWCells(t)
	kept := ssOCWKept(t)
	names := map[string]bool{}
	for _, c := range cells {
		names[c.name] = true
	}
	for name := range kept {
		if !names[name] {
			t.Fatalf("kept cell %s is not in the table", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := ssArms(t, ctx)
	for _, tc := range cells {
		k, isKept := kept[tc.name]
		t.Run(tc.name, func(t *testing.T) {
			got := make([]string, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res, err := arm.run(tc.sql, tc.ordered)
					if err != nil {
						res = "ERR " + sqlerr.StateOf(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
					}
					got[i] = res
				}()
			}
			wg.Wait()
			for i, arm := range arms {
				want, why := tc.pg, "PostgreSQL 17.11"
				if isKept && (!k.dagOnly || strings.HasPrefix(arm.name, "dag")) {
					want, why = k.want, "kept: "+k.why
				}
				if !ssMatches(got[i], want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, got[i], want, why)
				}
			}
		})
	}
}
