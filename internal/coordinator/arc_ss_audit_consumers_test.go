// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

// EVERY OPERAND KIND × EVERY CONSUMER THAT MATERIALIZES IT (#1422's class).
// A value declared numeric — a scalar subquery's answer, a subscript, a day
// count, an integer call, a correlated answer and the trees over them — is
// read by every operator that materializes a column, and each one either
// stores the declared type or raises #361's guard. The consumers here are
// enumerated from the EXEC side, one cell per consumer per operand kind:
//
//	c01 a projection        c10 sort keys           c19 a derived table's column
//	c02 a filter            c11 TopN                    read by an outer expression
//	c03 aggregates, HAVING  c12 hash-join keys      c20 a CASE arm
//	c04 the partial-        c13 a join residual     c21 CAST AS TEXT / BIGINT
//	    aggregate merge     c14 IN (subquery)       c27 a FROM-less LATERAL body
//	c05 a GROUP BY key      c15 a column carried    c28 string_agg
//	c06 DISTINCT                through a join
//	c07 UNION keys          c16 the DAG gather
//	c08 a window's input    c17 the coordinator's
//	c09 a window's              ordered merge
//	    PARTITION BY /      c18 a correlated
//	    ORDER BY keys           subquery's answer column
//
// The kinds (testdata names): a column (col), a literal (lit), numeric
// arithmetic (binNum), a scalar subquery inside arithmetic (sub), unary
// minus (neg), CAST with (p,s) (castPS), a bare CAST AS NUMERIC (castBare),
// abs() (abs), COALESCE (coal), an integer-armed CASE (caseInt), ascii()
// (ascii), integer arithmetic over a call (int64), a subscript (idx), a day
// count (dc), a correlated subquery whose body holds an integral EXTRACT
// (year), an integer-valued scalar subquery (nested), a DECIMAL scalar
// subquery (decSub) and a double (float); the user-defined function, which
// PostgreSQL cannot spell, is measured on the wire only. CTAS, INSERT …
// SELECT, DELETE, UPDATE and the binary encoder are the wire gate's
// (pgwire.TestArcSSAuditConsumersOnTheWire); the embedded reader is the
// single arm here.
//
// The dv/* and wk/* cells are a derived table's or a CTE's column read by an
// outer expression and a window key holding a scalar subquery beside a
// NUMERIC — the two consumers whose declaration and kernel disagreed.
//
// The r12/* cells are seven more consumers over the same kinds: c29 the
// ARRAY constructor beside an integer element, c30 WITH RECURSIVE seeded by
// the kind, c31 the outer value spelled into a correlated re-run, c32
// implicit text conversion (`||`, concat), c33 a LEFT JOIN's null extension,
// c34 a LATERAL body with a FROM clause, and c20b a CASE / COALESCE whose
// arms are the kind as a bare correlated answer and that answer + 1. The
// r12/b1, b2 and b3 cells are the choice arm, the array element and the
// recursive seed, each beside the form v0.25.3 already got wrong
// (`CASE … (SELECT q.m …) … (SELECT q.m …) + 1`, `ARRAY[t.n, 1]`, a recursion
// over `t.i * t.n`).
//
// Every want is PostgreSQL 17.11's over ssPGFixture
// (testdata/arc_ss_audit_consumers_pg17.tsv: name, ordered, sql, answer),
// except the cells in testdata/arc_ss_audit_consumers_kept.tsv, each a
// catalogued divergence or a recorded filing candidate asserted as it stands
// on the arms it names (all, local = single and spilled, dag = the three
// stage-DAG arms).

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

type ssAuditKeep struct {
	scope, want, why string
}

func ssAuditKept(t *testing.T) map[string][]ssAuditKeep {
	t.Helper()
	f, err := os.Open("testdata/arc_ss_audit_consumers_kept.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]ssAuditKeep{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 4)
		if len(p) != 4 || (p[1] != "all" && p[1] != "local" && p[1] != "dag") {
			t.Fatalf("malformed kept line %q", line)
		}
		out[p[0]] = append(out[p[0]], ssAuditKeep{scope: p[1], want: p[2], why: p[3]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestArcSSOperandKindTimesConsumerEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the operand kind × consumer cells")
	}
	f, err := os.Open("testdata/arc_ss_audit_consumers_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var cells []ssOCWCell
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
		cells = append(cells, ssOCWCell{name: p[0], ordered: p[1] == "true", sql: p[2], pg: p[3]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	kept := ssAuditKept(t)
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
				dag := strings.HasPrefix(arm.name, "dag")
				want, why := tc.pg, "PostgreSQL 17.11"
				for _, k := range kept[tc.name] {
					if k.scope == "all" || (k.scope == "dag") == dag {
						want, why = k.want, "kept: "+k.why
					}
				}
				if !ssMatches(got[i], want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, got[i], want, why)
				}
			}
		})
	}
}
