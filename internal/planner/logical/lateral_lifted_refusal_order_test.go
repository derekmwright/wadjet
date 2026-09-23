// SPDX-License-Identifier: MIT

package logical

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// THE AGGREGATED REFUSAL COMES BEFORE EVERY DECLINE — asserted, not commented.
//
// `publishLiftedRefs` answers a lifted non-equality correlated predicate two
// ways. It REFUSES when the body aggregates: there is no projection to publish
// the predicate's column in, and publishing one would join the GROUP BY and
// change what the aggregate computes. It DECLINES — returns the query to the
// disposition it had before the materialization existed — when publishing the
// column would disturb something else: a DISTINCT key, a name the body's own
// list or the enclosing relation already carries, or a star's published list.
//
// The two are not interchangeable and the refusal wins, because a decline on
// an aggregated body is a SILENT WRONG ANSWER: the predicate is dropped and
// every outer row gets the whole relation's aggregate, or NULL. Round 4 put
// the refusal in front of the DISTINCT / contested arm but left the STAR test
// as a `return nil, nil` above the loop, so one statement had two dispositions
// decided by the ENCLOSING SELECT list — `SELECT o.id, s.m FROM … ` refused
// and `SELECT * FROM …` answered three NULLs for PostgreSQL's `350 | 350 |
// NULL`, on all five arms (round-5 review, B1). A comment saying the refusal
// comes first did not stop that, so this test says it instead.
//
// The VALUES are gated on five arms by the `R5/*` cells of
// `coordinator.TestArcL1LateralAndWindowScopeAnswersPostgresOnEveryArm`.
func TestTheAggregatedRefusalPrecedesEveryLiftedRefDecline(t *testing.T) {
	// Every decline trigger the function tests, crossed with a body that
	// AGGREGATES. A new trigger that does not refuse here is the defect this
	// test exists to catch.
	for _, tc := range []struct {
		name, agg, ctl string
	}{
		{"a DISTINCT body",
			`SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT DISTINCT SUM(i.amount) AS m FROM lat_item i WHERE i.amount < o.total) s ON true`,
			`SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT DISTINCT i.amount AS m FROM lat_item i WHERE i.amount < o.total) s ON true`},
		{"the body's own alias carries the name",
			`SELECT o.id AS a, s.amount AS m FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT SUM(i.id) AS amount FROM lat_item i WHERE i.amount < o.total) s ON true`,
			`SELECT o.id AS a, s.amount AS m FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT i.id AS amount FROM lat_item i WHERE i.amount < o.total) s ON true`},
		{"the enclosing relation carries the name",
			`SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.id < o.id) s ON true`,
			`SELECT o.id AS a, s.m AS m FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT i.product AS m FROM lat_item i WHERE i.id < o.id) s ON true`},
		{"the enclosing query writes a star",
			`SELECT * FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.amount < o.total) s ON true`,
			`SELECT * FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT i.amount AS m FROM lat_item i WHERE i.amount < o.total) s ON true`},
		{"the enclosing query writes a QUALIFIED star",
			`SELECT s.* FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT SUM(i.amount) AS m FROM lat_item i WHERE i.amount < o.total) s ON true`,
			`SELECT s.* FROM lat_ord o LEFT JOIN LATERAL (` +
				`SELECT i.amount AS m FROM lat_item i WHERE i.amount < o.total) s ON true`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildLiftedRefPlan(t, tc.agg)
			if err == nil {
				t.Fatalf("an AGGREGATED body under %s built a plan: the decline ran "+
					"before the refusal, so the predicate is dropped and every outer row "+
					"gets the whole relation's aggregate — silently\n  %s", tc.name, tc.agg)
			}
			if !strings.Contains(err.Error(), "AGGREGATES and its correlated predicate") {
				t.Fatalf("refused with %q, want the aggregated-lifted-predicate refusal\n  %s",
					err, tc.agg)
			}
			// The CONTROL is the same trigger with a body that does NOT
			// aggregate. Since arc LT the four declines are REFUSALS of their
			// own (ADR-0021 §1s, `which would have to publish the column it
			// names`), so the control either plans — the enclosing-relation
			// trigger is decided on the ANNOTATED plan, which this builder
			// does not produce — or refuses with the lifted-predicate sentence;
			// what it must never carry is the AGGREGATED sentence, which is the
			// order this test exists to hold.
			if _, err := buildLiftedRefPlan(t, tc.ctl); err != nil {
				if strings.Contains(err.Error(), "AGGREGATES and its correlated predicate") ||
					!strings.Contains(err.Error(), "which would have to publish the column it names") {
					t.Errorf("the NON-aggregated control under %s refused with the wrong sentence: %v\n  %s",
						tc.name, err, tc.ctl)
				}
			}
		})
	}
}

// The same rule read off the SOURCE, so a decline added in the wrong place
// fails even when no cell above happens to reach it: inside
// `publishLiftedRefs`, the only `return nil, nil` allowed before the refusal
// is the `info == nil` guard, and every other one must sit after it.
func TestNoLiftedRefDeclineSitsBeforeTheAggregatedRefusal(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "lateral_correlated_refs.go", nil, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var fn *ast.FuncDecl
	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == "publishLiftedRefs" {
			fn = f
		}
	}
	if fn == nil {
		t.Fatal("publishLiftedRefs is gone — this property has no subject")
	}
	var refusal token.Pos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		ret, ok := n.(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 2 || isNilIdent(ret.Results[1]) {
			return true
		}
		if refusal == token.NoPos || ret.Pos() < refusal {
			refusal = ret.Pos()
		}
		return true
	})
	if refusal == token.NoPos {
		t.Fatal("publishLiftedRefs returns no error at all — the aggregated refusal is gone")
	}
	// A decline before the refusal is legal only as the `info == nil` guard,
	// which is a statement about the ARGUMENT and not about the body.
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		cond := exprText(fset, ifs.Cond)
		for _, st := range ifs.Body.List {
			ret, ok := st.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 2 ||
				!isNilIdent(ret.Results[0]) || !isNilIdent(ret.Results[1]) {
				continue
			}
			if ret.Pos() > refusal || cond == "info == nil" {
				continue
			}
			t.Errorf("`if %s { return nil, nil }` at %s DECLINES before the aggregated "+
				"refusal at %s: an aggregated body matching that condition is answered "+
				"silently instead of refused. Set a flag and fold it into the decline "+
				"arm below the refusal, the way the enclosing-star test does",
				cond, fset.Position(ret.Pos()), fset.Position(refusal))
		}
		return true
	})
}

func isNilIdent(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "nil"
}

func exprText(fset *token.FileSet, e ast.Expr) string {
	var b strings.Builder
	if err := printer.Fprint(&b, fset, e); err != nil {
		return ""
	}
	return b.String()
}

func buildLiftedRefPlan(t *testing.T, sql string) (*Node, error) {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	return BuildFromSelect(info)
}
