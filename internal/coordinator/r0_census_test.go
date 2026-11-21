// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// R0's binding census (epic #1678, ADR-0051 §2.6, the maintainer's ruling D2:
// "establish what bindings and per-occurrence identities are actually
// required and whether the existing binder can provide them"). MEASURED, not
// argued: for every statement of the corpus and TPC-H, the bound validation
// the coordinator runs first (the veto, which is L's binding) is taken, and
// every column reference reachable from its SELECT is classified by the
// clause it sits in and whether the binder stamped it (plansql.ColRef.Bound),
// with the identity it was given. D, the stage planner's plan, carries no
// stamp at all (r0LocalDRecord.BoundRefs, asserted 0 on every corpus cell).
//
// R0_CENSUS=<path> writes every reference (statement, clause path, spelling,
// binding). The kept assertion is the census's headline, so it moves when the
// binder's scope moves (stage W stamps WHERE and ON): no reference under a
// WHERE or a join condition carries a stamp today, and SELECT-list references
// do.

type r0CensusRef struct {
	clause  string
	text    string
	binding string
}

// r0CensusRefs walks a bound SELECT and returns every column reference with
// the clause path that reaches it: the chain of SelectInfo / JoinInfo / CTEDef
// / TableRef / UnionInfo fields from the root.
func r0CensusRefs(root *plansql.SelectInfo) []r0CensusRef {
	var out []r0CensusRef
	seen := map[uintptr]bool{}
	owners := map[reflect.Type]string{
		reflect.TypeOf(plansql.SelectInfo{}): "select",
		reflect.TypeOf(plansql.JoinInfo{}):   "join",
		reflect.TypeOf(plansql.CTEDef{}):     "cte",
		reflect.TypeOf(plansql.TableRef{}):   "from",
		reflect.TypeOf(plansql.UnionInfo{}):  "union",
	}
	colRef := reflect.TypeOf(&plansql.ColRef{})
	var walk func(x reflect.Value, path string)
	walk = func(x reflect.Value, path string) {
		switch x.Kind() {
		case reflect.Pointer:
			if x.IsNil() || seen[x.Pointer()] && x.Type() != colRef {
				return
			}
			if x.Type() == colRef {
				cr := x.Interface().(*plansql.ColRef)
				b := "-"
				if cr.Bound != nil {
					b = fmt.Sprintf("rel=%d ord=%d level=%d output=%v", cr.Bound.Rel, cr.Bound.Ord, cr.Bound.Level, cr.Bound.Output)
				}
				if !seen[x.Pointer()] {
					out = append(out, r0CensusRef{path, cr.String(), b})
				}
				seen[x.Pointer()] = true
				return
			}
			seen[x.Pointer()] = true
			walk(r0Open(x.Elem()), path)
		case reflect.Struct:
			owner, isOwner := owners[x.Type()]
			for i := 0; i < x.NumField(); i++ {
				p := path
				if isOwner {
					p = path + "/" + owner + "." + x.Type().Field(i).Name
				}
				walk(r0Open(x.Field(i)), p)
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < x.Len(); i++ {
				walk(r0Open(x.Index(i)), path)
			}
		case reflect.Map:
			iter := x.MapRange()
			for iter.Next() {
				e := reflect.New(x.Type().Elem()).Elem()
				e.Set(iter.Value())
				walk(e, path)
			}
		case reflect.Interface:
			if x.IsNil() {
				return
			}
			inner := reflect.New(x.Elem().Type()).Elem()
			inner.Set(x.Elem())
			walk(inner, path)
		}
	}
	v := reflect.New(reflect.TypeOf(root)).Elem()
	v.Set(reflect.ValueOf(root))
	walk(v, "")
	return out
}

// r0CensusClause names a reference's clause: the LAST owner field on its
// path (the innermost block's clause), with the nesting depth.
func r0CensusClause(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	depth := 0
	for _, p := range parts {
		if strings.HasPrefix(p, "from.") || strings.HasPrefix(p, "cte.") {
			depth++
		}
	}
	last := parts[len(parts)-1]
	switch {
	case strings.HasPrefix(last, "select.Columns"):
		last = "SELECT list"
	case strings.HasPrefix(last, "select.WhereExpr") || strings.HasPrefix(last, "select.Where"):
		last = "WHERE"
	case strings.HasPrefix(last, "join."):
		last = "JOIN ON/USING (" + strings.TrimPrefix(last, "join.") + ")"
	case strings.HasPrefix(last, "select.GroupBy"):
		last = "GROUP BY"
	case strings.HasPrefix(last, "select.Having"):
		last = "HAVING"
	case strings.HasPrefix(last, "select.Qualify"):
		last = "QUALIFY"
	case strings.HasPrefix(last, "select.OrderBy"):
		last = "ORDER BY"
	case strings.HasPrefix(last, "select.Windows"):
		last = "WINDOW spec"
	}
	return fmt.Sprintf("%s@depth%d", last, depth)
}

func TestR0BindingCensus(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands clusters up")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	type stmt struct{ set, name, sql string }
	coords := map[string]*Coordinator{}
	var stmts []stmt
	for _, fx := range []string{"own", "uniq", "dup"} {
		coords[fx] = r0Cluster(t, ctx, fx, nil)
	}
	for _, cell := range r0ReadCells(t) {
		stmts = append(stmts, stmt{cell.fixtures[0], cell.name, cell.sql})
	}
	// Census probes for identities the corpus does not exercise in a stamped
	// clause (not corpus rows: no verdict is pinned for them).
	probes := map[string]string{
		"probe_using_merged":    "SELECT id, p.v, s.v FROM tb_d p JOIN tb_bd s USING (id)",
		"probe_natural_merged":  "SELECT id FROM tb_d p NATURAL JOIN (SELECT id FROM tb_bd) s",
		"probe_cte_two_reads":   "WITH s AS (SELECT id, v AS r FROM tb_d) SELECT a.r, b.r FROM s a JOIN s b ON a.id = b.id",
		"probe_cte_two_depths":  "WITH s AS (SELECT id, v AS r FROM tb_bd) SELECT p.r, q.r FROM s p JOIN (SELECT a.id, a.r FROM s a) q ON p.id = q.id",
		"probe_outer_padded":    "SELECT p.v, s.r FROM tb_d p LEFT JOIN (SELECT id, coalesce(v,0)+1 AS r FROM tb_bd) s ON p.id = s.id",
		"probe_setop_arms":      "SELECT r + 100 FROM (SELECT v AS r FROM tb_d UNION SELECT v FROM tb_bd) s",
		"probe_scalar_select":   "SELECT p.id, (SELECT max(b.v) FROM tb_bd b WHERE b.id = p.id) FROM tb_d p",
		"probe_exists_hidden":   "SELECT p.id FROM tb_d p WHERE EXISTS (SELECT 1 FROM tb_bd s WHERE s.id = p.id)",
		"probe_group_order_out": "SELECT id AS k, count(*) FROM tb_d GROUP BY id ORDER BY k",
	}
	var pnames []string
	for n := range probes {
		pnames = append(pnames, n)
	}
	sort.Strings(pnames)
	for _, n := range pnames {
		stmts = append(stmts, stmt{"own", n, probes[n]})
	}
	byClause := map[string][2]int{} // clause -> {total, bound}
	var lines []string
	for _, s := range stmts {
		c := coords[s.set]
		parsed, err := plansql.Parse(s.sql)
		if err != nil {
			lines = append(lines, fmt.Sprintf("census\t%s\t%s\tparse refused: %v", s.set, s.name, err))
			continue
		}
		cctx, _ := c.statementAuth(ctx)
		cctx = statementGuard.WithIncarnationGuard(cctx, c.catalog)
		veto := c.validateBound(cctx, parsed)
		if veto.outcome != vetoAccepted {
			lines = append(lines, fmt.Sprintf("census\t%s\t%s\tveto %s: %v", s.set, s.name, veto.outcome, veto.err))
			continue
		}
		for _, r := range r0CensusRefs(veto.info) {
			cl := r0CensusClause(r.clause)
			n := byClause[cl]
			n[0]++
			if r.binding != "-" {
				n[1]++
			}
			byClause[cl] = n
			lines = append(lines, fmt.Sprintf("census\t%s\t%s\t%s\t%s\t%s\t%s", s.set, s.name, cl, r.text, r.binding, r.clause))
		}
	}
	// TPC-H on its own cluster.
	_, tc := setupTPCHDistributed(t)
	var qs []int
	for n := range tpch.TPCHQueries {
		qs = append(qs, n)
	}
	sort.Ints(qs)
	for _, n := range qs {
		parsed, err := plansql.Parse(tpch.TPCHQueries[n].SQL)
		if err != nil {
			t.Fatal(err)
		}
		cctx, _ := tc.statementAuth(ctx)
		cctx = statementGuard.WithIncarnationGuard(cctx, tc.catalog)
		veto := tc.validateBound(cctx, parsed)
		if veto.outcome != vetoAccepted {
			lines = append(lines, fmt.Sprintf("census\ttpch\tQ%02d\tveto %s: %v", n, veto.outcome, veto.err))
			continue
		}
		for _, r := range r0CensusRefs(veto.info) {
			cl := r0CensusClause(r.clause)
			k := "tpch:" + cl
			c := byClause[k]
			c[0]++
			if r.binding != "-" {
				c[1]++
			}
			byClause[k] = c
			lines = append(lines, fmt.Sprintf("census\ttpch\tQ%02d\t%s\t%s\t%s\t%s", n, cl, r.text, r.binding, r.clause))
		}
	}
	var summary []string
	for k, v := range byClause {
		summary = append(summary, fmt.Sprintf("%s: %d refs, %d stamped", k, v[0], v[1]))
	}
	sort.Strings(summary)
	t.Logf("census by clause:\n  %s", strings.Join(summary, "\n  "))
	if p := os.Getenv("R0_CENSUS"); p != "" {
		r0AppendFile(t, p, strings.Join(append(summary, lines...), "\n")+"\n")
	}
	// The headline, kept: nothing under WHERE or a join condition is stamped
	// (the binder's scope, physical/validate_binding.go), and the SELECT list
	// is. Stage W moves the first: it re-pins this assertion.
	var whereOn, selectBound int
	for k, v := range byClause {
		if strings.Contains(k, "WHERE@") || strings.Contains(k, "JOIN ON/USING") {
			whereOn += v[1]
		}
		if strings.Contains(k, "SELECT list@") {
			selectBound += v[1]
		}
	}
	if whereOn != 0 {
		t.Errorf("%d references under WHERE or a join condition carry a binder stamp: the binder's scope moved (re-pin with stage W)", whereOn)
	}
	if selectBound == 0 {
		t.Errorf("no SELECT-list reference carries a binder stamp: the census is vacuous")
	}
}
