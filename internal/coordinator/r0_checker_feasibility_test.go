// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// R0 deliverable 4 (epic #1678, ADR-0051 §4.3): can an EXACT emission checker
// be built on main? It needs, for every column leaf of every emitted stage
// expression, the slot the worker will read AND the slot the logical
// reference in D denotes. This test measures the second half's raw material:
// how many column references D carries and how many of them carry a binding
// (plansql.ColRef.Bound). D is planned unbound, so a binding exists nowhere in
// it; what each of its references denotes is not established from D's
// structure without a resolver (the ruling D2 defers choosing one), and the
// bound plan L is a separate parse whose references could be matched to D's
// only by spelling, which the brief forbids. It also counts the emitted
// stage-expression leaves the other half would have to resolve.
//
// R0_CHECKER_REPORT=1 writes the per-statement counts as r0_checker_report.tsv
// in a fresh directory whose path the test logs (r0WriteArtifact). The kept assertion
// is the measurement that stopped the checker: no D reference carries a
// binding, so "intent not established" covers every leaf.

// r0ColRefCount counts the column references reachable from v, and the bound
// ones.
func r0ColRefCount(v any) (total, bound int) {
	colRef := reflect.TypeOf(&plansql.ColRef{})
	r0WalkValues(v, func(x reflect.Value) {
		if x.Type() == colRef {
			total++
			if x.Interface().(*plansql.ColRef).Bound != nil {
				bound++
			}
		}
	})
	return total, bound
}

// r0EmittedLeaves counts the column leaves of the stage graph's expression
// strings (every string field whose name says it carries an expression or a
// key), parsed with the SQL expression parser; unparsable strings are
// counted apart.
func r0EmittedLeaves(stages []dagplan.Stage) (leaves, unparsable int) {
	exprField := func(name string) bool {
		for _, k := range []string{"Expr", "Filter", "Keys", "GroupBy", "Key", "Partition", "Order"} {
			if strings.Contains(name, k) {
				return true
			}
		}
		return false
	}
	var walk func(x reflect.Value, field string)
	walk = func(x reflect.Value, field string) {
		switch x.Kind() {
		case reflect.String:
			if field == "" || !exprField(field) || x.String() == "" {
				return
			}
			n, err := plansql.ParseExpression(x.String())
			if err != nil {
				unparsable++
				return
			}
			plansql.WalkColRefs(n, func(*plansql.ColRef) { leaves++ })
		case reflect.Pointer, reflect.Interface:
			if !x.IsNil() {
				walk(x.Elem(), field)
			}
		case reflect.Struct:
			for i := 0; i < x.NumField(); i++ {
				if !x.Type().Field(i).IsExported() {
					continue
				}
				walk(x.Field(i), x.Type().Field(i).Name)
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < x.Len(); i++ {
				walk(x.Index(i), field)
			}
		case reflect.Map:
			iter := x.MapRange()
			for iter.Next() {
				walk(iter.Value(), field)
			}
		}
	}
	for _, s := range stages {
		walk(reflect.ValueOf(s), "")
	}
	return leaves, unparsable
}

func TestR0CheckerFeasibility(t *testing.T) {
	r0RequireWriterUsage(t, "R0_CHECKER_REPORT")
	if testing.Short() {
		t.Skip("-short: stands clusters up")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	var lines []string
	var dTotal, dBound, emitted, unparsable, planned int
	record := func(set, name string, rec *r0LocalDRecord) {
		if rec.Frozen == nil {
			lines = append(lines, fmt.Sprintf("checker\t%s\t%s\tnot planned (%s)", set, name, rec.Status))
			return
		}
		planned++
		tot, b := r0ColRefCount(rec.Frozen)
		e, u := r0EmittedLeaves(rec.Stages)
		dTotal, dBound, emitted, unparsable = dTotal+tot, dBound+b, emitted+e, unparsable+u
		lines = append(lines, fmt.Sprintf("checker\t%s\t%s\tD_refs=%d D_bound=%d emitted_leaves=%d unparsable_exprs=%d exact=0 intent_not_established=%d reason=D-unbound status=%s",
			set, name, tot, b, e, u, tot-b, rec.Status))
	}
	cells := r0ReadCells(t)
	for _, fx := range []string{"own", "uniq", "dup"} {
		c := r0Cluster(t, ctx, fx, nil)
		for _, cell := range cells {
			if cell.fixtures[0] != fx {
				continue
			}
			_, rec, _ := r0LocalD(c, ctx, cell.sql)
			record("corpus/"+fx, cell.name, rec)
		}
	}
	_, tc := setupTPCHDistributed(t)
	var qs []int
	for n := range tpch.TPCHQueries {
		qs = append(qs, n)
	}
	sort.Ints(qs)
	for _, n := range qs {
		_, rec, _ := r0LocalD(tc, ctx, tpch.TPCHQueries[n].SQL)
		record("tpch", fmt.Sprintf("Q%02d", n), rec)
	}
	summary := fmt.Sprintf("checker summary: %d statements planned; D column references %d, bound %d; emitted stage-expression leaves %d (%d expression strings unparsable); leaves checked exact 0; intent not established %d (reason: D unbound)",
		planned, dTotal, dBound, emitted, unparsable, dTotal-dBound)
	t.Log(summary)
	r0WriteArtifact(t, "R0_CHECKER_REPORT", "r0_checker_report.tsv", summary+"\n"+strings.Join(lines, "\n")+"\n")
	if dBound != 0 {
		t.Errorf("%d references of D carry a binding: the checker's intent side may now be buildable (re-measure deliverable 4)", dBound)
	}
	if dTotal == 0 {
		t.Error("no D reference counted: the measurement is vacuous")
	}
}
