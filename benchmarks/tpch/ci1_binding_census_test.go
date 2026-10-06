// SPDX-License-Identifier: MIT

package tpch

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// TestCI1BindingCensusTPCH is the binder's stamp counted over the 22 TPC-H
// queries at SF0.01 on the single-process engine (ADR-0047 stage 1, RISKS M1 /
// M3 / M5): every block the binder validates is bound IN FULL (none mixed,
// none declined), no group-key match site compares a bound term with an
// unbound one, and every relation instance's column list is the list the
// built plan publishes for it. The answers themselves are TestTPCHQueries'.
func TestCI1BindingCensusTPCH(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()
	var nums []int
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	var reps []physical.BlockBindingReport
	var viol []string
	physical.BindingProbe = func(r physical.BlockBindingReport) { reps = append(reps, r) }
	plansql.MatchProbe = func(site string, n plansql.Node, keys []string) {
		tb, tu, kb := false, false, false
		plansql.WalkColRefs(n, func(r *plansql.ColRef) {
			if r.Slot || strings.HasPrefix(r.Column, "__") {
				return
			}
			if r.Bound != nil {
				tb = true
			} else {
				tu = true
			}
		})
		for _, k := range keys {
			kb = kb || strings.Contains(k, "\x00")
		}
		if tb && tu || tu && kb || tb && len(keys) > 0 && !kb {
			viol = append(viol, site+": "+n.String())
		}
	}
	t.Cleanup(func() { physical.BindingProbe = nil; plansql.MatchProbe = nil })
	var blocks, stamped, compared int
	for _, n := range nums {
		q := fmt.Sprintf("Q%02d", n)
		reps, viol = nil, nil
		if _, err := db.Query(ctx, TPCHQueries[n].SQL); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		for _, r := range reps {
			blocks++
			if r.Stamped {
				stamped++
			}
			if r.Mixed || r.Declined {
				t.Errorf("M1 %s: a block not bound in full (%s): %s", q, strings.Join(r.Unbound, ", "), r.Block)
			}
		}
		for _, v := range viol {
			t.Errorf("M3 %s: %s", q, v)
		}
		c, bad := ordinalCensus(ctx, t, db.Catalog(), TPCHQueries[n].SQL)
		compared += c
		for _, b := range bad {
			t.Errorf("M5 %s: %s", q, b)
		}
	}
	t.Logf("M1: %d blocks, %d stamped, 0 mixed; M3: 0 violations; M5: %d instances compared, 0 disagreements", blocks, stamped, compared)
	if stamped == 0 || compared == 0 {
		t.Fatalf("the census is vacuous: %d stamped blocks, %d compared instances", stamped, compared)
	}
}

// ordinalCensus binds, builds and annotates sql and compares every closed
// relation instance the binder registered with the list the plan publishes
// under the same qualifier (RISKS M5). A qualifier two instances of the
// statement share with different lists, or one the plan's walk does not reach
// (an expression subquery's body is planned on its own), is not compared.
func ordinalCensus(ctx context.Context, t *testing.T, cat *catalog.Catalog, sql string) (int, []string) {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return 0, nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return 0, nil
	}
	var reps []physical.BlockBindingReport
	saved := physical.BindingProbe
	physical.BindingProbe = func(r physical.BlockBindingReport) { reps = append(reps, r) }
	defer func() { physical.BindingProbe = saved }()
	if err := physical.BindColumnsUnderPolicy(ctx, cat, info, nil, nil); err != nil {
		return 0, nil
	}
	plan, err := logical.BuildFromSelect(info)
	if err != nil {
		return 0, nil
	}
	physical.NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	roots := []*logical.Node{plan}
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.DerivedAlias != "" || n.CTEName != "" {
			roots = append(roots, n.Children...)
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(plan)
	lists := map[string]map[string]bool{}
	for _, r := range reps {
		for _, in := range r.Instances {
			q := strings.ToLower(in.Qual)
			if lists[q] == nil {
				lists[q] = map[string]bool{}
			}
			lists[q][strings.ToLower(strings.Join(in.Cols, ","))] = true
		}
	}
	n := 0
	var bad []string
	for _, r := range reps {
		for _, in := range r.Instances {
			if in.Open || len(lists[strings.ToLower(in.Qual)]) > 1 {
				continue
			}
			var sc []logical.StarColumn
			for _, root := range roots {
				if sc = logical.StarSourceColumns(root, in.Qual); sc != nil {
					break
				}
			}
			if sc == nil {
				continue
			}
			got := make([]string, len(sc))
			for i, c := range sc {
				got[i] = c.Resolve
			}
			n++
			if !strings.EqualFold(strings.Join(got, ","), strings.Join(in.Cols, ",")) {
				bad = append(bad, in.Qual+": binder "+strings.Join(in.Cols, ",")+" plan "+strings.Join(got, ","))
			}
		}
	}
	return n, bad
}
