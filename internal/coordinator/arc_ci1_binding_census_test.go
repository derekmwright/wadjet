// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// THE BINDER'S STAMP, COUNTED (ADR-0047 stage 1, RISKS M1 / M3 / M5) over the
// group-key table — every cell of TestArcGKGroupKeySpellingEveryArm, on the
// single-process engine, which is the door that stamps:
//
//   - M1: no block is MIXED — bound in part, with the rest unbindable by its
//     scope (an unenumerable source, a star output, a field path, an
//     ambiguous or unresolved name). A mixed block would be matched by
//     spelling while its neighbours match by binding.
//   - M3: no match site compares a term of a bound block whose leaves lost
//     the binding (a carrier that re-parsed text), nor an unbound term with
//     bound keys, nor a bound term with keys that lost theirs.
//   - M5: every relation instance's column list is the list the built plan
//     publishes for it (logical.StarSourceColumns): the ordinal in a binding
//     names the column the plan carries at that position.
func TestArcCI1BindingCensusOverTheGroupKeyTable(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the binding census runs the group-key table")
	}
	cells := append(gkCells(), gkMoreCells(t)...)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	db := ssStandalone(t, ctx, 0)
	var reps []physical.BlockBindingReport
	var viol []string
	physical.BindingProbe = func(r physical.BlockBindingReport) { reps = append(reps, r) }
	plansql.MatchProbe = func(site string, n plansql.Node, keys []string) {
		if v := ci1MatchViolation(n, keys); v != "" {
			viol = append(viol, v+" at "+site+": "+n.String())
		}
	}
	t.Cleanup(func() { physical.BindingProbe = nil; plansql.MatchProbe = nil })
	var blocks, stamped, declined, compared int
	for _, tc := range cells {
		reps, viol = nil, nil
		_, _ = ssRunSingle(ctx, db, tc.sql, tc.ordered)
		for _, r := range reps {
			blocks++
			if r.Stamped {
				stamped++
			}
			if r.Declined {
				declined++
			}
			if r.Mixed {
				t.Errorf("M1 %s: a MIXED block (%s): %s", tc.name, strings.Join(r.Unbound, ", "), r.Block)
			}
		}
		for _, v := range viol {
			t.Errorf("M3 %s: %s", tc.name, v)
		}
		// M5. A LATERAL body's scans carry the body's alias in the logical
		// plan (setSubtreeAlias), so the plan's lookup by name answers the
		// LATERAL relation for them — a naming of the plan's, not of the
		// binding (stage 4); those cells are left out.
		if strings.Contains(strings.ToUpper(tc.sql), "LATERAL") {
			continue
		}
		n, bad := ci1OrdinalCensus(ctx, t, db.Catalog(), tc.sql)
		compared += n
		for _, b := range bad {
			t.Errorf("M5 %s: %s", tc.name, b)
		}
	}
	t.Logf("M1: %d blocks, %d stamped, %d declined (a dotted name or an unfolded FROM-less subquery), 0 mixed; M3: 0 violations; M5: %d instances compared, 0 disagreements",
		blocks, stamped, declined, compared)
	if stamped == 0 || compared == 0 {
		t.Fatalf("the census is vacuous: %d stamped blocks, %d compared instances", stamped, compared)
	}
}

// ci1MatchViolation is RISKS M3's rule for one comparison: the term's own
// leaves are all bound or all unbound, and the keys are on the same side.
// A planner-minted slot (`__agg_0`) is not a reference the query wrote.
func ci1MatchViolation(term plansql.Node, keys []string) string {
	tb, tu := false, false
	plansql.WalkColRefs(term, func(r *plansql.ColRef) {
		if r.Slot || strings.HasPrefix(r.Column, "__") {
			return
		}
		if r.Bound != nil {
			tb = true
		} else {
			tu = true
		}
	})
	kb := false
	for _, k := range keys {
		if strings.Contains(k, "\x00") {
			kb = true
		}
	}
	switch {
	case tb && tu:
		return "a term with bound and unbound leaves"
	case tu && kb:
		return "an unbound term against bound keys"
	case tb && len(keys) > 0 && !kb:
		return "a bound term against keys with no binding"
	}
	return ""
}

// ci1OrdinalCensus binds, builds and annotates sql and compares every closed
// relation instance the binder registered with the list the plan publishes
// under the same qualifier (RISKS M5). A qualifier two instances of the
// statement share with different lists, or one the plan's walk does not reach
// (an expression subquery's body is planned on its own), is not compared.
func ci1OrdinalCensus(ctx context.Context, t *testing.T, cat *catalog.Catalog, sql string) (int, []string) {
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
