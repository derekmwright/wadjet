// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// windowKeyColPrefix names a materialized window key. The "__" marks it
// derived, the same convention as __sortkey_N and __gb_expr_N.
const windowKeyColPrefix = string(plansql.SlotWindowKey)

// validateWindowKeyExprs checks that every materialized key a window stage
// names is one its fragment can compute. See the StageWindow case in
// native_dag_rewrite.go for why this is a plan-time check.
//
// The question is PROVENANCE, not spelling. `physical.PlanContext.ResolveWindowKeys` classes a
// PARTITION BY / ORDER BY term as either a BOUND reference — a column the
// input already carries — or a MATERIALIZED expression the fragment computes
// into a `__winkey_N` slot, and only the second owes a `WindowKeyExprs`
// entry. Reading the NAME alone cannot tell them apart, and a table written
// before the namespace was reserved really can STORE a column called
// `__winkey_1`: `PARTITION BY __winkey_1` over it is a bound reference the
// single-process path answers and both DAG arms refused (#745). So the
// producing stage's own stream is asked first — a name it emits is a column
// the window READS, whoever spelled it — and the refusal is kept for a name
// nothing computes and nothing supplies, which is the shape #585 filed.
func validateWindowKeyExprs(stages []Stage, idx map[string]int, s Stage) error {
	if len(s.WindowCols) == 0 {
		return nil
	}
	computed := make(map[string]bool, len(s.WindowKeyExprs))
	for _, k := range s.WindowKeyExprs {
		computed[strings.ToLower(k.Name)] = true
	}
	supplied := map[string]string{}
	if len(s.Dependencies) == 1 {
		if di, ok := idx[s.Dependencies[0]]; ok {
			supplied = emittedThroughPassThrough(stages, idx, &stages[di])
		}
	}
	check := func(name string) error {
		lc := strings.ToLower(name)
		if !strings.HasPrefix(lc, windowKeyColPrefix) || computed[lc] {
			return nil
		}
		if _, bound := supplied[lc]; bound {
			return nil
		}
		return fmt.Errorf("native-DAG: window stage %s keys on %q, which nothing computes "+
			"(WindowKeyExprs carries %d entries)", s.ID, name, len(s.WindowKeyExprs))
	}
	for _, wc := range s.WindowCols {
		if err := check(wc.InputCol); err != nil {
			return err
		}
		for _, pb := range wc.PartitionBy {
			if err := check(pb); err != nil {
				return err
			}
		}
		for _, ob := range wc.OrderBy {
			if err := check(ob.Column); err != nil {
				return err
			}
		}
	}
	return nil
}

// ownWindowKeyNames renames every key a window stage computes
// (Stage.WindowKeyExprs, `__winkey_N`) to a name only this stage mints,
// `__winkey_s<stage>_N`, and every reference the stage's window columns make
// to it.
//
// physical.PlanContext.ResolveWindowKeys numbers a window's keys from zero
// over the names its LOGICAL input carries, and the computed keys of a window
// BELOW are not among them: they ride the stage DAG's stream, where the
// window fragment appends them. Two stacked window stages therefore both
// minted `__winkey_0`, and the outer stage's key projection read the inner
// stage's column — an outer `LAG(x.b, 1, 7)` over an inner `LAG(b, 1, 0)`
// filled its first row with the inner default 0 where PostgreSQL answers 7.
func ownWindowKeyNames(stage *Stage) {
	if len(stage.WindowKeyExprs) == 0 {
		return
	}
	id := stage.ID
	if i := strings.LastIndexByte(id, '-'); i >= 0 {
		id = id[i+1:]
	}
	m := make(map[string]string, len(stage.WindowKeyExprs))
	for i := range stage.WindowKeyExprs {
		name := stage.WindowKeyExprs[i].Name
		lc := strings.ToLower(name)
		if !strings.HasPrefix(lc, windowKeyColPrefix) {
			continue
		}
		own := fmt.Sprintf("%ss%s_%s", windowKeyColPrefix, id, lc[len(windowKeyColPrefix):])
		m[lc] = own
		stage.WindowKeyExprs[i].Name = own
	}
	rename := func(s string) string {
		if n, ok := m[strings.ToLower(s)]; ok {
			return n
		}
		return s
	}
	for i := range stage.WindowCols {
		wc := &stage.WindowCols[i]
		wc.InputCol = rename(wc.InputCol)
		wc.LagLeadDefaultCol = rename(wc.LagLeadDefaultCol)
		for j := range wc.PartitionBy {
			wc.PartitionBy[j] = rename(wc.PartitionBy[j])
		}
		for j := range wc.OrderBy {
			wc.OrderBy[j].Column = rename(wc.OrderBy[j].Column)
		}
	}
}
