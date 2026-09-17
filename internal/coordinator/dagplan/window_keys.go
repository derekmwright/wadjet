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
