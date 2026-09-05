package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestWindowKeyGuardStillRefusesWhatNothingSupplies is the REFUSING side of
// #745's narrowing, and it exists because a narrowing is a claim with two
// sides and only one of them had a fixture.
//
// `validateWindowKeyExprs` used to read the NAME: a key spelled `__winkey_N`
// with no `WindowKeyExprs` entry was refused, full stop. That refused a BOUND
// reference — a table written before the namespace was reserved can store a
// column of that name — which is #745. The repair asks the producing stage
// what it emits before refusing.
//
// The refusal it must keep is #585's own: a key the fragment is supposed to
// COMPUTE, that no `WindowKeyExprs` entry computes and no input supplies.
// Shipping that stage produces a window that refuses its own key at the
// worker, three dispatch attempts later, which is the whole reason the check
// is at plan time.
func TestWindowKeyGuardStillRefusesWhatNothingSupplies(t *testing.T) {
	const win = "window-1"
	scan := Stage{ID: "scan-0", Type: StageScan, Columns: []string{"id", "plain"},
		ScanSchema: tmScanSchema("id", "plain")}

	for _, tc := range []struct {
		name    string
		key     string
		exprs   []ProjectExprSpec
		scanHas []string
		refuses bool
	}{
		{
			// #585: the fragment is told to key on a slot it was never given
			// the expression for, and nothing upstream carries the name.
			name: "a materialized slot nothing computes and nothing supplies",
			key:  "__winkey_1", scanHas: []string{"id", "plain"}, refuses: true,
		},
		{
			// The same stage WITH the expression: this is the ordinary
			// materialized key and it must plan.
			name:    "the same slot with the expression that computes it",
			key:     "__winkey_1",
			exprs:   []ProjectExprSpec{{Name: "__winkey_1", Expr: "id % 2"}},
			scanHas: []string{"id", "plain"},
		},
		{
			// #745: the input really carries a column of that name, so the
			// key is a BOUND reference and there is nothing to compute.
			name:    "a STORED column that happens to be spelled like a slot",
			key:     "__winkey_1",
			scanHas: []string{"id", "plain", "__winkey_1"},
		},
		{
			// An ordinary name is not in the reserved family at all and was
			// never the guard's business.
			name: "an ordinary column name", key: "plain", scanHas: []string{"id", "plain"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := scan
			s.Columns = tc.scanHas
			s.ScanSchema = tmScanSchema(tc.scanHas...)
			stages := []Stage{s, {
				ID: win, Type: StageWindow, Dependencies: []string{s.ID},
				WindowCols:     []WindowColSpec{{Func: "sum", InputCol: "id", OutputCol: "w", PartitionBy: []string{tc.key}}},
				WindowKeyExprs: tc.exprs,
			}}
			idx := map[string]int{stages[0].ID: 0, stages[1].ID: 1}
			err := validateWindowKeyExprs(stages, idx, stages[1])
			if tc.refuses {
				if err == nil {
					t.Fatalf("key %q: planned, want a refusal — a window that keys on a slot "+
						"nothing computes fails at the worker instead (#585)", tc.key)
				}
				if !strings.Contains(err.Error(), "which nothing computes") {
					t.Fatalf("key %q: refused with %v, want the #585 message", tc.key, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("key %q: refused with %v, want it planned", tc.key, err)
			}
		})
	}
}

// tmScanSchema is the declared schema of a scan carrying these columns.
// stageEmittedColumns intersects a scan's READ SET with its declared schema —
// a name the table does not have is not a column the fragment can ship — so a
// fixture that sets only Columns tests nothing.
func tmScanSchema(names ...string) []parquet.Column {
	out := make([]parquet.Column, len(names))
	for i, n := range names {
		out[i] = parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true}
	}
	return out
}
