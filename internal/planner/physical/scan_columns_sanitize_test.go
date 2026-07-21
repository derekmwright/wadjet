package physical

import (
	"strings"
	"testing"
)

// TestScanColumnsSanitized is the plan-level regression test for memo
// exchange-reuse.md §2 A1: scan stages must never carry alias-qualified
// or other-table column names — one such name trips the worker's
// all-or-nothing parquet projection guard and silently reverts the scan
// (and every shuffle it feeds) to full width. Before the
// sanitizeScanNeeds fix, Q21's l1 scan carried "l1.l_receiptdate" and
// "s_suppkey" (143 B/row shuffled vs 25 B/row on the clean sibling leg).
func TestScanColumnsSanitized(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	for name, sql := range tpchPlanQueries {
		t.Run(name, func(t *testing.T) {
			for _, s := range sqlToStages(t, cat, ctx, sql, 3) {
				if s.Type != "scan" || s.TableName == "" {
					continue
				}
				schema, ok := tpchSchemas[s.TableName]
				if !ok {
					continue
				}
				valid := make(map[string]bool, len(schema.Columns))
				for _, c := range schema.Columns {
					valid[strings.ToLower(c.Name)] = true
				}
				for _, col := range s.Columns {
					if strings.Contains(col, ".") {
						t.Errorf("stage %s (%s): alias-qualified column %q survived sanitization", s.ID, s.TableName, col)
						continue
					}
					if !valid[strings.ToLower(col)] && !strings.HasPrefix(col, "__") {
						t.Errorf("stage %s (%s): column %q is not in the table schema (other-table pollution)", s.ID, s.TableName, col)
					}
				}
			}
		})
	}
}
