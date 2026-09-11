package physical

import "github.com/derekmwright/wadjet/internal/optswitch"

// scanOutputPrune gates pruneScanOutputColumns.
// Kill switch WADJET_SCAN_OUTPUT_PRUNE=0.
var scanOutputPrune = optswitch.Register("scan-output-prune", "WADJET_SCAN_OUTPUT_PRUNE",
	"narrow dispatched scan-stage output to the union of consumer-declared columns")

// pruneScanOutputColumns narrows scan OUTPUT to consumers' needs while leaving
// Columns, the read set, untouched for pushed filters. Require a dispatched leaf
// StageScan with ScanFiles and EVERY consumer an exchange-repartition declaring
// non-empty Columns with no ComputedCols/ExtraReadCols machinery.
// Consumer Columns plus all Exchange keys must form a strict subset of the read
// set. Computed/extra reads have separate parquet passthrough widening rules.
// Anything outside this boundary keeps the full output.
func pruneScanOutputColumns(stages []Stage) {
	if !scanOutputPrune.On() {
		return
	}
	consumers := make(map[string][]*Stage)
	for i := range stages {
		for _, dep := range stages[i].Dependencies {
			consumers[dep] = append(consumers[dep], &stages[i])
		}
	}
	for i := range stages {
		s := &stages[i]
		if s.Type != StageScan || len(s.ScanFiles) == 0 || len(s.Columns) == 0 {
			continue
		}
		// A projection-carrying scan (absorbComputedSubqueryProjection,
		// #383) emits columns that are not in its read set — the computed
		// aliases — and OpColumnPrune runs AFTER OpProject, so a prune
		// derived from the read set would drop exactly them.
		if len(s.ProjectExprs) > 0 {
			continue
		}
		cons := consumers[s.ID]
		if len(cons) == 0 {
			continue
		}
		keep := map[string]bool{}
		eligible := true
		for _, c := range cons {
			if c.Type != StageExchangeRepartition || len(c.Columns) == 0 ||
				c.Exchange == nil || len(c.Exchange.ComputedCols) > 0 || len(c.Exchange.ExtraReadCols) > 0 {
				eligible = false
				break
			}
			for _, col := range c.Columns {
				keep[col] = true
			}
			for _, k := range c.Exchange.Keys {
				keep[k] = true
			}
		}
		if !eligible {
			continue
		}
		// Strict subset of the read set? (Consumer unions include sibling-
		// table columns — the "reader ignores nonexistent" convention — so
		// intersect with the scan's own read set.)
		var out []string
		for _, col := range s.Columns {
			if keep[col] {
				out = append(out, col)
			}
		}
		if len(out) == 0 || len(out) >= len(s.Columns) {
			continue
		}
		s.OutputColumns = out
	}
}
