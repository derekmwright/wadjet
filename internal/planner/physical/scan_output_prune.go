package physical

import "os"

// scanOutputPrune gates pruneScanOutputColumns.
// Kill switch WADJET_SCAN_OUTPUT_PRUNE=0.
var scanOutputPrune = os.Getenv("WADJET_SCAN_OUTPUT_PRUNE") != "0"

// pruneScanOutputColumns narrows each dispatched scan stage's OUTPUT to
// the union of what its consumers declare, leaving Columns (the read
// set) untouched so pushed filters still see their columns.
//
// The leak this closes (2026-07-28, Q13): a scan-consumed filter column
// (o_comment from the LEFT JOIN ON clause, pushed to the scan) stayed in
// the scan's materialized output AND rode the downstream repartition —
// 68.8 B/row shipped where 16 were consumed, twice, ≈16 GB excess I/O
// per SF100 run on the single worst Trino-gap query. The exchange's
// Columns projection was correct at plan time; the execution layer
// shipped the scan's full read set because WSHF inputs carry no
// projection.
//
// v1 eligibility is deliberately narrow — exactly the leaking shape:
//   - the stage is a dispatched leaf scan (StageScan with ScanFiles);
//   - EVERY consumer is an exchange-repartition with a non-empty
//     Columns declaration and no computed-column machinery
//     (ComputedCols/ExtraReadCols reads ride the parquet pass-through
//     path, not stage outputs, and carry their own widening rules);
//   - the union of consumer Columns (plus every consumer's Exchange
//     keys, defensively) is a strict subset of the scan's read set.
// Anything else keeps the full output — correct, just wider.
func pruneScanOutputColumns(stages []Stage) {
	if !scanOutputPrune {
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
