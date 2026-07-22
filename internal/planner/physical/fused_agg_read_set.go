package physical

// pruneFusedAggOutputCols removes aggregate OUTPUT column names from a fused
// scan-aggregate's read set. RequiredColumns accumulates ancestor needs, and
// for the fused shape those include the aggregate outputs (__having_0,
// __agg_N) that the scan fragment PRODUCES rather than reads; any name
// missing from the parquet schema trips the worker's all-or-nothing
// projection guard into a full-width read.
//
// Conservative toward keeping: an output name survives when it is also
// needed as an INPUT — a group-by key, an aggregate's bare input column, an
// identifier inside a derived input expression, or referenced by a
// scan-pushed filter (SUM(x) AS x must still read x).
func pruneFusedAggOutputCols(cols, groupBy []string, specs []AggSpec, filterExprs []string) []string {
	outputs := make(map[string]bool, len(specs))
	for _, s := range specs {
		if s.OutputCol != "" {
			outputs[s.OutputCol] = true
		}
	}
	if len(outputs) == 0 {
		return cols
	}
	// Anything read stays, even if it collides with an output name.
	for _, g := range groupBy {
		delete(outputs, g)
	}
	for _, s := range specs {
		if s.InputExpr != "" {
			for _, id := range extractIdentifiers(s.InputExpr) {
				delete(outputs, baseColName(id))
			}
		} else if s.InputCol != "" {
			delete(outputs, s.InputCol)
		}
	}
	for _, f := range filterExprs {
		for _, id := range extractIdentifiers(f) {
			delete(outputs, baseColName(id))
		}
	}
	if len(outputs) == 0 {
		return cols
	}
	kept := cols[:0]
	for _, c := range cols {
		if !outputs[c] {
			kept = append(kept, c)
		}
	}
	return kept
}
