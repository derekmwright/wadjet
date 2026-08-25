package physical

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// unifySetOpSchemas is the result type of a set operation: the first arm's
// column NAMES — SQL says the result takes them — over the COMMON TYPE of the
// two arms per position.
//
// Only the DECIMAL rung is widened here, because that is the one whose
// narrowing DESTROYS VALUES rather than merely renaming a type. The rows reach
// `batch.FromRows` as their rendered decimal TEXT, boxed at each arm's own
// scale, and FromRows re-reads that text at the schema's scale — so handing it
// the first arm's scale truncated the second arm's values: over
// `DECIMAL(9,2) UNION ALL DECIMAL(18,4)`, 12.7501 came back as 12.75 and
// 12.7499 as 12.74, and the UNION then counted 8 distinct values where
// PostgreSQL counts 9 (#532). numeric(9,2) ∪ numeric(18,4) is numeric(18,4)
// there, and parsing the narrower arm's text at the wider scale is exact in
// both directions.
//
// Anything else — two different TypeIDs — is left alone: the stage DAG
// reconciles those by CASTING each arm's projection
// (`reconcileSetOpArmTypes`), which is a plan-time rewrite this runtime
// adapter is not the place for.
func unifySetOpSchemas(left, right []parquet.Column) []parquet.Column {
	if len(left) == 0 {
		return right
	}
	if len(right) != len(left) {
		return left
	}
	var out []parquet.Column
	for i := range left {
		l, r := left[i], right[i]
		if l.Type != parquet.TypeDecimal || r.Type != parquet.TypeDecimal {
			continue
		}
		if r.Scale <= l.Scale && r.Precision <= l.Precision {
			continue
		}
		if out == nil {
			out = append(out, left...)
		}
		out[i].Scale = max(l.Scale, r.Scale)
		out[i].Precision = max(l.Precision, r.Precision)
	}
	if out == nil {
		return left
	}
	return out
}
