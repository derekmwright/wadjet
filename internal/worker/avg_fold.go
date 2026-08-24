package worker

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// avgSumPrefix and avgCountPrefix mirror the synthetic naming convention
// emitted by the coordinator's decomposeAvg (avg_decompose.go). Pairs of
// "__avg_sum#X" and "__avg_count#X" are the partial-aggregate outputs of
// what was originally AVG(_) AS X. After the final_aggregate stage's
// HashAggregate has merged the partials, applyAvgFold turns each pair
// back into a single AVG column with the original output name.
//
// The "#" separator is invalid in SQL identifiers, so user column names
// cannot collide with these synthetics.
const (
	avgSumPrefix   = "__avg_sum#"
	avgCountPrefix = "__avg_count#"
)

// avgFoldPair tracks one (SUM, COUNT) pair to fold into AVG.
type avgFoldPair struct {
	sumIdx    int    // column index of __avg_sum#X in input batch schema
	countIdx  int    // column index of __avg_count#X
	outputCol string // original output name X
}

// findAvgFoldPairs scans the schema for synthetic AVG-decomposition
// columns and returns the (sum, count, output) triples. Returns nil
// when no synthetic columns are present (the no-AVG hot path).
func findAvgFoldPairs(schema []parquet.Column) []avgFoldPair {
	var sumCols, countCols map[string]int
	for i, c := range schema {
		if strings.HasPrefix(c.Name, avgSumPrefix) {
			if sumCols == nil {
				sumCols = make(map[string]int)
			}
			sumCols[c.Name[len(avgSumPrefix):]] = i
		} else if strings.HasPrefix(c.Name, avgCountPrefix) {
			if countCols == nil {
				countCols = make(map[string]int)
			}
			countCols[c.Name[len(avgCountPrefix):]] = i
		}
	}
	if len(sumCols) == 0 {
		return nil
	}
	pairs := make([]avgFoldPair, 0, len(sumCols))
	for outName, sumIdx := range sumCols {
		countIdx, ok := countCols[outName]
		if !ok {
			// Orphan __avg_sum# without matching __avg_count# — leave
			// it alone; the schema mismatch would surface as a
			// downstream lookup miss rather than silent data loss.
			continue
		}
		pairs = append(pairs, avgFoldPair{
			sumIdx:    sumIdx,
			countIdx:  countIdx,
			outputCol: outName,
		})
	}
	return pairs
}

// applyAvgFold replaces every (__avg_sum#X, __avg_count#X) pair with a
// single Float64 column named X holding sum/count for each row. Pass-
// through when no synthetic columns are present.
//
// Called from executor_stage.go::executeStageAggregate ONLY in mergeMode
// (final_aggregate / merge_aggregate tasks). Partial tasks emit the
// synthetic columns intact so downstream merges can keep summing.
//
// SQL semantics: when count=0 the AVG is NULL.
func applyAvgFold(batches []*batch.RecordBatch) ([]*batch.RecordBatch, error) {
	if len(batches) == 0 {
		return batches, nil
	}
	pairs := findAvgFoldPairs(batches[0].Schema)
	if len(pairs) == 0 {
		return batches, nil
	}
	out := make([]*batch.RecordBatch, len(batches))
	for i, b := range batches {
		fb, err := foldOneBatch(b, pairs)
		if err != nil {
			return nil, err
		}
		out[i] = fb
	}
	return out, nil
}

// foldOneBatch builds a new RecordBatch with the synthetic columns
// replaced by AVG outputs. The output schema preserves the position of
// the SUM column (renamed to the original output) and drops the COUNT
// column.
func foldOneBatch(in *batch.RecordBatch, pairs []avgFoldPair) (*batch.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	dropIdx := make(map[int]bool, len(pairs))
	pairBySumIdx := make(map[int]avgFoldPair, len(pairs))
	for _, p := range pairs {
		dropIdx[p.countIdx] = true
		pairBySumIdx[p.sumIdx] = p
	}

	// Build new schema: keep every column except dropped count columns, and
	// rename sum columns to their AVG output names.
	//
	// The AVG column's type follows the SUM partial's: float64 for every
	// numeric type but DECIMAL, over which AVG answers in DECIMAL at
	// batch.AvgScale(sum scale) — the same declaration the single-process
	// HashAggregate makes for the same query (exec.outputSchema), because
	// the two paths owe the client one answer and one type (#455).
	newSchema := make([]parquet.Column, 0, len(in.Schema)-len(pairs))
	for i, c := range in.Schema {
		if dropIdx[i] {
			continue
		}
		if p, ok := pairBySumIdx[i]; ok {
			out := parquet.Column{Name: p.outputCol, Type: parquet.TypeFloat64, Nullable: true}
			if c.Type == parquet.TypeDecimal {
				out.Type = parquet.TypeDecimal
				out.Precision = batch.MaxDecimalPrecision
				out.Scale = batch.AvgScale(in.Columns[i].DecimalData.Scale)
			}
			newSchema = append(newSchema, out)
			continue
		}
		newSchema = append(newSchema, c)
	}

	out := batch.NewRecordBatch(newSchema, in.Len)
	out.Len = in.Len
	out.Sel = in.Sel

	// Copy / compute each output column.
	dstIdx := 0
	for i := range in.Schema {
		if dropIdx[i] {
			continue
		}
		if p, ok := pairBySumIdx[i]; ok {
			if err := writeAvgColumn(out.Columns[dstIdx], in.Columns[p.sumIdx], in.Columns[p.countIdx], in.Len); err != nil {
				return nil, fmt.Errorf("avg-fold column %q: %w", p.outputCol, err)
			}
		} else {
			// Pass-through: shallow-copy the source vector reference.
			// HashAggregate's output is immutable from here on, so
			// sharing the underlying arrays is safe.
			out.Columns[dstIdx] = in.Columns[i]
		}
		dstIdx++
	}
	return out, nil
}

// writeAvgColumn computes dst[i] = sum[i] / count[i] for n rows. The
// SUM partial is whatever numeric type AVG was over (int or float, etc.);
// the COUNT partial is always int64. We cast both to float64 for the
// divide. count==0 → null.
//
// A DECIMAL sum is the exception and takes the exact path: Int128 division
// at batch.AvgScale, which is what the single-process engine computes for
// the same query. Casting it to a float64 here would put the digits back
// where #455 found them, on one of the two paths only.
func writeAvgColumn(dst, sumCol, countCol *batch.Vector, n int) error {
	if sumCol.Type == parquet.TypeDecimal {
		return writeDecimalAvgColumn(dst, sumCol, countCol, n)
	}
	for i := 0; i < n; i++ {
		// COUNT is always int64; null-or-zero count means no rows
		// contributed, so AVG is NULL.
		if countCol.Nulls.IsNullFast(i) {
			dst.Nulls.SetNull(i)
			continue
		}
		c := countCol.Int64Data[i]
		if c == 0 {
			dst.Nulls.SetNull(i)
			continue
		}
		// SUM is float64 (TypeFloat32/Float64 input) or int64
		// (TypeInt32/Int64 input) here; DECIMAL took the exact path above.
		if sumCol.Nulls.IsNullFast(i) {
			dst.Nulls.SetNull(i)
			continue
		}
		var sumF float64
		switch sumCol.Type {
		case parquet.TypeFloat64:
			sumF = sumCol.Float64Data[i]
		case parquet.TypeFloat32:
			sumF = float64(sumCol.Float32Data[i])
		case parquet.TypeInt64, parquet.TypeTimestamp,
			parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			sumF = float64(sumCol.Int64Data[i])
		case parquet.TypeInt32, parquet.TypePort,
			parquet.TypeProtocol, parquet.TypeDate:
			sumF = float64(sumCol.Int32Data[i])
		default:
			return fmt.Errorf("unsupported AVG sum type: %v", sumCol.Type)
		}
		dst.Float64Data[i] = sumF / float64(c)
		dst.Nulls.SetValid(i)
	}
	return nil
}

// writeDecimalAvgColumn is writeAvgColumn's exact arm: dst[i] is the Int128
// quotient of the DECIMAL sum by the row count, at dst's own scale.
//
// A quotient with no Int128 is a query error rather than an approximation —
// the same position SUM overflow takes (ADR-0012 item 9) — because the whole
// point of answering AVG(DECIMAL) in DECIMAL is that the digits are the ones
// the data had.
func writeDecimalAvgColumn(dst, sumCol, countCol *batch.Vector, n int) error {
	addScale := dst.DecimalData.Scale - sumCol.DecimalData.Scale
	if addScale < 0 {
		return fmt.Errorf("avg-fold: AVG output scale %d is narrower than its SUM partial's %d",
			dst.DecimalData.Scale, sumCol.DecimalData.Scale)
	}
	for i := 0; i < n; i++ {
		if countCol.Nulls.IsNullFast(i) || sumCol.Nulls.IsNullFast(i) {
			dst.WriteNullAt(i)
			continue
		}
		c := countCol.Int64Data[i]
		if c == 0 {
			dst.WriteNullAt(i)
			continue
		}
		q, ok := batch.DecimalAvg(sumCol.DecimalData.Data[i], c, addScale)
		if !ok {
			return fmt.Errorf("avg-fold: AVG over a DECIMAL column has no exact 128-bit value "+
				"(sum %s over %d rows at scale %d)",
				sumCol.DecimalData.Data[i].FormatDecimal(sumCol.DecimalData.Scale), c, dst.DecimalData.Scale)
		}
		dst.DecimalData.Data[i] = q
		dst.Nulls.SetValid(i)
	}
	return nil
}
