package worker

import (
	"fmt"
	"strings"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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

	// Build new schema: keep every column except dropped count columns,
	// rename sum columns to their AVG output names with type Float64.
	newSchema := make([]parquet.Column, 0, len(in.Schema)-len(pairs))
	for i, c := range in.Schema {
		if dropIdx[i] {
			continue
		}
		if p, ok := pairBySumIdx[i]; ok {
			newSchema = append(newSchema, parquet.Column{
				Name:     p.outputCol,
				Type:     parquet.TypeFloat64,
				Nullable: true,
			})
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
func writeAvgColumn(dst, sumCol, countCol *batch.Vector, n int) error {
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
		// SUM may be float64 (TypeFloat32/Float64 input), int64
		// (TypeInt32/Int64 input), or decimal. AVG of decimal isn't
		// expected in the TPC-H suite at the time of this write —
		// fall back to nil if encountered.
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
