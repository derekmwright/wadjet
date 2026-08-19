package worker

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// varStatePrefix mirrors the synthetic naming the coordinator's decomposeVar
// emits (var_decompose.go): __var_state#<kind>#<original output name>. The
// column holds one encoded (count, mean, M2) triple per group — the partial
// state of a STDDEV/VARIANCE, which unlike a finished standard deviation
// can be merged. applyVarFold turns it back into the value the query asked
// for, on the FINAL aggregate stage only.
//
// "#" is invalid in a SQL identifier, so no user column name collides.
const varStatePrefix = "__var_state#"

// varFoldCol is one state column to finish.
type varFoldCol struct {
	stateIdx  int    // column index of __var_state#kind#X in the input schema
	kind      string // stddev_samp | var_samp | stddev_pop | var_pop
	outputCol string // original output name X
}

// findVarFoldCols scans the schema for decomposed variance columns.
// Returns nil when none are present (the hot path pays one prefix test per
// column).
func findVarFoldCols(schema []parquet.Column) []varFoldCol {
	var out []varFoldCol
	for i, c := range schema {
		if !strings.HasPrefix(c.Name, varStatePrefix) {
			continue
		}
		rest := c.Name[len(varStatePrefix):]
		sep := strings.IndexByte(rest, '#')
		if sep < 0 {
			// Malformed synthetic: leave it alone rather than guess a
			// kind. It surfaces as a downstream lookup miss, not as a
			// silently wrong number.
			continue
		}
		out = append(out, varFoldCol{stateIdx: i, kind: rest[:sep], outputCol: rest[sep+1:]})
	}
	return out
}

// applyVarFold replaces every __var_state#kind#X column with a Float64
// column named X holding the finished STDDEV/VARIANCE for each group.
// Pass-through when no synthetic columns are present.
//
// Called from the final_aggregate fragment only (the same FoldAvg gate
// applyAvgFold rides): intermediate merge_aggregate tasks must keep
// shipping the state, or the stage above them would be re-aggregating
// finished values again — the defect this decomposition exists to remove.
func applyVarFold(batches []*batch.RecordBatch) ([]*batch.RecordBatch, error) {
	if len(batches) == 0 {
		return batches, nil
	}
	cols := findVarFoldCols(batches[0].Schema)
	if len(cols) == 0 {
		return batches, nil
	}
	out := make([]*batch.RecordBatch, len(batches))
	for i, b := range batches {
		fb, err := foldOneVarBatch(b, cols)
		if err != nil {
			return nil, err
		}
		out[i] = fb
	}
	return out, nil
}

// foldOneVarBatch builds a new RecordBatch with each state column replaced
// by its finished value, in place (same position, original name).
func foldOneVarBatch(in *batch.RecordBatch, cols []varFoldCol) (*batch.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	byIdx := make(map[int]varFoldCol, len(cols))
	for _, c := range cols {
		byIdx[c.stateIdx] = c
	}

	newSchema := make([]parquet.Column, len(in.Schema))
	copy(newSchema, in.Schema)
	for _, c := range cols {
		newSchema[c.stateIdx] = parquet.Column{Name: c.outputCol, Type: parquet.TypeFloat64, Nullable: true}
	}

	out := batch.NewRecordBatch(newSchema, in.Len)
	out.Len = in.Len
	out.Sel = in.Sel

	for i := range in.Schema {
		c, isState := byIdx[i]
		if !isState {
			// Pass-through: shallow-copy the source vector reference, as
			// applyAvgFold does — HashAggregate's output is immutable here.
			out.Columns[i] = in.Columns[i]
			continue
		}
		if err := writeVarColumn(out.Columns[i], in.Columns[c.stateIdx], c.kind, in.Len); err != nil {
			return nil, fmt.Errorf("var-fold column %q: %w", c.outputCol, err)
		}
	}
	return out, nil
}

// writeVarColumn decodes each row's partial state and writes the finished
// value. A NULL, unparseable, or too-small state is SQL NULL — STDDEV over
// fewer than two rows has no sample answer, and over none no answer at all.
func writeVarColumn(dst, stateCol *batch.Vector, kind string, n int) error {
	if stateCol.Type != parquet.TypeString {
		return fmt.Errorf("expected an encoded state string, got %v", stateCol.Type)
	}
	for i := 0; i < n; i++ {
		if stateCol.Nulls.IsNullFast(i) {
			dst.Nulls.SetNull(i)
			continue
		}
		v, ok := exec.FinalizeVarianceState(stateCol.BytesData.StringValue(i), kind)
		if !ok {
			dst.Nulls.SetNull(i)
			continue
		}
		dst.Float64Data[i] = v
		dst.Nulls.SetValid(i)
	}
	return nil
}
