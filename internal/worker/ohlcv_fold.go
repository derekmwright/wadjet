package worker

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ohlcvStatePrefix mirrors the synthetic naming coordinator.decomposeOhlcv
// emits: `__ohlcv_state#<original output name>`. The column holds one encoded
// bar state per group, which — unlike a finished bar — can be merged.
//
// It has no <kind> segment, unlike the variance and covariance prefixes: one
// state, one way to finish it (ADR-0035).
const ohlcvStatePrefix = exec.OhlcvStateColumnPrefix

// applyOhlcvFold replaces every `__ohlcv_state#X` column with a ROW column
// named X holding the finished bar for each group.
//
// It is a sibling of applyVarFold rather than a caller of applyStateFold,
// because a bar's answer is a ROW and applyStateFold's `finalize` returns a
// float64. Sharing the walk would mean widening that signature for every
// caller; a bar is the first state whose answer is not a number and it will not
// be the last (ADR-0035 names TDIGEST and TOP_K), so the shape to converge on
// is a value-returning fold rather than a float-returning one — a change to
// make when the second such state arrives, with two callers to test it
// against, not with one.
//
// Called from the final_aggregate fragment only, on the same spec.FoldAvg
// gate: intermediate merge_aggregate stages MUST keep shipping the state, or
// the stage above them would re-aggregate finished bars — which is a MAX over
// two ROWs, the defect the decomposition exists to remove.
//
// aggs is the fragment's own spec list; it carries each bar's DECLARED FIELDS
// (distributed.AggSpec.OutputFields), which is the only source for them out
// here — the worker has no catalog, and the state's own header carries the
// carrier and the scales but not the parquet types.
func applyOhlcvFold(batches []*batch.RecordBatch, aggs []distributed.AggSpec) ([]*batch.RecordBatch, error) {
	if len(batches) == 0 {
		return batches, nil
	}
	cols := findOhlcvFoldCols(batches[0].Schema)
	if len(cols) == 0 {
		return batches, nil
	}
	fields := ohlcvFieldsBySpec(aggs)
	out := make([]*batch.RecordBatch, len(batches))
	for i, b := range batches {
		fb, err := foldOneOhlcvBatch(b, cols, fields)
		if err != nil {
			return nil, err
		}
		out[i] = fb
	}
	return out, nil
}

// ohlcvFoldCol is one bar state column to finish.
type ohlcvFoldCol struct {
	stateIdx  int
	outputCol string
}

func findOhlcvFoldCols(schema []parquet.Column) []ohlcvFoldCol {
	var out []ohlcvFoldCol
	for i, c := range schema {
		if !strings.HasPrefix(c.Name, ohlcvStatePrefix) {
			continue
		}
		out = append(out, ohlcvFoldCol{stateIdx: i, outputCol: c.Name[len(ohlcvStatePrefix):]})
	}
	return out
}

// ohlcvFieldsBySpec indexes the declared bar fields by the ORIGINAL output
// column name, which is what the synthetic's suffix is.
func ohlcvFieldsBySpec(aggs []distributed.AggSpec) map[string][]parquet.Column {
	out := map[string][]parquet.Column{}
	for _, a := range aggs {
		name, ok := exec.OhlcvStateOutput(a.OutputCol)
		if !ok || len(a.OutputFields) == 0 {
			continue
		}
		out[name] = aggSpecOutputFields(a)
	}
	return out
}

func foldOneOhlcvBatch(in *batch.RecordBatch, cols []ohlcvFoldCol,
	fields map[string][]parquet.Column) (*batch.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	byIdx := make(map[int]ohlcvFoldCol, len(cols))
	for _, c := range cols {
		byIdx[c.stateIdx] = c
	}
	newSchema := make([]parquet.Column, len(in.Schema))
	copy(newSchema, in.Schema)
	for _, c := range cols {
		f := fields[c.outputCol]
		if len(f) == 0 {
			// No declaration reached this fragment. Refuse rather than
			// inventing one: a ROW vector built from a guessed field list
			// writes the right numbers into the wrong fields, silently.
			return nil, fmt.Errorf("ohlcv fold %q: the bar's declared fields did not "+
				"reach this fragment (AggSpec.OutputFields)", c.outputCol)
		}
		newSchema[c.stateIdx] = parquet.Column{
			Name: c.outputCol, Type: parquet.TypeRow, Nullable: true, Fields: f,
		}
	}

	out := batch.NewRecordBatch(newSchema, in.Len)
	out.Len = in.Len
	out.Sel = in.Sel
	for i := range in.Schema {
		c, isState := byIdx[i]
		if !isState {
			out.Columns[i] = in.Columns[i]
			continue
		}
		if err := writeOhlcvColumn(out.Columns[i], in.Columns[c.stateIdx],
			fields[c.outputCol], in.Len); err != nil {
			return nil, fmt.Errorf("ohlcv fold column %q: %w", c.outputCol, err)
		}
	}
	return out, nil
}

// writeOhlcvColumn decodes each row's merged state and writes the finished
// bar. A NULL or unparseable state, and a group that kept no rows, are SQL
// NULL — an aggregate over no rows is NULL, and a bar of NULLs would be a bar
// pretending to exist.
func writeOhlcvColumn(dst, stateCol *batch.Vector, fields []parquet.Column, n int) error {
	if stateCol.Type != parquet.TypeString {
		return fmt.Errorf("expected an encoded bar state string, got %v", stateCol.Type)
	}
	for i := 0; i < n; i++ {
		if stateCol.Nulls.IsNullFast(i) {
			dst.Nulls.SetNull(i)
			continue
		}
		v, ok, err := exec.FinalizeOhlcvState(stateCol.BytesData.StringValue(i), fields)
		if err != nil {
			return err
		}
		if !ok {
			dst.Nulls.SetNull(i)
			continue
		}
		dst.SetValue(i, v)
		dst.Nulls.SetValid(i)
	}
	return nil
}
