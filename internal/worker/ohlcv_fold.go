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
// The declaration comes from the PLAN — `distributed.AggSpec.OutputFields`,
// derived once by `physical.aggOhlcvOutputFields` from the input columns'
// declared types, for a bare argument and a COMPUTED one alike. The encoded
// state's own header is the fallback, for a spec that carried none.
//
// Neither is invented. A fold that reached this point with no declaration used
// to substitute FLOAT64 for every field, which is how the same statement
// declared DECIMAL(18,4) against a standalone server and FLOAT64 against a
// coordinator — OID 1700 against 701 in the RowDescription (#965 round 2, B1).
func applyOhlcvFold(batches []*batch.RecordBatch, aggs []distributed.AggSpec) ([]*batch.RecordBatch, error) {
	if len(batches) == 0 {
		return batches, nil
	}
	cols := findOhlcvFoldCols(batches[0].Schema)
	if len(cols) == 0 {
		return batches, nil
	}
	declared := ohlcvFieldsBySpec(aggs)
	out := make([]*batch.RecordBatch, len(batches))
	for i, b := range batches {
		fb, err := foldOneOhlcvBatch(b, cols, declared)
		if err != nil {
			return nil, err
		}
		out[i] = fb
	}
	return out, nil
}

// ohlcvFieldsBySpec indexes the PLAN's declaration by the ORIGINAL output
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

func foldOneOhlcvBatch(in *batch.RecordBatch, cols []ohlcvFoldCol,
	declared map[string][]parquet.Column) (*batch.RecordBatch, error) {
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
		f, err := ohlcvFoldFields(declared[c.outputCol], in.Columns[c.stateIdx], in.Len)
		if err != nil {
			return nil, fmt.Errorf("ohlcv fold %q: %w", c.outputCol, err)
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
		if err := writeOhlcvColumn(out.Columns[i], in.Columns[c.stateIdx], in.Len); err != nil {
			return nil, fmt.Errorf("ohlcv fold column %q: %w", c.outputCol, err)
		}
	}
	return out, nil
}

// ohlcvFoldFields is the ROW this column is folded into: the PLAN's
// declaration when the spec carried one, and otherwise the declaration the
// first non-empty state carries in its own header.
//
// Nothing is invented. The previous fallback — FLOAT64 for every field when
// every state in the column was empty — was reached exactly for a zero-row
// result, which is where a wrong declaration is INVISIBLE in the values and
// fully visible on the wire: the same statement sent OID 1700 from a
// standalone server and 701 from a coordinator (#965 round 2, B1). A stage
// that has neither source fails loudly instead.
func ohlcvFoldFields(declared []parquet.Column, stateCol *batch.Vector, n int) ([]parquet.Column, error) {
	if stateCol.Type != parquet.TypeString {
		return nil, fmt.Errorf("expected an encoded bar state string, got %v", stateCol.Type)
	}
	if len(declared) == len(exec.OhlcvFieldNames) {
		return declared, nil
	}
	for i := 0; i < n; i++ {
		if stateCol.Nulls.IsNullFast(i) {
			continue
		}
		_, f, _, err := exec.FinalizeOhlcvState(stateCol.BytesData.StringValue(i))
		if err != nil {
			return nil, err
		}
		if len(f) > 0 {
			return f, nil
		}
	}
	return nil, fmt.Errorf("the bar's declared fields reached neither the spec " +
		"(AggSpec.OutputFields) nor any state in this column, so there is no ROW to fold into")
}

// writeOhlcvColumn decodes each row's merged state and writes the finished
// bar. A NULL or unparseable state, and a group that kept no rows, are SQL
// NULL — an aggregate over no rows is NULL, and a bar of NULLs would be a bar
// pretending to exist.
func writeOhlcvColumn(dst, stateCol *batch.Vector, n int) error {
	if stateCol.Type != parquet.TypeString {
		return fmt.Errorf("expected an encoded bar state string, got %v", stateCol.Type)
	}
	for i := 0; i < n; i++ {
		if stateCol.Nulls.IsNullFast(i) {
			dst.Nulls.SetNull(i)
			continue
		}
		v, _, ok, err := exec.FinalizeOhlcvState(stateCol.BytesData.StringValue(i))
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
