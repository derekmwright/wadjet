package worker

import (
	"fmt"
	"strings"

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
// It takes NO declaration from the plan: the encoded state carries the ROW it
// was COMPUTED for, in its own header. The planner cannot always supply one —
// it has no catalog column to walk for a COMPUTED argument, and
// `ohlcv(ts, price*2, volume)` is exactly that — so a fold that depended on
// the spec answered in process and failed loud on the DAG.
func applyOhlcvFold(batches []*batch.RecordBatch) ([]*batch.RecordBatch, error) {
	if len(batches) == 0 {
		return batches, nil
	}
	cols := findOhlcvFoldCols(batches[0].Schema)
	if len(cols) == 0 {
		return batches, nil
	}
	out := make([]*batch.RecordBatch, len(batches))
	for i, b := range batches {
		fb, err := foldOneOhlcvBatch(b, cols)
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

func foldOneOhlcvBatch(in *batch.RecordBatch, cols []ohlcvFoldCol) (*batch.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	byIdx := make(map[int]ohlcvFoldCol, len(cols))
	for _, c := range cols {
		byIdx[c.stateIdx] = c
	}
	// The output ROW's shape comes from the states themselves, so the schema
	// is decided by reading the first one that is not NULL. A column of only
	// NULL bars declares the bar's field NAMES with no types, which is what a
	// zero-row group can honestly say and what SetValue never has to write
	// into.
	newSchema := make([]parquet.Column, len(in.Schema))
	copy(newSchema, in.Schema)
	for _, c := range cols {
		f, err := ohlcvFieldsInColumn(in.Columns[c.stateIdx], in.Len)
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

// ohlcvFieldsInColumn reads the declared ROW out of the first state in the
// column that carries one. Every state in one column came from one aggregate
// and so declares the same thing; a column of NULLs declares the names only.
func ohlcvFieldsInColumn(stateCol *batch.Vector, n int) ([]parquet.Column, error) {
	if stateCol.Type != parquet.TypeString {
		return nil, fmt.Errorf("expected an encoded bar state string, got %v", stateCol.Type)
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
	// Every state in this column is NULL or empty — a stage whose tasks all
	// filtered everything away. The bar's field NAMES are what it can
	// honestly declare; nothing will be written into the children, and the
	// alternative (guessing types) is what the state's own header exists to
	// stop. This is the identity-row shape #685 records for a DECIMAL.
	out := make([]parquet.Column, len(exec.OhlcvFieldNames))
	for i, name := range exec.OhlcvFieldNames {
		out[i] = parquet.Column{Name: name, Type: parquet.TypeFloat64, Nullable: true}
	}
	return out, nil
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
