package worker

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// varStatePrefix and covarStatePrefix mirror the synthetic naming the
// coordinator's decomposeVar / decomposeCovar emit (var_decompose.go,
// covar_decompose.go):
//
//	__var_state#<kind>#<original output name>
//	__covar_state#<kind>#<original output name>
//
// The column holds one encoded state per group — the (count, mean, M2)
// triple of a STDDEV/VARIANCE, or the (count, meanX, meanY, C, M2x, M2y)
// sextuple of a CORR/COVAR — which, unlike the finished statistic, can be
// merged. applyVarFold / applyCovarFold turn it back into the value the
// query asked for, on the FINAL aggregate stage only.
//
// "#" is invalid in a SQL identifier, so no user column name collides.
const (
	varStatePrefix   = "__var_state#"
	covarStatePrefix = "__covar_state#"
)

// stateFoldCol is one state column to finish.
type stateFoldCol struct {
	stateIdx  int    // column index of the synthetic in the input schema
	kind      string // stddev_samp | var_samp | … | corr | covar_samp | covar_pop
	outputCol string // original output name X
}

// findStateFoldCols scans the schema for decomposed state columns carrying
// the given prefix. Returns nil when none are present (the hot path pays
// one prefix test per column).
func findStateFoldCols(schema []parquet.Column, prefix string) []stateFoldCol {
	var out []stateFoldCol
	for i, c := range schema {
		if !strings.HasPrefix(c.Name, prefix) {
			continue
		}
		rest := c.Name[len(prefix):]
		sep := strings.IndexByte(rest, '#')
		if sep < 0 {
			// Malformed synthetic: leave it alone rather than guess a
			// kind. It surfaces as a downstream lookup miss, not as a
			// silently wrong number.
			continue
		}
		out = append(out, stateFoldCol{stateIdx: i, kind: rest[:sep], outputCol: rest[sep+1:]})
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
	return applyStateFold(batches, varStatePrefix, exec.FinalizeVarianceState)
}

// applyCovarFold does the same for __covar_state#kind#X — the CORR,
// COVAR_SAMP and COVAR_POP partial state (#353).
func applyCovarFold(batches []*batch.RecordBatch) ([]*batch.RecordBatch, error) {
	return applyStateFold(batches, covarStatePrefix, exec.FinalizeCovarianceState)
}

// applyStateFold is the shared body: find the synthetics carrying prefix,
// and rewrite each into a Float64 column holding finalize's answer.
func applyStateFold(
	batches []*batch.RecordBatch,
	prefix string,
	finalize func(encoded, kind string) (float64, bool),
) ([]*batch.RecordBatch, error) {
	if len(batches) == 0 {
		return batches, nil
	}
	cols := findStateFoldCols(batches[0].Schema, prefix)
	if len(cols) == 0 {
		return batches, nil
	}
	out := make([]*batch.RecordBatch, len(batches))
	for i, b := range batches {
		fb, err := foldOneStateBatch(b, cols, finalize)
		if err != nil {
			return nil, err
		}
		out[i] = fb
	}
	return out, nil
}

// foldOneStateBatch builds a new RecordBatch with each state column replaced
// by its finished value, in place (same position, original name).
func foldOneStateBatch(
	in *batch.RecordBatch,
	cols []stateFoldCol,
	finalize func(encoded, kind string) (float64, bool),
) (*batch.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	byIdx := make(map[int]stateFoldCol, len(cols))
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
		if err := writeStateColumn(out.Columns[i], in.Columns[c.stateIdx], c.kind, in.Len, finalize); err != nil {
			return nil, fmt.Errorf("state-fold column %q: %w", c.outputCol, err)
		}
	}
	return out, nil
}

// writeStateColumn decodes each row's partial state and writes the finished
// value. A NULL, unparseable, or too-small state is SQL NULL — STDDEV over
// fewer than two rows has no sample answer, and over none no answer at all;
// CORR and COVAR_SAMP apply the same thresholds.
func writeStateColumn(
	dst, stateCol *batch.Vector,
	kind string,
	n int,
	finalize func(encoded, kind string) (float64, bool),
) error {
	if stateCol.Type != parquet.TypeString {
		return fmt.Errorf("expected an encoded state string, got %v", stateCol.Type)
	}
	for i := 0; i < n; i++ {
		if stateCol.Nulls.IsNullFast(i) {
			dst.Nulls.SetNull(i)
			continue
		}
		v, ok := finalize(stateCol.BytesData.StringValue(i), kind)
		if !ok {
			dst.Nulls.SetNull(i)
			continue
		}
		dst.Float64Data[i] = v
		dst.Nulls.SetValid(i)
	}
	return nil
}
