package coordinator

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// readScalarFromStageOutput extracts a single scalar value from a producer
// stage's output. The producer is expected to emit exactly one WSHF file
// containing exactly one row with one column (MAX over a single-group
// aggregate, for example). Returns the value formatted as a SQL literal
// ready for string substitution into a filter expression.
//
// When projection is non-nil (the producer's terminal stage carries
// OutputRenames from its subquery's SELECT list), the SCALAR is the value
// of the projection's expression rather than the raw first column. Required
// for Q11 where the subquery is "SELECT SUM(...) * 0.0001 FROM ..." — the
// producer chain only emits the raw SUM under __agg_0 and the * 0.0001
// wrapper has to be applied at extract time.
//
// Used by executeStageDAG to late-bind CTE-referencing scalar subqueries
// so both the filter-carrying stage and the subquery share the same
// distributed accumulation path — see Stage.ScalarDependencies.
func (c *Coordinator) readScalarFromStageOutput(ctx context.Context, out StageOutput, projection []physical.OutputRename) (string, error) {
	files := flattenStageFiles(out)
	if len(files) == 0 {
		// Empty producer output is a legitimate SQL state, not a protocol
		// error (#292): a scalar SUM/MIN/MAX/AVG over zero input rows IS
		// NULL, and comparisons against it evaluate to unknown — exactly
		// what substituting the null literal produces. COUNT-family
		// producers never reach here: their fragments emit the identity
		// row (COUNT=0) even over an empty source, so their output is
		// never file-less (worker executeFragment empty-source exemption).
		return "null", nil
	}
	// Singleton producer: a single WSHF file with one row. If multiple
	// files surfaced (e.g. a zero-row shard plus a one-row shard), scan
	// until we find the first non-empty row.
	for _, path := range files {
		data, err := c.fetchStageOutputData(ctx, path)
		if err != nil {
			return "", fmt.Errorf("fetch %s: %w", path, err)
		}
		decoded, err := decompressShuffleData(data)
		if err != nil {
			return "", fmt.Errorf("decompress %s: %w", path, err)
		}
		if !(len(decoded) >= 4 && decoded[0] == 'W' && decoded[1] == 'S' && decoded[2] == 'H' && decoded[3] == 'F') {
			return "", fmt.Errorf("producer output %s is not WSHF", path)
		}
		batches, err := readShuffleBatches(decoded)
		if err != nil {
			return "", fmt.Errorf("read shuffle %s: %w", path, err)
		}
		lit, ok := scalarFromBatches(batches, projection)
		if ok {
			return lit, nil
		}
	}
	return "", fmt.Errorf("producer stage output has no rows")
}

// scalarFromBatches extracts the scalar value from the producer's output
// batches. When projection contains an Expr-bearing rename (the subquery's
// wrapped-aggregate SELECT — e.g. SUM(...) * 0.0001), compile and evaluate
// the expression for row 0 and return its formatted literal. Otherwise fall
// back to firstScalarLiteral on the raw first column.
func scalarFromBatches(batches []*batch.RecordBatch, projection []physical.OutputRename) (string, bool) {
	for _, r := range projection {
		if r.Expr == nil {
			continue
		}
		compiled, err := expr.Compile(r.Expr)
		if err != nil {
			break // fall back to raw extraction
		}
		for _, b := range batches {
			if b == nil || b.ActiveLen() == 0 {
				continue
			}
			row := 0
			if b.Sel != nil && len(b.Sel) > 0 {
				row = int(b.Sel[0])
			}
			v := compiled.Eval(b, row)
			return formatGoValue(v), true
		}
	}
	return firstScalarLiteral(batches)
}

// formatGoValue stringifies an arbitrary Go value into its SQL-literal
// form for filter substitution. Mirrors planner.scalarToLiteral.
func formatGoValue(v any) string {
	if v == nil {
		return "null"
	}
	switch x := v.(type) {
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "null"
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "null"
		}
		return strconv.FormatFloat(f, 'f', -1, 32)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int:
		return strconv.FormatInt(int64(x), 10)
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(x), "'", "''") + "'"
	default:
		return fmt.Sprint(v)
	}
}

// firstScalarLiteral returns the SQL-literal rendering of the first column of
// the first non-empty row across batches. Returns ok=false when batches have
// no rows.
func firstScalarLiteral(batches []*batch.RecordBatch) (string, bool) {
	for _, b := range batches {
		n := b.ActiveLen()
		if n == 0 {
			continue
		}
		if len(b.Columns) == 0 {
			continue
		}
		vec := b.Columns[0]
		typ := b.Schema[0].Type
		row := 0
		if b.Sel != nil && len(b.Sel) > 0 {
			row = int(b.Sel[0])
		}
		if vec.Nulls.IsNull(row) {
			return "null", true
		}
		return formatScalar(vec, row, typ), true
	}
	return "", false
}

// formatScalar renders a column value at row index as a SQL literal suitable
// for substitution into a filter expression. Mirrors planner.scalarToLiteral
// so the worker's expression compiler sees the same text it would have seen
// from an inline literal.
func formatScalar(vec *batch.Vector, row int, typ parquet.TypeID) string {
	switch typ {
	case parquet.TypeBool:
		if vec.BoolData[row] {
			return "true"
		}
		return "false"
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		return strconv.FormatInt(int64(vec.Int32Data[row]), 10)
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		return strconv.FormatInt(vec.Int64Data[row], 10)
	case parquet.TypeFloat32:
		v := float64(vec.Float32Data[row])
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "null"
		}
		return strconv.FormatFloat(v, 'f', -1, 32)
	case parquet.TypeFloat64:
		v := vec.Float64Data[row]
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "null"
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		s := string(vec.BytesData.Value(row))
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	case parquet.TypeDecimal:
		if vec.DecimalData.Data == nil {
			return "0"
		}
		d := vec.DecimalData.Data[row]
		// Render as signed 128-bit integer; exact scale is metadata
		// that's typically embedded in the schema, which isn't available
		// through this helper. Q15's aggregate is float64 so decimal is
		// not the Q15 path; handle int-only for now.
		if d.Hi == 0 {
			return strconv.FormatUint(d.Lo, 10)
		}
		if d.Hi == -1 {
			return "-" + strconv.FormatUint(^d.Lo+1, 10)
		}
		return fmt.Sprintf("%d%020d", d.Hi, d.Lo)
	default:
		return "null"
	}
}

// substituteScalarDependencies rewrites a stage's FilterExprs and any
// AggSpec.InputExpr entries by string-replacing each :<placeholder> with
// the concrete literal extracted from the named producer stage's output.
// Returns a copy of the stage; the input stage is not modified so concurrent
// reads elsewhere remain safe. When the producer has no ScalarDependencies
// the input is returned as-is.
//
// producerStages provides the producer terminal stage so that any
// projection (OutputRenames) attached to the producer — used for wrapped-
// aggregate subqueries like Q11's "SUM(...) * 0.0001" — gets applied
// during scalar extraction.
func (c *Coordinator) substituteScalarDependencies(ctx context.Context, stage physical.Stage, producerOutputs map[string]StageOutput, producerStages map[string]physical.Stage) (physical.Stage, error) {
	if len(stage.ScalarDependencies) == 0 {
		return stage, nil
	}
	// Resolve each placeholder to its literal text.
	literals := make(map[string]string, len(stage.ScalarDependencies))
	for placeholder, producerID := range stage.ScalarDependencies {
		out, ok := producerOutputs[producerID]
		if !ok {
			return stage, fmt.Errorf("missing producer output for %s (stage %s)", producerID, stage.ID)
		}
		var projection []physical.OutputRename
		if ps, hasStage := producerStages[producerID]; hasStage {
			projection = ps.OutputRenames
		}
		lit, err := c.readScalarFromStageOutput(ctx, out, projection)
		if err != nil {
			return stage, fmt.Errorf("extracting scalar for %s: %w", placeholder, err)
		}
		// One line per placeholder per query: the substituted literal is
		// otherwise invisible anywhere (dispatched predicates are never
		// logged), which made #272 — a wrong Q11 threshold at SF100 —
		// undiagnosable from run artifacts.
		c.logger.Info("scalar substitution",
			"stage_id", stage.ID, "placeholder", placeholder,
			"producer", producerID, "producer_files", len(flattenStageFiles(out)),
			"projection_exprs", len(projection), "literal", lit)
		literals[":"+placeholder] = lit
	}
	// Deep-copy mutable slice / map fields before rewrite.
	out := stage
	if len(out.FilterExprs) > 0 {
		newFE := make([]string, len(out.FilterExprs))
		for i, e := range out.FilterExprs {
			newFE[i] = replacePlaceholders(e, literals)
		}
		out.FilterExprs = newFE
	}
	if len(out.AggSpecs) > 0 {
		newAgg := make([]physical.AggSpec, len(out.AggSpecs))
		for i, a := range out.AggSpecs {
			if a.InputExpr != "" {
				a.InputExpr = replacePlaceholders(a.InputExpr, literals)
			}
			newAgg[i] = a
		}
		out.AggSpecs = newAgg
	}
	if len(out.FusedAggSpecs) > 0 {
		newFused := make([]physical.AggSpec, len(out.FusedAggSpecs))
		for i, a := range out.FusedAggSpecs {
			if a.InputExpr != "" {
				a.InputExpr = replacePlaceholders(a.InputExpr, literals)
			}
			newFused[i] = a
		}
		out.FusedAggSpecs = newFused
	}
	if out.JoinFilter != "" {
		out.JoinFilter = replacePlaceholders(out.JoinFilter, literals)
	}
	return out, nil
}

func replacePlaceholders(s string, literals map[string]string) string {
	for ph, lit := range literals {
		s = strings.ReplaceAll(s, ph, lit)
	}
	return s
}
