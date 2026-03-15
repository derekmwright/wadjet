// Package physical converts logical plans to physical execution plans.
package physical

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/engine/expr"
	"github.com/derekmwright/caelum/internal/planner/logical"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// PhysicalPlan represents an executable query plan.
type PhysicalPlan struct {
	Pipeline *exec.Pipeline
	Stages   []Stage // for distributed execution
}

// Stage represents a unit of distributed work.
type Stage struct {
	ID           string
	Type         string // scan, aggregate, sort
	Dependencies []string
	Tasks        int
}

// PrettyPrint returns a formatted string representation of the physical plan.
func (p *PhysicalPlan) PrettyPrint() string {
	if len(p.Stages) == 0 {
		return "Single-stage local execution"
	}
	var b strings.Builder
	for i, stage := range p.Stages {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(fmt.Sprintf("Stage %s [%s] (%d tasks)", stage.ID, stage.Type, stage.Tasks))
		if len(stage.Dependencies) > 0 {
			b.WriteString(fmt.Sprintf(" <- depends on %s", strings.Join(stage.Dependencies, ", ")))
		}
	}
	return b.String()
}

// Planner converts logical plans to physical plans.
type Planner struct {
	catalog *catalog.Catalog
}

// NewPlanner creates a new physical planner.
func NewPlanner(cat *catalog.Catalog) *Planner {
	return &Planner{catalog: cat}
}

// Plan converts a logical plan to a physical plan for local execution.
func (p *Planner) Plan(ctx context.Context, node *logical.Node) (*PhysicalPlan, error) {
	source, ops, sink, err := p.buildPipeline(ctx, node)
	if err != nil {
		return nil, err
	}

	plan := &PhysicalPlan{
		Pipeline: &exec.Pipeline{
			Source: source,
			Ops:    ops,
			Sink:   sink,
		},
	}

	// Generate distributed stages for coordinator dispatch
	plan.Stages = p.generateStages(node)

	return plan, nil
}

// PlanDistributed generates a stage DAG for distributed execution.
// Returns stages with dependency ordering suitable for coordinator dispatch.
func (p *Planner) PlanDistributed(ctx context.Context, node *logical.Node) ([]Stage, error) {
	return p.generateStages(node), nil
}

func (p *Planner) generateStages(node *logical.Node) []Stage {
	var stages []Stage
	p.walkStages(node, &stages, nil)
	return stages
}

func (p *Planner) walkStages(node *logical.Node, stages *[]Stage, parentID *string) {
	switch node.Type {
	case logical.NodeScan:
		stageID := fmt.Sprintf("scan-%d", len(*stages))
		// Estimate tasks from table partitions
		tasks := 1
		if meta, err := p.catalog.GetManifest(context.Background(), node.TableName); err == nil {
			totalFiles := 0
			for _, part := range meta.Partitions {
				totalFiles += len(part.Files)
			}
			if totalFiles > 0 {
				tasks = totalFiles
			}
		}
		stage := Stage{
			ID:    stageID,
			Type:  "scan",
			Tasks: tasks,
		}
		*stages = append(*stages, stage)
		if parentID != nil {
			// Link parent dependency
			for i := range *stages {
				if (*stages)[i].ID == *parentID {
					(*stages)[i].Dependencies = append((*stages)[i].Dependencies, stageID)
				}
			}
		}

	case logical.NodeAggregate:
		stageID := fmt.Sprintf("aggregate-%d", len(*stages))
		// Walk children first
		for _, child := range node.Children {
			p.walkStages(child, stages, &stageID)
		}
		stage := Stage{
			ID:    stageID,
			Type:  "aggregate",
			Tasks: 1,
		}
		// Dependencies: all child scan stages
		for _, s := range *stages {
			if s.Type == "scan" {
				stage.Dependencies = append(stage.Dependencies, s.ID)
			}
		}
		*stages = append(*stages, stage)

	case logical.NodeSort:
		stageID := fmt.Sprintf("sort-%d", len(*stages))
		for _, child := range node.Children {
			p.walkStages(child, stages, &stageID)
		}
		stage := Stage{
			ID:    stageID,
			Type:  "sort",
			Tasks: 1,
		}
		// Depends on all prior stages
		for _, s := range *stages {
			stage.Dependencies = append(stage.Dependencies, s.ID)
		}
		*stages = append(*stages, stage)

	case logical.NodeJoin:
		// Check if right side is small enough for broadcast join
		stageID := fmt.Sprintf("join-%d", len(*stages))
		for _, child := range node.Children {
			p.walkStages(child, stages, &stageID)
		}
		joinType := "hash_join"
		if p.isBroadcastCandidate(node) {
			joinType = "broadcast_join"
		}
		stage := Stage{
			ID:    stageID,
			Type:  joinType,
			Tasks: 1,
		}
		for _, s := range *stages {
			if s.Type == "scan" {
				stage.Dependencies = append(stage.Dependencies, s.ID)
			}
		}
		*stages = append(*stages, stage)

	default:
		// Passthrough nodes (Filter, Project, Limit) — walk children
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
	}
}

// isBroadcastCandidate returns true if the right (build) side of a join is
// small enough to broadcast to all workers (< 100 MB estimated).
const broadcastThresholdBytes = 100 * 1024 * 1024

func (p *Planner) isBroadcastCandidate(joinNode *logical.Node) bool {
	if len(joinNode.Children) < 2 {
		return false
	}
	rightChild := joinNode.Children[1]
	if rightChild.Type != logical.NodeScan {
		return false
	}
	// Estimate size from file count
	manifest, err := p.catalog.GetManifest(context.Background(), rightChild.TableName)
	if err != nil {
		return false
	}
	totalFiles := 0
	for _, part := range manifest.Partitions {
		totalFiles += len(part.Files)
	}
	// Heuristic: assume ~10MB per file, broadcast if < 10 files
	return totalFiles <= 10
}

func (p *Planner) buildPipeline(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	switch node.Type {
	case logical.NodeLimit:
		return p.buildLimit(ctx, node)
	case logical.NodeSort:
		return p.buildSort(ctx, node)
	case logical.NodeProject:
		return p.buildProject(ctx, node)
	case logical.NodeAggregate:
		return p.buildAggregate(ctx, node)
	case logical.NodeFilter:
		return p.buildFilter(ctx, node)
	case logical.NodeScan:
		return p.buildScan(ctx, node)
	case logical.NodeJoin:
		return p.buildJoin(ctx, node)
	default:
		return nil, nil, nil, fmt.Errorf("unsupported plan node: %s", node.Type)
	}
}

func (p *Planner) buildScan(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	scanner := p.newScanner(ctx, node.TableName)
	return scanner, nil, &exec.CollectSink{}, nil
}

func (p *Planner) buildJoin(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) < 2 {
		return nil, nil, nil, fmt.Errorf("join requires two children")
	}

	joinType := exec.InnerJoin
	if strings.Contains(strings.ToLower(node.JoinType), "left") {
		joinType = exec.LeftJoin
	}

	// Parse join condition to extract key columns
	// Handles "left.col = right.col" patterns
	leftKeys, rightKeys := parseJoinKeys(node.JoinCond)
	if len(leftKeys) == 0 {
		return nil, nil, nil, fmt.Errorf("could not extract join keys from: %s", node.JoinCond)
	}

	hj := exec.NewHashJoin(joinType, leftKeys, rightKeys)

	// Build right side (small table) into hash table
	rightSource, rightOps, _, err := p.buildPipeline(ctx, node.Children[1])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building join right side: %w", err)
	}

	// Wrap right side source + ops into a single source for Build()
	buildSource := &pipelineSource{
		source: rightSource,
		ops:    rightOps,
	}

	if err := hj.Build(ctx, buildSource); err != nil {
		return nil, nil, nil, fmt.Errorf("building hash table: %w", err)
	}

	// Left side (probe) streams through
	leftSource, leftOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building join left side: %w", err)
	}

	// Add hash join probe as a unary operator on the left side
	leftOps = append(leftOps, hj.Probe())

	return leftSource, leftOps, &exec.CollectSink{}, nil
}

// pipelineSource wraps a Source + UnaryOps into a single Source.
type pipelineSource struct {
	source exec.Source
	ops    []exec.UnaryOperator
	inited bool
}

func (ps *pipelineSource) Init(ctx context.Context) error {
	if ps.inited {
		return nil
	}
	ps.inited = true
	if err := ps.source.Init(ctx); err != nil {
		return err
	}
	for _, op := range ps.ops {
		if err := op.Init(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (ps *pipelineSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		b, err := ps.source.Next(ctx)
		if err != nil || b == nil {
			return b, err
		}
		for _, op := range ps.ops {
			b, err = op.Execute(ctx, b)
			if err != nil {
				return nil, err
			}
			if b == nil {
				break
			}
		}
		if b != nil {
			return b, nil
		}
	}
}

func (ps *pipelineSource) Close() error {
	err := ps.source.Close()
	for _, op := range ps.ops {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// parseJoinKeys extracts left and right key columns from a join condition.
// Handles patterns like "e.user_id = u.user_id" or "user_id = user_id".
func parseJoinKeys(cond string) (leftKeys, rightKeys []string) {
	// Split on " and " for compound keys
	parts := strings.Split(strings.ToLower(cond), " and ")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		eqParts := strings.SplitN(part, "=", 2)
		if len(eqParts) != 2 {
			continue
		}
		left := cleanExpr(strings.TrimSpace(eqParts[0]))
		right := cleanExpr(strings.TrimSpace(eqParts[1]))
		leftKeys = append(leftKeys, left)
		rightKeys = append(rightKeys, right)
	}
	return
}

func (p *Planner) buildFilter(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("filter has no child")
	}

	source, ops, sink, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	for _, pred := range node.Predicates {
		filter := buildFilterOp(pred)
		if filter != nil {
			ops = append(ops, filter)
		}
	}

	return source, ops, sink, nil
}

func (p *Planner) buildProject(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("project has no child")
	}

	child := node.Children[0]

	// If the child is an Aggregate, skip the projection — the aggregate already
	// produces correctly named output columns (group-by cols + agg output cols).
	// Adding a Project on top would fail because ColumnRef can't find aggregate
	// output names by their SQL expression strings.
	if child.Type == logical.NodeAggregate {
		return p.buildPipeline(ctx, child)
	}

	source, ops, sink, err := p.buildPipeline(ctx, child)
	if err != nil {
		return nil, nil, nil, err
	}

	var projCols []exec.ProjectColumn
	for _, proj := range node.Projections {
		colRef := proj.Column
		if colRef == "" {
			colRef = cleanExpr(proj.Expr)
		}
		name := proj.Alias
		if name == "" {
			name = colRef // use unqualified column name
		}

		// Try to compile from AST expression first, fall back to ColumnRef
		var expression exec.Expression
		if proj.ASTExpr != nil && !proj.IsAgg {
			compiled, err := expr.Compile(proj.ASTExpr)
			if err == nil {
				expression = wrapExpr(compiled)
			}
		}
		if expression == nil {
			expression = exec.ColumnRef(colRef)
		}

		projCols = append(projCols, exec.ProjectColumn{
			Name: name,
			Type: parquet.TypeString, // Will be resolved at runtime
			Expr: expression,
		})
	}

	if len(projCols) > 0 {
		ops = append(ops, exec.NewProject(projCols))
	}

	return source, ops, sink, nil
}

func (p *Planner) buildAggregate(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("aggregate has no child")
	}

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	var aggCols []exec.AggColumn
	for _, agg := range node.AggExprs {
		aggCols = append(aggCols, exec.AggColumn{
			Func:       parseAggFunc(agg.Func),
			InputCol:   cleanExpr(agg.InputCol),
			OutputCol:  agg.OutputCol,
			OutputType: aggOutputType(agg.Func),
		})
	}

	groupByCols := make([]string, len(node.GroupBy))
	for i, gb := range node.GroupBy {
		groupByCols[i] = cleanExpr(gb)
	}

	hashAgg := exec.NewHashAggregate(groupByCols, aggCols)

	// The aggregate acts as both sink and source
	// We need to run childSource -> childOps -> hashAgg(sink), then hashAgg(source) -> collectSink
	return &aggSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		agg:         hashAgg,
	}, nil, &exec.CollectSink{}, nil
}

func (p *Planner) buildSort(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("sort has no child")
	}

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	var keys []exec.SortKey
	for _, ob := range node.OrderBy {
		order := exec.Ascending
		if ob.Desc {
			order = exec.Descending
		}
		keys = append(keys, exec.SortKey{
			Column: cleanExpr(ob.Column),
			Order:  order,
		})
	}

	sortOp := exec.NewSort(keys)

	return &sortSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		sort:        sortOp,
	}, nil, &exec.CollectSink{}, nil
}

func (p *Planner) buildLimit(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("limit has no child")
	}

	source, ops, sink, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	limit := exec.NewLimit(int64(node.LimitVal))
	ops = append(ops, limit)

	return source, ops, sink, nil
}

func (p *Planner) newScanner(ctx context.Context, tableName string) exec.Source {
	// Get table schema
	tableMeta, err := p.catalog.GetTable(ctx, tableName)
	if err != nil {
		return &exec.SliceSource{}
	}
	_ = tableMeta

	// Create a scanner source that reads from the catalog
	return &catalogScanSource{
		catalog:   p.catalog,
		tableName: tableName,
	}
}

// catalogScanSource adapts the scan.Scanner to exec.Source
type catalogScanSource struct {
	catalog   *catalog.Catalog
	tableName string
	inner     exec.Source
}

func (s *catalogScanSource) Init(ctx context.Context) error {
	// Use the scan package scanner
	sc := newScannerSource(s.catalog, s.tableName)
	s.inner = sc
	return s.inner.Init(ctx)
}

func (s *catalogScanSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return s.inner.Next(ctx)
}

func (s *catalogScanSource) Close() error {
	if s.inner != nil {
		return s.inner.Close()
	}
	return nil
}

// RecordBatch type alias for convenience
type RecordBatch = batch.RecordBatch

func buildFilterOp(pred logical.Predicate) exec.UnaryOperator {
	// Try to compile from AST expression first (full expression engine)
	if pred.ASTExpr != nil {
		compiled, err := expr.Compile(pred.ASTExpr)
		if err == nil {
			return exec.NewFilter(wrapPredicate(compiled))
		}
	}

	// Fall back to raw string parsing
	if pred.Raw != "" {
		p := parseSimplePredicate(pred.Raw)
		if p != nil {
			return p
		}
	}

	if pred.Column != "" && pred.Op != "" {
		op := parseCompareOp(pred.Op)
		return exec.NewFilter(exec.ColumnCompare(pred.Column, op, pred.Value))
	}

	return nil
}

func parseSimplePredicate(raw string) exec.UnaryOperator {
	// Parse "column op value" patterns
	operators := []struct {
		sql string
		op  exec.CompareOp
	}{
		{">=", exec.OpGe},
		{"<=", exec.OpLe},
		{"!=", exec.OpNe},
		{">", exec.OpGt},
		{"<", exec.OpLt},
		{"=", exec.OpEq},
	}

	for _, o := range operators {
		parts := strings.SplitN(raw, o.sql, 2)
		if len(parts) == 2 {
			col := cleanExpr(strings.TrimSpace(parts[0]))
			valStr := strings.TrimSpace(parts[1])
			val := parseValue(valStr)
			return exec.NewFilter(exec.ColumnCompare(col, o.op, val))
		}
	}

	// IS NULL / IS NOT NULL
	upper := strings.ToUpper(raw)
	if strings.Contains(upper, "IS NOT NULL") {
		col := cleanExpr(strings.TrimSpace(raw[:strings.Index(upper, "IS NOT NULL")]))
		return exec.NewFilter(exec.ColumnCompare(col, exec.OpIsNotNull, nil))
	}
	if strings.Contains(upper, "IS NULL") {
		col := cleanExpr(strings.TrimSpace(raw[:strings.Index(upper, "IS NULL")]))
		return exec.NewFilter(exec.ColumnCompare(col, exec.OpIsNull, nil))
	}

	// BETWEEN: "col between X and Y" → col >= X AND col <= Y
	if idx := strings.Index(upper, " BETWEEN "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		rest := strings.TrimSpace(raw[idx+len(" BETWEEN "):])
		andIdx := strings.Index(strings.ToUpper(rest), " AND ")
		if andIdx >= 0 {
			lo := parseValue(strings.TrimSpace(rest[:andIdx]))
			hi := parseValue(strings.TrimSpace(rest[andIdx+len(" AND "):]))
			return exec.NewFilter(exec.And(
				exec.ColumnCompare(col, exec.OpGe, lo),
				exec.ColumnCompare(col, exec.OpLe, hi),
			))
		}
	}

	// IN: "col in (v1, v2, v3)" → col = v1 OR col = v2 OR col = v3
	if idx := strings.Index(upper, " IN "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		rest := strings.TrimSpace(raw[idx+len(" IN "):])
		rest = strings.TrimPrefix(rest, "(")
		rest = strings.TrimSuffix(rest, ")")
		parts := strings.Split(rest, ",")
		var preds []exec.Predicate
		for _, part := range parts {
			val := parseValue(strings.TrimSpace(part))
			preds = append(preds, exec.ColumnCompare(col, exec.OpEq, val))
		}
		if len(preds) > 0 {
			return exec.NewFilter(exec.Or(preds...))
		}
	}

	return nil
}

func parseValue(s string) any {
	s = strings.TrimSpace(s)
	// Remove quotes
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	// Try integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func parseCompareOp(op string) exec.CompareOp {
	switch op {
	case "=":
		return exec.OpEq
	case "!=", "<>":
		return exec.OpNe
	case "<":
		return exec.OpLt
	case "<=":
		return exec.OpLe
	case ">":
		return exec.OpGt
	case ">=":
		return exec.OpGe
	default:
		return exec.OpEq
	}
}

func parseAggFunc(s string) exec.AggFunc {
	switch strings.ToLower(s) {
	case "sum":
		return exec.AggSum
	case "count":
		return exec.AggCount
	case "min":
		return exec.AggMin
	case "max":
		return exec.AggMax
	case "avg":
		return exec.AggAvg
	default:
		return exec.AggCount
	}
}

func aggOutputType(funcName string) parquet.TypeID {
	switch strings.ToLower(funcName) {
	case "count":
		return parquet.TypeInt64
	default:
		return parquet.TypeFloat64
	}
}

func cleanExpr(s string) string {
	s = strings.TrimSpace(s)
	if parts := strings.SplitN(s, ".", 2); len(parts) == 2 {
		return parts[1]
	}
	return s
}

// aggSourceAdapter wraps a child pipeline + hash aggregate into a Source.
type aggSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	agg         *exec.HashAggregate
	initialized bool
}

func (a *aggSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (a *aggSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !a.initialized {
		a.initialized = true
		// Run child pipeline into aggregate
		pipe := &exec.Pipeline{
			Source: a.childSource,
			Ops:    a.childOps,
			Sink:   a.agg,
		}
		if err := pipe.Run(ctx); err != nil {
			return nil, err
		}
	}
	return a.agg.Next(ctx)
}

func (a *aggSourceAdapter) Close() error {
	a.agg.Close()
	return a.childSource.Close()
}

// sortSourceAdapter wraps a child pipeline + sort into a Source.
type sortSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	sort        *exec.Sort
	initialized bool
}

func (s *sortSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (s *sortSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.initialized {
		s.initialized = true
		pipe := &exec.Pipeline{
			Source: s.childSource,
			Ops:    s.childOps,
			Sink:   s.sort,
		}
		if err := pipe.Run(ctx); err != nil {
			return nil, err
		}
	}
	return s.sort.Next(ctx)
}

func (s *sortSourceAdapter) Close() error {
	s.sort.Close()
	return s.childSource.Close()
}

// newScannerSource creates a scanner exec.Source from the catalog
func newScannerSource(cat *catalog.Catalog, tableName string) exec.Source {
	return &scannerExecSource{
		catalog:   cat,
		tableName: tableName,
	}
}

type scannerExecSource struct {
	catalog   *catalog.Catalog
	tableName string
	scanner   *scanSourceInner
}

type scanSourceInner struct {
	cat       *catalog.Catalog
	tableName string
	files     []catalog.FileEntry
	idx       int
	schema    []parquet.Column
}

func (s *scannerExecSource) Init(ctx context.Context) error {
	manifest, err := s.catalog.GetManifest(ctx, s.tableName)
	if err != nil {
		return err
	}
	tableMeta, err := s.catalog.GetTable(ctx, s.tableName)
	if err != nil {
		return err
	}

	var files []catalog.FileEntry
	for _, p := range manifest.Partitions {
		files = append(files, p.Files...)
	}

	s.scanner = &scanSourceInner{
		cat:       s.catalog,
		tableName: s.tableName,
		files:     files,
		schema:    tableMeta.Schema.Columns,
	}
	return nil
}

func (s *scannerExecSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return s.scanner.next(ctx)
}

func (s *scannerExecSource) Close() error { return nil }

func (inner *scanSourceInner) next(ctx context.Context) (*batch.RecordBatch, error) {
	for inner.idx < len(inner.files) {
		file := inner.files[inner.idx]
		inner.idx++

		rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), file.Path)
		if err != nil {
			continue
		}

		data, err := readAll(rc)
		rc.Close()
		if err != nil {
			continue
		}

		reader, err := parquet.NewReader(bytesReader(data), int64(len(data)))
		if err != nil {
			continue
		}

		rows, err := reader.ReadRows(nil)
		if err != nil || len(rows) == 0 {
			continue
		}

		return fromRows(inner.schema, rows), nil
	}
	return nil, nil
}

// wrapExpr adapts an expr.Expr into an exec.Expression function.
func wrapExpr(e expr.Expr) exec.Expression {
	return func(b *batch.RecordBatch, row int) any {
		return e.Eval(b, row)
	}
}

// wrapPredicate adapts an expr.Expr into an exec.Predicate function.
func wrapPredicate(e expr.Expr) exec.Predicate {
	return func(b *batch.RecordBatch, row int) bool {
		v := e.Eval(b, row)
		if v == nil {
			return false
		}
		if bv, ok := v.(bool); ok {
			return bv
		}
		return false
	}
}
