// Package physical converts logical plans to physical execution plans.
package physical

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/engine/expr"
	"github.com/derekmwright/caelum/internal/engine/memory"
	"github.com/derekmwright/caelum/internal/planner/logical"
	plansql "github.com/derekmwright/caelum/internal/planner/sql"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// PhysicalPlan represents an executable query plan.
type PhysicalPlan struct {
	Pipeline *exec.Pipeline
	Stages   []Stage // for distributed execution
}

// Stage represents a unit of distributed work with metadata for task creation.
type Stage struct {
	ID           string
	Type         string // scan, aggregate, sort, hash_join, broadcast_join, window
	ClusterID    string // target cluster for routing ("" = local/coordinator's cluster)
	Dependencies []string
	Tasks        int

	// Scan metadata
	TableName       string
	Columns         []string
	PartitionFilter map[string]string
	ScanFiles       []string // files to distribute across scan tasks
	FilterExprs     []string // SQL filter expressions pushed down to scan

	// Aggregate metadata
	GroupByCols []string
	AggSpecs   []AggSpec

	// Sort metadata
	SortKeys []SortKeySpec
	Limit    int

	// Join metadata
	JoinType      string // inner, left, right, full, cross
	JoinLeftKeys  []string
	JoinRightKeys []string
	LeftDepStage  string // stage providing probe (left) side
	RightDepStage string // stage providing build (right) side

	// Window metadata
	WindowCols []WindowColSpec
}

// WindowColSpec defines a window function column in a stage.
type WindowColSpec struct {
	Func        string
	InputCol    string
	OutputCol   string
	PartitionBy []string
	OrderBy     []SortKeySpec
	Frame       *logical.WindowFrameSpec
}

// AggSpec defines an aggregation in a stage.
type AggSpec struct {
	Func      string
	InputCol  string
	OutputCol string
}

// SortKeySpec defines a sort key in a stage.
type SortKeySpec struct {
	Column    string
	Desc      bool
	NullsLast bool
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
	catalog        *catalog.Catalog
	subqueryRunner expr.SubqueryRunner
	planCtx        context.Context   // context from the current Plan() call, used by subquery runner
	ctes           []plansql.CTEDef  // CTE definitions from the current query, for subquery resolution
	MemoryBudget   int64             // per-query memory budget in bytes (0 = unlimited)
	SpillDir       string            // directory for spill files (empty = os temp dir)
}

// NewPlanner creates a new physical planner.
func NewPlanner(cat *catalog.Catalog) *Planner {
	p := &Planner{catalog: cat}
	// Create a subquery runner that re-uses this planner for nested queries
	p.subqueryRunner = p.makeSubqueryRunner()
	return p
}

// makeSubqueryRunner creates a SubqueryRunner that executes SQL via this planner.
// Uses the planCtx stored during Plan() so subqueries respect the parent context's
// cancellation and timeout.
func (p *Planner) makeSubqueryRunner() expr.SubqueryRunner {
	return func(sql string) ([]map[string]any, error) {
		ctx := p.planCtx
		if ctx == nil {
			ctx = context.Background()
		}
		return p.executeSubquery(ctx, sql)
	}
}

// executeSubquery parses and executes a SQL subquery, returning result rows.
func (p *Planner) executeSubquery(ctx context.Context, sql string) ([]map[string]any, error) {
	// Parse using our SQL parser
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("subquery parse error: %w", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return nil, fmt.Errorf("subquery extract error: %w", err)
	}

	// Build logical plan — merge outer CTEs so subqueries can reference
	// CTE tables defined in the enclosing WITH clause.
	var logicalPlan *logical.Node
	if len(p.ctes) > 0 {
		merged := append(p.ctes, info.CTEs...)
		logicalPlan, err = logical.BuildFromSelectWithCTEs(info, merged)
	} else {
		logicalPlan, err = logical.BuildFromSelect(info)
	}
	if err != nil {
		return nil, fmt.Errorf("subquery plan error: %w", err)
	}

	// Build physical pipeline
	source, ops, sink, err := p.buildPipeline(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("subquery execution plan error: %w", err)
	}

	// Execute
	pipeline := &exec.Pipeline{Source: source, Ops: ops, Sink: sink}
	if err := pipeline.Run(ctx); err != nil {
		return nil, fmt.Errorf("subquery execution error: %w", err)
	}

	// Collect results from the sink
	collectSink, ok := sink.(*exec.CollectSink)
	if !ok {
		return nil, fmt.Errorf("unexpected sink type for subquery")
	}
	return collectSink.Rows, nil
}

// AnnotateScanColumns walks the logical plan tree and populates ScanColumns
// on Scan nodes from the catalog. This enables the logical optimizer to resolve
// unqualified column references for filter pushdown through joins.
func (p *Planner) AnnotateScanColumns(ctx context.Context, node *logical.Node) {
	if node == nil {
		return
	}
	if node.Type == logical.NodeScan && node.TableName != "" {
		table, err := p.catalog.GetTable(ctx, node.TableName)
		if err == nil {
			cols := make([]string, len(table.Schema.Columns))
			for i, c := range table.Schema.Columns {
				cols[i] = c.Name
			}
			node.ScanColumns = cols
		}
	}
	for _, child := range node.Children {
		p.AnnotateScanColumns(ctx, child)
	}
}

// Plan converts a logical plan to a physical plan for local execution.
func (p *Planner) Plan(ctx context.Context, node *logical.Node) (*PhysicalPlan, error) {
	p.planCtx = ctx // store for subquery runner context propagation
	// Propagate CTE definitions from the logical plan so scalar subqueries
	// (e.g., in WHERE/HAVING) can resolve CTE table references.
	if len(node.CTEs) > 0 {
		p.ctes = node.CTEs
	}
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

// ExpandFederatedScans checks if scan stages reference tables that exist on
// multiple clusters. If so, it splits the scan into per-cluster scan stages
// and rewrites downstream dependencies. Returns stages unchanged if only one
// cluster has the table (or if federation lookup fails).
func (p *Planner) ExpandFederatedScans(stages []Stage) []Stage {
	clusters, err := p.catalog.ListClusters()
	if err != nil || len(clusters) <= 1 {
		return stages // single cluster or error — no expansion needed
	}

	// Build cluster → table set
	clusterTables := make(map[string]map[string]bool, len(clusters))
	for _, c := range clusters {
		m := make(map[string]bool, len(c.Tables))
		for _, t := range c.Tables {
			m[t] = true
		}
		clusterTables[c.ClusterID] = m
	}

	localID := p.catalog.ClusterID()
	var expanded []Stage
	oldToNew := map[string][]string{} // original stageID → replacement stageIDs

	for _, stage := range stages {
		if stage.Type != "scan" || stage.TableName == "" {
			expanded = append(expanded, stage)
			continue
		}

		// Find all clusters that have this table
		var targetClusters []string
		for _, c := range clusters {
			if clusterTables[c.ClusterID][stage.TableName] {
				targetClusters = append(targetClusters, c.ClusterID)
			}
		}

		if len(targetClusters) <= 1 {
			// Single cluster — tag with local ID, no split
			stage.ClusterID = localID
			expanded = append(expanded, stage)
			continue
		}

		// Split into per-cluster scan stages
		var replacements []string
		for _, cid := range targetClusters {
			newStage := stage // copy value
			newStage.ID = fmt.Sprintf("%s-%s", stage.ID, cid)
			newStage.ClusterID = cid

			if cid != localID {
				// Get scan files from remote manifest
				manifest, err := p.catalog.GetRemoteManifest(cid, stage.TableName)
				if err != nil {
					continue // skip clusters we can't read
				}
				var files []string
				for _, part := range manifest.Partitions {
					if len(stage.PartitionFilter) > 0 && len(part.Values) > 0 {
						if !matchesPartitionFilter(part.Values, stage.PartitionFilter) {
							continue
						}
					}
					for _, f := range part.Files {
						files = append(files, f.Path)
					}
				}
				newStage.ScanFiles = files
				if len(files) > 0 {
					newStage.Tasks = len(files)
				}
			}

			replacements = append(replacements, newStage.ID)
			expanded = append(expanded, newStage)
		}
		oldToNew[stage.ID] = replacements
	}

	if len(oldToNew) == 0 {
		return expanded // nothing was split
	}

	// Rewrite dependencies: any stage depending on a split scan stage
	// now depends on all its replacement stages
	for i := range expanded {
		var newDeps []string
		for _, dep := range expanded[i].Dependencies {
			if replacements, ok := oldToNew[dep]; ok {
				newDeps = append(newDeps, replacements...)
			} else {
				newDeps = append(newDeps, dep)
			}
		}
		expanded[i].Dependencies = newDeps
	}

	return expanded
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
		tasks := 1
		var scanFiles []string
		partFilter := node.PartitionFilter
		if meta, err := p.catalog.GetManifest(context.Background(), node.TableName); err == nil {
			for _, part := range meta.Partitions {
				if len(partFilter) > 0 && len(part.Values) > 0 {
					if !matchesPartitionFilter(part.Values, partFilter) {
						continue
					}
				}
				for _, f := range part.Files {
					scanFiles = append(scanFiles, f.Path)
				}
			}
			if len(scanFiles) > 0 {
				tasks = len(scanFiles)
			}
		}
		stage := Stage{
			ID:              stageID,
			Type:            "scan",
			Tasks:           tasks,
			TableName:       node.TableName,
			PartitionFilter: partFilter,
			ScanFiles:       scanFiles,
		}
		*stages = append(*stages, stage)
		if parentID != nil {
			for i := range *stages {
				if (*stages)[i].ID == *parentID {
					(*stages)[i].Dependencies = append((*stages)[i].Dependencies, stageID)
				}
			}
		}

	case logical.NodeAggregate:
		stageID := fmt.Sprintf("aggregate-%d", len(*stages))
		for _, child := range node.Children {
			p.walkStages(child, stages, &stageID)
		}
		var aggSpecs []AggSpec
		for _, agg := range node.AggExprs {
			aggSpecs = append(aggSpecs, AggSpec{
				Func:      agg.Func,
				InputCol:  agg.InputCol,
				OutputCol: agg.OutputCol,
			})
		}
		groupBy := make([]string, len(node.GroupBy))
		copy(groupBy, node.GroupBy)

		stage := Stage{
			ID:          stageID,
			Type:        "aggregate",
			Tasks:       1,
			GroupByCols: groupBy,
			AggSpecs:    aggSpecs,
		}
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
		var sortKeys []SortKeySpec
		for _, ob := range node.OrderBy {
			sortKeys = append(sortKeys, SortKeySpec{Column: ob.Column, Desc: ob.Desc, NullsLast: resolveNullsLast(ob)})
		}
		stage := Stage{
			ID:       stageID,
			Type:     "sort",
			Tasks:    1,
			SortKeys: sortKeys,
		}
		for _, s := range *stages {
			stage.Dependencies = append(stage.Dependencies, s.ID)
		}
		// Propagate limit from parent if present
		*stages = append(*stages, stage)

	case logical.NodeLimit:
		// Pass limit info down to sort stage if child is sort
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		// Tag the last sort stage with our limit
		for i := len(*stages) - 1; i >= 0; i-- {
			if (*stages)[i].Type == "sort" {
				(*stages)[i].Limit = node.LimitVal
				break
			}
		}

	case logical.NodeJoin:
		stageID := fmt.Sprintf("join-%d", len(*stages))
		stagesBefore := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, &stageID)
		}
		joinType := "hash_join"
		if p.isBroadcastCandidate(node) {
			joinType = "broadcast_join"
		}

		// Identify left (probe) and right (build) dependency stages
		var leftDep, rightDep string
		stagesAfter := *stages
		newStages := stagesAfter[stagesBefore:]
		if len(newStages) >= 2 {
			leftDep = newStages[0].ID  // first child = left
			rightDep = newStages[1].ID // second child = right
		} else if len(newStages) == 1 {
			leftDep = newStages[0].ID
		}

		// Map logical join type to canonical short form
		jt := mapJoinType(node.JoinType)

		// Extract join keys from condition (cross joins have no ON clause)
		var leftKeys, rightKeys []string
		if jt != "cross" {
			leftKeys, rightKeys = parseJoinKeys(node.JoinCond)
		}

		stage := Stage{
			ID:            stageID,
			Type:          joinType,
			Tasks:         1,
			JoinType:      jt,
			JoinLeftKeys:  leftKeys,
			JoinRightKeys: rightKeys,
			LeftDepStage:  leftDep,
			RightDepStage: rightDep,
		}
		for _, s := range *stages {
			if s.Type == "scan" {
				stage.Dependencies = append(stage.Dependencies, s.ID)
			}
		}
		*stages = append(*stages, stage)

	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		// Each side of the set operation runs independently; merge results at the end.
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}

	case logical.NodeFilter:
		// Walk children first. If a child is a scan, attach filter expressions
		// to the scan stage for predicate pushdown.
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		// Attach filter expressions to the most recent scan stage
		if len(node.Predicates) > 0 && len(*stages) > 0 {
			lastStage := &(*stages)[len(*stages)-1]
			if lastStage.Type == "scan" {
				for _, pred := range node.Predicates {
					if pred.Raw != "" {
						lastStage.FilterExprs = append(lastStage.FilterExprs, pred.Raw)
					} else if pred.ASTExpr != nil {
						lastStage.FilterExprs = append(lastStage.FilterExprs, pred.ASTExpr.String())
					}
				}
			}
		}

	case logical.NodeWindow:
		stageID := fmt.Sprintf("window-%d", len(*stages))
		for _, child := range node.Children {
			p.walkStages(child, stages, &stageID)
		}
		var winCols []WindowColSpec
		for _, we := range node.WindowExprs {
			var orderBy []SortKeySpec
			for _, ob := range we.OrderBy {
				orderBy = append(orderBy, SortKeySpec{Column: ob.Column, Desc: ob.Desc, NullsLast: resolveNullsLast(ob)})
			}
			winCols = append(winCols, WindowColSpec{
				Func:        we.Func,
				InputCol:    we.InputCol,
				OutputCol:   we.OutputCol,
				PartitionBy: we.PartitionBy,
				OrderBy:     orderBy,
				Frame:       we.Frame,
			})
		}
		stage := Stage{
			ID:         stageID,
			Type:       "window",
			Tasks:      1,
			WindowCols: winCols,
		}
		for _, s := range *stages {
			stage.Dependencies = append(stage.Dependencies, s.ID)
		}
		*stages = append(*stages, stage)

	default:
		// Passthrough nodes (Project, Distinct) — walk children
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
	}
}

// isBroadcastCandidate returns true if the right (build) side of a join is
// small enough to broadcast to all workers (heuristic: <= 10 files).
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
	case logical.NodeDistinct:
		return p.buildDistinct(ctx, node)
	case logical.NodeWindow:
		return p.buildWindow(ctx, node)
	case logical.NodeUnion:
		return p.buildSetOp(ctx, node, "union")
	case logical.NodeIntersect:
		return p.buildSetOp(ctx, node, "intersect")
	case logical.NodeExcept:
		return p.buildSetOp(ctx, node, "except")
	default:
		return nil, nil, nil, fmt.Errorf("unsupported plan node: %s", node.Type)
	}
}

func (p *Planner) buildScan(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	scanner := p.newScanner(ctx, node.TableName, node.PartitionFilter, node.RequiredColumns, node.ScanPredicates)
	return scanner, nil, &exec.CollectSink{}, nil
}

func (p *Planner) buildJoin(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) < 2 {
		return nil, nil, nil, fmt.Errorf("join requires two children")
	}

	jt := mapJoinType(node.JoinType)
	joinType := mapExecJoinType(jt)

	// Parse join condition to extract key columns (cross joins have no ON clause)
	var leftKeys, rightKeys []string
	if jt != "cross" {
		leftKeys, rightKeys = parseJoinKeys(node.JoinCond)
		if len(leftKeys) == 0 {
			return nil, nil, nil, fmt.Errorf("could not extract join keys from: %s", node.JoinCond)
		}
	}

	hj := exec.NewHashJoin(joinType, leftKeys, rightKeys)

	// Set build-side table alias for column disambiguation in self-joins
	if alias := findScanAlias(node.Children[1]); alias != "" {
		hj.BuildTableAlias = alias
	}

	// Attach memory tracker and spill manager if budget is configured
	if p.MemoryBudget > 0 {
		tracker := memory.NewTracker("hash_join", p.MemoryBudget)
		hj.MemTracker = tracker
		spillDir := p.SpillDir
		if spillDir == "" {
			spillDir = os.TempDir()
		}
		if sm, err := memory.NewSpillManager(spillDir, tracker); err == nil {
			hj.Spill = sm
		}
	}

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

	// Fix key assignment: parseJoinKeys takes columns from the SQL literally
	// (left of "=" → leftKey, right → rightKey), but the SQL may put the
	// build-side column on the left (e.g., "JOIN t ON t.id = probe.id").
	// After building, we know the build schema; swap any misassigned pairs.
	hj.FixKeyAssignment()

	// For semi/anti joins with non-equality filter conditions (from decorrelated EXISTS),
	// compile the filter into a function that checks each candidate build row.
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter != "" {
		hj.SemiAntiFilter = buildSemiAntiFilter(node.JoinFilter)
	}

	// Left side (probe) streams through
	leftSource, leftOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building join left side: %w", err)
	}

	probe := hj.Probe()
	leftOps = append(leftOps, probe)

	// For RIGHT and FULL OUTER joins, unmatched build-side rows must be
	// flushed after all probe batches have been processed. Wrap the source
	// so that FlushUnmatched is called at the end.
	if joinType == exec.RightJoin || joinType == exec.FullOuterJoin {
		return &joinFlushSource{
			inner:    leftSource,
			innerOps: leftOps,
			probe:    probe,
		}, nil, &exec.CollectSink{}, nil
	}

	return leftSource, leftOps, &exec.CollectSink{}, nil
}

// joinFlushSource wraps a join probe pipeline and appends unmatched build-side
// rows (via FlushUnmatched) after the probe side is exhausted.
type joinFlushSource struct {
	inner      exec.Source
	innerOps   []exec.UnaryOperator
	probe      *exec.HashJoinProbe
	leftSchema []parquet.Column
	pipeline   *pipelineSource
	flushed    bool
	flushBatch *batch.RecordBatch
}

func (s *joinFlushSource) Init(ctx context.Context) error {
	s.pipeline = &pipelineSource{source: s.inner, ops: s.innerOps}
	return s.pipeline.Init(ctx)
}

func (s *joinFlushSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.flushed {
		b, err := s.pipeline.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b != nil {
			// Capture left-side schema from first result batch
			if s.leftSchema == nil && len(b.Schema) > 0 {
				s.leftSchema = b.Schema
			}
			return b, nil
		}
		// Probe exhausted — flush unmatched build-side rows
		s.flushed = true
		if s.leftSchema != nil {
			s.flushBatch = s.probe.FlushUnmatched(s.leftSchema)
		}
	}
	if s.flushBatch != nil {
		b := s.flushBatch
		s.flushBatch = nil
		return b, nil
	}
	return nil, nil
}

func (s *joinFlushSource) Close() error {
	return s.pipeline.Close()
}

// mapJoinType converts a join type string (e.g. "join", "left join",
// "right join", "full outer join", "cross join") to a canonical short
// form used by the distributed planner.
func mapJoinType(vt string) string {
	lower := strings.ToLower(strings.TrimSpace(vt))
	switch {
	case lower == "cross" || strings.Contains(lower, "cross"):
		return "cross"
	case strings.Contains(lower, "full"):
		return "full"
	case strings.Contains(lower, "right"):
		return "right"
	case strings.Contains(lower, "semi"):
		return "semi"
	case strings.Contains(lower, "anti"):
		return "anti"
	case strings.Contains(lower, "left"):
		return "left"
	default:
		return "inner"
	}
}

// mapExecJoinType converts a canonical join type string to exec.JoinType.
func mapExecJoinType(jt string) exec.JoinType {
	switch jt {
	case "left":
		return exec.LeftJoin
	case "right":
		return exec.RightJoin
	case "full":
		return exec.FullOuterJoin
	case "cross":
		return exec.CrossJoin
	case "semi":
		return exec.SemiJoin
	case "anti":
		return exec.AntiJoin
	default:
		return exec.InnerJoin
	}
}

// buildSemiAntiFilter compiles a non-equality join filter string (e.g., "l_suppkey != l_suppkey")
// into a function that evaluates the condition on probe and build batch rows.
// Convention: left of operator = probe column, right = build column.
func buildSemiAntiFilter(filter string) func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	type filterCond struct {
		probeCol string
		op       string
		buildCol string
	}
	var conds []filterCond
	parts := strings.Split(strings.ToLower(filter), " and ")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Try operators from longest to shortest to avoid partial matches
		for _, op := range []string{"!=", ">=", "<=", "<>", ">", "<"} {
			idx := strings.Index(part, " "+op+" ")
			if idx >= 0 {
				left := cleanExpr(strings.TrimSpace(part[:idx]))
				right := cleanExpr(strings.TrimSpace(part[idx+len(op)+2:]))
				conds = append(conds, filterCond{probeCol: left, op: op, buildCol: right})
				break
			}
		}
	}

	if len(conds) == 0 {
		return nil
	}

	return func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		for _, c := range conds {
			pv := probe.ColumnByName(c.probeCol)
			bv := build.ColumnByName(c.buildCol)
			if pv == nil || bv == nil {
				return false
			}
			probeVal := pv.GetValue(probeRow)
			buildVal := bv.GetValue(buildRow)
			if !evalJoinFilterOp(probeVal, buildVal, c.op) {
				return false
			}
		}
		return true
	}
}

// evalJoinFilterOp evaluates a comparison operator on two values.
func evalJoinFilterOp(a, b any, op string) bool {
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	switch op {
	case "!=", "<>":
		return as != bs
	case ">":
		return as > bs
	case "<":
		return as < bs
	case ">=":
		return as >= bs
	case "<=":
		return as <= bs
	default:
		return as == bs
	}
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

	// Collect outer table aliases and columns for correlated subquery detection
	outerTables := collectTableAliases(node.Children[0])
	outerCols := collectOuterColumns(node.Children[0])

	for _, pred := range node.Predicates {
		filter := p.buildFilterOp(pred, outerTables, outerCols)
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

	// If the child (or child chain through Filter/HAVING) leads to an Aggregate,
	// skip the projection when possible — the aggregate already produces correctly
	// named output columns (group-by cols + agg output cols).
	// Keep the projection when:
	//   1. Any non-aggregate projection has a complex AST expression (e.g., SUM(x) * 0.0001)
	//   2. Any projection renames a column via alias (e.g., l_suppkey AS supplier_no)
	if hasAggregateAncestor(child) {
		needsProject := false
		for _, proj := range node.Projections {
			if proj.ASTExpr != nil && !proj.IsAgg && !isSimpleColRef(proj.ASTExpr) {
				needsProject = true
				break
			}
			// Aggregate projection with a wrapping scalar function
			// e.g., format_bytes(SUM(rx_bytes)) — the outer function must be
			// applied as a post-aggregate projection.
			if proj.IsAgg && proj.ASTExpr != nil {
				if fn, ok := proj.ASTExpr.(*plansql.FuncCallNode); ok {
					if !plansql.IsAggregate(fn.Name) {
						needsProject = true
						break
					}
				}
				if _, ok := proj.ASTExpr.(*plansql.BinaryOp); ok {
					needsProject = true
					break
				}
			}
			// Check for column rename on non-aggregate columns: alias differs
			// from source column/expression (aggregate columns already use
			// the alias as their OutputCol, so no rename needed).
			if !proj.IsAgg && proj.Alias != "" {
				src := proj.Column
				if src == "" {
					src = proj.Expr
				}
				if proj.Alias != src {
					needsProject = true
					break
				}
			}
		}
		if !needsProject {
			return p.buildPipeline(ctx, child)
		}
	}

	source, ops, sink, err := p.buildPipeline(ctx, child)
	if err != nil {
		return nil, nil, nil, err
	}

	aggNode := findAggregateAncestor(child)
	isOverAggregate := aggNode != nil

	// Build a map from GROUP BY expression string → synthetic column name.
	// When a SELECT expression matches a GROUP BY expression (e.g.,
	// SUBSTR(c_phone, 1, 2)), the original columns may not exist in the
	// aggregate output — use the synthetic column instead.
	gbExprToSyn := map[string]string{}
	if isOverAggregate && len(aggNode.GroupByExprs) == len(aggNode.GroupBy) {
		for i, gbExpr := range aggNode.GroupByExprs {
			if gbExpr != nil && !isSimpleColRef(gbExpr) {
				gbExprToSyn[gbExpr.String()] = fmt.Sprintf("__gb_expr_%d", i)
			}
		}
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

		// When projecting over an aggregate, aggregate columns should reference
		// their output column name (the alias), not the raw expression.
		if isOverAggregate && proj.IsAgg && proj.Alias != "" {
			colRef = proj.Alias
		}

		// Try to compile from AST expression first, fall back to ColumnRef
		var expression exec.Expression

		// When projecting over an aggregate, check if this SELECT expression
		// matches a GROUP BY expression that was pre-computed into a synthetic
		// column. If so, use a ColumnRef to the synthetic column instead of
		// re-evaluating the expression (the original columns are gone).
		if isOverAggregate && proj.ASTExpr != nil && !proj.IsAgg {
			if synName, ok := gbExprToSyn[proj.ASTExpr.String()]; ok {
				expression = exec.ColumnRef(synName)
			}
		}

		// Handle aggregate projections with wrapping scalar functions,
		// e.g., format_bytes(SUM(rx_bytes)). Replace the inner aggregate
		// AST node with a ColRef to the aggregate output column, then
		// compile the modified AST as a scalar expression.
		if expression == nil && proj.IsAgg && proj.ASTExpr != nil && isOverAggregate {
			innerAgg := plansql.FindNestedAggregate(proj.ASTExpr)
			if innerAgg != nil {
				outerFn, isFunc := proj.ASTExpr.(*plansql.FuncCallNode)
				if isFunc && !plansql.IsAggregate(outerFn.Name) {
					// Build the aggregate output column name
					aggOutputCol := strings.ToLower(innerAgg.Name) + "("
					if innerAgg.Distinct {
						aggOutputCol += "distinct "
					}
					if innerAgg.Star {
						aggOutputCol += "*"
					} else if len(innerAgg.Args) > 0 {
						var argStrs []string
						for _, a := range innerAgg.Args {
							argStrs = append(argStrs, a.String())
						}
						aggOutputCol += strings.Join(argStrs, ", ")
					}
					aggOutputCol += ")"
					// Replace inner aggregate with a column reference in the AST
					rewritten := replaceAggWithColRef(proj.ASTExpr, innerAgg, aggOutputCol)
					compiled, compErr := expr.CompileWithRunner(rewritten, p.subqueryRunner)
					if compErr == nil {
						expression = wrapExpr(compiled)
					}
				}
			}
		}

		if expression == nil && proj.ASTExpr != nil && !proj.IsAgg {
			outerTables := collectTableAliases(child)
			outerCols := collectOuterColumns(child)
			var compiled expr.Expr
			var compErr error
			if len(outerTables) > 0 {
				if len(outerCols) > 0 {
					compiled, compErr = expr.CompileWithFullScope(proj.ASTExpr, p.subqueryRunner, outerTables, outerCols)
				} else {
					compiled, compErr = expr.CompileWithScope(proj.ASTExpr, p.subqueryRunner, outerTables)
				}
			} else {
				compiled, compErr = expr.CompileWithRunner(proj.ASTExpr, p.subqueryRunner)
			}
			if compErr == nil {
				expression = wrapExpr(compiled)
			}
		}
		if expression == nil {
			expression = exec.ColumnRef(colRef)
		}

		// Infer output type: TypeString is the default, resolved at runtime from
		// input schema when column names match. For arithmetic expressions that
		// won't match an input column (e.g., nested aggregate rewrites like
		// __agg_0 * 0.0001), use TypeFloat64.
		outType := parquet.TypeString
		if proj.ASTExpr != nil && !proj.IsAgg {
			if _, isBinOp := proj.ASTExpr.(*plansql.BinaryOp); isBinOp {
				outType = parquet.TypeFloat64
			}
		}

		projCols = append(projCols, exec.ProjectColumn{
			Name: name,
			Type: outType, // Will be resolved at runtime if input column matches
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

	// Detect aggregate inputs that are expressions (not simple column refs).
	// For each, compile the expression and add a pre-aggregate projection
	// that evaluates it into a synthetic column.
	var preProjectCols []exec.ProjectColumn
	syntheticNames := make(map[int]string) // agg index → synthetic column name

	for i, agg := range node.AggExprs {
		if agg.InputExpr != nil && !isSimpleColRef(agg.InputExpr) {
			synName := fmt.Sprintf("__agg_expr_%d", i)
			compiled, compErr := expr.CompileWithRunner(agg.InputExpr, p.subqueryRunner)
			if compErr == nil {
				preProjectCols = append(preProjectCols, exec.ProjectColumn{
					Name: synName,
					Type: parquet.TypeFloat64,
					Expr: wrapExpr(compiled),
				})
				syntheticNames[i] = synName
			}
		}
	}

	var aggCols []exec.AggColumn
	for i, agg := range node.AggExprs {
		fn := parseAggFunc(agg.Func)
		if agg.Distinct && fn == exec.AggCount {
			fn = exec.AggCountDistinct
		}
		inputCol := cleanExpr(agg.InputCol)
		// Use synthetic column name if the input was an expression
		if synName, ok := syntheticNames[i]; ok {
			inputCol = synName
		}
		ac := exec.AggColumn{
			Func:       fn,
			InputCol:   inputCol,
			OutputCol:  agg.OutputCol,
			OutputType: aggOutputType(agg.Func, agg.Distinct),
		}
		// Parse STRING_AGG separator from InputCol (format: "col, 'sep'")
		if fn == exec.AggStringAgg && strings.Contains(inputCol, ",") {
			parts := strings.SplitN(inputCol, ",", 2)
			ac.InputCol = strings.TrimSpace(parts[0])
			sep := strings.TrimSpace(parts[1])
			sep = strings.Trim(sep, "'\"")
			ac.Separator = sep
		}
		// Parse two-column aggregates (format: "col1, col2")
		if (fn == exec.AggCorr || fn == exec.AggCovarSamp || fn == exec.AggCovarPop ||
			fn == exec.AggMinBy || fn == exec.AggMaxBy) && strings.Contains(inputCol, ",") {
			parts := strings.SplitN(inputCol, ",", 2)
			ac.InputCol = strings.TrimSpace(parts[0])
			ac.InputCol2 = strings.TrimSpace(parts[1])
		}
		// Parse percentile value (format: "percentile, col") — e.g., percentile_cont(0.5, amount)
		if (fn == exec.AggPercentileCont || fn == exec.AggPercentileDisc) && strings.Contains(inputCol, ",") {
			parts := strings.SplitN(inputCol, ",", 2)
			pctStr := strings.TrimSpace(parts[0])
			if pv, err := strconv.ParseFloat(pctStr, 64); err == nil {
				ac.Percentile = pv
				ac.InputCol = strings.TrimSpace(parts[1])
			}
		}
		aggCols = append(aggCols, ac)
	}

	groupByCols := make([]string, len(node.GroupBy))
	for i, gb := range node.GroupBy {
		// Preserve table qualifiers for self-join disambiguation (e.g., n1.n_name vs n2.n_name).
		// The aggregate operator resolves qualified names with fallback to unqualified.
		groupByCols[i] = strings.TrimSpace(gb)
	}

	// Handle GROUP BY expressions (e.g., SUBSTR(c_phone, 1, 2)).
	// Compile expression-valued GROUP BY entries into pre-aggregate projections
	// so the aggregate can group by the computed result.
	if len(node.GroupByExprs) == len(node.GroupBy) {
		for i, gbExpr := range node.GroupByExprs {
			if gbExpr != nil && !isSimpleColRef(gbExpr) {
				synName := fmt.Sprintf("__gb_expr_%d", i)
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner)
				if compErr == nil {
					preProjectCols = append(preProjectCols, exec.ProjectColumn{
						Name: synName,
						Type: parquet.TypeString,
						Expr: wrapExpr(compiled),
					})
					groupByCols[i] = synName
				}
			}
		}
	}

	// If we have expression inputs or GROUP BY expressions, build a
	// pass-through projection that keeps all input columns and adds
	// the computed ones.
	if len(preProjectCols) > 0 {
		childOps = append(childOps, &aggPreProject{computed: preProjectCols})
	}

	hashAgg := exec.NewHashAggregate(groupByCols, aggCols)

	// For GROUPING SETS: wrap with a projection that adds NULL columns
	// for the group-by columns not in this particular set.
	var postOps []exec.UnaryOperator
	if len(node.GroupingSetNulls) > 0 {
		hashAgg.NullGroupCols = node.GroupingSetNulls
	}

	// The aggregate acts as both sink and source
	// We need to run childSource -> childOps -> hashAgg(sink), then hashAgg(source) -> collectSink
	return &aggSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		agg:         hashAgg,
	}, postOps, &exec.CollectSink{}, nil
}

// isSimpleColRef returns true if the AST node is a simple column reference
// (no arithmetic, function calls, etc).
// replaceAggWithColRef returns a copy of the AST with the target aggregate
// function node replaced by a ColRef to the given column name.
func replaceAggWithColRef(node plansql.Node, target *plansql.FuncCallNode, colName string) plansql.Node {
	if node == nil {
		return nil
	}
	if fn, ok := node.(*plansql.FuncCallNode); ok && fn == target {
		return &plansql.ColRef{Column: colName}
	}
	switch n := node.(type) {
	case *plansql.FuncCallNode:
		newArgs := make([]plansql.Node, len(n.Args))
		for i, a := range n.Args {
			newArgs[i] = replaceAggWithColRef(a, target, colName)
		}
		return &plansql.FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  replaceAggWithColRef(n.Left, target, colName),
			Op:    n.Op,
			Right: replaceAggWithColRef(n.Right, target, colName),
		}
	case *plansql.ParenNode:
		return &plansql.ParenNode{Inner: replaceAggWithColRef(n.Inner, target, colName)}
	case *plansql.CastNode:
		return &plansql.CastNode{Inner: replaceAggWithColRef(n.Inner, target, colName), TypeName: n.TypeName}
	default:
		return node
	}
}

func isSimpleColRef(node plansql.Node) bool {
	switch node.(type) {
	case *plansql.ColRef:
		return true
	case *plansql.Lit:
		return true
	default:
		return false
	}
}

// aggPreProject is a UnaryOperator that passes through all input columns
// and adds computed expression columns for aggregate inputs.
type aggPreProject struct {
	computed []exec.ProjectColumn
}

func (a *aggPreProject) Init(_ context.Context) error { return nil }

func (a *aggPreProject) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	// If the input has a selection vector, materialize (compact) the pass-through
	// columns first. BytesColumn.Set requires sequential writes (i=0,1,2,...),
	// so we can't write computed columns at sparse selection indices.
	if in.Sel != nil {
		in = a.materialize(in)
	}

	// Build output schema: all input columns + computed columns
	schema := make([]parquet.Column, 0, len(in.Schema)+len(a.computed))
	schema = append(schema, in.Schema...)
	for _, c := range a.computed {
		schema = append(schema, parquet.Column{
			Name:     c.Name,
			Type:     c.Type,
			Nullable: true,
		})
	}

	out := batch.NewRecordBatch(schema, in.Len)

	// Share existing column vectors (zero-copy for pass-through columns)
	for j := 0; j < len(in.Schema); j++ {
		out.Columns[j] = in.Columns[j]
	}

	// Compute expression columns (sequential writes, no selection vector)
	computedOffset := len(in.Schema)
	for i := 0; i < in.Len; i++ {
		for k, c := range a.computed {
			val := c.Expr(in, i)
			out.Columns[computedOffset+k].SetValue(i, val)
		}
	}

	return out, nil
}

// materialize compacts a batch with a selection vector into a dense batch
// with only the selected rows, removing the selection vector.
func (a *aggPreProject) materialize(in *batch.RecordBatch) *batch.RecordBatch {
	n := len(in.Sel)
	out := batch.NewRecordBatch(in.Schema, n)
	for j := range in.Schema {
		src := in.Columns[j]
		dst := out.Columns[j]
		for di, si := range in.Sel {
			if src.Nulls.IsNullFast(int(si)) {
				dst.Nulls.SetNull(di)
				if dst.Type == batch.TypeString || dst.Type == batch.TypeBytes ||
					dst.Type == batch.TypeIPv6 || dst.Type == batch.TypeCIDR || dst.Type == batch.TypeUUID {
					dst.BytesData.Set(di, nil)
				}
			} else {
				dst.Nulls.SetValid(di)
				switch dst.Type {
				case batch.TypeBool:
					dst.BoolData[di] = src.BoolData[si]
				case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
					dst.Int32Data[di] = src.Int32Data[si]
				case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
					dst.Int64Data[di] = src.Int64Data[si]
				case batch.TypeFloat32:
					dst.Float32Data[di] = src.Float32Data[si]
				case batch.TypeFloat64:
					dst.Float64Data[di] = src.Float64Data[si]
				case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
					dst.BytesData.Set(di, src.BytesData.Value(int(si)))
				case batch.TypeDecimal:
					dst.DecimalData.Data[di] = src.DecimalData.Data[si]
				}
			}
		}
	}
	return out
}

func (a *aggPreProject) Close() error { return nil }

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
			Column:    cleanExpr(ob.Column),
			Order:     order,
			NullsLast: resolveNullsLast(ob),
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

func (p *Planner) buildWindow(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("window has no child")
	}

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	var winCols []exec.WindowColumn
	for _, we := range node.WindowExprs {
		var orderKeys []exec.SortKey
		for _, ob := range we.OrderBy {
			order := exec.Ascending
			if ob.Desc {
				order = exec.Descending
			}
			orderKeys = append(orderKeys, exec.SortKey{
				Column:    ob.Column,
				Order:     order,
				NullsLast: resolveNullsLast(ob),
			})
		}
		wc := exec.WindowColumn{
			Func:        parseWindowFunc(we.Func),
			InputCol:    cleanExpr(we.InputCol),
			OutputCol:   we.OutputCol,
			OutputType:  windowOutputType(we.Func),
			PartitionBy: we.PartitionBy,
			OrderBy:     orderKeys,
		}
		if we.Frame != nil {
			wc.Frame = &exec.WindowFrameSpec{
				Mode: we.Frame.Mode,
				Start: exec.WindowBound{Type: we.Frame.Start.Type, Offset: we.Frame.Start.Offset},
				End:   exec.WindowBound{Type: we.Frame.End.Type, Offset: we.Frame.End.Offset},
			}
		}
		// Parse function-specific arguments from InputCol
		fn := strings.ToLower(we.Func)
		if fn == "ntile" {
			if n, err := strconv.Atoi(strings.TrimSpace(wc.InputCol)); err == nil {
				wc.NtileBuckets = n
			}
			wc.InputCol = ""
		} else if fn == "nth_value" {
			parts := strings.SplitN(wc.InputCol, ",", 2)
			if len(parts) > 0 {
				wc.InputCol = strings.TrimSpace(parts[0])
			}
			if len(parts) >= 2 {
				if n, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					wc.NthValueN = n
				}
			}
		} else if fn == "lag" || fn == "lead" {
			parts := strings.SplitN(wc.InputCol, ",", 3)
			if len(parts) > 0 {
				wc.InputCol = strings.TrimSpace(parts[0])
			}
			if len(parts) >= 2 {
				if offset, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					wc.LagLeadOffset = offset
				}
			}
			if len(parts) >= 3 {
				defStr := strings.TrimSpace(parts[2])
				if v, err := strconv.ParseFloat(defStr, 64); err == nil {
					wc.LagLeadDefault = v
				} else {
					wc.LagLeadDefault = defStr
				}
			}
		}
		winCols = append(winCols, wc)
	}

	winOp := exec.NewWindow(winCols)

	return &windowSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		win:         winOp,
	}, nil, &exec.CollectSink{}, nil
}

func (p *Planner) buildDistinct(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("distinct has no child")
	}

	source, ops, sink, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	ops = append(ops, exec.NewDistinct())
	return source, ops, sink, nil
}

func (p *Planner) buildSetOp(ctx context.Context, node *logical.Node, op string) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) < 2 {
		return nil, nil, nil, fmt.Errorf("%s requires two children", op)
	}

	leftSource, leftOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building %s left side: %w", op, err)
	}

	rightSource, rightOps, _, err := p.buildPipeline(ctx, node.Children[1])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("building %s right side: %w", op, err)
	}

	src := &setOpSourceAdapter{
		leftSource:  leftSource,
		leftOps:     leftOps,
		rightSource: rightSource,
		rightOps:    rightOps,
		all:         node.UnionAll,
		op:          op,
	}

	return src, nil, &exec.CollectSink{}, nil
}

// setOpSourceAdapter executes both child pipelines and applies the set operation
// (union, intersect, or except) to produce the result.
type setOpSourceAdapter struct {
	leftSource  exec.Source
	leftOps     []exec.UnaryOperator
	rightSource exec.Source
	rightOps    []exec.UnaryOperator
	all         bool
	op          string // "union", "intersect", "except"

	batches     []*batch.RecordBatch
	idx         int
	initialized bool
}

func (u *setOpSourceAdapter) Init(_ context.Context) error { return nil }

func (u *setOpSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !u.initialized {
		u.initialized = true

		// Run left pipeline
		leftSink := &exec.CollectSink{}
		leftPipe := &exec.Pipeline{
			Source: u.leftSource,
			Ops:    u.leftOps,
			Sink:   leftSink,
		}
		if err := leftPipe.Run(ctx); err != nil {
			return nil, fmt.Errorf("executing %s left side: %w", u.op, err)
		}

		// Run right pipeline
		rightSink := &exec.CollectSink{}
		rightPipe := &exec.Pipeline{
			Source: u.rightSource,
			Ops:    u.rightOps,
			Sink:   rightSink,
		}
		if err := rightPipe.Run(ctx); err != nil {
			return nil, fmt.Errorf("executing %s right side: %w", u.op, err)
		}

		var resultRows []map[string]any

		switch u.op {
		case "intersect":
			resultRows = intersectRows(leftSink.Rows, rightSink.Rows, u.all)
		case "except":
			resultRows = exceptRows(leftSink.Rows, rightSink.Rows, u.all)
		default: // "union"
			resultRows = append(leftSink.Rows, rightSink.Rows...)
			if !u.all {
				resultRows = deduplicateRows(resultRows)
			}
		}

		if len(resultRows) > 0 {
			var schema []parquet.Column
			if leftBatches := leftSink.Batches(); len(leftBatches) > 0 {
				schema = leftBatches[0].Schema
			} else if rightBatches := rightSink.Batches(); len(rightBatches) > 0 {
				schema = rightBatches[0].Schema
			}
			if schema != nil {
				u.batches = []*batch.RecordBatch{batch.FromRows(schema, resultRows)}
			}
		}
	}

	if u.idx >= len(u.batches) {
		return nil, nil
	}
	b := u.batches[u.idx]
	u.idx++
	return b, nil
}

func (u *setOpSourceAdapter) Close() error {
	err := u.leftSource.Close()
	if e := u.rightSource.Close(); e != nil && err == nil {
		err = e
	}
	for _, op := range u.leftOps {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	for _, op := range u.rightOps {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (u *setOpSourceAdapter) RowsScanned() int64 {
	var total int64
	if sp, ok := u.leftSource.(exec.ScanStatsProvider); ok {
		total += sp.RowsScanned()
	}
	if sp, ok := u.rightSource.(exec.ScanStatsProvider); ok {
		total += sp.RowsScanned()
	}
	return total
}

// intersectRows returns rows that appear in both left and right.
// If all is true, preserves duplicate counts (min of left/right occurrences).
func intersectRows(left, right []map[string]any, all bool) []map[string]any {
	rightSet := make(map[string]int, len(right))
	for _, row := range right {
		rightSet[rowHashKey(row)]++
	}

	if all {
		result := make([]map[string]any, 0)
		for _, row := range left {
			key := rowHashKey(row)
			if rightSet[key] > 0 {
				result = append(result, row)
				rightSet[key]--
			}
		}
		return result
	}

	// INTERSECT (distinct): deduplicate, then keep only rows in both
	seen := make(map[string]struct{}, len(left))
	result := make([]map[string]any, 0)
	for _, row := range left {
		key := rowHashKey(row)
		if _, already := seen[key]; already {
			continue
		}
		seen[key] = struct{}{}
		if rightSet[key] > 0 {
			result = append(result, row)
		}
	}
	return result
}

// exceptRows returns rows from left that do not appear in right.
// If all is true, each right occurrence removes one left occurrence.
func exceptRows(left, right []map[string]any, all bool) []map[string]any {
	rightSet := make(map[string]int, len(right))
	for _, row := range right {
		rightSet[rowHashKey(row)]++
	}

	if all {
		result := make([]map[string]any, 0)
		for _, row := range left {
			key := rowHashKey(row)
			if rightSet[key] > 0 {
				rightSet[key]--
			} else {
				result = append(result, row)
			}
		}
		return result
	}

	// EXCEPT (distinct): deduplicate left, exclude rows in right
	seen := make(map[string]struct{}, len(left))
	result := make([]map[string]any, 0)
	for _, row := range left {
		key := rowHashKey(row)
		if _, already := seen[key]; already {
			continue
		}
		seen[key] = struct{}{}
		if rightSet[key] == 0 {
			result = append(result, row)
		}
	}
	return result
}

// deduplicateRows removes duplicate rows from a slice of row maps.
func deduplicateRows(rows []map[string]any) []map[string]any {
	seen := make(map[string]struct{}, len(rows))
	result := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		key := rowHashKey(row)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// rowHashKey generates a unique string key from a row's column values.
func rowHashKey(row map[string]any) string {
	// Sort keys for deterministic hashing
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	// Simple sort for determinism
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(0)
		}
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(fmt.Sprintf("%v", row[k]))
	}
	return b.String()
}

func (p *Planner) newScanner(ctx context.Context, tableName string, partFilter map[string]string, requiredCols []string, scanPreds []logical.Predicate) exec.Source {
	// Get table schema
	tableMeta, err := p.catalog.GetTable(ctx, tableName)
	if err != nil {
		return &exec.SliceSource{}
	}
	_ = tableMeta

	// Create a scanner source that reads from the catalog
	return &catalogScanSource{
		catalog:         p.catalog,
		tableName:       tableName,
		partitionFilter: partFilter,
		requiredCols:    requiredCols,
		scanPreds:       scanPreds,
	}
}

// catalogScanSource adapts the scan.Scanner to exec.Source
type catalogScanSource struct {
	catalog         *catalog.Catalog
	tableName       string
	partitionFilter map[string]string
	requiredCols    []string
	scanPreds       []logical.Predicate
	inner           exec.Source
}

func (s *catalogScanSource) Init(ctx context.Context) error {
	// Use the scan package scanner
	sc := newScannerSource(s.catalog, s.tableName, s.partitionFilter, s.requiredCols, s.scanPreds)
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

func (s *catalogScanSource) RowsScanned() int64 {
	if sp, ok := s.inner.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

// RecordBatch type alias for convenience
type RecordBatch = batch.RecordBatch

func (p *Planner) buildFilterOp(pred logical.Predicate, outerTables map[string]bool, outerCols map[string]string) exec.UnaryOperator {
	// Try to compile from AST expression first (full expression engine)
	if pred.ASTExpr != nil {
		var compiled expr.Expr
		var err error
		if len(outerTables) > 0 {
			if len(outerCols) > 0 {
				compiled, err = expr.CompileWithFullScope(pred.ASTExpr, p.subqueryRunner, outerTables, outerCols)
			} else {
				compiled, err = expr.CompileWithScope(pred.ASTExpr, p.subqueryRunner, outerTables)
			}
		} else {
			compiled, err = expr.CompileWithRunner(pred.ASTExpr, p.subqueryRunner)
		}
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

// collectTableAliases recursively collects all table names and aliases from
// scan nodes in a logical plan subtree. Used to provide outer scope context
// for correlated subquery detection.
func collectTableAliases(node *logical.Node) map[string]bool {
	aliases := make(map[string]bool)
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan {
			aliases[strings.ToLower(n.TableName)] = true
			if n.TableAlias != "" {
				aliases[strings.ToLower(n.TableAlias)] = true
			}
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(node)
	return aliases
}

// findScanAlias returns the table alias of the scan node in a subtree.
// Used to set BuildTableAlias for column disambiguation in self-joins.
func findScanAlias(node *logical.Node) string {
	if node == nil {
		return ""
	}
	if node.Type == logical.NodeScan {
		if node.TableAlias != "" {
			return node.TableAlias
		}
		return node.TableName
	}
	for _, child := range node.Children {
		if alias := findScanAlias(child); alias != "" {
			return alias
		}
	}
	return ""
}

// collectOuterColumns recursively collects a column-name→table mapping from
// scan nodes in a logical plan subtree. Used to resolve unqualified column
// references in correlated subqueries.
func collectOuterColumns(node *logical.Node) map[string]string {
	colMap := make(map[string]string)
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan {
			tableID := strings.ToLower(n.TableAlias)
			if tableID == "" {
				tableID = strings.ToLower(n.TableName)
			}
			for _, col := range n.ScanColumns {
				colMap[strings.ToLower(col)] = tableID
			}
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(node)
	return colMap
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

	// LIKE / NOT LIKE
	upper := strings.ToUpper(raw)
	if idx := strings.Index(upper, " NOT LIKE "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		pattern := strings.TrimSpace(raw[idx+len(" NOT LIKE "):])
		pattern = strings.Trim(pattern, "'")
		return exec.NewFilter(exec.ColumnLike(col, pattern, true))
	}
	if idx := strings.Index(upper, " LIKE "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		pattern := strings.TrimSpace(raw[idx+len(" LIKE "):])
		pattern = strings.Trim(pattern, "'")
		return exec.NewFilter(exec.ColumnLike(col, pattern, false))
	}

	// IS NULL / IS NOT NULL
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
	case "string_agg":
		return exec.AggStringAgg
	case "bool_and", "every":
		return exec.AggBoolAnd
	case "bool_or":
		return exec.AggBoolOr
	case "stddev", "stddev_samp":
		return exec.AggStddev
	case "variance", "var_samp":
		return exec.AggVariance
	case "stddev_pop":
		return exec.AggStddevPop
	case "var_pop":
		return exec.AggVarPop
	case "approx_distinct":
		return exec.AggApproxDistinct
	case "corr":
		return exec.AggCorr
	case "covar_samp":
		return exec.AggCovarSamp
	case "covar_pop":
		return exec.AggCovarPop
	case "percentile_cont":
		return exec.AggPercentileCont
	case "percentile_disc":
		return exec.AggPercentileDisc
	case "mode":
		return exec.AggMode
	case "min_by":
		return exec.AggMinBy
	case "max_by":
		return exec.AggMaxBy
	case "median":
		return exec.AggMedian
	default:
		return exec.AggCount
	}
}

func parseWindowFunc(s string) exec.WindowFunc {
	switch strings.ToLower(s) {
	case "row_number":
		return exec.WinRowNumber
	case "rank":
		return exec.WinRank
	case "dense_rank":
		return exec.WinDenseRank
	case "sum":
		return exec.WinSum
	case "count":
		return exec.WinCount
	case "avg":
		return exec.WinAvg
	case "min":
		return exec.WinMin
	case "max":
		return exec.WinMax
	case "lag":
		return exec.WinLag
	case "lead":
		return exec.WinLead
	case "first_value":
		return exec.WinFirstValue
	case "last_value":
		return exec.WinLastValue
	case "ntile":
		return exec.WinNtile
	case "percent_rank":
		return exec.WinPercentRank
	case "cume_dist":
		return exec.WinCumeDist
	case "nth_value":
		return exec.WinNthValue
	default:
		return exec.WinRowNumber
	}
}

func windowOutputType(funcName string) parquet.TypeID {
	switch strings.ToLower(funcName) {
	case "row_number", "rank", "dense_rank", "count", "ntile":
		return parquet.TypeInt64
	case "lag", "lead", "first_value", "last_value", "nth_value":
		return parquet.TypeFloat64 // value functions default to float64
	case "percent_rank", "cume_dist":
		return parquet.TypeFloat64
	default:
		return parquet.TypeFloat64
	}
}

func aggOutputType(funcName string, distinct bool) parquet.TypeID {
	switch strings.ToLower(funcName) {
	case "count", "approx_distinct":
		return parquet.TypeInt64
	case "string_agg":
		return parquet.TypeString
	case "bool_and", "every", "bool_or":
		return parquet.TypeBool
	default:
		return parquet.TypeFloat64
	}
}

// hasAggregateAncestor checks if a node is an Aggregate, or if it's a
// passthrough node (e.g., Filter for HAVING) whose child is an Aggregate.
func hasAggregateAncestor(node *logical.Node) bool {
	return findAggregateAncestor(node) != nil
}

// findAggregateAncestor returns the Aggregate node if the given node is one,
// or traverses through passthrough nodes (Filter/HAVING) to find it.
func findAggregateAncestor(node *logical.Node) *logical.Node {
	if node.Type == logical.NodeAggregate {
		return node
	}
	if node.Type == logical.NodeFilter && len(node.Children) > 0 {
		return findAggregateAncestor(node.Children[0])
	}
	return nil
}

// resolveNullsLast determines whether nulls should sort last for a given order expression.
// Default SQL behavior: NULLS LAST for ASC, NULLS FIRST for DESC.
// When NullsFirst is explicitly set, use the explicit value.
func resolveNullsLast(ob logical.OrderExpr) bool {
	if ob.NullsFirst != nil {
		return !*ob.NullsFirst // NullsFirst=true => NullsLast=false, and vice versa
	}
	// Default: ASC => NULLS LAST, DESC => NULLS FIRST
	return !ob.Desc
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

func (a *aggSourceAdapter) RowsScanned() int64 {
	if sp, ok := a.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
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

func (s *sortSourceAdapter) RowsScanned() int64 {
	if sp, ok := s.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

// windowSourceAdapter wraps a child pipeline + window into a Source.
type windowSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	win         *exec.Window
	initialized bool
}

func (w *windowSourceAdapter) Init(_ context.Context) error { return nil }

func (w *windowSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !w.initialized {
		w.initialized = true
		pipe := &exec.Pipeline{
			Source: w.childSource,
			Ops:    w.childOps,
			Sink:   w.win,
		}
		if err := pipe.Run(ctx); err != nil {
			return nil, err
		}
	}
	return w.win.Next(ctx)
}

func (w *windowSourceAdapter) RowsScanned() int64 {
	if sp, ok := w.childSource.(exec.ScanStatsProvider); ok {
		return sp.RowsScanned()
	}
	return 0
}

func (w *windowSourceAdapter) Close() error {
	w.win.Close()
	return w.childSource.Close()
}

// newScannerSource creates a scanner exec.Source from the catalog
func newScannerSource(cat *catalog.Catalog, tableName string, partFilter map[string]string, requiredCols []string, scanPreds []logical.Predicate) exec.Source {
	return &scannerExecSource{
		catalog:         cat,
		tableName:       tableName,
		partitionFilter: partFilter,
		requiredCols:    requiredCols,
		scanPreds:       scanPreds,
	}
}

type scannerExecSource struct {
	catalog         *catalog.Catalog
	tableName       string
	partitionFilter map[string]string
	requiredCols    []string
	scanPreds       []logical.Predicate
	scanner         *scanSourceInner
}

type scanSourceInner struct {
	cat          *catalog.Catalog
	tableName    string
	files        []catalog.FileEntry
	idx          int64 // atomic index for parallel workers
	schema       []parquet.Column
	requiredCols []string
	scanPreds    []scanPredicate // converted predicates for row-group pruning
	rowsScanned  int64

	// parallel scan
	batchCh chan *batch.RecordBatch
	errCh   chan error
	wg      sync.WaitGroup
	cancel  context.CancelFunc
}

// scanPredicate is a simple predicate for row-group stats pruning.
type scanPredicate struct {
	Column string
	Op     string
	Value  any
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
		// Prune partitions that don't match the filter
		if len(s.partitionFilter) > 0 && len(p.Values) > 0 {
			if !matchesPartitionFilter(p.Values, s.partitionFilter) {
				continue
			}
		}
		files = append(files, p.Files...)
	}

	scanCtx, cancel := context.WithCancel(ctx)
	// Convert logical predicates to scan predicates for row-group pruning
	var sp []scanPredicate
	for _, pred := range s.scanPreds {
		if pred.Column != "" && pred.Op != "" && pred.Value != nil {
			sp = append(sp, scanPredicate{Column: pred.Column, Op: pred.Op, Value: pred.Value})
		}
	}

	inner := &scanSourceInner{
		cat:          s.catalog,
		tableName:    s.tableName,
		files:        files,
		schema:       tableMeta.Schema.Columns,
		requiredCols: s.requiredCols,
		scanPreds:    sp,
		batchCh:      make(chan *batch.RecordBatch, 4),
		errCh:        make(chan error, 1),
		cancel:       cancel,
	}
	s.scanner = inner

	// Launch parallel file readers
	workers := runtime.NumCPU()
	if workers > 8 {
		workers = 8
	}
	if workers > len(files) {
		workers = len(files)
	}
	if workers < 1 {
		workers = 1
	}

	inner.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go inner.scanWorker(scanCtx)
	}

	// Close batchCh when all workers are done
	go func() {
		inner.wg.Wait()
		close(inner.batchCh)
	}()

	return nil
}

// matchesPartitionFilter returns true if all filter keys match the partition values.
func matchesPartitionFilter(partValues, filter map[string]string) bool {
	for k, v := range filter {
		pv, ok := partValues[k]
		if !ok {
			continue // partition doesn't have this key, skip
		}
		if pv != v {
			return false
		}
	}
	return true
}

func (s *scannerExecSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return s.scanner.next(ctx)
}

func (s *scannerExecSource) Close() error {
	if s.scanner != nil && s.scanner.cancel != nil {
		s.scanner.cancel()
	}
	return nil
}

func (s *scannerExecSource) RowsScanned() int64 {
	if s.scanner != nil {
		return atomic.LoadInt64(&s.scanner.rowsScanned)
	}
	return 0
}

// scanWorker reads files in parallel, writing decoded batches to batchCh.
func (inner *scanSourceInner) scanWorker(ctx context.Context) {
	defer inner.wg.Done()

	for {
		idx := int(atomic.AddInt64(&inner.idx, 1) - 1)
		if idx >= len(inner.files) {
			return
		}
		if ctx.Err() != nil {
			return
		}

		file := inner.files[idx]
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

		b := readBatchDirect(reader, inner.schema, inner.requiredCols, inner.scanPreds...)
		if b == nil || b.Len == 0 {
			continue
		}

		atomic.AddInt64(&inner.rowsScanned, int64(b.Len))

		select {
		case inner.batchCh <- b:
		case <-ctx.Done():
			return
		}
	}
}

func (inner *scanSourceInner) next(ctx context.Context) (*batch.RecordBatch, error) {
	select {
	case b, ok := <-inner.batchCh:
		if !ok {
			// Channel closed, check for errors
			select {
			case err := <-inner.errCh:
				return nil, err
			default:
				return nil, nil
			}
		}
		return b, nil
	case err := <-inner.errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
