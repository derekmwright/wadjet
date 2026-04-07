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

	"github.com/citc-tech/wadjet/internal/config"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// PhysicalPlan represents an executable query plan.
type PhysicalPlan struct {
	Pipeline *exec.Pipeline
	Stages   []Stage // for distributed execution
	Cleanup  func()  // optional: called after pipeline finishes to clean up spill files
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
	ScanAlias       string // unique scan identity: "table" or "table:N" for Nth duplicate
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
	JoinType        string // inner, left, right, full, cross
	JoinLeftKeys    []string
	JoinRightKeys   []string
	LeftDepStage    string // stage providing probe (left) side
	RightDepStage   string // stage providing build (right) side
	BuildTableAlias string // build-side table alias for column disambiguation in self-joins
	JoinFilter      string // semi/anti join inequality filter (e.g., "l2.l_suppkey != l1.l_suppkey")

	// Fused broadcast joins absorbed into this stage (avoids separate
	// shuffle+join stages for small dimension tables like nation, region).
	FusedJoins []FusedJoinSpec

	// Window metadata
	WindowCols []WindowColSpec

	// Shuffle metadata
	ShuffleKeys   []string // columns to hash-partition on
	NumPartitions int      // number of output partitions (also used on join stages)

	// Fused scan-aggregate: partial aggregation is performed at the scan
	// level, eliminating the scan→aggregate S3 round-trip. Workers produce
	// partial aggregate results instead of raw rows.
	FusedAggGroupBy []string
	FusedAggSpecs   []AggSpec

	// Probe-split pipeline: partition the probe table's files across workers.
	// Each worker scans build tables in full and probes its file partition.
	ProbeSplitAlias string   // scan alias to partition (e.g., "lineitem")
	ProbeSplitFiles []string // full file list to split across tasks

	// BuildCachePreScans holds pre-scanned result file paths for large build
	// tables. When populated by the coordinator (after pre-scanning them once),
	// workers load these cached files via PreScannedInputs instead of scanning
	// the large source table N times — eliminating the N× build-side duplication
	// that causes OOM on Q09 at SF100. Keyed by scan alias (e.g., "orders").
	BuildCachePreScans map[string][]string

	// Multi-level merge: partitions upstream results among parallel merge groups.
	// When MergeGroupCount > 0, this stage processes only the MergeGroup-th
	// fraction of its dependency results. Independent merge groups run on
	// different workers for parallel merging.
	MergeGroup      int // 0-based index of this merge group
	MergeGroupCount int // total groups (0 = not grouped, process all results)

	// Cost estimation (populated at plan time from manifest metadata)
	EstimatedBytes int64
	EstimatedRows  int64
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

// FusedJoinSpec describes a broadcast join absorbed into a parent join stage.
type FusedJoinSpec struct {
	JoinType        string
	JoinLeftKeys    []string
	JoinRightKeys   []string
	BuildDepStage   string // stage providing build-side data
	BuildTableAlias string
	JoinFilter      string
	FilterExprs     []string
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
	QueryLimits    *config.QueryLimits // cost-based query guard (nil = no limits)
	cteCache       map[string]*cteMaterialized // materialized CTE results
	scanCache      map[string]*scanCached       // cached scan results for duplicate table scans
	spillMgr       *memory.SpillManager // shared per-query spill manager (lazy-initialized)
	memTracker     *memory.Tracker      // shared per-query memory tracker (lazy-initialized)
	WorkerCount    int                  // number of distributed workers (for shuffle partitioning)

	// MaterializedInputs holds pre-scanned data for scan-split pipeline mode.
	// When populated, buildScan uses these batches instead of reading from the
	// object store, allowing parallel scan I/O with single-worker compute.
	// Keyed by scan alias: "table" or "table:N" for self-joins.
	MaterializedInputs map[string][]*batch.RecordBatch

	// StreamingSources holds lazy sources for scan-split pipeline mode.
	// Unlike MaterializedInputs, these yield batches on demand without
	// materializing all data upfront. Checked before MaterializedInputs.
	// Keyed by scan alias: "table" or "table:N" for self-joins.
	StreamingSources map[string]exec.Source

	// ScanFileFilter restricts which files each scan alias reads. Used in
	// probe-split pipeline mode where the probe table is partitioned across
	// workers while build tables read all files. Keyed by scan alias.
	ScanFileFilter map[string][]string

	// ProbePartitionBlooms maps build-table column names to bloom filters
	// built from the probe partition's join key values. When a non-probe scan
	// reads a column with a matching bloom, rows not in the bloom are skipped.
	// This reduces build-side I/O proportional to the probe partition size:
	// with N workers, ~(N-1)/N of build rows are filtered out.
	// Keyed by build-side column name (e.g., "o_orderkey").
	ProbePartitionBlooms map[string]*exec.BloomScanFilter

	scanCounter map[string]int // tracks N-th scan of each table for alias resolution
}

// getSpillManager returns a shared per-query spill manager, creating it on
// first call. Uses MemoryBudget if set, otherwise auto-detects from system
// memory (cgroup or physical). Returns nil if no memory limit can be determined.
func (p *Planner) getSpillManager() *memory.SpillManager {
	if p.spillMgr != nil {
		return p.spillMgr
	}
	budget := p.MemoryBudget
	if budget <= 0 {
		budget = memory.DetectBudget()
	}
	if budget <= 0 {
		// Fall back to 75% of physical memory
		if phys := memory.DetectPhysicalMemory(); phys > 0 {
			budget = int64(float64(phys) * 0.75)
		}
	}
	if budget <= 0 {
		return nil
	}
	p.memTracker = memory.NewTracker("query", budget)
	dir := p.SpillDir
	if dir == "" {
		dir = os.TempDir()
	}
	sm, err := memory.NewSpillManager(dir, p.memTracker)
	if err != nil {
		return nil
	}
	p.spillMgr = sm
	return sm
}

// getMemTracker returns the shared per-query memory tracker.
// Must be called after getSpillManager().
func (p *Planner) getMemTracker() *memory.Tracker {
	return p.memTracker
}

// cteMaterialized stores the pre-computed results of a CTE.
type cteMaterialized struct {
	schema []parquet.Column
	rows   []map[string]any
}

// scanCached stores columnar scan results for a table that is scanned multiple
// times within a single query (e.g., decorrelated subqueries). The first scan
// populates the cache, and subsequent scans replay from it. Thread-safe for
// concurrent builds: subsequent scans block on ready until the first completes.
type scanCached struct {
	mu      sync.Mutex
	batches []*batch.RecordBatch
	schema  []parquet.Column
	done    bool          // true when the first scan is complete
	ready   chan struct{} // closed when first scan completes; nil until first scan claims the cache
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

	// Annotate scan nodes with column metadata so the optimizer can resolve
	// unqualified column references (needed for subquery decorrelation).
	p.AnnotateScanColumns(ctx, logicalPlan)

	// Optimize — pass scan annotator so new scans created by IN-to-SemiJoin
	// conversion get column metadata for scalar subquery decorrelation.
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		p.AnnotateScanColumns(ctx, plan)
	})

	// Build physical pipeline
	source, ops, sink, err := p.buildPipeline(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("subquery execution plan error: %w", err)
	}

	// Ensure a CollectSink — buildPipeline may return nil sink for
	// non-blocking plans (e.g., CTE cache lookups, table-less SELECTs).
	collectSink, ok := sink.(*exec.CollectSink)
	if !ok {
		collectSink = &exec.CollectSink{}
		sink = collectSink
	}

	// Execute
	pipeline := &exec.Pipeline{Source: source, Ops: ops, Sink: sink}
	if err := pipeline.Run(ctx); err != nil {
		return nil, fmt.Errorf("subquery execution error: %w", err)
	}

	return collectSink.ToRows(), nil
}

// resolveFilterSubqueries finds embedded SQL subqueries in a filter expression
// string, executes them using the planner's standalone pipeline, and substitutes
// the scalar results as literals. This is needed for distributed mode where
// workers don't have catalog access to execute subqueries themselves.
//
// Subqueries that reference CTEs are left unresolved — the coordinator will
// resolve them from intermediate results to avoid float-precision divergence
// between standalone and distributed computation paths.
func (p *Planner) resolveFilterSubqueries(exprStr string) string {
	// Quick check: no subquery to resolve
	if !strings.Contains(strings.ToUpper(exprStr), "SELECT") {
		return exprStr
	}

	// If the subquery references a CTE computed by the distributed pipeline,
	// defer resolution to the coordinator. This ensures the scalar value
	// comes from the same float computation path as the pipeline results,
	// avoiding precision divergence (e.g., Q15 total_revenue = MAX(...)).
	if p.subqueryReferencesCTE(exprStr) {
		return exprStr
	}

	ctx := p.planCtx
	if ctx == nil {
		return exprStr
	}

	// Parse the expression to find SubqueryNode elements
	ast, err := plansql.ParseExpression(exprStr)
	if err != nil {
		return exprStr
	}

	resolved := p.resolveSubqueryAST(ctx, ast)
	if resolved != nil {
		return resolved.String()
	}
	return exprStr
}

// subqueryReferencesCTE returns true if the expression contains a scalar
// subquery whose FROM clause references a CTE defined in the current query.
func (p *Planner) subqueryReferencesCTE(exprStr string) bool {
	upper := strings.ToUpper(exprStr)
	for _, cte := range p.ctes {
		// Check if the CTE name appears after FROM in the subquery.
		// Use case-insensitive match since SQL is case-insensitive.
		if strings.Contains(upper, "FROM "+strings.ToUpper(cte.Name)) {
			return true
		}
	}
	return false
}

// resolveSubqueryAST recursively walks an AST node, replacing SubqueryNode
// elements with literal values obtained by executing the subquery.
func (p *Planner) resolveSubqueryAST(ctx context.Context, node plansql.Node) plansql.Node {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *plansql.SubqueryNode:
		rows, err := p.executeSubquery(ctx, n.SQL)
		if err != nil || len(rows) == 0 {
			return node
		}
		// Extract scalar value from first row, first column
		for _, v := range rows[0] {
			return scalarToLiteral(v)
		}
		return node

	case *plansql.CmpExpr:
		return &plansql.CmpExpr{
			Left:  p.resolveSubqueryAST(ctx, n.Left),
			Op:    n.Op,
			Right: p.resolveSubqueryAST(ctx, n.Right),
		}

	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  p.resolveSubqueryAST(ctx, n.Left),
			Op:    n.Op,
			Right: p.resolveSubqueryAST(ctx, n.Right),
		}

	case *plansql.UnaryOp:
		return &plansql.UnaryOp{
			Op:    n.Op,
			Inner: p.resolveSubqueryAST(ctx, n.Inner),
		}

	case *plansql.ParenNode:
		inner := p.resolveSubqueryAST(ctx, n.Inner)
		if inner != nil {
			return &plansql.ParenNode{Inner: inner}
		}
		return node

	default:
		return node
	}
}

// scalarToLiteral converts a Go value to an AST literal node.
func scalarToLiteral(v any) plansql.Node {
	switch val := v.(type) {
	case float64:
		return &plansql.Lit{Value: strconv.FormatFloat(val, 'f', -1, 64), Kind: plansql.LitNumber}
	case float32:
		return &plansql.Lit{Value: strconv.FormatFloat(float64(val), 'f', -1, 32), Kind: plansql.LitNumber}
	case int64:
		return &plansql.Lit{Value: fmt.Sprintf("%d", val), Kind: plansql.LitNumber}
	case int:
		return &plansql.Lit{Value: fmt.Sprintf("%d", val), Kind: plansql.LitNumber}
	case string:
		return &plansql.Lit{Value: val, Kind: plansql.LitString}
	default:
		return &plansql.Lit{Value: fmt.Sprint(v), Kind: plansql.LitNumber}
	}
}

// AnnotateScanColumns walks the logical plan tree and populates ScanColumns
// on Scan nodes from the catalog. This enables the logical optimizer to resolve
// unqualified column references for filter pushdown through joins.
func (p *Planner) AnnotateScanColumns(ctx context.Context, node *logical.Node) {
	if node == nil {
		return
	}
	if node.Type == logical.NodeScan && node.TableName != "" && !node.IsTableFunc {
		table, err := p.catalog.GetTable(ctx, node.TableName)
		if err == nil {
			cols := make([]string, len(table.Schema.Columns))
			for i, c := range table.Schema.Columns {
				cols[i] = c.Name
			}
			node.ScanColumns = cols
		}
		// Estimate row count from manifest for join reordering
		if manifest, err := p.catalog.GetManifest(ctx, node.TableName); err == nil {
			var total int64
			for _, part := range manifest.Partitions {
				for _, f := range part.Files {
					total += f.NumRows
				}
			}
			node.ScanRowEstimate = total

			// Aggregate per-column stats for CBO selectivity estimation
			if colStats, err := p.catalog.AggregateColumnStats(ctx, node.TableName); err == nil && colStats != nil {
				scanStats := make(map[string]logical.ScanColumnStats, len(colStats))
				for col, cs := range colStats {
					scanStats[col] = logical.ScanColumnStats{
						MinValue:  cs.MinValue,
						MaxValue:  cs.MaxValue,
						NullCount: cs.NullCount,
						TotalRows: cs.TotalRows,
					}
				}
				node.ScanColStats = scanStats
			}
		}
	}
	for _, child := range node.Children {
		p.AnnotateScanColumns(ctx, child)
	}
}

// materializeCTEs pre-computes any CTE that is referenced more than once in
// the plan tree (including inside scalar subqueries). Both the main pipeline
// and any subquery pipelines will read from the cached result, ensuring they
// see bit-identical data.
func (p *Planner) materializeCTEs(ctx context.Context, root *logical.Node) {
	if len(root.CTEs) == 0 {
		return
	}
	// Count how many times each CTE name appears as a CTEName tag.
	refCounts := map[string]int{}
	var countRefs func(n *logical.Node)
	countRefs = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.CTEName != "" {
			refCounts[n.CTEName]++
		}
		for _, c := range n.Children {
			countRefs(c)
		}
	}
	countRefs(root)

	// Also count CTE references inside scalar subquery expressions.
	// These are in predicate ASTExpr nodes that reference CTE table names.
	// A simple heuristic: if a CTE name appears in the CTE list AND has
	// at least 1 reference in the plan tree, check if any scalar subquery
	// in the plan text also references it. We conservatively materialize
	// any CTE that has >=1 ref in the plan tree AND appears in the CTE
	// list (since scalar subqueries may reference it too).
	for _, cte := range root.CTEs {
		if refCounts[cte.Name] > 0 {
			// Conservatively mark as multi-ref since scalar subqueries
			// (not visible in the logical tree) may also reference it.
			refCounts[cte.Name] = 2
		}
	}

	p.cteCache = make(map[string]*cteMaterialized)

	for _, cte := range root.CTEs {
		if cte.Recursive {
			p.materializeRecursiveCTE(ctx, cte)
			continue
		}
		if refCounts[cte.Name] < 2 {
			continue
		}
		// Build and execute the CTE pipeline to materialize its results
		rows, err := p.executeSubquery(ctx, cte.SQL)
		if err != nil {
			continue // fall back to inline expansion
		}
		schema := p.inferCTESchema(cte.SQL, rows)
		if schema == nil {
			continue
		}
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: rows}
	}
}

// inferCTESchema derives column types from a CTE's SQL and data rows.
func (p *Planner) inferCTESchema(sql string, rows []map[string]any) []parquet.Column {
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return nil
	}
	schema := make([]parquet.Column, len(info.Columns))
	for i, col := range info.Columns {
		name := col.Alias
		if name == "" {
			name = col.Expr
		}
		typ := parquet.TypeString
		if len(rows) > 0 {
			if v, ok := rows[0][name]; ok {
				switch v.(type) {
				case int64:
					typ = parquet.TypeInt64
				case int32:
					typ = parquet.TypeInt32
				case float64:
					typ = parquet.TypeFloat64
				case bool:
					typ = parquet.TypeBool
				case string:
					// Check if the string value is actually a numeric literal
					// (SELECT 1 returns "1" as a string from the expression evaluator)
					s := v.(string)
					if _, err := strconv.ParseInt(s, 10, 64); err == nil {
						typ = parquet.TypeInt64
						// Convert all rows' values from string to int64
						for _, row := range rows {
							if sv, ok := row[name].(string); ok {
								if iv, err := strconv.ParseInt(sv, 10, 64); err == nil {
									row[name] = iv
								}
							}
						}
					} else if _, err := strconv.ParseFloat(s, 64); err == nil {
						typ = parquet.TypeFloat64
						for _, row := range rows {
							if sv, ok := row[name].(string); ok {
								if fv, err := strconv.ParseFloat(sv, 64); err == nil {
									row[name] = fv
								}
							}
						}
					}
				}
			}
		}
		schema[i] = parquet.Column{Name: name, Type: typ, Nullable: true}
	}
	return schema
}

const maxRecursiveIterations = 1000

// materializeRecursiveCTE executes a recursive CTE using fixed-point iteration.
// The CTE body must contain UNION ALL separating the anchor query from the
// recursive query. The recursive query references the CTE name itself.
func (p *Planner) materializeRecursiveCTE(ctx context.Context, cte plansql.CTEDef) {
	anchorSQL, recursiveSQL, ok := splitRecursiveUnion(cte.SQL)
	if !ok {
		// No UNION ALL found — fall back to non-recursive execution
		rows, err := p.executeSubquery(ctx, cte.SQL)
		if err != nil {
			return
		}
		schema := p.inferCTESchema(cte.SQL, rows)
		if schema != nil {
			p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: rows}
		}
		return
	}

	// Step 1: Execute anchor query
	anchorRows, err := p.executeSubquery(ctx, anchorSQL)
	if err != nil {
		return
	}
	if len(anchorRows) == 0 {
		schema := p.inferCTESchema(anchorSQL, nil)
		if schema != nil {
			p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: nil}
		}
		return
	}

	// Infer schema from anchor results
	schema := p.inferCTESchema(anchorSQL, anchorRows)
	if schema == nil {
		return
	}

	// Apply column aliases if specified: WITH t(a, b) AS (...)
	if len(cte.Columns) > 0 && len(cte.Columns) <= len(schema) {
		anchorRows = renameRowColumns(anchorRows, schema, cte.Columns)
		for i, name := range cte.Columns {
			schema[i].Name = name
		}
	}

	// Accumulate all results
	allRows := make([]map[string]any, len(anchorRows))
	copy(allRows, anchorRows)

	// Derive the expected column names from the schema (aliases already applied).
	schemaNames := make([]string, len(schema))
	for i, col := range schema {
		schemaNames[i] = col.Name
	}

	// Parse the recursive SQL to get its output column names so we can
	// positionally rename them to match the CTE schema. Strip table alias
	// prefixes (e.g., "e.id" → "id") because the Project operator outputs
	// unqualified column names.
	var recursiveColNames []string
	if rpq, err := plansql.Parse(recursiveSQL); err == nil {
		if ri, err := plansql.ExtractSelect(rpq); err == nil {
			for _, col := range ri.Columns {
				name := col.Alias
				if name == "" {
					name = cleanExpr(col.Expr)
				}
				recursiveColNames = append(recursiveColNames, name)
			}
		}
	}

	// Step 2: Fixed-point iteration
	workTable := anchorRows
	for iter := 0; iter < maxRecursiveIterations; iter++ {
		// Seed the CTE cache with the current work table so the recursive
		// query's reference to the CTE name resolves to these rows.
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: workTable}

		newRows, err := p.executeSubquery(ctx, recursiveSQL)
		if err != nil {
			break
		}
		if len(newRows) == 0 {
			break
		}

		// Rename output columns to match CTE schema. The recursive SQL may
		// produce different column names (e.g., "n + 1" vs "n").
		newRows = renameRowColumnsFromTo(newRows, recursiveColNames, schemaNames)

		allRows = append(allRows, newRows...)
		workTable = newRows
	}

	// Store final accumulated results
	p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: allRows}
}

// splitRecursiveUnion splits a recursive CTE body at the top-level UNION ALL.
// Returns (anchor, recursive, true) or ("", "", false) if no UNION ALL found.
func splitRecursiveUnion(sql string) (anchor, recursive string, ok bool) {
	upper := strings.ToUpper(sql)
	depth := 0
	inStr := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inStr {
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++ // escaped quote
				} else {
					inStr = false
				}
			}
			continue
		}
		if ch == '\'' {
			inStr = true
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}
		// Only match UNION ALL at depth 0 (not inside subqueries)
		if depth == 0 && i+9 < len(upper) {
			if upper[i:i+5] == "UNION" {
				rest := strings.TrimSpace(upper[i+5:])
				if strings.HasPrefix(rest, "ALL") {
					// Find the exact position after "UNION ALL"
					unionEnd := i + 5
					for unionEnd < len(sql) && (sql[unionEnd] == ' ' || sql[unionEnd] == '\t' || sql[unionEnd] == '\n' || sql[unionEnd] == '\r') {
						unionEnd++
					}
					unionEnd += 3 // skip "ALL"
					anchor = strings.TrimSpace(sql[:i])
					recursive = strings.TrimSpace(sql[unionEnd:])
					return anchor, recursive, true
				}
			}
		}
	}
	return "", "", false
}

// renameRowColumnsFromTo remaps row keys from srcNames[i] to dstNames[i].
func renameRowColumnsFromTo(rows []map[string]any, srcNames, dstNames []string) []map[string]any {
	if len(rows) == 0 || len(srcNames) == 0 || len(dstNames) == 0 {
		return rows
	}
	needsRename := false
	for i := range srcNames {
		if i < len(dstNames) && srcNames[i] != dstNames[i] {
			needsRename = true
			break
		}
	}
	if !needsRename {
		return rows
	}
	result := make([]map[string]any, len(rows))
	for ri, row := range rows {
		newRow := make(map[string]any, len(row))
		for k, v := range row {
			newRow[k] = v
		}
		for i, src := range srcNames {
			if i < len(dstNames) && src != dstNames[i] {
				newRow[dstNames[i]] = row[src]
				delete(newRow, src)
			}
		}
		result[ri] = newRow
	}
	return result
}

// renameRowColumns remaps row keys from schema column names to the target aliases.
func renameRowColumns(rows []map[string]any, schema []parquet.Column, aliases []string) []map[string]any {
	// Check if rename is needed
	needsRename := false
	for i, alias := range aliases {
		if i < len(schema) && schema[i].Name != alias {
			needsRename = true
			break
		}
	}
	if !needsRename {
		return rows
	}
	result := make([]map[string]any, len(rows))
	for ri, row := range rows {
		newRow := make(map[string]any, len(row))
		for i, col := range schema {
			if i < len(aliases) {
				newRow[aliases[i]] = row[col.Name]
			} else {
				newRow[col.Name] = row[col.Name]
			}
		}
		result[ri] = newRow
	}
	return result
}

// mergeDuplicateScans detects tables scanned multiple times in a query
// (e.g., from decorrelated subqueries) and merges their required columns.
// When duplicates are found, a scanCache entry is created so the first scan
// caches its results and subsequent scans replay from memory.
func (p *Planner) mergeDuplicateScans(node *logical.Node) {
	// Collect all scan nodes grouped by table name.
	scansByTable := map[string][]*logical.Node{}
	var walkScans func(n *logical.Node)
	walkScans = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan && n.TableName != "" {
			scansByTable[n.TableName] = append(scansByTable[n.TableName], n)
		}
		for _, c := range n.Children {
			walkScans(c)
		}
	}
	walkScans(node)

	// For tables scanned more than once, merge RequiredColumns across all scans.
	for table, scans := range scansByTable {
		if len(scans) < 2 {
			continue
		}
		// Skip if any scan has predicates or partition filters
		// (different filters = different result sets, not cacheable)
		incompatible := false
		for _, s := range scans {
			if len(s.ScanPredicates) > 0 || len(s.PartitionFilter) > 0 {
				incompatible = true
				break
			}
		}
		if incompatible {
			continue
		}
		// Compute the union of required columns.
		colSet := map[string]bool{}
		for _, s := range scans {
			for _, col := range s.RequiredColumns {
				colSet[col] = true
			}
		}
		merged := make([]string, 0, len(colSet))
		for col := range colSet {
			merged = append(merged, col)
		}
		// Update all scan nodes to use the merged column set.
		for _, s := range scans {
			s.RequiredColumns = merged
		}
		// Initialize the scan cache entry.
		if p.scanCache == nil {
			p.scanCache = make(map[string]*scanCached)
		}
		p.scanCache[table] = &scanCached{}
	}
}

// Plan converts a logical plan to a physical plan for local execution.
func (p *Planner) Plan(ctx context.Context, node *logical.Node) (*PhysicalPlan, error) {
	p.planCtx = ctx   // store for subquery runner context propagation
	p.scanCache = nil // reset per-query scan cache
	p.spillMgr = nil  // reset per-query spill manager
	p.memTracker = nil
	// Propagate CTE definitions from the logical plan so scalar subqueries
	// (e.g., in WHERE/HAVING) can resolve CTE table references.
	if len(node.CTEs) > 0 {
		p.ctes = node.CTEs
	}

	// Materialize CTEs referenced multiple times. Each CTE is computed once
	// and cached so that all references (main query + subqueries) see the
	// exact same data. This prevents float64 accumulation-order divergence
	// that would break exact equality comparisons (e.g., TPC-H Q15).
	p.materializeCTEs(ctx, node)

	// Detect tables scanned multiple times and merge their column needs.
	// The first scan caches decoded batches; subsequent scans replay from cache.
	p.mergeDuplicateScans(node)

	source, ops, sink, err := p.buildPipeline(ctx, node)
	if err != nil {
		return nil, err
	}

	// Front-load bloom filters whose key columns exist in the source scan.
	// In multi-way joins with selective semi/anti-join bloom filters (e.g.,
	// Q18's HAVING filter), this eliminates most rows at the source before
	// expensive join probes run.
	ops = frontLoadBlooms(source, ops)

	// Enable parallel pipeline execution when the source supports
	// concurrent Next() calls (channel-based scan sources).
	pipelineWorkers := 0
	switch source.(type) {
	case *catalogScanSource, *scannerExecSource, *deferredJoinBridge:
		pipelineWorkers = runtime.NumCPU()
	}

	plan := &PhysicalPlan{
		Pipeline: &exec.Pipeline{
			Source:  source,
			Ops:    ops,
			Sink:   sink,
			Workers: pipelineWorkers,
		},
	}

	// Attach spill file cleanup
	if sm := p.spillMgr; sm != nil {
		plan.Cleanup = func() { sm.Cleanup() }
	}

	// Generate distributed stages for coordinator dispatch
	plan.Stages = p.generateStages(node)

	if err := p.enforceQueryLimits(plan.Stages, node); err != nil {
		return nil, err
	}

	return plan, nil
}

// PlanDistributed generates a stage DAG for distributed execution.
// Returns stages with dependency ordering suitable for coordinator dispatch.
func (p *Planner) PlanDistributed(ctx context.Context, node *logical.Node) ([]Stage, error) {
	p.planCtx = ctx // store for scalar subquery evaluation during stage generation
	if len(node.CTEs) > 0 {
		p.ctes = node.CTEs
	}
	// Ensure scan nodes have column metadata — needed by fixJoinKeyOrder
	// to assign shuffle keys to the correct child side.
	p.AnnotateScanColumns(ctx, node)
	stages := p.generateStages(node)
	if err := p.enforceQueryLimits(stages, node); err != nil {
		return nil, err
	}
	return stages, nil
}

// QueryCost summarizes the estimated cost of a query across all scan stages.
type QueryCost struct {
	TotalBytes int64
	TotalRows  int64
	TotalFiles int
	HasFilter  bool
	HasLimit   bool
}

// EstimateCost computes the aggregate cost of a set of stages.
func EstimateCost(stages []Stage, node *logical.Node) QueryCost {
	var cost QueryCost
	for _, s := range stages {
		if s.Type == "scan" {
			cost.TotalBytes += s.EstimatedBytes
			cost.TotalRows += s.EstimatedRows
			cost.TotalFiles += len(s.ScanFiles)
		}
	}
	cost.HasFilter = hasFilterOrPartition(node)
	cost.HasLimit = hasLimit(node)
	return cost
}

// ProbeSplitMinBytes is the minimum size of the largest scan required to
// activate probe-split. Below this, the orchestration overhead exceeds the
// parallelism benefit. Exported so tests can lower it to exercise the
// distributed path on tiny datasets — otherwise every test silently runs
// the single-worker path and distributed-only bugs (like the SF100 build
// cache Q02 regression) never get caught.
var ProbeSplitMinBytes int64 = 64 * 1024 * 1024

// CanProbeSplit returns the scan alias and file list for probe-split pipeline
// routing. Probe-split distributes the dominant probe table's files across
// workers while each worker scans build tables in full. This enables parallel
// execution for join-heavy queries where compute is the bottleneck.
//
// Returns the probe scan alias, its file list, and true if probe-split is viable.
func CanProbeSplit(stages []Stage, workerCount int) (probeAlias string, probeFiles []string, ok bool) {
	if workerCount <= 1 {
		return "", nil, false
	}

	// Pick the largest scan as the probe-split candidate. No exclusions:
	// the physical planner uses RightSemiJoin/RightAntiJoin to swap the
	// local join's build/probe when the inner table is much larger than
	// the outer. This makes it safe to partition the inner (large) table
	// across workers — each worker builds the small outer as hash table
	// and probes with its partition of the large inner table.
	var bestAlias string
	var bestFiles []string
	var bestBytes int64
	for _, s := range stages {
		if s.Type == "scan" && s.EstimatedBytes > bestBytes {
			bestAlias = s.ScanAlias
			bestFiles = s.ScanFiles
			bestBytes = s.EstimatedBytes
		}
	}

	// Need enough files to give each worker meaningful work. For large
	// datasets (> 1 GB), relax from 2 files/worker to 1 file/worker since
	// each file is substantial. At SF100, tables may have only 3-6 files
	// but each is multi-GB.
	minFiles := workerCount * 2
	if bestBytes > 1<<30 {
		minFiles = workerCount
	}
	if bestAlias == "" || len(bestFiles) < minFiles || bestBytes < ProbeSplitMinBytes {
		return "", nil, false
	}

	return bestAlias, bestFiles, true
}

// LargeBuildScans returns scan stages that are build-side (not the probe alias)
// and whose estimated size exceeds the given threshold. These are candidates for
// the build-side broadcast cache: the coordinator pre-scans them once, caches the
// result in S3, and each worker loads the shared cache instead of independently
// scanning the large source table N times.
func LargeBuildScans(stages []Stage, probeAlias string, thresholdBytes int64) []Stage {
	var large []Stage
	for _, s := range stages {
		if s.Type != "scan" {
			continue
		}
		if s.ScanAlias == probeAlias {
			continue // skip probe table
		}
		if s.EstimatedBytes >= thresholdBytes {
			large = append(large, s)
		}
	}
	return large
}

// CountJoinStages returns the total number of joins in the stage list,
// including hash_join and broadcast_join stages plus fused joins that were
// absorbed into parent stages by fuseJoinStages().
func CountJoinStages(stages []Stage) int {
	n := 0
	for _, s := range stages {
		if s.Type == "hash_join" || s.Type == "broadcast_join" {
			n++
		}
		n += len(s.FusedJoins)
	}
	return n
}

// canFuseScanAggregate returns true when child stages are all scans (or
// filter-pushed scans). This means partial aggregation can be fused directly
// into scan tasks, eliminating the separate aggregate stage and its S3 round-trip.
func canFuseScanAggregate(childStages []Stage) bool {
	if len(childStages) == 0 {
		return false
	}
	for _, s := range childStages {
		if s.Type != "scan" {
			return false
		}
	}
	return true
}

func hasFilterOrPartition(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeFilter {
		return true
	}
	if n.Type == logical.NodeScan && (len(n.PartitionFilter) > 0 || len(n.ScanPredicates) > 0) {
		return true
	}
	for _, c := range n.Children {
		if hasFilterOrPartition(c) {
			return true
		}
	}
	return false
}

func hasLimit(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeLimit {
		return true
	}
	for _, c := range n.Children {
		if hasLimit(c) {
			return true
		}
	}
	return false
}

// enforceQueryLimits checks estimated query cost against configured limits.
func (p *Planner) enforceQueryLimits(stages []Stage, node *logical.Node) error {
	if p.QueryLimits == nil {
		return nil
	}
	limits := p.QueryLimits
	cost := EstimateCost(stages, node)

	if limits.MaxScanBytes > 0 && cost.TotalBytes > limits.MaxScanBytes {
		return fmt.Errorf("query would scan %s (%d bytes) across %d files, exceeding limit of %s — add a WHERE clause or partition filter",
			formatBytes(cost.TotalBytes), cost.TotalBytes, cost.TotalFiles, formatBytes(limits.MaxScanBytes))
	}
	if limits.MaxScanRows > 0 && cost.TotalRows > limits.MaxScanRows {
		return fmt.Errorf("query would scan %d rows across %d files, exceeding limit of %d rows — add a WHERE clause or LIMIT",
			cost.TotalRows, cost.TotalFiles, limits.MaxScanRows)
	}
	if limits.MaxScanFiles > 0 && cost.TotalFiles > limits.MaxScanFiles {
		return fmt.Errorf("query would scan %d files, exceeding limit of %d — add a partition filter",
			cost.TotalFiles, limits.MaxScanFiles)
	}
	if limits.RequireFilterAboveBytes > 0 && cost.TotalBytes > limits.RequireFilterAboveBytes && !cost.HasFilter {
		return fmt.Errorf("query scans %s without a WHERE clause (filter required above %s)",
			formatBytes(cost.TotalBytes), formatBytes(limits.RequireFilterAboveBytes))
	}
	if limits.RequireLimitAboveRows > 0 && cost.TotalRows > limits.RequireLimitAboveRows && !cost.HasLimit {
		return fmt.Errorf("query scans %d rows without a LIMIT (limit required above %d rows)",
			cost.TotalRows, limits.RequireLimitAboveRows)
	}
	return nil
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1fTB", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(1<<20))
	default:
		return fmt.Sprintf("%dB", b)
	}
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
	if p.WorkerCount > 1 {
		stages = fuseJoinStages(stages)
	}
	return stages
}

// fuseJoinStages absorbs broadcast join stages into their downstream consumer
// join stage when the broadcast join's output feeds directly as the probe or
// build side of another join. This avoids materializing the intermediate result
// to S3 and re-reading it — the worker chains probes batch-by-batch instead.
//
// Example: join-A (lineitem ⨝ part) → join-B (broadcast_join with nation)
// becomes: join-A with FusedJoins=[nation join spec], join-B removed.
func fuseJoinStages(stages []Stage) []Stage {
	stageByID := make(map[string]*Stage, len(stages))
	for i := range stages {
		stageByID[stages[i].ID] = &stages[i]
	}

	// Find broadcast join stages that can be absorbed.
	// A broadcast join can be fused into its consumer if:
	// 1. It's a broadcast_join (small build side, no shuffle)
	// 2. Exactly one other stage depends on its output
	// 3. That consumer is also a join stage
	absorbed := make(map[string]bool)

	// Count how many stages depend on each stage
	depCount := make(map[string]int)
	for i := range stages {
		for _, dep := range stages[i].Dependencies {
			depCount[dep]++
		}
	}

	for i := range stages {
		s := &stages[i]
		if s.Type != "broadcast_join" {
			continue
		}
		// Only absorb leaf broadcast joins (no existing fused joins).
		// Deep fusion chains are fragile and unnecessary — single-level
		// handles the important case (small dimension tables like nation).
		if len(s.FusedJoins) > 0 {
			continue
		}
		// Only fuse if exactly one consumer depends on this stage
		if depCount[s.ID] != 1 {
			continue
		}
		// Find the consumer stage
		var consumer *Stage
		for j := range stages {
			if stages[j].ID == s.ID {
				continue
			}
			for _, dep := range stages[j].Dependencies {
				if dep == s.ID {
					consumer = &stages[j]
					break
				}
			}
			if consumer != nil {
				break
			}
		}
		if consumer == nil {
			continue
		}
		// Consumer must be a join stage
		if consumer.Type != "hash_join" && consumer.Type != "broadcast_join" {
			continue
		}

		// Absorb: move this broadcast join's spec into the consumer as a FusedJoin.
		// The consumer's probe stream will be chained through this join.
		consumer.FusedJoins = append(consumer.FusedJoins, FusedJoinSpec{
			JoinType:        s.JoinType,
			JoinLeftKeys:    s.JoinLeftKeys,
			JoinRightKeys:   s.JoinRightKeys,
			BuildDepStage:   s.RightDepStage,
			BuildTableAlias: s.BuildTableAlias,
			JoinFilter:      s.JoinFilter,
			FilterExprs:     s.FilterExprs,
		})

		// Rewire: consumer's dependency on this stage → dependency on this stage's probe dep
		for k, dep := range consumer.Dependencies {
			if dep == s.ID {
				if s.LeftDepStage != "" {
					consumer.Dependencies[k] = s.LeftDepStage
				}
				// Update LeftDepStage/RightDepStage if they pointed to the absorbed stage
				if consumer.LeftDepStage == s.ID {
					consumer.LeftDepStage = s.LeftDepStage
				}
				if consumer.RightDepStage == s.ID {
					consumer.RightDepStage = s.LeftDepStage
				}
				break
			}
		}

		// Add the build-side dependency of the fused join to consumer's deps
		if s.RightDepStage != "" {
			consumer.Dependencies = append(consumer.Dependencies, s.RightDepStage)
		}

		absorbed[s.ID] = true
	}

	if len(absorbed) == 0 {
		return stages
	}

	// Remove absorbed stages
	result := make([]Stage, 0, len(stages)-len(absorbed))
	for _, s := range stages {
		if !absorbed[s.ID] {
			result = append(result, s)
		}
	}
	return result
}

// resolveShuffleKey resolves a join key name through any Project alias nodes
// in the child subtree. For example, a CTE with `l_suppkey AS supplier_no`
// creates a Project that renames the column — the shuffle key `supplier_no`
// must be mapped back to `l_suppkey` so the executor can find it in the data.
func resolveShuffleKey(key string, child *logical.Node) string {
	if child == nil {
		return key
	}
	// Walk down through pass-through nodes looking for a Project with an alias.
	for n := child; n != nil; {
		if n.Type == logical.NodeProject {
			for _, proj := range n.Projections {
				if strings.EqualFold(proj.Alias, key) && proj.Column != "" {
					return proj.Column
				}
			}
		}
		// Continue down to single-child nodes (Filter, Sort, Limit, Project, Aggregate)
		if len(n.Children) == 1 {
			n = n.Children[0]
		} else {
			break
		}
	}
	return key
}

func (p *Planner) walkStages(node *logical.Node, stages *[]Stage, parentID *string) {
	switch node.Type {
	case logical.NodeScan:
		stageID := fmt.Sprintf("scan-%d", len(*stages))
		tasks := 1
		var scanFiles []string
		var estBytes, estRows int64
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
					estBytes += f.SizeBytes
					estRows += f.NumRows
				}
			}
			if len(scanFiles) > 0 {
				tasks = len(scanFiles)
			}
		}
		// Build unique ScanAlias: "table" for first scan, "table:1", "table:2"
		// for duplicates. This disambiguates multiple scans of the same table
		// (e.g., self-joins) in scan-split pipeline mode.
		scanAlias := node.TableName
		dupCount := 0
		for _, s := range *stages {
			if s.Type == "scan" && s.TableName == node.TableName {
				dupCount++
			}
		}
		if dupCount > 0 {
			scanAlias = fmt.Sprintf("%s:%d", node.TableName, dupCount)
		}

		stage := Stage{
			ID:              stageID,
			Type:            "scan",
			Tasks:           tasks,
			TableName:       node.TableName,
			ScanAlias:       scanAlias,
			Columns:         node.RequiredColumns,
			PartitionFilter: partFilter,
			ScanFiles:       scanFiles,
			EstimatedBytes:  estBytes,
			EstimatedRows:   estRows,
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
		preCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, nil)
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

		// Optimization: fuse aggregation into scan when the only child
		// stages are scans (no joins or sorts in between). This eliminates
		// the scan→aggregate S3 round-trip by doing partial aggregation at
		// the scan level. Each scan task produces partial aggregate results
		// instead of raw rows, massively reducing data volume.
		childStages := (*stages)[preCount:]
		if canFuseScanAggregate(childStages) {
			for i := range *stages {
				if i < preCount {
					continue
				}
				if (*stages)[i].Type == "scan" {
					(*stages)[i].FusedAggGroupBy = groupBy
					(*stages)[i].FusedAggSpecs = aggSpecs
				}
			}
			// Skip the separate aggregate stage — scans produce partial aggs.
			// Final aggregate merges partial results from all scan tasks.
			leafIDs := leafStages(childStages)
			emitMergeAggregateTree(stages, leafIDs, groupBy, aggSpecs, childStages)
		} else {
			// Standard two-phase distributed aggregation
			stageID := fmt.Sprintf("aggregate-%d", len(*stages))
			stage := Stage{
				ID:          stageID,
				Type:        "aggregate",
				Tasks:       1,
				GroupByCols: groupBy,
				AggSpecs:    aggSpecs,
			}
			stage.Dependencies = leafStages(childStages)
			*stages = append(*stages, stage)

			finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:           finalStageID,
				Type:         "final_aggregate",
				Tasks:        1,
				GroupByCols:  groupBy,
				AggSpecs:     aggSpecs,
				Dependencies: []string{stageID},
			})
		}

	case logical.NodeSort:
		preCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, nil)
		}
		sortStageID := fmt.Sprintf("sort-%d", len(*stages))
		var sortKeys []SortKeySpec
		for _, ob := range node.OrderBy {
			sortKeys = append(sortKeys, SortKeySpec{Column: ob.Column, Desc: ob.Desc, NullsLast: resolveNullsLast(ob)})
		}

		// Phase 1: partial sort (coordinator splits into parallel tasks at runtime)
		sortStage := Stage{
			ID:       sortStageID,
			Type:     "sort",
			Tasks:    1,
			SortKeys: sortKeys,
		}
		// Only depend on leaf stages from subtree (not transitive deps like scan).
		sortStage.Dependencies = leafStages((*stages)[preCount:])
		*stages = append(*stages, sortStage)

		// Phase 2: merge sort — multi-level tree when many partial sort tasks.
		emitMergeSortTree(stages, sortStageID, sortKeys, (*stages)[preCount:])

	case logical.NodeLimit:
		// Pass limit info down to sort stage if child is sort
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		// Propagate limit to both merge_sort and sort stages
		for i := len(*stages) - 1; i >= 0; i-- {
			if (*stages)[i].Type == "merge_sort" {
				(*stages)[i].Limit = node.LimitVal
			} else if (*stages)[i].Type == "sort" {
				(*stages)[i].Limit = node.LimitVal
				break
			}
		}

	case logical.NodeJoin:
		// Track leaf stages from each child separately so we get the
		// correct left (probe) and right (build) dependencies — even
		// when a child is itself a multi-stage subtree (e.g., nested join).
		var childLeaves [][]string
		for _, child := range node.Children {
			childStart := len(*stages)
			p.walkStages(child, stages, nil)
			childLeaves = append(childLeaves, leafStages((*stages)[childStart:]))
		}
		isBroadcast := p.isBroadcastCandidate(node)
		joinType := "hash_join"
		if isBroadcast {
			joinType = "broadcast_join"
		}

		// Identify left (probe) and right (build) dependency stages
		var leftDep, rightDep string
		if len(childLeaves) >= 1 && len(childLeaves[0]) > 0 {
			leftDep = childLeaves[0][len(childLeaves[0])-1]
		}
		if len(childLeaves) >= 2 && len(childLeaves[1]) > 0 {
			rightDep = childLeaves[1][len(childLeaves[1])-1]
		}

		// Map logical join type to canonical short form
		jt := mapJoinType(node.JoinType)

		// Extract join keys from condition (cross joins have no ON clause)
		var leftKeys, rightKeys []string
		if jt != "cross" {
			leftKeys, rightKeys = parseJoinKeys(node.JoinCond)
			// parseJoinKeys assigns left/right based on position in the "="
			// expression, not based on which child subtree owns the column.
			// Fix the assignment so leftKeys are from the probe (left) child
			// and rightKeys are from the build (right) child.
			if len(node.Children) >= 2 {
				fixJoinKeyOrder(leftKeys, rightKeys, node.Children[1])
			}
		}

		// Resolve join keys through CTE/Project aliases so shuffle keys
		// match the actual column names in the data (e.g., supplier_no → l_suppkey).
		if len(node.Children) >= 2 {
			for i, key := range leftKeys {
				leftKeys[i] = resolveShuffleKey(key, node.Children[0])
			}
			for i, key := range rightKeys {
				rightKeys[i] = resolveShuffleKey(key, node.Children[1])
			}
		}

		// Insert shuffle stages for non-broadcast joins when distributed
		numPartitions := 0
		if !isBroadcast && jt != "cross" && len(leftKeys) > 0 && p.WorkerCount > 1 {
			// Use 8x workers as partition count to reduce per-task join memory.
			// Each partition receives 1/numPartitions of the shuffled data, so
			// higher counts reduce peak hash table memory on each worker.
			// At SF100 with 3 workers, 24 partitions halves per-partition
			// memory compared to the previous 12.
			numPartitions = p.WorkerCount * 8
			if numPartitions < 16 {
				numPartitions = 16
			}

			// Compute columns the shuffle must preserve: join keys + all
			// columns needed downstream (from the join's NeededColumns).
			// Both sides get the full set — the Parquet reader ignores
			// columns that don't exist in the file.
			var shuffleCols []string
			if len(node.NeededColumns) > 0 {
				seen := make(map[string]bool, len(node.NeededColumns)+len(leftKeys)+len(rightKeys))
				for _, col := range node.NeededColumns {
					if !seen[col] {
						shuffleCols = append(shuffleCols, col)
						seen[col] = true
					}
				}
				for _, col := range leftKeys {
					if !seen[col] {
						shuffleCols = append(shuffleCols, col)
						seen[col] = true
					}
				}
				for _, col := range rightKeys {
					if !seen[col] {
						shuffleCols = append(shuffleCols, col)
						seen[col] = true
					}
				}
			}

			// Left (probe) side shuffle
			leftShuffleID := fmt.Sprintf("shuffle-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:            leftShuffleID,
				Type:          "shuffle",
				Tasks:         1,
				Columns:       shuffleCols,
				ShuffleKeys:   leftKeys,
				NumPartitions: numPartitions,
				Dependencies:  []string{leftDep},
			})

			// Right (build) side shuffle
			rightShuffleID := fmt.Sprintf("shuffle-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:            rightShuffleID,
				Type:          "shuffle",
				Tasks:         1,
				Columns:       shuffleCols,
				ShuffleKeys:   rightKeys,
				NumPartitions: numPartitions,
				Dependencies:  []string{rightDep},
			})

			leftDep = leftShuffleID
			rightDep = rightShuffleID
		}

		joinTasks := 1
		if numPartitions > 0 {
			joinTasks = numPartitions
		} else if isBroadcast && p.WorkerCount > 1 {
			// Parallel broadcast: split probe side across workers
			joinTasks = p.WorkerCount
		}
		stageID := fmt.Sprintf("join-%d", len(*stages))
		stage := Stage{
			ID:            stageID,
			Type:          joinType,
			Tasks:         joinTasks,
			Columns:       node.NeededColumns,
			JoinType:      jt,
			JoinLeftKeys:  leftKeys,
			JoinRightKeys: rightKeys,
			LeftDepStage:  leftDep,
			RightDepStage: rightDep,
			NumPartitions: numPartitions,
		}
		// Propagate build-side table alias for column disambiguation in self-joins
		// (e.g., nation n1 JOIN nation n2 — prevents duplicate columns from being dropped).
		if len(node.Children) >= 2 {
			if alias := findScanAlias(node.Children[1]); alias != "" {
				stage.BuildTableAlias = alias
			}
		}
		// Propagate semi/anti join inequality filters
		if node.JoinFilter != "" {
			stage.JoinFilter = node.JoinFilter
		}
		if leftDep != "" {
			stage.Dependencies = append(stage.Dependencies, leftDep)
		}
		if rightDep != "" {
			stage.Dependencies = append(stage.Dependencies, rightDep)
		}
		*stages = append(*stages, stage)

	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		// Each side of the set operation runs independently; merge results at the end.
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}

	case logical.NodeDual:
		// Table-less SELECT: single-row source, runs locally on coordinator.
		stageID := fmt.Sprintf("dual-%d", len(*stages))
		*stages = append(*stages, Stage{
			ID:    stageID,
			Type:  "dual",
			Tasks: 1,
		})

	case logical.NodeFilter:
		// Walk children first. Try to push filter expressions down to the
		// appropriate stage: scan stages get predicate pushdown, join/aggregate
		// stages evaluate filters post-execution.
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		if len(node.Predicates) > 0 && len(*stages) > 0 {
			lastStage := &(*stages)[len(*stages)-1]
			for _, pred := range node.Predicates {
				var exprStr string
				if pred.Raw != "" {
					exprStr = pred.Raw
				} else if pred.ASTExpr != nil {
					exprStr = pred.ASTExpr.String()
				}
				// Resolve scalar subqueries in-place — workers don't have catalog
				// access to execute them. The planner evaluates subqueries here
				// during stage generation and substitutes literal results.
				if exprStr != "" {
					exprStr = p.resolveFilterSubqueries(exprStr)
					lastStage.FilterExprs = append(lastStage.FilterExprs, exprStr)
				}
			}
		}

	case logical.NodeWindow:
		preCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, nil)
		}
		stageID := fmt.Sprintf("window-%d", len(*stages))
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
		// Only depend on leaf stages from subtree (not transitive deps like scan).
		stage.Dependencies = leafStages((*stages)[preCount:])
		*stages = append(*stages, stage)

	default:
		// Passthrough nodes (Project, Distinct) — walk children
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
	}
}

// isBroadcastCandidate returns true if the right (build) side of a join is
// small enough to broadcast to all workers. When broadcast, the build side
// is sent to every worker and the probe side is split round-robin across
// workers — no shuffle stages needed for either side.
func (p *Planner) isBroadcastCandidate(joinNode *logical.Node) bool {
	if len(joinNode.Children) < 2 {
		return false
	}
	// Walk through Filter/Project/Limit wrappers to find the underlying Scan.
	// Small dimension tables (e.g., region with r_name='EUROPE') are often
	// wrapped in Filter nodes, but are still small enough to broadcast.
	scan := findScanNode(joinNode.Children[1])
	if scan == nil {
		return false
	}
	// Estimate size from file count
	manifest, err := p.catalog.GetManifest(context.Background(), scan.TableName)
	if err != nil {
		return false
	}
	var totalBytes int64
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			totalBytes += f.SizeBytes
		}
	}
	// Broadcast if build side is under 100 MB.  At SF100 partsupp is ~10 GB
	// and was incorrectly broadcast under the old file-count heuristic,
	// forcing every worker to build a 10 GB hash table.
	return totalBytes <= 100*1024*1024
}

// findScanNode walks through pass-through nodes (Filter, Project, Limit)
// to find the underlying Scan node, if any.
func findScanNode(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeScan:
			return n
		case logical.NodeFilter, logical.NodeProject, logical.NodeLimit:
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
		}
		return nil
	}
	return nil
}

func (p *Planner) buildPipeline(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// If this subtree is a materialized CTE, serve from cache instead of
	// re-executing the full sub-plan.
	if node.CTEName != "" && p.cteCache != nil {
		if mat, ok := p.cteCache[node.CTEName]; ok {
			source := exec.NewSliceSource(mat.schema, mat.rows)
			return source, nil, &exec.CollectSink{}, nil
		}
	}

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
	case logical.NodeDual:
		return &exec.DualSource{}, nil, &exec.CollectSink{}, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported plan node: %s", node.Type)
	}
}

func (p *Planner) buildScan(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// Track scan alias for both MaterializedInputs and ScanFileFilter.
	// Alias scheme matches walkStages: "table" for first, "table:N" for duplicates.
	if p.scanCounter == nil {
		p.scanCounter = make(map[string]int)
	}
	n := p.scanCounter[node.TableName]
	p.scanCounter[node.TableName] = n + 1

	scanAlias := node.TableName
	if n > 0 {
		scanAlias = fmt.Sprintf("%s:%d", node.TableName, n)
	}

	// Scan-split pipeline mode: use streaming or materialized pre-scanned data.
	// StreamingSources is preferred — yields batches lazily without upfront
	// memory allocation. Falls back to MaterializedInputs for compatibility.
	if p.StreamingSources != nil {
		if src, ok := p.StreamingSources[scanAlias]; ok {
			return src, nil, &exec.CollectSink{}, nil
		}
	}
	if p.MaterializedInputs != nil {
		if batches, ok := p.MaterializedInputs[scanAlias]; ok && len(batches) > 0 {
			return exec.NewBatchSource(batches), nil, &exec.CollectSink{}, nil
		}
	}

	// Table functions (read_json, read_csv, etc.) bypass the catalog scan
	if node.IsTableFunc {
		if node.FuncName == "unnest" {
			source, err := newUnnestSource(node.FuncArgs, node.WithOrdinality, node.FuncColAliases)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("unnest: %w", err)
			}
			return source, nil, &exec.CollectSink{}, nil
		}
		source, err := buildTableFunctionSource(node.FuncName, node.FuncArgs, node.FuncNamedArgs)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("table function %s: %w", node.FuncName, err)
		}
		return source, nil, &exec.CollectSink{}, nil
	}
	scanner := p.newScanner(ctx, node.TableName, node.PartitionFilter, node.RequiredColumns, node.ScanPredicates)

	// Probe-split pipeline mode: restrict this scan to only allowed files.
	if p.ScanFileFilter != nil {
		if files, ok := p.ScanFileFilter[scanAlias]; ok {
			if cs, ok := scanner.(*catalogScanSource); ok {
				cs.allowedFiles = files
			}
		}
	}

	var ops []exec.UnaryOperator

	// Inject probe partition bloom filters as BloomFilterOp operators.
	// These filter out build-side rows whose join key is not in the probe
	// partition's bloom, reducing hash table memory by ~(N-1)/N.
	if len(p.ProbePartitionBlooms) > 0 {
		// Collect known column names. When RequiredColumns is nil (inner join
		// build sides read all columns), fall back to the full table schema.
		allCols := append(node.RequiredColumns, node.ScanColumns...)
		if len(allCols) == 0 {
			if meta, err := p.catalog.GetTable(ctx, node.TableName); err == nil {
				for _, col := range meta.Schema.Columns {
					allCols = append(allCols, col.Name)
				}
			}
		}
		for colName, bloom := range p.ProbePartitionBlooms {
			for _, col := range allCols {
				if col == colName {
					bloomOp := exec.NewBloomFilterOp(bloom.Bloom, bloom.BloomMask,
						[]string{colName}, bloom.UseIntKey)
					ops = append(ops, bloomOp)
					break
				}
			}
		}
	}
	if node.SampleMethod != "" && node.SamplePercent > 0 {
		ops = append(ops, newSampleOperator(node.SampleMethod, node.SamplePercent))
	}
	return scanner, ops, &exec.CollectSink{}, nil
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
		// Fix key assignment using plan-level column info: ensure left keys are
		// probe-side and right keys are build-side. This avoids the expensive
		// post-build FixKeyAssignment hash table rebuild.
		fixJoinKeyOrder(leftKeys, rightKeys, node.Children[1])
	}

	hj := exec.NewHashJoin(joinType, leftKeys, rightKeys)

	// Set build-side table alias for column disambiguation in self-joins
	if alias := findScanAlias(node.Children[1]); alias != "" {
		hj.BuildTableAlias = alias
	}

	// Grace Hash Join spill-to-disk: prevents OOM on large build sides (e.g.
	// SF100 orders table at 150M rows). The shared MemTracker means multi-join
	// queries may spill earlier than strictly necessary, but OOM is worse.
	if sm := p.getSpillManager(); sm != nil {
		hj.Spill = sm
		hj.MemTracker = p.getMemTracker()
	}

	// Semi/anti join build/probe swap: when the inner (build) table is much
	// larger than the outer (probe) table, swap to RightSemiJoin/RightAntiJoin.
	// This builds the SMALL outer table as hash table and streams the LARGE
	// inner table as probe, then emits matched (semi) or unmatched (anti)
	// build rows. Dramatically reduces memory: e.g. Q04 at SF100 goes from
	// 16GB lineitem hash table to 1.7GB orders hash table per worker.
	rightEst := findScanRowEstimate(node.Children[1])
	leftEst := findScanRowEstimate(node.Children[0])
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) &&
		rightEst > 0 && leftEst > 0 && rightEst > 3*leftEst {
		// Swap: build the small outer table, probe with the large inner table
		if joinType == exec.SemiJoin {
			joinType = exec.RightSemiJoin
		} else {
			joinType = exec.RightAntiJoin
		}
		hj.JoinType = joinType
		// Swap children
		node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
		// Swap keys and update the hash join
		leftKeys, rightKeys = rightKeys, leftKeys
		hj.LeftKeys = leftKeys
		hj.RightKeys = rightKeys
		fixJoinKeyOrder(leftKeys, rightKeys, node.Children[1])
		// Update build-side alias after swap
		if alias := findScanAlias(node.Children[1]); alias != "" {
			hj.BuildTableAlias = alias
		}
	}

	// For semi/anti joins without a filter, enable key-only build:
	// only build the key index and bloom filter, skip batch storage and arena refs.
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter == "" {
		hj.SemiAntiKeyOnly = true
	}

	// Pass build-side row estimate to pre-allocate arena and hash table.
	est := findScanRowEstimate(node.Children[1])
	if est > 0 {
		hj.BuildRowHint = est
	}

	// Determine if build should be deferred: large semi/anti key-only builds
	// overlap with the early probe pipeline for better I/O utilization.
	const deferBuildThreshold int64 = 1_000_000
	deferBuild := est > deferBuildThreshold && hj.SemiAntiKeyOnly

	// Reverse bloom: for very large builds (>10M rows), run the probe side
	// first, build a bloom from its join key values, then filter the build
	// scan. Sacrifices I/O overlap for massive scan reduction.
	//
	// For semi/anti joins: bloom has no false negatives, correctness preserved.
	// For inner joins with large builds (>50M rows): each probe-split worker
	// only sees 1/N of the probe keys, so ~(N-1)/N of build rows are filtered
	// out. At SF100 with 3 workers, this reduces orders (150M) to ~50M and
	// partsupp (80M) to ~27M per worker — fitting in 4GB memory budget.
	const reverseBloomThreshold int64 = 10_000_000
	const reverseBloomInnerThreshold int64 = 50_000_000
	useReverseBloom := (est > reverseBloomThreshold && (joinType == exec.SemiJoin || joinType == exec.AntiJoin)) ||
		(est > reverseBloomInnerThreshold && joinType == exec.InnerJoin)

	// Pre-compute post-build operations that can run in the build goroutine.
	var keepCols []string
	if joinType == exec.SemiJoin || joinType == exec.AntiJoin {
		keepCols = extractFilterBuildColumns(node.JoinFilter)
	}
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter != "" {
		hj.SemiAntiFilter = BuildSemiAntiFilter(node.JoinFilter)
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

	// Launch hash table build in a goroutine while concurrently preparing
	// the left (probe) side. For multi-way joins, the left side recursively
	// calls buildJoin → each level overlaps its build with the next level's
	// preparation, so all independent hash table builds run concurrently.
	//
	// For reverse bloom, the build goroutine waits for a signal before
	// starting — the probe side's child pipeline must finish first so we
	// can inject a bloom filter into the build-side scan.
	var buildStart chan struct{}
	if useReverseBloom {
		buildStart = make(chan struct{})
	}
	buildDone := make(chan struct{})
	var buildErr error
	var rbBuildSource exec.Source = buildSource // may be wrapped with bloom
	go func() {
		defer close(buildDone)
		if buildStart != nil {
			<-buildStart // wait for reverse bloom injection
		}
		if err := hj.Build(ctx, rbBuildSource); err != nil {
			buildErr = fmt.Errorf("building hash table: %w", err)
			return
		}
		if deferBuild || useReverseBloom {
			hj.FixKeyAssignment()
			if joinType == exec.SemiJoin || joinType == exec.AntiJoin {
				hj.PruneBuildColumns(keepCols)
			}
		}
	}()

	// Left side (probe) streams through — prepared concurrently with build.
	leftSource, leftOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		if buildStart != nil {
			close(buildStart)
		}
		<-buildDone // prevent goroutine leak
		return nil, nil, nil, fmt.Errorf("building join left side: %w", err)
	}

	if useReverseBloom {
		// Reverse bloom: run probe-side child pipeline first, build a bloom
		// from its join key values, inject as filter on build-side scan, then
		// signal the build goroutine to start with the filtered scan.
		probe := hj.Probe()
		if len(node.NeededColumns) > 0 {
			filter := make(map[string]bool, len(node.NeededColumns))
			for _, col := range node.NeededColumns {
				filter[col] = true
			}
			probe.OutputFilter = filter
		}

		bridge := &reverseBloomBridge{
			childSource:    leftSource,
			childOps:       leftOps,
			rbBuildSource:  &rbBuildSource,
			buildSource:    buildSource,
			buildStart:     buildStart,
			barrier:        buildDone,
			buildErr:       &buildErr,
			probeKey:       leftKeys[0],
			buildKey:       rightKeys[0],
			workers:        innerPipelineWorkers(leftSource),
		}
		return bridge, []exec.UnaryOperator{probe}, &exec.CollectSink{}, nil
	}

	if deferBuild {
		// Pipeline break: run early probes (scan → inner joins) as a child
		// pipeline that overlaps with the deferred build. The bridge collects
		// filtered batches, waits for the build barrier, then replays them
		// through the deferred probe operators.
		probe := hj.Probe()
		if len(node.NeededColumns) > 0 {
			filter := make(map[string]bool, len(node.NeededColumns))
			for _, col := range node.NeededColumns {
				filter[col] = true
			}
			probe.OutputFilter = filter
		}

		bridge := &deferredJoinBridge{
			childSource: leftSource,
			childOps:    leftOps,
			barrier:     buildDone,
			buildErr:    &buildErr,
			workers:     innerPipelineWorkers(leftSource),
		}

		if joinType == exec.RightJoin || joinType == exec.FullOuterJoin {
			return &joinFlushSource{
				inner:    bridge,
				innerOps: []exec.UnaryOperator{probe},
				probe:    probe,
			}, nil, &exec.CollectSink{}, nil
		}
		return bridge, []exec.UnaryOperator{probe}, &exec.CollectSink{}, nil
	}

	// Immediate: wait for build to complete before accessing hash table state.
	<-buildDone
	if buildErr != nil {
		return nil, nil, nil, buildErr
	}

	// Fix key assignment: parseJoinKeys takes columns from the SQL literally
	// (left of "=" → leftKey, right → rightKey), but the SQL may put the
	// build-side column on the left (e.g., "JOIN t ON t.id = probe.id").
	// After building, we know the build schema; swap any misassigned pairs.
	hj.FixKeyAssignment()

	// SemiAntiFilter already set above (pre-goroutine).

	// For SEMI/ANTI joins, prune build-side batches to only columns needed by
	// the SemiAntiFilter. The build side never appears in the output, so after
	// the hash index is built, only filter columns need to be retained.
	if joinType == exec.SemiJoin || joinType == exec.AntiJoin {
		hj.PruneBuildColumns(keepCols)
	}

	// Insert bloom filter pre-check before probe for early row elimination.
	// Rows whose join key is definitely not in the build side are filtered
	// out via selection vector before they reach the probe operator.
	if bf := hj.BloomPushdownOp(); bf != nil {
		leftOps = append(leftOps, bf)

		// Push bloom filter deeper into the scan layer for row-group-level
		// pruning. If the probe source is a catalog scan with integer join
		// keys, attach a BloomScanFilter so entire row groups whose key
		// range has no hits in the bloom filter are skipped before I/O.
		if bsf := bf.BloomScanFilter(); bsf != nil {
			attachBloomToScanSource(leftSource, bsf)
		}
	}

	// Push dynamic min/max range filter to the scan layer for row-group pruning.
	// Complements the bloom filter: works for all types (string, date, wide int
	// ranges) and is cheaper to evaluate (single range comparison per row group).
	if ranges := hj.BuildKeyRange(); len(ranges) > 0 {
		attachDynamicFilterToScanSource(leftSource, ranges)
	}
	probe := hj.Probe()
	// Push output filter into the probe to avoid materializing intermediate
	// columns not needed by upstream operators. In multi-way joins, this
	// eliminates allocation and gather work for columns that would otherwise
	// be built then immediately dropped.
	if len(node.NeededColumns) > 0 {
		filter := make(map[string]bool, len(node.NeededColumns))
		for _, col := range node.NeededColumns {
			filter[col] = true
		}
		probe.OutputFilter = filter
	}
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

	// For RightSemiJoin/RightAntiJoin, the probe phase marks matched build
	// entries but outputs nothing. After probing, emit matched (semi) or
	// unmatched (anti) build rows.
	if joinType == exec.RightSemiJoin || joinType == exec.RightAntiJoin {
		return &rightSemiFlushSource{
			inner:    leftSource,
			innerOps: leftOps,
			probe:    probe,
			joinType: joinType,
		}, nil, &exec.CollectSink{}, nil
	}

	return leftSource, leftOps, &exec.CollectSink{}, nil
}

// deferredJoinBridge creates a pipeline break for deferred hash join builds.
// Init runs the child pipeline (scan → early probes) with parallel workers,
// overlapping with the deferred build goroutine. After the child pipeline
// completes, it waits for the build barrier, then replays collected batches
// as a Source for the deferred probe operators.
type deferredJoinBridge struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	barrier     <-chan struct{}
	buildErr    *error
	workers     int

	batches []*batch.RecordBatch
	idx     int
	mu      sync.Mutex
}

func (d *deferredJoinBridge) Init(ctx context.Context) error {
	// Run child pipeline (scan → early probes) to collect filtered batches.
	// This overlaps with the deferred build goroutine(s) running in background.
	sink := &exec.CollectSink{}
	pipe := &exec.Pipeline{
		Source:  d.childSource,
		Ops:     d.childOps,
		Sink:    sink,
		Workers: d.workers,
	}
	if err := pipe.Run(ctx); err != nil {
		// Wait for build goroutine to prevent leak
		select {
		case <-d.barrier:
		default:
		}
		return fmt.Errorf("deferred join child pipeline: %w", err)
	}
	d.batches = sink.Batches()

	// Wait for deferred build to complete
	select {
	case <-d.barrier:
	case <-ctx.Done():
		return ctx.Err()
	}
	if *d.buildErr != nil {
		return *d.buildErr
	}
	return nil
}

func (d *deferredJoinBridge) Next(_ context.Context) (*batch.RecordBatch, error) {
	d.mu.Lock()
	if d.idx >= len(d.batches) {
		d.mu.Unlock()
		return nil, nil
	}
	b := d.batches[d.idx]
	d.idx++
	d.mu.Unlock()
	return b, nil
}

func (d *deferredJoinBridge) Close() error {
	return nil
}

// reverseBloomBridge runs the probe-side child pipeline first, builds a bloom
// filter from the collected join key values, injects it into the build-side
// scan, then signals the build goroutine to start. This drastically reduces
// build-side I/O for semi/anti joins where the probe result is much smaller
// than the build table (e.g. Q21: 60M lineitem rows reduced to ~2M when only
// ~500K orders match).
type reverseBloomBridge struct {
	childSource   exec.Source
	childOps      []exec.UnaryOperator
	rbBuildSource *exec.Source // pointer to the goroutine's build source (swappable)
	buildSource   exec.Source  // original build source (unwrapped)
	buildStart    chan struct{}
	barrier       <-chan struct{}
	buildErr      *error
	probeKey      string // probe-side column to extract bloom from
	buildKey      string // build-side column to filter
	workers       int

	batches []*batch.RecordBatch
	idx     int
	mu      sync.Mutex
}

func (rb *reverseBloomBridge) Init(ctx context.Context) error {
	// Phase 1: Run child pipeline to collect probe-side batches.
	sink := &exec.CollectSink{}
	pipe := &exec.Pipeline{
		Source:  rb.childSource,
		Ops:    rb.childOps,
		Sink:   sink,
		Workers: rb.workers,
	}
	if err := pipe.Run(ctx); err != nil {
		close(rb.buildStart)
		<-rb.barrier
		return fmt.Errorf("reverse bloom child pipeline: %w", err)
	}
	rb.batches = sink.Batches()

	// Phase 2: Build bloom from collected batches' join key column.
	bloom, bloomMask := exec.BuildBloomFromBatches(rb.batches, rb.probeKey)

	// Phase 3: Inject bloom filter into build-side pipeline.
	if bloom != nil {
		useIntKey := false
		if len(rb.batches) > 0 {
			if ci := rb.batches[0].ColumnIndex(rb.probeKey); ci >= 0 {
				col := rb.batches[0].Columns[ci]
				useIntKey = col.Type == batch.TypeInt32 || col.Type == batch.TypeInt64 ||
					col.Type == batch.TypePort || col.Type == batch.TypeProtocol ||
					col.Type == batch.TypeDate
			}
		}
		bloomOp := exec.NewBloomFilterOp(bloom, bloomMask, []string{rb.buildKey}, useIntKey)
		*rb.rbBuildSource = &pipelineSource{
			source: rb.buildSource,
			ops:    []exec.UnaryOperator{bloomOp},
		}
	}

	// Phase 4: Signal build goroutine to start with the bloom-filtered scan.
	close(rb.buildStart)

	// Wait for build to complete.
	select {
	case <-rb.barrier:
	case <-ctx.Done():
		return ctx.Err()
	}
	if *rb.buildErr != nil {
		return *rb.buildErr
	}
	return nil
}

func (rb *reverseBloomBridge) Next(_ context.Context) (*batch.RecordBatch, error) {
	rb.mu.Lock()
	if rb.idx >= len(rb.batches) {
		rb.mu.Unlock()
		return nil, nil
	}
	b := rb.batches[rb.idx]
	rb.idx++
	rb.mu.Unlock()
	return b, nil
}

func (rb *reverseBloomBridge) Close() error {
	return nil
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

// rightSemiFlushSource wraps a join probe pipeline for RightSemiJoin/RightAntiJoin.
// During probing, no rows are output (probe marks matched build entries).
// After probing completes, emits matched (RightSemi) or unmatched (RightAnti) build rows.
type rightSemiFlushSource struct {
	inner    exec.Source
	innerOps []exec.UnaryOperator
	probe    *exec.HashJoinProbe
	joinType exec.JoinType
	pipeline *pipelineSource
	flushed  bool
	result   *batch.RecordBatch
}

func (s *rightSemiFlushSource) Init(ctx context.Context) error {
	s.pipeline = &pipelineSource{source: s.inner, ops: s.innerOps}
	return s.pipeline.Init(ctx)
}

func (s *rightSemiFlushSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.flushed {
		// Drain the probe pipeline — RightSemi/RightAnti probe returns nil
		// for each batch (just marks matched entries), so we loop until exhausted.
		for {
			b, err := s.pipeline.Next(ctx)
			if err != nil {
				return nil, err
			}
			if b == nil {
				break
			}
			// probe.Execute returned nil for each batch, but the pipeline
			// source wraps probe as an op, so we just keep pulling
		}
		s.flushed = true
		if s.joinType == exec.RightSemiJoin {
			s.result = s.probe.FlushMatched()
		} else {
			s.result = s.probe.FlushAntiMatched()
		}
	}
	if s.result != nil {
		b := s.result
		s.result = nil
		return b, nil
	}
	return nil, nil
}

func (s *rightSemiFlushSource) Close() error {
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

// BuildSemiAntiFilter compiles a non-equality join filter string (e.g., "l_suppkey != l_suppkey")
// into a function that evaluates the condition on probe and build batch rows.
// Convention: left of operator = probe column, right = build column.
//
// The returned closure lazily resolves column indices on first call and caches
// them, avoiding per-row ColumnByName lookups. Comparisons use typed dispatch
// (int32, int64, float64, string) instead of fmt.Sprint conversion.
func BuildSemiAntiFilter(filter string) func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
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

	// Pre-encode operator as int for switch-free dispatch in hot path.
	const (
		opNE = iota
		opGT
		opLT
		opGE
		opLE
		opEQ
	)
	ops := make([]int, len(conds))
	for i, c := range conds {
		switch c.op {
		case "!=", "<>":
			ops[i] = opNE
		case ">":
			ops[i] = opGT
		case "<":
			ops[i] = opLT
		case ">=":
			ops[i] = opGE
		case "<=":
			ops[i] = opLE
		default:
			ops[i] = opEQ
		}
	}

	// Lazily resolved column indices; reset when batch pointers change.
	probeIdxs := make([]int, len(conds))
	buildIdxs := make([]int, len(conds))
	var prevProbe, prevBuild *batch.RecordBatch

	return func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		// Resolve column indices once per batch pair.
		if probe != prevProbe || build != prevBuild {
			for i, c := range conds {
				probeIdxs[i] = probe.ColumnIndex(c.probeCol)
				buildIdxs[i] = build.ColumnIndex(c.buildCol)
			}
			prevProbe = probe
			prevBuild = build
		}

		for i := range conds {
			pi, bi := probeIdxs[i], buildIdxs[i]
			if pi < 0 || bi < 0 {
				return false
			}
			pv := probe.Columns[pi]
			bv := build.Columns[bi]
			if !evalFilterTyped(pv, bv, probeRow, buildRow, ops[i]) {
				return false
			}
		}
		return true
	}
}

// evalFilterTyped compares two vector values at given rows using typed dispatch.
// Avoids interface boxing and fmt.Sprint allocation on every comparison.
func evalFilterTyped(pv, bv *batch.Vector, pRow, bRow, op int) bool {
	const (
		opNE = iota
		opGT
		opLT
		opGE
		opLE
		opEQ
	)
	switch pv.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		a, b := pv.Int32Data[pRow], bv.Int32Data[bRow]
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		a, b := pv.Int64Data[pRow], bv.Int64Data[bRow]
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	case batch.TypeFloat64:
		a, b := pv.Float64Data[pRow], bv.Float64Data[bRow]
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	case batch.TypeFloat32:
		a, b := pv.Float32Data[pRow], bv.Float32Data[bRow]
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	case batch.TypeString:
		a, b := pv.BytesData.StringValue(pRow), bv.BytesData.StringValue(bRow)
		switch op {
		case opNE:
			return a != b
		case opGT:
			return a > b
		case opLT:
			return a < b
		case opGE:
			return a >= b
		case opLE:
			return a <= b
		default:
			return a == b
		}
	default:
		// Fallback for other types: use GetValue + fmt.Sprint
		as := fmt.Sprint(pv.GetValue(pRow))
		bs := fmt.Sprint(bv.GetValue(bRow))
		switch op {
		case opNE:
			return as != bs
		case opGT:
			return as > bs
		case opLT:
			return as < bs
		case opGE:
			return as >= bs
		case opLE:
			return as <= bs
		default:
			return as == bs
		}
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

// fixJoinKeyOrder ensures left keys are probe-side and right keys are build-side
// by checking against the build subtree's available columns from the plan.
// This avoids the expensive post-build FixKeyAssignment hash table rebuild
// which was 31% of Q09's time at SF10.
func fixJoinKeyOrder(leftKeys, rightKeys []string, buildNode *logical.Node) {
	buildCols := collectPlanColumns(buildNode)
	if len(buildCols) == 0 {
		return
	}
	for i := range leftKeys {
		leftInBuild := buildCols[leftKeys[i]]
		rightInBuild := buildCols[rightKeys[i]]
		if leftInBuild && !rightInBuild {
			leftKeys[i], rightKeys[i] = rightKeys[i], leftKeys[i]
		}
	}
}

// collectPlanColumns returns all column names available from scan nodes
// in the logical plan subtree (lowercased).
func collectPlanColumns(n *logical.Node) map[string]bool {
	if n == nil {
		return nil
	}
	result := make(map[string]bool)
	collectPlanColumnsRec(n, result)
	return result
}

func collectPlanColumnsRec(n *logical.Node, result map[string]bool) {
	if n == nil {
		return
	}
	if n.Type == logical.NodeScan {
		for _, col := range n.ScanColumns {
			result[strings.ToLower(col)] = true
		}
	}
	// Semi/anti joins only output probe-side (child[0]) columns.
	// Skip build side so fixJoinKeyOrder doesn't see build-only columns
	// as available from this subtree.
	if n.Type == logical.NodeJoin && len(n.Children) == 2 {
		jt := strings.ToLower(n.JoinType)
		if jt == "semi" || jt == "anti" {
			collectPlanColumnsRec(n.Children[0], result)
			return
		}
	}
	for _, child := range n.Children {
		collectPlanColumnsRec(child, result)
	}
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

	// CSE: deduplicate identical computed expressions across projections.
	// When the same expression appears multiple times (e.g., EXTRACT(year FROM date)
	// in both SELECT and ORDER BY), compute it once and reuse via ColumnRef.
	projExprDedup := make(map[string]string) // expr string → first output column name

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
		var compiledExpr expr.Expr
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
						compiledExpr = compiled
					}
				}
			}
		}

		if expression == nil && proj.ASTExpr != nil && !proj.IsAgg {
			// CSE: if we already compiled this exact expression for a prior
			// projection column, reuse it via a ColumnRef instead of re-evaluating.
			exprKey := proj.ASTExpr.String()
			if prevCol, ok := projExprDedup[exprKey]; ok {
				expression = exec.ColumnRef(prevCol)
			} else {
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
					compiledExpr = compiled
					projExprDedup[exprKey] = name
				}
			}
		}
		isDirectCopy := expression == nil
		if expression == nil {
			expression = exec.ColumnRef(colRef)
		}

		// Infer output type: TypeString is the default, resolved at runtime from
		// input schema when column names match. For arithmetic expressions that
		// won't match an input column (e.g., nested aggregate rewrites like
		// __agg_0 * 0.0001), use TypeFloat64.
		outType := parquet.TypeString
		if proj.ASTExpr != nil && !proj.IsAgg {
			outType = inferProjectionType(proj.ASTExpr, outType)
		}

		pc := exec.ProjectColumn{
			Name: name,
			Type: outType, // Will be resolved at runtime if input column matches
			Expr: expression,
		}
		// For column renames (e.g., l_suppkey AS supplier_no), record the
		// source column so Project.Execute can resolve the correct type.
		if name != colRef {
			pc.SourceCol = colRef
		}
		// For simple column references (no computed expression), use bulk vector
		// copy instead of per-row evaluation.
		if isDirectCopy {
			pc.DirectCopy = colRef
		}
		// Use vectorized column evaluation when the expression supports it.
		// VecExpr handles any output type (string, numeric, etc.) and is checked
		// before the Float64-specific paths.
		if compiledExpr != nil {
			if ve, ok := compiledExpr.(expr.VecExpr); ok {
				evalVec := ve.EvalVec
				pc.VecEval = func(b *batch.RecordBatch, out *batch.Vector, n int) {
					evalVec(b, out, n)
				}
			}
		}
		// Use typed evaluation to avoid interface{} boxing in the inner loop.
		// Only safe when the output type is explicitly Float64 (arithmetic exprs),
		// not when resolved from input schema (could be Decimal, Timestamp, etc.).
		if compiledExpr != nil && outType == parquet.TypeFloat64 {
			if ve, ok := compiledExpr.(expr.VecFloat64Expr); ok {
				pc.VecFloat64Eval = ve.EvalFloat64Vec
				if binop, ok := ve.(*expr.BinOpFloat64); ok {
					pc.VecFloat64Clone = func() exec.VecFloat64Expression {
						return binop.CloneVec().EvalFloat64Vec
					}
				}
			}
			if fe, ok := compiledExpr.(expr.Float64Expr); ok {
				pc.Float64Eval = fe.EvalFloat64
			} else if ie, ok := compiledExpr.(expr.Int64Expr); ok {
				pc.Int64Eval = ie.EvalInt64
			}
		}
		projCols = append(projCols, pc)
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
	// CSE: deduplicate identical expressions by their string representation.
	var preProjectCols []exec.ProjectColumn
	syntheticNames := make(map[int]string) // agg index → synthetic column name
	exprDedup := make(map[string]string)   // expr string → synthetic column name

	for i, agg := range node.AggExprs {
		if agg.InputExpr != nil && !isSimpleColRef(agg.InputExpr) {
			exprStr := agg.InputExpr.String()
			if existing, ok := exprDedup[exprStr]; ok {
				// Reuse previously compiled expression
				syntheticNames[i] = existing
				continue
			}
			synName := fmt.Sprintf("__agg_expr_%d", i)
			compiled, compErr := expr.CompileWithRunner(agg.InputExpr, p.subqueryRunner)
			if compErr == nil {
				pc := exec.ProjectColumn{
					Name: synName,
					Type: parquet.TypeFloat64,
					Expr: wrapExpr(compiled),
				}
				// Use general vectorized evaluation when available.
				if ve, ok := compiled.(expr.VecExpr); ok {
					evalVec := ve.EvalVec
					pc.VecEval = func(b *batch.RecordBatch, out *batch.Vector, n int) {
						evalVec(b, out, n)
					}
				}
				// Use vectorized float64 evaluation when available (entire column at once),
				// falling back to typed per-row eval.
				if ve, ok := compiled.(expr.VecFloat64Expr); ok {
					pc.VecFloat64Eval = ve.EvalFloat64Vec
					if binop, ok := ve.(*expr.BinOpFloat64); ok {
						pc.VecFloat64Clone = func() exec.VecFloat64Expression {
							return binop.CloneVec().EvalFloat64Vec
						}
					}
				}
				if fe, ok := compiled.(expr.Float64Expr); ok {
					pc.Float64Eval = fe.EvalFloat64
				} else if ie, ok := compiled.(expr.Int64Expr); ok {
					pc.Int64Eval = ie.EvalInt64
				}
				preProjectCols = append(preProjectCols, pc)
				syntheticNames[i] = synName
				exprDedup[exprStr] = synName
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
	if est := findScanRowEstimate(node.Children[0]); est > 0 {
		hashAgg.InputRowHint = est
	}
	if sm := p.getSpillManager(); sm != nil {
		hashAgg.Spill = sm
	}

	// For GROUPING SETS: single-pass mode — convert column names to indices
	var postOps []exec.UnaryOperator
	if len(node.GroupingSets) > 0 {
		colIndex := make(map[string]int, len(groupByCols))
		for i, c := range groupByCols {
			colIndex[c] = i
		}
		sets := make([][]int, len(node.GroupingSets))
		for i, set := range node.GroupingSets {
			indices := make([]int, 0, len(set))
			for _, col := range set {
				if idx, ok := colIndex[col]; ok {
					indices = append(indices, idx)
				}
			}
			sets[i] = indices
		}
		hashAgg.GroupingSets = sets
	} else if len(node.GroupingSetNulls) > 0 {
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

// binOpInvolvesInterval returns true if either operand of a BinaryOp is an
// IntervalLit or a date/timestamp function (current_date, current_timestamp).
// Date ± interval produces a date string, not a numeric value.
// inferProjectionType infers the output parquet type from an AST expression node.
// Returns the inferred type, or the fallback if inference isn't possible.
func inferProjectionType(node plansql.Node, fallback parquet.TypeID) parquet.TypeID {
	switch n := node.(type) {
	case *plansql.BinaryOp:
		if !binOpInvolvesInterval(n) {
			return parquet.TypeFloat64
		}
	case *plansql.FuncCallNode:
		if isNumericFunc(n.Name) {
			return parquet.TypeFloat64
		}
	case *plansql.CastNode:
		return inferCastType(n.TypeName)
	}
	return fallback
}

// isNumericFunc returns true for scalar functions known to return numeric output.
func isNumericFunc(name string) bool {
	switch strings.ToLower(name) {
	case "round", "abs", "ceil", "ceiling", "floor", "sqrt", "ln", "log", "log2",
		"log10", "exp", "pow", "power", "sign", "trunc", "truncate",
		"sin", "cos", "tan", "asin", "acos", "atan", "atan2",
		"radians", "degrees", "pi", "mod", "greatest", "least",
		"coalesce", "nullif", "random":
		return true
	}
	return false
}

// inferCastType maps SQL type names to parquet types for CAST expressions.
func inferCastType(typeName string) parquet.TypeID {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "INTEGER", "INT", "BIGINT", "INT64":
		return parquet.TypeInt64
	case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "FLOAT64", "NUMERIC", "DECIMAL":
		return parquet.TypeFloat64
	case "BOOLEAN", "BOOL":
		return parquet.TypeBool
	default:
		return parquet.TypeString
	}
}

func binOpInvolvesInterval(b *plansql.BinaryOp) bool {
	return nodeIsDateOrInterval(b.Left) || nodeIsDateOrInterval(b.Right)
}

func nodeIsDateOrInterval(n plansql.Node) bool {
	switch v := n.(type) {
	case *plansql.IntervalLit:
		return true
	case *plansql.FuncCallNode:
		lower := strings.ToLower(v.Name)
		return lower == "current_date" || lower == "current_timestamp" ||
			lower == "current_time" || lower == "now" ||
			lower == "date_add" || lower == "date_sub"
	case *plansql.BinaryOp:
		// Nested: (CURRENT_DATE - INTERVAL '1' DAY) + INTERVAL '2' HOUR
		return binOpInvolvesInterval(v)
	default:
		return false
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
	computed          []exec.ProjectColumn
	cachedSchema      []parquet.Column    // cached output schema (computed once)
	cachedOutput      *batch.RecordBatch  // reused output batch across calls
	computedCap       int                 // row capacity of cached computed vectors
	canPassSelThrough bool                // true if all computed columns are numeric (no BytesColumn)
	checkedSelPass    bool                // true after first call resolves canPassSelThrough
	matPool           *batch.BatchPool    // pool for materialize buffers (avoids per-call allocation)
}

func (a *aggPreProject) Init(_ context.Context) error { return nil }

// Clone returns a copy with deep-cloned VecFloat64Eval expression trees.
// Each parallel worker must have its own BinOpFloat64.vecBuf scratch buffers
// to avoid data races during concurrent vectorized evaluation.
func (a *aggPreProject) Clone() exec.UnaryOperator {
	clonedComputed := make([]exec.ProjectColumn, len(a.computed))
	copy(clonedComputed, a.computed)
	for i, c := range clonedComputed {
		if c.VecFloat64Clone != nil {
			clonedComputed[i].VecFloat64Eval = c.VecFloat64Clone()
		}
	}
	return &aggPreProject{computed: clonedComputed}
}

func (a *aggPreProject) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	// Check once whether all computed columns are numeric. If so, we can keep
	// the selection vector and write computed values at sparse indices — avoiding
	// the full materialize copy. BytesColumn.Set requires sequential writes,
	// so string-typed computed columns still need materialize.
	if !a.checkedSelPass {
		a.checkedSelPass = true
		a.canPassSelThrough = true
		for _, c := range a.computed {
			switch c.Type {
			case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
				a.canPassSelThrough = false
			}
		}
	}

	hasSel := in.Sel != nil
	if hasSel && !a.canPassSelThrough {
		in = a.materialize(in)
		hasSel = false
	}
	// When vectorized eval is available, materialize to get the dense non-sel
	// path. The materialize copy cost (~24KB for 500 rows × 6 cols) is far
	// cheaper than per-row Float64Eval (0.65s vs 0.07s vectorized at SF1).
	if hasSel && a.hasVecFloat64() {
		in = a.materialize(in)
		hasSel = false
	}

	// Cache output schema on first call (avoids per-batch allocation)
	if a.cachedSchema == nil {
		schema := make([]parquet.Column, 0, len(in.Schema)+len(a.computed))
		schema = append(schema, in.Schema...)
		for _, c := range a.computed {
			schema = append(schema, parquet.Column{
				Name:     c.Name,
				Type:     c.Type,
				Nullable: true,
			})
		}
		a.cachedSchema = schema
	}

	computedOffset := len(in.Schema)

	if a.cachedOutput == nil || in.Len > a.computedCap {
		// First call or input exceeds computed vector capacity — allocate
		a.cachedOutput = &batch.RecordBatch{
			Schema:  a.cachedSchema,
			Columns: make([]*batch.Vector, len(a.cachedSchema)),
		}
		for k, c := range a.computed {
			a.cachedOutput.Columns[computedOffset+k] = batch.NewVector(c.Type, in.Len)
		}
		a.computedCap = in.Len
	} else {
		// Reuse computed vectors — reset null bitmaps and bytes columns
		for k := range a.computed {
			col := a.cachedOutput.Columns[computedOffset+k]
			col.Len = in.Len
			col.Nulls.ResetNonNull(in.Len)
			switch col.Type {
			case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
				col.BytesData.Reset()
			}
		}
	}
	a.cachedOutput.Len = in.Len
	a.cachedOutput.Sel = in.Sel // pass through selection vector (nil if materialized)

	// Share existing column vectors (zero-copy for pass-through columns).
	// Detach the input batch from its pool so that the pipeline's
	// prev.Release() becomes a no-op — the shared column data must remain
	// valid while cachedOutput is consumed by downstream operators/sinks.
	in.Detach()
	for j := 0; j < len(in.Schema); j++ {
		a.cachedOutput.Columns[j] = in.Columns[j]
	}

	// Compute expression columns.
	// When selection vector is present and all computed columns are numeric,
	// write values only at selected indices (avoiding full materialize).
	for k, c := range a.computed {
		col := a.cachedOutput.Columns[computedOffset+k]
		if hasSel {
			if c.Float64Eval != nil {
				for _, idx := range in.Sel {
					v, ok := c.Float64Eval(in, int(idx))
					if ok {
						col.Float64Data[idx] = v
					} else {
						col.Nulls.SetNull(int(idx))
					}
				}
			} else if c.Int64Eval != nil {
				for _, idx := range in.Sel {
					v, ok := c.Int64Eval(in, int(idx))
					if ok {
						col.Int64Data[idx] = v
					} else {
						col.Nulls.SetNull(int(idx))
					}
				}
			} else {
				for _, idx := range in.Sel {
					col.SetValue(int(idx), c.Expr(in, int(idx)))
				}
			}
		} else {
			if c.VecEval != nil {
				c.VecEval(in, col, in.Len)
			} else if c.VecFloat64Eval != nil {
				c.VecFloat64Eval(in, col.Float64Data, in.Len)
			} else if c.Float64Eval != nil {
				for i := 0; i < in.Len; i++ {
					v, ok := c.Float64Eval(in, i)
					if ok {
						col.Float64Data[i] = v
					} else {
						col.Nulls.SetNull(i)
					}
				}
			} else if c.Int64Eval != nil {
				for i := 0; i < in.Len; i++ {
					v, ok := c.Int64Eval(in, i)
					if ok {
						col.Int64Data[i] = v
					} else {
						col.Nulls.SetNull(i)
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					col.SetValue(i, c.Expr(in, i))
				}
			}
		}
	}

	return a.cachedOutput, nil
}

// materialize compacts a batch with a selection vector into a dense batch
// with only the selected rows, removing the selection vector.
// Uses a pooled batch to avoid per-call allocation overhead. GatherColumn
// handles both data and null bitmap gathering internally.
func (a *aggPreProject) materialize(in *batch.RecordBatch) *batch.RecordBatch {
	n := len(in.Sel)
	if a.matPool == nil {
		a.matPool = batch.NewBatchPool(in.Schema, batch.DefaultBatchSize)
	}
	out := a.matPool.GetForSize(n)
	for j := range in.Schema {
		exec.GatherColumn(out.Columns[j], in.Columns[j], in.Sel)
	}
	return out
}

// hasVecFloat64 returns true if any computed column has vectorized eval.
func (a *aggPreProject) hasVecFloat64() bool {
	for _, c := range a.computed {
		if c.VecEval != nil || c.VecFloat64Eval != nil {
			return true
		}
	}
	return false
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
	if sm := p.getSpillManager(); sm != nil {
		sortOp.Spill = sm
	}

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

	child := node.Children[0]

	// Optimization: Limit(Sort(...)) → TopN sort (heap-based, keeps only N rows)
	if child.Type == logical.NodeSort && node.OffsetVal == 0 {
		return p.buildTopN(ctx, child, node.LimitVal)
	}

	source, ops, sink, err := p.buildPipeline(ctx, child)
	if err != nil {
		return nil, nil, nil, err
	}

	// Push LIMIT hint to scan source: enables lazy file downloading instead
	// of the eager "download all files upfront" strategy. Safe when no pipeline
	// breaker (sort/aggregate/join) sits between LIMIT and SCAN — if there is
	// one, the source won't be a catalogScanSource and the assertion fails.
	if cs, ok := source.(*catalogScanSource); ok {
		cs.rowLimit = int64(node.LimitVal) + int64(node.OffsetVal)
	}

	limit := exec.NewLimit(int64(node.LimitVal), int64(node.OffsetVal))
	ops = append(ops, limit)

	return source, ops, sink, nil
}

func (p *Planner) buildTopN(ctx context.Context, sortNode *logical.Node, n int) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	childSource, childOps, _, err := p.buildPipeline(ctx, sortNode.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	var keys []exec.SortKey
	for _, ob := range sortNode.OrderBy {
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
	sortOp.Limit = n // Top-K: only materialize top N rows

	return &sortSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		sort:        sortOp,
		limitN:      n,
	}, nil, &exec.CollectSink{}, nil
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
	if sm := p.getSpillManager(); sm != nil {
		winOp.Spill = sm
	}

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
			resultRows = intersectRows(leftSink.ToRows(), rightSink.ToRows(), u.all)
		case "except":
			resultRows = exceptRows(leftSink.ToRows(), rightSink.ToRows(), u.all)
		default: // "union"
			resultRows = append(leftSink.ToRows(), rightSink.ToRows()...)
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
	src := &catalogScanSource{
		catalog:         p.catalog,
		tableName:       tableName,
		partitionFilter: partFilter,
		requiredCols:    requiredCols,
		scanPreds:       scanPreds,
	}
	// Attach scan cache if this table is scanned multiple times in this query.
	if p.scanCache != nil {
		if cached, ok := p.scanCache[tableName]; ok {
			src.cache = cached
		}
	}
	return src
}

// catalogScanSource adapts the scan.Scanner to exec.Source.
//
// Note: Pipeline.runParallel calls Source.Next() concurrently from multiple
// worker goroutines on a single source instance, so replayIdx must be atomic.
// Previously it was a plain int and the race detector caught it producing
// non-deterministic Q02 row counts (4/5/6 rows depending on which goroutine
// won the increment).
type catalogScanSource struct {
	catalog         *catalog.Catalog
	tableName       string
	partitionFilter map[string]string
	requiredCols    []string
	scanPreds       []logical.Predicate
	allowedFiles    []string // probe-split: only scan these files (nil = all)
	inner           exec.Source
	cache           *scanCached           // non-nil when this table is scanned multiple times
	replayIdx       atomic.Int64          // position in cache replay (atomic for parallel pipeline)
	isReplay        bool                  // true when reading from cache instead of scanning; written once in Init before runParallel starts, so no synchronization needed
	bloomFilter     *exec.BloomScanFilter // bloom filter pushdown from hash join build side
	dynamicFilter   []exec.DynamicRange   // dynamic min/max range filter from hash join build side
	rowLimit        int64                 // LIMIT pushdown: enables lazy file downloading (0 = eager)
}

// SetBloomFilter attaches a bloom filter for scan-level row group pruning.
func (s *catalogScanSource) SetBloomFilter(bf *exec.BloomScanFilter) {
	s.bloomFilter = bf
}

// SetDynamicFilter attaches a dynamic min/max range filter for row group pruning.
func (s *catalogScanSource) SetDynamicFilter(ranges []exec.DynamicRange) {
	s.dynamicFilter = ranges
}

func (s *catalogScanSource) Init(ctx context.Context) error {
	if s.cache != nil {
		s.cache.mu.Lock()
		if s.cache.done {
			s.cache.mu.Unlock()
			// Scan already complete — replay from cache.
			s.isReplay = true
			s.replayIdx.Store(0)
			return nil
		}
		if s.cache.ready != nil {
			// Another goroutine is populating the cache. Wait for it.
			s.cache.mu.Unlock()
			select {
			case <-s.cache.ready:
			case <-ctx.Done():
				return ctx.Err()
			}
			s.isReplay = true
			s.replayIdx.Store(0)
			return nil
		}
		// First scan claims the cache.
		s.cache.ready = make(chan struct{})
		s.cache.mu.Unlock()
	}
	// First scan (or no cache) — scan from storage.
	sc := newScannerSource(s.catalog, s.tableName, s.partitionFilter, s.requiredCols, s.scanPreds)
	if ses, ok := sc.(*scannerExecSource); ok {
		if s.bloomFilter != nil {
			ses.bloomFilter = s.bloomFilter
		}
		if s.dynamicFilter != nil {
			ses.dynamicFilter = s.dynamicFilter
		}
		if s.allowedFiles != nil {
			ses.allowedFiles = s.allowedFiles
		}
		if s.rowLimit > 0 {
			ses.rowLimit = s.rowLimit
		}
	}
	s.inner = sc
	return s.inner.Init(ctx)
}

func (s *catalogScanSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if s.isReplay {
		// cache.batches is stable (read-only) after cache.done=true.
		// replayIdx is atomic because Pipeline.runParallel may call Next()
		// from multiple worker goroutines on this same source instance.
		idx := s.replayIdx.Add(1) - 1
		if idx >= int64(len(s.cache.batches)) {
			return nil, nil
		}
		cached := s.cache.batches[idx]
		// Return a shallow copy: shared column vectors (read-only), independent Sel.
		// This prevents downstream operators from corrupting cached data via in-place
		// Sel mutation.
		clone := &batch.RecordBatch{
			Schema:  cached.Schema,
			Columns: make([]*batch.Vector, len(cached.Columns)),
			Len:     cached.Len,
		}
		copy(clone.Columns, cached.Columns)
		return clone, nil
	}

	// When this scan is populating a shared cache, the entire pull-and-cache
	// step must run under cache.mu — otherwise the parallel pipeline races
	// where one worker pulls nil and sets cache.done=true while OTHER workers
	// still hold real batches that they've pulled but not yet appended. Those
	// late workers see done==true and skip the append, silently dropping
	// rows that the second (replay) scanner of this same table needs. This
	// surfaced as Q02's intermittent 4-rows-instead-of-5 result at SF0.01.
	if s.cache != nil {
		s.cache.mu.Lock()
		defer s.cache.mu.Unlock()
		if s.cache.done {
			// Another worker finished the scan while we were waiting on the
			// lock. Tell our caller "no more batches" so they fall through.
			return nil, nil
		}
		b, err := s.inner.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			s.cache.done = true
			if s.cache.ready != nil {
				close(s.cache.ready)
			}
			return nil, nil
		}
		// Detach from pool so the pipeline's b.Release() is a no-op.
		// Without this, the pool recycles the batch and the scanner
		// overwrites the Vectors that the cache references.
		b.Detach()
		// Cache a shallow copy so the first consumer's operators don't
		// corrupt cached data by setting Sel in-place.
		cached := &batch.RecordBatch{
			Schema:  b.Schema,
			Columns: make([]*batch.Vector, len(b.Columns)),
			Len:     b.Len,
		}
		copy(cached.Columns, b.Columns)
		s.cache.batches = append(s.cache.batches, cached)
		return b, nil
	}

	// No cache: inner.Next() is thread-safe for channel-based scan sources.
	return s.inner.Next(ctx)
}

func (s *catalogScanSource) Close() error {
	if s.isReplay {
		return nil
	}
	if s.inner != nil {
		return s.inner.Close()
	}
	return nil
}

func (s *catalogScanSource) RowsScanned() int64 {
	if s.isReplay {
		var total int64
		for _, b := range s.cache.batches {
			total += int64(b.Len)
		}
		return total
	}
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
			// Try to extract vectorized filter for simple comparison patterns.
			// First try full vectorization, then partial (vectorize what we can
			// from AND chains, keep the rest as row-at-a-time predicates).
			if vf := tryVectorizeFilter(compiled); vf != nil {
				return vf
			}
			if vf := tryPartialVectorize(compiled); vf != nil {
				return vf
			}
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

// tryVectorizeFilter inspects a compiled expression tree and returns a vectorized
// filter operator when the pattern is a simple comparison (col op col, col op const)
// or an AND chain of such comparisons. Returns nil for complex expressions.
func tryVectorizeFilter(e expr.Expr) exec.UnaryOperator {
	ops := extractFilterOps(e)
	if len(ops) == 0 {
		return nil
	}
	if len(ops) == 1 {
		return ops[0]
	}
	return exec.NewChainFilter(ops)
}

// tryPartialVectorize handles AND chains where some operands are vectorizable and
// some are not. Vectorized operands run first (narrowing the selection vector),
// followed by row-at-a-time predicates for the rest. This is better than falling
// back entirely to row-at-a-time when any part of an AND chain isn't vectorizable.
func tryPartialVectorize(e expr.Expr) exec.UnaryOperator {
	parts := flattenAnds(e)
	if len(parts) < 2 {
		return nil // not an AND chain
	}
	var vectorized []exec.UnaryOperator
	var nonVectorized []expr.Expr
	for _, part := range parts {
		ops := extractFilterOps(part)
		if ops != nil {
			vectorized = append(vectorized, ops...)
		} else {
			nonVectorized = append(nonVectorized, part)
		}
	}
	if len(vectorized) == 0 {
		return nil
	}
	// Put vectorized filters first to narrow selection, then slow predicates
	allOps := make([]exec.UnaryOperator, 0, len(vectorized)+len(nonVectorized))
	allOps = append(allOps, vectorized...)
	for _, e := range nonVectorized {
		allOps = append(allOps, exec.NewFilter(wrapPredicate(e)))
	}
	if len(allOps) == 1 {
		return allOps[0]
	}
	return exec.NewChainFilter(allOps)
}

// flattenAnds recursively flattens nested AND expressions into a flat list.
func flattenAnds(e expr.Expr) []expr.Expr {
	if and, ok := e.(*expr.And); ok {
		return append(flattenAnds(and.Left), flattenAnds(and.Right)...)
	}
	return []expr.Expr{e}
}

// extractFilterOps recursively extracts vectorizable filter ops from AND combinations.
func extractFilterOps(e expr.Expr) []exec.UnaryOperator {
	switch v := e.(type) {
	case *expr.Cmp:
		op := cmpToExecOp(v.Op)
		// col op col
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				return []exec.UnaryOperator{exec.NewColColFilter(lc.Name, rc.Name, op)}
			}
		}
		// col op const
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{exec.NewKernelFilter(lc.Name, op, lit.Val)}
			}
		}
		// const op col → flip
		if lit, lok := v.Left.(*expr.Lit); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				return []exec.UnaryOperator{exec.NewKernelFilter(rc.Name, flipOp(op), lit.Val)}
			}
		}
	case *expr.CmpInt64:
		op := cmpToExecOp(v.Op)
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				return []exec.UnaryOperator{exec.NewColColFilter(lc.Name, rc.Name, op)}
			}
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{exec.NewKernelFilter(lc.Name, op, lit.Val)}
			}
		}
	case *expr.CmpFloat64:
		op := cmpToExecOp(v.Op)
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				return []exec.UnaryOperator{exec.NewColColFilter(lc.Name, rc.Name, op)}
			}
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{exec.NewKernelFilter(lc.Name, op, lit.Val)}
			}
		}
	case *expr.And:
		leftOps := extractFilterOps(v.Left)
		if leftOps == nil {
			return nil
		}
		rightOps := extractFilterOps(v.Right)
		if rightOps == nil {
			return nil
		}
		return append(leftOps, rightOps...)
	case *expr.Or:
		leftOps := extractFilterOps(v.Left)
		rightOps := extractFilterOps(v.Right)
		if leftOps != nil && rightOps != nil {
			var left, right exec.UnaryOperator
			if len(leftOps) == 1 {
				left = leftOps[0]
			} else {
				left = exec.NewChainFilter(leftOps)
			}
			if len(rightOps) == 1 {
				right = rightOps[0]
			} else {
				right = exec.NewChainFilter(rightOps)
			}
			return []exec.UnaryOperator{exec.NewOrFilter(left, right)}
		}
	case *expr.Between:
		// col BETWEEN low AND high → two kernel filters: col >= low AND col <= high
		// col NOT BETWEEN low AND high → col < low OR col > high
		if col, ok := v.Expr.(*expr.ColRef); ok {
			if lo, lok := v.Low.(*expr.Lit); lok {
				if hi, hok := v.Hi.(*expr.Lit); hok {
					if v.Not {
						return []exec.UnaryOperator{exec.NewOrFilter(
							exec.NewKernelFilter(col.Name, exec.OpLt, lo.Val),
							exec.NewKernelFilter(col.Name, exec.OpGt, hi.Val),
						)}
					}
					return []exec.UnaryOperator{
						exec.NewKernelFilter(col.Name, exec.OpGe, lo.Val),
						exec.NewKernelFilter(col.Name, exec.OpLe, hi.Val),
					}
				}
			}
		}
	case *expr.In:
		// col IN (lit, lit, ...) or col NOT IN (lit, lit, ...)
		if col, ok := v.Expr.(*expr.ColRef); ok {
			values := make([]any, 0, len(v.Values))
			for _, val := range v.Values {
				if lit, ok := val.(*expr.Lit); ok {
					values = append(values, lit.Val)
				} else {
					return nil // non-literal in IN list
				}
			}
			return []exec.UnaryOperator{exec.NewInFilter(col.Name, values, v.Not)}
		}
	case *expr.Like:
		// col LIKE 'pattern' or col NOT LIKE 'pattern'
		if col, ok := v.Expr.(*expr.ColRef); ok {
			if pat, ok := v.Pattern.(*expr.Lit); ok {
				if s, ok := pat.Val.(string); ok {
					return []exec.UnaryOperator{exec.NewLikeFilter(col.Name, s, v.Not)}
				}
			}
		}
	case *expr.IsNull:
		// col IS NULL / col IS NOT NULL — vectorized null bitmap scan
		if col, ok := v.Operand.(*expr.ColRef); ok {
			return []exec.UnaryOperator{exec.NewNullCheckFilter(col.Name, !v.Not)}
		}
	case *expr.Not:
		// NOT (expr) — try to vectorize the inner expression
		inner := extractFilterOps(v.Operand)
		if inner != nil {
			return inner
		}
	}
	return nil
}

func cmpToExecOp(op expr.CmpOp) exec.CompareOp {
	switch op {
	case expr.CmpEq:
		return exec.OpEq
	case expr.CmpNe:
		return exec.OpNe
	case expr.CmpLt:
		return exec.OpLt
	case expr.CmpLe:
		return exec.OpLe
	case expr.CmpGt:
		return exec.OpGt
	case expr.CmpGe:
		return exec.OpGe
	default:
		return exec.OpEq
	}
}

func flipOp(op exec.CompareOp) exec.CompareOp {
	switch op {
	case exec.OpLt:
		return exec.OpGt
	case exec.OpLe:
		return exec.OpGe
	case exec.OpGt:
		return exec.OpLt
	case exec.OpGe:
		return exec.OpLe
	default:
		return op
	}
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

// frontLoadBlooms reorders the operator chain to move bloom filter operators
// whose key columns exist in the source scan schema to the front. In multi-way
// join pipelines, this allows selective bloom filters (e.g., from semi-joins
// with HAVING filters) to eliminate rows before expensive join probes.
func frontLoadBlooms(source exec.Source, ops []exec.UnaryOperator) []exec.UnaryOperator {
	if len(ops) < 3 {
		return ops // need at least bloom+probe+bloom to benefit
	}

	// Get source scan columns
	var scanCols map[string]bool
	switch s := source.(type) {
	case *catalogScanSource:
		scanCols = make(map[string]bool, len(s.requiredCols))
		for _, c := range s.requiredCols {
			scanCols[c] = true
		}
	case *scannerExecSource:
		scanCols = make(map[string]bool, len(s.requiredCols))
		for _, c := range s.requiredCols {
			scanCols[c] = true
		}
	}
	if len(scanCols) == 0 {
		return ops
	}

	// Find bloom filters that can be applied to the source scan (their key
	// columns all exist in the scan schema) and that are NOT already at the
	// front of the pipeline (i.e., there's a non-bloom op before them).
	firstNonBloom := -1
	for i, op := range ops {
		if _, ok := op.(*exec.BloomFilterOp); !ok {
			firstNonBloom = i
			break
		}
	}
	if firstNonBloom < 0 {
		return ops // all ops are blooms (unlikely)
	}

	var front, rest []exec.UnaryOperator
	for i, op := range ops {
		bf, isBF := op.(*exec.BloomFilterOp)
		if isBF && i > firstNonBloom {
			// This bloom is after a non-bloom op — check if it can be front-loaded
			allPresent := true
			for _, key := range bf.KeyColumns() {
				if !scanCols[key] {
					allPresent = false
					break
				}
			}
			if allPresent {
				front = append(front, op)
				continue
			}
		}
		rest = append(rest, op)
	}

	if len(front) == 0 {
		return ops
	}
	return append(front, rest...)
}

// attachBloomToScanSource walks the probe-side source chain to find a
// catalogScanSource and attaches a bloom filter for row-group-level pruning.
func attachBloomToScanSource(source exec.Source, bsf *exec.BloomScanFilter) {
	switch s := source.(type) {
	case *catalogScanSource:
		s.SetBloomFilter(bsf)
	case *pipelineSource:
		attachBloomToScanSource(s.source, bsf)
	}
}

// attachDynamicFilterToScanSource walks the probe-side source chain to find a
// catalogScanSource and attaches a dynamic min/max range filter for row-group pruning.
func attachDynamicFilterToScanSource(source exec.Source, ranges []exec.DynamicRange) {
	switch s := source.(type) {
	case *catalogScanSource:
		s.SetDynamicFilter(ranges)
	case *pipelineSource:
		attachDynamicFilterToScanSource(s.source, ranges)
	}
}

// findScanRowEstimate returns the total row estimate from scan nodes in a subtree.
// Used to pre-allocate hash join arena and index.
func findScanRowEstimate(node *logical.Node) int64 {
	if node == nil {
		return 0
	}
	if node.Type == logical.NodeScan {
		return node.ScanRowEstimate
	}
	// For aggregates, row count is much smaller than scan (assume 10% or 2M max).
	// At SF100, high-cardinality GROUP BY (e.g. Q17: ~20M l_partkey values)
	// can produce millions of groups; 100K was too low and forced repeated
	// hash table doublings during execution.
	if node.Type == logical.NodeAggregate {
		est := findScanRowEstimate(node.Children[0])
		reduced := est / 10
		if reduced > 2_000_000 {
			reduced = 2_000_000
		}
		if reduced < 1 {
			reduced = 1
		}
		return reduced
	}
	var total int64
	for _, child := range node.Children {
		total += findScanRowEstimate(child)
	}
	return total
}

// extractColumnRefs extracts raw column name references from a string that
// may be a simple column ("l_shipdate"), a qualified column ("n1.n_name"),
// or an expression ("substr(l_shipdate, 1, 4)", "l_extendedprice * (1 - l_discount)").
// Returns the string itself if it's a simple/qualified column name.
func extractColumnRefs(s string) []string {
	// Simple or qualified column name: contains only alphanumerics, underscores, dots
	isSimple := true
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
			isSimple = false
			break
		}
	}
	if isSimple {
		return []string{s}
	}

	// Expression: extract identifier tokens that look like column references.
	// Tokenize by splitting on non-identifier characters, then filter out
	// SQL keywords and numeric literals.
	var refs []string
	seen := make(map[string]bool)
	start := -1
	for i, c := range s {
		isIdent := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '.' || (c >= '0' && c <= '9')
		if isIdent {
			if start == -1 {
				start = i
			}
		} else {
			if start >= 0 {
				tok := s[start:i]
				start = -1
				if isColumnRef(tok) && !seen[tok] {
					refs = append(refs, tok)
					seen[tok] = true
				}
			}
		}
	}
	if start >= 0 {
		tok := s[start:]
		if isColumnRef(tok) && !seen[tok] {
			refs = append(refs, tok)
			seen[tok] = true
		}
	}
	return refs
}

// isColumnRef returns true if a token looks like a column reference:
// not a number, not a SQL keyword, contains at least one underscore or letter.
func isColumnRef(tok string) bool {
	if len(tok) == 0 {
		return false
	}
	// Pure number
	allDigit := true
	for _, c := range tok {
		if c < '0' || c > '9' {
			allDigit = false
			break
		}
	}
	if allDigit {
		return false
	}
	// SQL keywords to skip
	lower := strings.ToLower(tok)
	switch lower {
	case "case", "when", "then", "else", "end", "and", "or", "not", "in",
		"is", "null", "true", "false", "like", "between", "as", "asc", "desc":
		return false
	}
	return true
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
			return exec.NewKernelFilter(col, o.op, val)
		}
	}

	// LIKE / NOT LIKE
	upper := strings.ToUpper(raw)
	if idx := strings.Index(upper, " NOT LIKE "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		pattern := strings.TrimSpace(raw[idx+len(" NOT LIKE "):])
		pattern = strings.Trim(pattern, "'")
		return exec.NewLikeFilter(col, pattern, true)
	}
	if idx := strings.Index(upper, " LIKE "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		pattern := strings.TrimSpace(raw[idx+len(" LIKE "):])
		pattern = strings.Trim(pattern, "'")
		return exec.NewLikeFilter(col, pattern, false)
	}

	// IS NULL / IS NOT NULL — vectorized null bitmap scan
	if strings.Contains(upper, "IS NOT NULL") {
		col := cleanExpr(strings.TrimSpace(raw[:strings.Index(upper, "IS NOT NULL")]))
		return exec.NewNullCheckFilter(col, false)
	}
	if strings.Contains(upper, "IS NULL") {
		col := cleanExpr(strings.TrimSpace(raw[:strings.Index(upper, "IS NULL")]))
		return exec.NewNullCheckFilter(col, true)
	}

	// BETWEEN: "col between X and Y" → col >= X AND col <= Y
	if idx := strings.Index(upper, " BETWEEN "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		rest := strings.TrimSpace(raw[idx+len(" BETWEEN "):])
		andIdx := strings.Index(strings.ToUpper(rest), " AND ")
		if andIdx >= 0 {
			lo := parseValue(strings.TrimSpace(rest[:andIdx]))
			hi := parseValue(strings.TrimSpace(rest[andIdx+len(" AND "):]))
			return exec.NewChainFilter([]exec.UnaryOperator{
				exec.NewKernelFilter(col, exec.OpGe, lo),
				exec.NewKernelFilter(col, exec.OpLe, hi),
			})
		}
	}

	// IN: "col in (v1, v2, v3)" → vectorized set membership
	if idx := strings.Index(upper, " IN "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		rest := strings.TrimSpace(raw[idx+len(" IN "):])
		rest = strings.TrimPrefix(rest, "(")
		rest = strings.TrimSuffix(rest, ")")
		parts := strings.Split(rest, ",")
		values := make([]any, 0, len(parts))
		for _, part := range parts {
			values = append(values, parseValue(strings.TrimSpace(part)))
		}
		if len(values) > 0 {
			return exec.NewInFilter(col, values, false)
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

// leafStages returns the IDs of stages that are not depended upon by any other
// stage in the slice. These are the "output" stages of a subtree whose results
// the parent stage should read.
func leafStages(stages []Stage) []string {
	depended := make(map[string]bool, len(stages))
	for _, s := range stages {
		for _, d := range s.Dependencies {
			depended[d] = true
		}
	}
	var leaves []string
	for _, s := range stages {
		if !depended[s.ID] {
			leaves = append(leaves, s.ID)
		}
	}
	return leaves
}

// mergeFanout is the maximum number of upstream results a single merge task
// should handle. When upstream tasks exceed this, a multi-level merge tree
// is emitted with intermediate merge stages for parallel merging.
const mergeFanout = 16

// estimateUpstreamTasks sums the Tasks field of leaf stages in a subtree.
func estimateUpstreamTasks(childStages []Stage, leafIDs []string) int {
	leafSet := make(map[string]bool, len(leafIDs))
	for _, id := range leafIDs {
		leafSet[id] = true
	}
	total := 0
	for _, s := range childStages {
		if leafSet[s.ID] {
			if s.Tasks > 0 {
				total += s.Tasks
			} else {
				total++
			}
		}
	}
	return total
}

// emitMergeAggregateTree emits a final_aggregate stage, or a two-level merge
// tree when upstream tasks exceed mergeFanout for parallel merging.
func emitMergeAggregateTree(stages *[]Stage, leafIDs []string, groupBy []string, aggSpecs []AggSpec, childStages []Stage) {
	upstream := estimateUpstreamTasks(childStages, leafIDs)
	if upstream <= mergeFanout {
		// Single-level: one final_aggregate merges all results
		finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
		*stages = append(*stages, Stage{
			ID:           finalStageID,
			Type:         "final_aggregate",
			Tasks:        1,
			GroupByCols:  groupBy,
			AggSpecs:     aggSpecs,
			Dependencies: leafIDs,
		})
		return
	}

	// Multi-level: split into groups of mergeFanout
	numGroups := (upstream + mergeFanout - 1) / mergeFanout
	intermIDs := make([]string, numGroups)
	for g := 0; g < numGroups; g++ {
		id := fmt.Sprintf("merge_aggregate-%d-%d", len(*stages), g)
		intermIDs[g] = id
		*stages = append(*stages, Stage{
			ID:              id,
			Type:            "final_aggregate",
			Tasks:           1,
			GroupByCols:     groupBy,
			AggSpecs:        aggSpecs,
			Dependencies:    leafIDs,
			MergeGroup:      g,
			MergeGroupCount: numGroups,
		})
	}
	// Final merge of intermediate results
	finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
	*stages = append(*stages, Stage{
		ID:           finalStageID,
		Type:         "final_aggregate",
		Tasks:        1,
		GroupByCols:  groupBy,
		AggSpecs:     aggSpecs,
		Dependencies: intermIDs,
	})
}

// emitMergeSortTree emits a merge_sort stage, or a two-level merge tree
// when upstream sort tasks exceed mergeFanout.
func emitMergeSortTree(stages *[]Stage, sortStageID string, sortKeys []SortKeySpec, childStages []Stage) {
	// Estimate upstream sort tasks from child scan tasks
	upstream := 0
	for _, s := range childStages {
		if s.Tasks > 0 {
			upstream += s.Tasks
		} else {
			upstream++
		}
	}
	if upstream <= mergeFanout {
		// Single-level: one merge_sort merges all partial results
		mergeStageID := fmt.Sprintf("merge_sort-%d", len(*stages))
		*stages = append(*stages, Stage{
			ID:           mergeStageID,
			Type:         "merge_sort",
			Tasks:        1,
			SortKeys:     sortKeys,
			Dependencies: []string{sortStageID},
		})
		return
	}

	// Multi-level: split into groups of mergeFanout
	numGroups := (upstream + mergeFanout - 1) / mergeFanout
	intermIDs := make([]string, numGroups)
	for g := 0; g < numGroups; g++ {
		id := fmt.Sprintf("merge_sort-%d-%d", len(*stages), g)
		intermIDs[g] = id
		*stages = append(*stages, Stage{
			ID:              id,
			Type:            "merge_sort",
			Tasks:           1,
			SortKeys:        sortKeys,
			Dependencies:    []string{sortStageID},
			MergeGroup:      g,
			MergeGroupCount: numGroups,
		})
	}
	// Final merge of intermediate sorted results
	finalMergeID := fmt.Sprintf("merge_sort-%d", len(*stages))
	*stages = append(*stages, Stage{
		ID:           finalMergeID,
		Type:         "merge_sort",
		Tasks:        1,
		SortKeys:     sortKeys,
		Dependencies: intermIDs,
	})
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

// extractFilterBuildColumns extracts the build-side column names from a
// semi/anti join filter string. Convention: right of operator = build column.
func extractFilterBuildColumns(filter string) []string {
	if filter == "" {
		return nil
	}
	seen := make(map[string]bool)
	parts := strings.Split(strings.ToLower(filter), " and ")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		for _, op := range []string{"!=", ">=", "<=", "<>", ">", "<", "="} {
			sep := " " + op + " "
			idx := strings.Index(part, sep)
			if idx >= 0 {
				right := cleanExpr(strings.TrimSpace(part[idx+len(sep):]))
				if right != "" {
					seen[right] = true
				}
				break
			}
		}
	}
	cols := make([]string, 0, len(seen))
	for c := range seen {
		cols = append(cols, c)
	}
	return cols
}

// innerPipelineWorkers returns the number of parallel workers for an inner
// pipeline (aggregate/sort child). Returns 0 (serial) unless the source is
// a concurrent-safe scan source.
func innerPipelineWorkers(src exec.Source) int {
	switch src.(type) {
	case *catalogScanSource, *scannerExecSource, *deferredJoinBridge:
		return runtime.NumCPU()
	}
	return 0
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
			Source:  a.childSource,
			Ops:    a.childOps,
			Sink:   a.agg,
			Workers: innerPipelineWorkers(a.childSource),
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
// When limitN > 0, it truncates results to the top N rows after sorting
// (Top-K optimization: avoids materializing the full sorted result).
type sortSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	sort        *exec.Sort
	limitN      int // 0 = no limit
	initialized bool
}

func (s *sortSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (s *sortSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.initialized {
		s.initialized = true
		pipe := &exec.Pipeline{
			Source:  s.childSource,
			Ops:    s.childOps,
			Sink:   s.sort,
			Workers: innerPipelineWorkers(s.childSource),
		}
		if err := pipe.Run(ctx); err != nil {
			return nil, err
		}
		// Top-K truncation: discard everything beyond limitN rows
		if s.limitN > 0 {
			s.sort.Truncate(s.limitN)
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
	allowedFiles    []string // probe-split: only scan these files (nil = all)
	scanner         *scanSourceInner
	bloomFilter     *exec.BloomScanFilter
	dynamicFilter   []exec.DynamicRange
	rowLimit        int64 // LIMIT pushdown: enables lazy file downloading (0 = eager)
}

type scanSourceInner struct {
	cat            *catalog.Catalog
	tableName      string
	files          []catalog.FileEntry
	idx            int64 // atomic index for parallel file workers (fallback path)
	schema         []parquet.Column
	requiredCols   []string
	scanPreds      []scanPredicate // converted predicates for row-group pruning
	rowsScanned    int64
	deleteMarkers  map[string]map[int64]bool // file path -> set of row indices to skip
	hasNestedTypes bool                      // true if schema has ARRAY/ROW/MAP types
	rowLimit       int64                     // >0: lazy file downloading (LIMIT pushdown)

	// row-group-level parallel scan
	rgUnits   []rgUnit // flat list of row group work units
	rgIdx     int64    // atomic index for parallel RG workers
	useNative bool     // true if native page decoder can be used (no Decimal/Array/Map)

	// batch pooling — reuse batch allocations across row groups
	pool *batch.BatchPool

	cachedReadSchema     []parquet.Column // projected schema, computed once
	cachedReadSchemaOnce sync.Once        // guards cachedReadSchema for concurrent rgWorker access

	// parallel scan
	batchCh chan *batch.RecordBatch
	errCh   chan error
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	// Bloom filter pushdown from hash join build side.
	bloomFilter *exec.BloomScanFilter

	// Dynamic min/max range filter from hash join build side.
	dynamicFilter []exec.DynamicRange

	// failedFiles counts files that failed to read during buildRGUnits.
	// When > 0, Init returns an error to prevent silent data loss.
	failedFiles  int
	firstFileErr error // sample error from the first file failure

	// pooledBufs tracks []byte buffers obtained from readBufPool during
	// buildRGUnits. These are returned to the pool when the scan source
	// is closed, enabling cross-query buffer reuse.
	pooledBufsMu sync.Mutex
	pooledBufs   [][]byte
}

// trackPooledBuf records a buffer obtained from readBufPool so it can be
// returned when the scan source is closed. Thread-safe for parallel readers.
func (inner *scanSourceInner) trackPooledBuf(buf []byte) {
	inner.pooledBufsMu.Lock()
	inner.pooledBufs = append(inner.pooledBufs, buf)
	inner.pooledBufsMu.Unlock()
}

// releasePooledBufs returns all tracked buffers to readBufPool.
// Safe to call multiple times — subsequent calls are no-ops.
func (inner *scanSourceInner) releasePooledBufs() {
	inner.pooledBufsMu.Lock()
	bufs := inner.pooledBufs
	inner.pooledBufs = nil
	inner.pooledBufsMu.Unlock()
	// Nil out rgUnits to break pqFile → bytes.Reader → []byte reference
	// chain before returning buffers, so GC doesn't pin old data.
	inner.rgUnits = nil
	for _, buf := range bufs {
		putReadBuf(buf)
	}
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

	// Probe-split: restrict to only allowed files for this scan alias.
	if len(s.allowedFiles) > 0 {
		allowed := make(map[string]bool, len(s.allowedFiles))
		for _, f := range s.allowedFiles {
			allowed[f] = true
		}
		filtered := files[:0]
		for _, f := range files {
			if allowed[f.Path] {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	scanCtx, cancel := context.WithCancel(ctx)
	// Convert logical predicates to scan predicates for row-group pruning
	var sp []scanPredicate
	for _, pred := range s.scanPreds {
		if pred.Column != "" && pred.Op != "" && pred.Value != nil {
			sp = append(sp, scanPredicate{Column: pred.Column, Op: pred.Op, Value: pred.Value})
		}
	}

	// Load delete markers for merge-on-read deletes
	var delMarkers map[string]map[int64]bool
	if len(manifest.DeleteMarkers) > 0 {
		delMarkers = make(map[string]map[int64]bool, len(manifest.DeleteMarkers))
		for _, dm := range manifest.DeleteMarkers {
			idxSet := make(map[int64]bool, len(dm.RowIndices))
			for _, idx := range dm.RowIndices {
				idxSet[idx] = true
			}
			delMarkers[dm.FilePath] = idxSet
		}
	}

	// Use a smaller batch channel for LIMIT queries to bound in-flight downloads.
	batchChSize := runtime.NumCPU()
	if s.rowLimit > 0 {
		batchChSize = 2
	}

	inner := &scanSourceInner{
		cat:           s.catalog,
		tableName:     s.tableName,
		files:         files,
		schema:        tableMeta.Schema.Columns,
		requiredCols:  s.requiredCols,
		scanPreds:     sp,
		deleteMarkers: delMarkers,
		bloomFilter:   s.bloomFilter,
		dynamicFilter: s.dynamicFilter,
		rowLimit:      s.rowLimit,
		batchCh:       make(chan *batch.RecordBatch, batchChSize),
		errCh:         make(chan error, 1),
		cancel:        cancel,
	}
	s.scanner = inner

	if inner.rowLimit > 0 || inner.hasNestedTypes {
		// Lazy file-level scan: download files on-demand, one at a time per worker.
		// Used for LIMIT pushdown (avoids downloading all files upfront) and
		// nested types (which need row-level reading).
		// Workers stop when context is cancelled (pipeline cancels after LIMIT satisfied).
		workers := runtime.NumCPU()
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
	} else {
		// Eager row-group-level parallel scan: download all files, enumerate
		// row groups, apply predicate pruning, then process RGs in parallel.
		inner.buildRGUnits(scanCtx)

		// Fail the scan if all files failed to read — prevents silent 0-row
		// results that are indistinguishable from correct empty results.
		if inner.failedFiles > 0 && len(inner.rgUnits) == 0 && len(inner.files) > 0 {
			cancel()
			sampleErr := ""
			if inner.firstFileErr != nil {
				sampleErr = fmt.Sprintf(": %v", inner.firstFileErr)
			}
			return fmt.Errorf("scan %s: all %d files failed to read (%d failures)%s", s.tableName, len(inner.files), inner.failedFiles, sampleErr)
		}

		// Initialize batch pool from the most common row group size
		if len(inner.rgUnits) > 0 {
			rgSize := int(inner.rgUnits[0].numRows)
			readSchema := inner.readSchema()
			if rgSize > 0 && len(readSchema) > 0 {
				inner.pool = batch.NewBatchPool(readSchema, rgSize)
				inner.pool.PreWarm(runtime.NumCPU())
			}
			// Pre-compute whether native page decoding can be used.
			inner.useNative = !scan.HasUnsupportedColumnarTypes(readSchema)
		}

		workers := runtime.NumCPU()
		if workers > len(inner.rgUnits) {
			workers = len(inner.rgUnits)
		}
		if workers < 1 {
			workers = 1
		}
		inner.wg.Add(workers)
		for i := 0; i < workers; i++ {
			go inner.rgWorker(scanCtx)
		}
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
	if s.scanner != nil {
		if s.scanner.cancel != nil {
			s.scanner.cancel()
		}
		s.scanner.releasePooledBufs()
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
		var reader *parquet.Reader
		if ras, ok := inner.cat.Store().(objstore.ReaderAtStore); ok {
			rac, size, err := ras.GetReaderAt(ctx, inner.cat.Bucket(), file.Path)
			if err != nil {
				continue
			}
			reader, err = parquet.NewReader(rac, size)
			if err != nil {
				rac.Close()
				continue
			}
		} else {
			rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), file.Path)
			if err != nil {
				continue
			}
			data, err := readAllSized(rc, file.SizeBytes, true)
			rc.Close()
			if err != nil {
				continue
			}
			inner.trackPooledBuf(data)
			reader, err = parquet.NewReaderFromBytes(data)
			if err != nil {
				continue
			}
		}

		b := readBatchDirect(reader, inner.schema, inner.requiredCols, inner.scanPreds...)
		if b == nil || b.Len == 0 {
			continue
		}

		// Apply delete markers: skip rows marked for deletion
		if delSet := inner.deleteMarkers[file.Path]; len(delSet) > 0 {
			sel := make([]uint32, 0, b.Len)
			for i := 0; i < b.Len; i++ {
				if !delSet[int64(i)] {
					sel = append(sel, uint32(i))
				}
			}
			if len(sel) == 0 {
				continue
			}
			if len(sel) < b.Len {
				b.Sel = sel
			}
		}

		atomic.AddInt64(&inner.rowsScanned, int64(b.ActiveLen()))

		select {
		case inner.batchCh <- b:
		case <-ctx.Done():
			return
		}
	}
}

// readSchema returns the column-projected schema for this scan.
// Multiple rgWorker goroutines call this concurrently for the same source,
// so the cache is guarded by sync.Once to avoid a data race on the
// cachedReadSchema field.
func (inner *scanSourceInner) readSchema() []parquet.Column {
	inner.cachedReadSchemaOnce.Do(func() {
		if len(inner.requiredCols) == 0 {
			inner.cachedReadSchema = inner.schema
			return
		}
		needed := make(map[string]bool, len(inner.requiredCols))
		for _, c := range inner.requiredCols {
			needed[c] = true
		}
		filtered := make([]parquet.Column, 0, len(inner.requiredCols))
		for _, col := range inner.schema {
			if needed[col.Name] {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) > 0 {
			inner.cachedReadSchema = filtered
		} else {
			inner.cachedReadSchema = inner.schema
		}
	})
	return inner.cachedReadSchema
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
// Uses BoolExpr.EvalBool() when available to avoid interface{} boxing.
func wrapPredicate(e expr.Expr) exec.Predicate {
	if be, ok := e.(expr.BoolExpr); ok {
		return func(b *batch.RecordBatch, row int) bool {
			return be.EvalBool(b, row)
		}
	}
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
