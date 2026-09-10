// Package physical converts logical plans to physical execution plans.
package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// ScalarDeferToggle gates deferring ALL uncorrelated scalar subqueries to
// distributed producer stages (not only CTE-referencing ones). Off
// (WADJET_SCALAR_DEFER=0) reverts them to eager plan-time execution on the
// coordinator's single-process pipeline.
//
// REGISTERED rather than read with a bare os.Getenv since #659: the switch
// decides whether a SELECT-list item becomes a producer stage or is left to
// the coordinator-local route, so it changes which ENGINE answers a query --
// and the rule is that a switch which can change the row set extends the
// invariance oracle. Reading it off the registry also lets a gate flip it
// without an env round trip.
var ScalarDeferToggle = optswitch.Register("scalar-defer", "WADJET_SCALAR_DEFER",
	"defer every uncorrelated scalar subquery to a distributed producer stage")

// ProbeSplitMinBytes is the minimum size of the largest scan required to
// activate probe-split. Below this, the orchestration overhead exceeds the
// parallelism benefit. Exported so tests can lower it to exercise the
// distributed path on tiny datasets — otherwise every test silently runs
// the single-worker path and distributed-only bugs (like the SF100 build
// cache Q02 regression) never get caught.
var ProbeSplitMinBytes int64 = 64 * 1024 * 1024

// ReverseBloomThreshold and ReverseBloomInnerThreshold gate the reverse-bloom
// optimization (see buildJoin). Declared as vars so regression tests can lower
// them to fire on tiny SF0.x datasets — TestTPCHReverseBloomForcedSF001 does
// exactly that — and so they can be raised at runtime to turn the optimization
// off without rebuilding.
//
// These lines used to say the vars existed "to disable the optimization while
// we hunt the SF100 Q05 0-rows bug whose triggering code path is somewhere in
// this optimization", and that the semi/anti threshold stayed at 10M because
// there was "no evidence of bugs there yet". Both halves are settled now, and
// not in the direction the second one guessed.
//
// A 0-rows MECHANISM in this optimization is identified and fixed (#543):
// reverseBloomBridge installed the bloom whether or not the key column had
// been found in the probe output, so a probeKey that did not resolve produced
// an EMPTY bloom that rejected every build row — a join answering over an
// empty build side, which is 0 rows for an inner or semi join. Forcing both
// thresholds to 100 over the SF0.01 corpus fires it on exactly one query,
// Q21, whose probeKey arrives alias-qualified as "l1.l_orderkey" against
// batches carrying "l_orderkey": on the parent commit Q21 returns 0 rows
// where the answer is 1 (and 0 where it is 100 at SF1). Init now refuses to
// install a bloom whose column never resolved or that received no keys.
//
// Whether that mechanism is what produced the Q05 incident at SF100 was never
// reduced to a repro and is not claimed here: Q05's own reverse blooms resolve
// their columns at SF0.01, and the corpus-wide forced run shows Q21 as the
// only unresolved one. What IS claimed is that this optimization could return
// 0 rows for a reason that had nothing to do with the query, that the reason
// is now gone, and that a gate runs the whole corpus with both thresholds
// forced down so the next one cannot hide behind a production threshold.
//
// The semi/anti threshold's "no evidence of bugs there yet" was wrong twice
// over: #543's key-encoding divergence was semi/anti-only in practice, since
// that is where string keys appear, and the empty-bloom mechanism above fires
// on a semi/anti query. The threshold stays at 10M for COST reasons.
//
// Init reads WADJET_REVERSE_BLOOM_INNER_THRESHOLD if set, so the bench can
// disable the inner-join path on SF100 without rebuilding the binary.
var (
	ReverseBloomThreshold      int64 = 10_000_000
	ReverseBloomInnerThreshold int64 = 50_000_000
)

// reverseBloomToggle is the kill switch for the whole reverse-bloom path
// (#287's convention: WADJET_REVERSE_BLOOM=0 disables). The optimization
// removes build-side rows before they reach the hash table, so a defect in it
// is a defect in the ANSWER — #543 dropped every row of a string-keyed
// semi/anti build — and the invariance oracle can only compare against a run
// without it if there is a switch to turn it off.
var reverseBloomToggle = optswitch.Register("reverse-bloom", "WADJET_REVERSE_BLOOM",
	"reverse-bloom pushdown: build a bloom from the probe side's join keys and filter the build-side scan with it")

func init() {
	if v := os.Getenv("WADJET_REVERSE_BLOOM_INNER_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			ReverseBloomInnerThreshold = n
		}
	}
}

// maxFusedBuildBytes is the per-fused-build EstimatedBytes ceiling above
// which fuseJoinStages refuses to absorb a broadcast join. Above this size,
// the cluster-wide S3 amplification of replicating the cache to every
// probe-split shard task outweighs the savings from skipping the
// intermediate exchange-replicate materialization. Tune via SF100+ deploys
// once we have measured numbers; 1 GB is conservative.
//
// Var rather than const so tests can lower it to exercise the skip path on
// small fixtures.
var maxFusedBuildBytes int64 = 1 * 1024 * 1024 * 1024

func (p *Planner) buildPipeline(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// If this subtree is a materialized CTE, serve from cache instead of
	// re-executing the full sub-plan. The columnar form replays from the
	// collector (disk-backed past budget) without consuming it, so every
	// reference — main pipeline, subqueries, recursive steps — streams the
	// same data; the boxed form (recursive work table) keeps SliceSource.
	if node.CTEName != "" && p.cteCache != nil {
		if mat, ok := p.cteCache[node.CTEName]; ok {
			if mat.coll != nil {
				return mat.coll.NewReplaySource(), nil, &exec.CollectSink{}, nil
			}
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
		// ...and so they bypassed every access check, which is why they are
		// authorized HERE, at the one place a table-function source is built.
		// A subquery and a CTE body are planned as SEPARATE plans inside this
		// planner, so an authorization pass over the statement's plan alone
		// cannot see the `read_csv` inside `(SELECT COUNT(*) FROM read_csv(…))`
		// — the same bypass #859's column policies had. The decision itself
		// lives in `internal/auth`, which imports this package, so it arrives
		// as a guard on the context (#943).
		if guard := logical.TableFuncGuardFromContext(ctx); guard != nil {
			if err := guard(node.FuncName, node.FuncArgs, node.FuncNamedArgs); err != nil {
				return nil, nil, nil, err
			}
		}
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

	// Lengths-only decode for columns the logical analysis proved are
	// consumed for their SHAPE only (logical/shape_only_columns.go). Skipped
	// for a scan feeding the multi-consumer scan cache: a replay consumer
	// was not part of the analyzed plan, exactly as scan-filter pushdown
	// excludes it.
	if len(node.ShapeOnlyColumns) > 0 {
		if cs, ok := scanner.(*catalogScanSource); ok && cs.cache == nil {
			cs.shapeOnlyCols = make(map[string]bool, len(node.ShapeOnlyColumns))
			for _, c := range node.ShapeOnlyColumns {
				cs.shapeOnlyCols[strings.ToLower(c)] = true
			}
			ShapeOnlyColumnsPlanned.Add(int64(len(node.ShapeOnlyColumns)))
		}
	}

	// Probe-split pipeline mode: restrict this scan to only allowed files.
	if p.ScanFileFilter != nil {
		if files, ok := p.ScanFileFilter[scanAlias]; ok {
			if cs, ok := scanner.(*catalogScanSource); ok {
				cs.allowedFiles = files
			}
		}
	}

	var ops []exec.UnaryOperator

	if node.SampleMethod != "" && node.SamplePercent > 0 {
		ops = append(ops, newSampleOperator(node.SampleMethod, node.SamplePercent))
	}
	return scanner, ops, &exec.CollectSink{}, nil
}

// ReverseBloomsInstalled counts reverse-bloom filters actually pushed onto a
// build-side scan. A gate that means to exercise this path asserts on it:
// without it, a test can only prove the query answered, not that the
// optimization it was written for ever engaged.
var ReverseBloomsInstalled atomic.Int64

// BuildSemiAntiFilter compiles a non-equality join filter string (e.g., "l_suppkey != l_suppkey")
// into a function that evaluates the condition on probe and build batch rows.
// Convention: left of operator = probe column, right = build column.
//
// The returned closure lazily resolves column indices on first call and caches
// them, avoiding per-row ColumnByName lookups. Comparisons use typed dispatch
// (int32, int64, float64, string) instead of fmt.Sprint conversion.
//
// HashJoin's probe runs in parallel — multiple workers call this filter
// concurrently against probe and build batches whose schemas are stable
// across the lifetime of the query (same logical plan → same projected
// columns). Use sync.Once to resolve indices safely on first call; later
// calls become a single relaxed atomic load on the once.done flag.
// SemiAntiNE gates the distinct-pair semi/anti build fast path
// (exec/join_semianti_ne.go). Kill switch WADJET_SEMIANTI_NE=0.
var SemiAntiNE atomic.Bool

func init() {
	SemiAntiNE.Store(os.Getenv("WADJET_SEMIANTI_NE") != "0")
}

// pipelineSource wraps a Source + UnaryOps into a single Source.
//
// It honours the bounded-output protocol (exec.BoundedOutputOperator, #317):
// an operator whose output for one input batch can be far larger than that
// batch — a hash-join probe fans one probe row out to every build row sharing
// its key — emits a bounded slice and suspends the rest, and a Next that finds
// pending output resumes it instead of pulling new input. Being a pull driver
// makes that natural: one Next, one batch.
//
// Resumption goes DEEPEST first. A suspended operator's pending output was
// produced from an input the operators after it have not seen yet, so it must
// drain before the operator above it is asked for its next slice.
type pipelineSource struct {
	source  exec.Source
	ops     []exec.UnaryOperator
	bounded []exec.BoundedOutputOperator // parallel to ops; nil = single-shot op
	inited  bool
	// flushIdx is the operator whose SPILLED partitions this source is
	// draining now that its input is exhausted; len(ops) means every one of
	// them has been drained. See nextFlushed (#1010).
	flushIdx int
	// drained latches the input's end. Once the source has answered nil it is
	// never pulled again: a flushed batch is a real return, so the consumer
	// calls Next once more, and an exhausted source is not owed a second
	// question.
	drained bool
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
	// The opt-in is this driver's promise to drain pending output before
	// supplying the next input batch.
	exec.EnableBoundedOutput(ps.ops)
	ps.bounded = make([]exec.BoundedOutputOperator, len(ps.ops))
	for i, op := range ps.ops {
		if bo, ok := op.(exec.BoundedOutputOperator); ok {
			ps.bounded[i] = bo
		}
	}
	return nil
}

func (ps *pipelineSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		if i := ps.pendingFrom(); i >= 0 {
			out, err := ps.bounded[i].NextOutput(ctx)
			if err != nil {
				return nil, err
			}
			if out == nil {
				continue // that operator finished; look for the next one
			}
			b, err := ps.runFrom(ctx, i+1, out)
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			continue
		}
		if ps.drained {
			return ps.nextFlushed(ctx)
		}
		b, err := ps.source.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			ps.drained = true
			return ps.nextFlushed(ctx)
		}
		b, err = ps.runFrom(ctx, 0, b)
		if err != nil {
			return nil, err
		}
		if b != nil {
			return b, nil
		}
	}
}

// nextFlushed drains the SPILLED partitions of every operator in this chain
// once the input is exhausted, one batch per call, pushing each through the
// operators above it.
//
// A join that evicted partitions holds rows on DISK, and the only thing that
// puts them back in the answer is its own flush. `exec.Pipeline.flushSpilledOps`
// runs that drain for the operators of the TOP pipeline — and this source is
// how a nested chain is driven: a join's build side, a set-operation arm, the
// inner side of a lateral. Its operators are in `ps.ops`, never in the outer
// pipeline's, so nothing flushed them, and a spilled join under one answered
// with the evicted partitions' rows simply missing (#1010).
//
// It is a SILENT loss and the whole result can be empty: with two decorrelated
// LATERALs joined to a fourth relation under a 512 KiB budget, every probe row
// routed to a spilled partition, `HashJoinProbe.Execute` returned nil for each
// of them, and the query answered `cols=[] rows=0` — no rows, and, being a
// star over more than one join, no declared columns either — where PostgreSQL
// 17 and the same query with a budget that does not spill answer four rows.
//
// `joinFlushSource` already carries this rule for the ONE shape it covers (a
// RIGHT or FULL join's own probe, #550) and its comment states the general
// case: "the probe sits in innerOps here, never in the outer Pipeline's Ops,
// so exec.Pipeline.flushSpilledOps never sees it". This is that sentence
// applied to every nested chain rather than to one join type. The drain is
// `exec.FlushableOperator`, the same interface and the same ascending order
// the top pipeline uses, so a flushed batch passes through the operators ABOVE
// its producer exactly as an ordinary one does.
func (ps *pipelineSource) nextFlushed(ctx context.Context) (*batch.RecordBatch, error) {
	for ps.flushIdx < len(ps.ops) {
		fo, ok := ps.ops[ps.flushIdx].(exec.FlushableOperator)
		if !ok || !fo.HasPendingFlush() {
			ps.flushIdx++
			continue
		}
		b, err := fo.NextFlush(ctx)
		if err != nil {
			return nil, fmt.Errorf("flushing spilled data: %w", err)
		}
		if b == nil {
			ps.flushIdx++
			continue
		}
		out, err := ps.runFrom(ctx, ps.flushIdx+1, b)
		if err != nil {
			return nil, err
		}
		if out == nil {
			continue
		}
		return out, nil
	}
	return nil, nil
}

// pendingFrom returns the index of the deepest operator with output still to
// emit, or -1. nil bounded (Init not run) means nothing ever suspends.
func (ps *pipelineSource) pendingFrom() int {
	for i := len(ps.bounded) - 1; i >= 0; i-- {
		if bo := ps.bounded[i]; bo != nil && bo.HasPendingOutput() {
			return i
		}
	}
	return -1
}

// runFrom pushes b through ops[i:] and returns what comes out the end. An
// operator that suspends keeps its remainder; the next Next resumes it.
func (ps *pipelineSource) runFrom(ctx context.Context, i int, b *batch.RecordBatch) (*batch.RecordBatch, error) {
	for ; i < len(ps.ops); i++ {
		op := ps.ops[i]
		exec.FlattenForConsumer(b, op)
		var err error
		b, err = op.Execute(ctx, b)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, nil
		}
	}
	return b, nil
}

// Close is nil-receiver and nil-source safe. Wrappers assign their
// pipelineSource in Init and delegate their own Close to it, so a source
// closed without ever being initialized arrives here as a nil receiver —
// which used to be a segfault, i.e. the whole server (#510). Every Close in
// the teardown path has to be reachable from a half-built plan.
func (ps *pipelineSource) Close() error {
	if ps == nil || ps.source == nil {
		return nil
	}
	err := ps.source.Close()
	for _, op := range ps.ops {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
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

	// Scan-level filter pushdown: when the filter sits directly on a
	// catalog scan, eligible conjuncts move into the scan (dictionary-mask
	// evaluation, no materialization of filter-only columns) and only the
	// residue compiles into exec filter ops. See scan_filter_pushdown.go.
	preds := node.Predicates
	if css, ok := source.(*catalogScanSource); ok && len(ops) == 0 &&
		!node.PolicyFilter && !subtreeHasSecurityBarrier(node.Children[0]) {
		// NOT below a security projection. Scan-level pushdown evaluates the
		// predicate against the FILE, so pushing one that sits ABOVE a
		// barrier makes it read the STORED column — the in-process twin of
		// the DAG's single filter slot. `IN (SELECT id FROM t WHERE ssn =
		// '***')` compared the stored SSN against the mask and answered no
		// rows; `… WHERE bal > 300` over a masked `bal` answered exactly the
		// rows above the threshold (#859 round 3).
		//
		// The POLICY's own filter is exempt for the reason it always is: it
		// is supposed to read the row as stored, and it sits BELOW the
		// barrier, so pushing it into the scan is the same evaluation.
		preds = p.tryPushFilterIntoScan(ctx, node, css)
	}

	for _, pred := range preds {
		filter, err := p.buildFilterOp(pred, outerTables, outerCols)
		if err != nil {
			return nil, nil, nil, err
		}
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

	// A star sharing its SELECT list with other items — `SELECT t.*, ctid` is
	// how DataGrip opens a table — reaches the planner as a projection of the
	// literal column "*". logical.Optimize expands it (before column pruning,
	// which is what #315 turned on); this catches the unoptimized-plan case.
	p.expandStarProjections(ctx, node, child)
	if err := refuseUnexpandedStarBesideItems(node); err != nil {
		return nil, nil, nil, err
	}

	// If the child (or child chain through Filter/HAVING) leads to an Aggregate,
	// skip the projection when possible — the aggregate already produces correctly
	// named output columns (group-by cols + agg output cols).
	// Keep the projection when:
	//   1. Any non-aggregate projection has a complex AST expression (e.g., SUM(x) * 0.0001)
	//   2. Any projection renames a column via alias (e.g., l_suppkey AS supplier_no)
	if hasAggregateAncestor(child) {
		// A group key is decided against the AGGREGATE'S INPUT, which is
		// where a ROW column and its fields live — the aggregate's own
		// output carries neither.
		var elideKeyDecls colDecls
		if agg := findAggregateAncestor(child); agg != nil && len(agg.Children) == 1 {
			elideKeyDecls = inputColDecls(agg.Children[0])
		}
		needsProject := false
		for _, proj := range node.Projections {
			// A literal select item is NOT elidable: the aggregate's output
			// carries it as a synthetic __gb_expr_N key column, and only the
			// projection renames it to the select-list name.
			if proj.ASTExpr != nil && !proj.IsAgg && !isPlainGroupKey(proj.ASTExpr, elideKeyDecls) {
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
			// Every check above asks whether a projection would COMPUTE
			// anything; none asks whether the aggregate's output is the
			// answer's SHAPE. It routinely is not. A HAVING over an
			// aggregate the SELECT list does not carry adds a synthetic
			// `__having_N` output column (logical/builder.go), and
			// `GROUP BY a, b` with only `a` selected adds `b` — eliding
			// the projection published both to the client, which is how
			// `SELECT k FROM g GROUP BY k HAVING BOOL_OR(flag)` answered
			// with a `__having_0` column nobody asked for (#591), and how
			// a grouped-but-unselected key reached psql. A projection over
			// an aggregate is elidable only when the aggregate already
			// emits exactly the projected columns, in order; anything this
			// cannot determine (grouping sets, an unrecognized node below)
			// keeps the projection, which is always sound and merely costs
			// a copy.
			// …and not across a node that ADDS a column. aggregateOutputNames
			// answers what the AGGREGATE publishes, which is what the #575 slot
			// pinning needs; a WINDOW below this Project appends `__win_N` to
			// that, so a projection whose list happens to equal the aggregate's
			// outputs is NOT the node's whole output and eliding it would put
			// the window's slot on the wire. Sort and LIMIT add nothing and are
			// safe to look through.
			if names, ok := aggregateOutputNames(child); ok &&
				!wrapsAWindow(child) && namesMatchProjections(names, node.Projections) {
				return p.buildPipeline(ctx, child)
			}
		}
	}

	source, ops, sink, err := p.buildPipeline(ctx, child)
	if err != nil {
		return nil, nil, nil, err
	}

	aggNode := findAggregateAncestor(child)
	isOverAggregate := aggNode != nil

	// Which column a SELECT item that IS a derived GROUP BY key reads.
	// `SUBSTR(c_phone, 1, 2)` is computed below the aggregate and published
	// under one name; above the aggregate its source columns are gone, so
	// re-evaluating the expression there answers NULL for every row.
	//
	// The lookup is by plansql.ExprIdentity, not by rendered text. The two
	// spellings of one key differ in ways SQL does not distinguish —
	// `(g + 1)` against `g + 1`, `G + 1` against `g + 1` — and comparing the
	// renderings made which spelling was used decide whether the query
	// answered or came back with a NULL key column (#723).
	gbExprToSyn := groupKeyByIdentity(aggNode)

	// Catalog types of what feeds these projections, resolved once for the
	// whole list: a bare column reference inside a projection expression
	// decides its type from them (see nodeDeclaredType, #333). The second
	// map is for a SELECT expression that maps to a synthetic group column —
	// a rename of a value computed BELOW the aggregate, so it types against
	// the aggregate's input rather than its output.
	childColTypes := emittedColDecls(child)
	// A SELECT-list scalar subquery types against its OWN plan, not against
	// this projection's input columns (#874).
	childColTypes.subqueryDecl = p.subqueryOutputColumn
	var aggInputColTypes colDecls
	if isOverAggregate && len(aggNode.Children) > 0 {
		aggInputColTypes = inputColDecls(aggNode.Children[0])
		aggInputColTypes.subqueryDecl = p.subqueryOutputColumn

	}

	// When the aggregate below emits two output columns of one NAME, a
	// name-based DirectCopy resolves both projections to the FIRST such
	// column and collapses them to one value (#575). The fix pins each such
	// projection to the physical slot its TRUE PROVENANCE names, so
	// appearance order is never assumed to equal slot order — it does not
	// when an aggregate shares its alias with a group key (`SELECT COUNT(*)
	// AS k, k AS x GROUP BY k`) or when the select list orders aggregates
	// and keys differently from the aggregate's [keys…, aggs…] output.
	//
	// keySlotByName / aggSlotByName carry the ABSOLUTE indices of the
	// duplicated names in the child's output, split by class: a group-key
	// projection consumes key slots, an aggregate projection consumes
	// aggregate slots. Only built when the child is a clean
	// [group keys…, aggregates…] aggregate output (no grouping sets, no
	// elided-literal reordering); anything else leaves every reference on
	// the existing name path.
	keySlotByName := map[string][]int{}
	aggSlotByName := map[string][]int{}
	if isOverAggregate && aggNode != nil {
		// The EMITTED names, not the planner's spelling of them: the whole
		// point of this map is to find a name TWO columns of the operator's
		// output batch answer to, and a key the planner spells `x.a` is a
		// column the operator calls `a` (#968).
		if full, ok := aggregateEmittedOutputNames(child); ok {
			nAgg := len(aggNode.AggExprs)
			nKey := len(full) - nAgg
			clean := nKey >= 0
			for j := 0; clean && j < nAgg; j++ {
				if !strings.EqualFold(strings.TrimSpace(full[nKey+j]),
					strings.TrimSpace(aggNode.AggExprs[j].OutputCol)) {
					clean = false
				}
			}
			if clean {
				total := map[string]int{}
				for _, n := range full {
					total[strings.ToLower(strings.TrimSpace(n))]++
				}
				for i, n := range full {
					key := strings.ToLower(strings.TrimSpace(n))
					if total[key] < 2 {
						continue // unambiguous by name; leave it on the name path
					}
					if i < nKey {
						keySlotByName[key] = append(keySlotByName[key], i)
					} else {
						aggSlotByName[key] = append(aggSlotByName[key], i)
					}
				}
			}
		}
	}
	keySlotSeen := map[string]int{}
	aggSlotSeen := map[string]int{}

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
		var synSource string
		if isOverAggregate && proj.ASTExpr != nil && !proj.IsAgg {
			if synName, ok := gbExprToSyn[plansql.ExprIdentity(proj.ASTExpr)]; ok {
				expression = exec.ColumnRef(synName)
				synSource = synName
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
					compiled, compErr := expr.CompileWithRunner(rewritten, p.subqueryRunner, p.subqueryBudgetOption())
					if expr.IsCompileRefusal(compErr) {
						return nil, nil, nil, compErr
					}
					if compErr == nil {
						expression = wrapExpr(compiled)
						compiledExpr = compiled
					}
				}
			}
		}

		// An expression OVER a group key — `(g + 1) * 2` — is not the key, so
		// nothing above resolves it as one, and the aggregate's output does
		// not carry `g` to rebuild it from. Re-point its group-key SUBTERMS
		// at the columns the aggregate publishes, which is what the DAG's
		// requoteAggOutputRefs does for the same shape; without it the whole
		// item evaluated to NULL for every row (#723).
		astExpr := proj.ASTExpr
		if isOverAggregate && astExpr != nil && !proj.IsAgg {
			astExpr = plansql.ReplaceGroupKeyRefs(astExpr, gbExprToSyn)
		}

		if expression == nil && astExpr != nil && !proj.IsAgg {
			// CSE within a single Project operator is unsafe: prevCol below
			// is the OUTPUT column name of an earlier projection, but at
			// runtime each ColumnRef is resolved against the INPUT batch's
			// schema — which doesn't yet have the earlier output column.
			// Pointing the duplicate at prevCol resolves to NULL at every
			// row (e.g. `SELECT 1 AS n, 0 AS a, 1 AS b` produced
			// {n: 1, a: 0, b: NULL} because the second `1` literal mapped
			// to ColumnRef("n") and n wasn't in the input — regression
			// surfaced by TestRecursiveCTE_Fibonacci).
			//
			// Safe CSE for SELECT-list duplicates would require either
			// (a) materialising shared expressions as a synthetic column
			// the Project then references, or (b) compiling each
			// projection independently. (b) is what we do — recompiling
			// a literal or already-compiled expression is cheap.
			outerTables := collectTableAliases(child)
			outerCols := collectOuterColumns(child)
			var compiled expr.Expr
			var compErr error
			if len(outerTables) > 0 {
				if len(outerCols) > 0 {
					compiled, compErr = expr.CompileWithScopeResolver(astExpr, p.subqueryRunner, outerTables, outerCols, p.subqueryInnerColumns(), p.subqueryDeclOption(), p.subqueryBudgetOption())
				} else {
					compiled, compErr = expr.CompileWithScope(astExpr, p.subqueryRunner, outerTables, p.subqueryDeclOption(), p.subqueryBudgetOption())
				}
			} else {
				// With the child's DECLARED column types in hand, so a pair
				// that cannot be exact fixed-point — a FLOAT column against a
				// fractional literal — keeps the vectorized float node it has
				// always compiled to instead of deferring the question to the
				// first batch (#555 review).
				compiled, compErr = expr.CompileWithColumnTypes(
					astExpr, p.subqueryRunner, childColTypes.types, p.subqueryBudgetOption())
			}
			// A name nothing implements has no input column to fall back to,
			// so the direct-copy path below would only re-report it as a
			// missing column. Propagate instead (#341).
			if expr.IsCompileRefusal(compErr) {
				return nil, nil, nil, compErr
			}
			if compErr == nil {
				expression = wrapExpr(compiled)
				compiledExpr = compiled
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
		outDecl := expr.Decl(parquet.TypeString)
		if proj.ASTExpr != nil && !proj.IsAgg {
			// A select expression mapped to a synthetic group column is a
			// RENAME of a value computed BELOW the aggregate — type it
			// against the aggregate's input, or the declared Float64
			// coerces the pre-projected int64 keys on the copy (#297).
			strictInt := strictIntArithCols(child)
			colTypes := childColTypes
			// The RESPELLED expression is the one that gets evaluated, so it
			// is the one to type. `(c_dec + 1) * 2` over `GROUP BY c_dec + 1`
			// names `c_dec` — a column the aggregate's output does not carry
			// — so typing the original left the DECIMAL key unresolved and
			// the whole term fell to the float rule, which is a scale-0
			// vector over exact fixed point (ADR-0024 item 2). Typing the
			// respelled form reads the key's own declared type instead.
			typeExpr := astExpr
			if isOverAggregate {
				if _, ok := gbExprToSyn[plansql.ExprIdentity(proj.ASTExpr)]; ok {
					// The WHOLE item is a key: a rename of a value computed
					// BELOW the aggregate, so it types against the
					// aggregate's input or the declared Float64 coerces the
					// pre-projected int64 keys on the copy (#297).
					strictInt = strictIntArithCols(aggNode.Children[0])
					colTypes = aggInputColTypes
					typeExpr = proj.ASTExpr
				}
			}
			outDecl = inferProjectionDeclType(typeExpr, outDecl.ID, strictInt, colTypes)
		}
		outType := outDecl.ID
		// The planner's declaration is the AUTHORITY for this projection's
		// arithmetic mode, and the compiled tree is told it here rather than
		// deriving its own. Two walks over two representations of one
		// expression is how a float came to be computed under an INT64
		// declaration and TRUNCATED into the vector (round-1 review, B3);
		// expr.StampArithMode is the seam that makes it one decision.
		if compiledExpr != nil {
			expr.StampArithMode(compiledExpr, outType == parquet.TypeInt64)
		}

		pc := exec.ProjectColumn{
			Name: name,
			Type: outType, // Will be resolved at runtime if input column matches
			Expr: expression,
			// A computed DECIMAL's (p,s): the output column exists in no
			// input schema, so exec.Project has nothing to read the scale
			// off and a scale-0 vector reads every value back a hundredfold
			// out (ADR-0024 item 2; #529, #555).
			Precision: outDecl.Precision,
			Scale:     outDecl.Scale,
		}
		// VECTOR-returning functions (embed()) need their output dimension
		// carried so the runtime sizes the output vector. Resolve it from the
		// registry at plan time (embed() derives it from the live provider).
		if outType == parquet.TypeVector {
			if fc, ok := proj.ASTExpr.(*plansql.FuncCallNode); ok {
				if dim, ok := expr.DefaultRegistry.VecReturnDim(fc.Name); ok {
					pc.Dimension = dim
				}
			}
		}
		// For column renames (e.g., l_suppkey AS supplier_no), record the
		// source column so Project.Execute can resolve the correct type.
		if name != colRef {
			pc.SourceCol = colRef
		}
		// A QUALIFIED reference names ONE SIDE, and the DECLARATION has to be
		// read off the column the VALUE comes from.
		//
		// `proj.Column` is the BARE name — the parser records `z.d92` as the
		// column `d92` — so where both join sides carry that name the value
		// was resolved through `z.d92` (the compiled expression keeps the
		// qualifier) and the type through the first bare `d92`, which is the
		// OTHER arm's. Over two tables holding `d92` at (9,2) and (18,4) that
		// rendered one arm's digits at the other arm's scale, silently, and
		// raised 22003 in the direction where the value does not fit —
		// `numeric field overflow: 1.1111 does not fit a DECIMAL at scale 2`
		// on a query PostgreSQL answers (#706).
		//
		// This is strictly more precise rather than a different rule:
		// `columnIndexFallback` resolves a qualified name with the same
		// ladder the value path uses — exact, then bare, then the
		// unambiguous suffix — so a stream that carries only the bare name
		// still resolves. Per-side resolution is what #551 gave set-op arms
		// and #653 gave filters; this is the projection's half of it.
		if cr, isRef := bareColRefOf(proj.ASTExpr); isRef && cr.Table != "" && !proj.IsAgg {
			pc.SourceCol = cr.Table + "." + cr.Column
		}
		// A ROW FIELD PATH records the WHOLE path, qualifier included, even
		// when the output name matches the field name. It is the only
		// spelling exec.Project can resolve the field's declaration from —
		// colRef here has already lost the `rw.` through cleanExpr — and it
		// is what carries the shape a bare TypeID cannot: a DECIMAL field's
		// (p,s) and a nested ROW/ARRAY/MAP field's own structure, which
		// colRefDeclaredType declines for the same reason it declines them
		// for a column (#568).
		if fp, ok := fieldPathRef(proj.ASTExpr, childColTypes); ok {
			pc.SourceCol = fp
		}
		// A SELECT item that maps to a synthetic GROUP BY key column reads
		// that column, so it is the source exec.Project must type from. The
		// planner's own declaration cannot carry a parameterized type
		// (colRefDeclaredType declines DECIMAL and the containers), which
		// left `SELECT rw.d ... GROUP BY rw.d` declaring STRING over a
		// DECIMAL key the aggregate had already emitted correctly (#568).
		if synSource != "" {
			pc.SourceCol = synSource
		}
		// Tell the runtime this output is computed, so it does not type the
		// output vector from an input column that merely shares the alias
		// (#327). A bare column reference — the only projection whose value
		// really does come from a same-named input — is excluded.
		pc.Computed = isComputedProjection(proj.ASTExpr)
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
			// Exact fixed-point arithmetic into a DECIMAL output: the one
			// kernel that writes DecimalData, so it is the one projection
			// that may skip exec.Project's checked per-row box (ADR-0024
			// item 3, #555). Gated on the DECLARED type, because a node whose
			// runtime mode turns out not to be decimal writes nothing.
			if outType == parquet.TypeDecimal {
				if dv, ok := compiledExpr.(expr.DecimalVecExpr); ok {
					pc.VecDecimalEval = dv.EvalDecimalVec
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
		// A plain direct copy of a DUPLICATED output column: pin it to the
		// next physical slot of its PROVENANCE class — aggregate projections
		// take aggregate slots, group-key references take key slots — so two
		// projections reading `u` read the two distinct `u` columns and an
		// aggregate never reads the group-key column it happens to share a
		// name with (#575). SourceIdx is exact and beats the name path.
		if isDirectCopy {
			key := strings.ToLower(strings.TrimSpace(colRef))
			seen, slots := keySlotSeen, keySlotByName
			if proj.IsAgg {
				seen, slots = aggSlotSeen, aggSlotByName
			}
			if idxs := slots[key]; len(idxs) > 0 {
				if k := seen[key]; k < len(idxs) {
					pc.SourceIdx = idxs[k]
					pc.SourceIdxSet = true
					seen[key] = k + 1
				}
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

	// Bare COUNT(*) over a plain scan answers from the catalog manifest —
	// no scan pipeline at all (see metadata_count.go). Un-grouped MIN/MAX
	// (optionally alongside COUNT(*)) answers the same way from parquet
	// footer statistics (see metadata_minmax.go).
	if src, ok := p.tryBuildMetadataCount(ctx, node); ok {
		return src, nil, &exec.CollectSink{}, nil
	}
	if src, ok := p.tryBuildMetadataMinMax(ctx, node); ok {
		return src, nil, &exec.CollectSink{}, nil
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
	var preProjectMeta []parquet.Column
	syntheticNames := make(map[int]string) // agg index → synthetic column name
	exprDedup := make(map[string]string)   // expr string → synthetic column name
	// synDecl is the DECLARATION each synthetic column is materialized under —
	// the same triple the stage DAG carries as AggSpec.InputType/Precision/
	// Scale. The aggregate's OUTPUT declaration is read off it below, so this
	// path and the DAG declare the same thing for the same query, identity row
	// included (#685; see aggOutputFromInputDecl).
	synDecl := make(map[string]parquet.Column)

	// The declarations of what feeds the aggregate, resolved once: they are
	// what tells a ROW FIELD PATH apart from a table-qualified column, and a
	// field path is NOT a simple column reference however much it looks like
	// one. exec.HashAggregate resolves its inputs by NAME through
	// columnIndexFallback, which has no ROW arm, so `MIN(rw.n)` failed with
	// `aggregate input "n" is not a column of its input` — cleanExpr having
	// dropped the `rw.` on the way. Routing it through the synthetic
	// pre-projection below is what materializes the field as a real column,
	// at its declared type (#568).
	//
	// emittedColDecls, not inputColDecls: the walk has to cross a DERIVED
	// TABLE. TPC-H Q08 is `SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0
	// END)` over `(SELECT …, l_extendedprice * (1 - l_discount) AS volume …)`,
	// and inputColTypes stops at that subquery's Project — so `volume`
	// decided nothing, the CASE declared its integer ELSE, and the branch's
	// DECIMAL text met an INT64 vector at the #361 store guard. It is the
	// same decline #529 hit one site over, where the SELECT list already
	// resolves through emittedColDecls (see TestDecimalDecidesThroughParens
	// AndDerivedTables), and the same walk declaredOutputSchema uses — so the
	// aggregate's input, the SELECT list and the plan-declared schema now
	// answer from one map.
	aggInputDecls := emittedColDecls(node.Children[0])

	for i, agg := range node.AggExprs {
		if agg.InputExpr != nil && (!isSimpleColRef(agg.InputExpr) || astIsFieldPath(agg.InputExpr, aggInputDecls)) {
			exprStr := agg.InputExpr.String()
			if existing, ok := exprDedup[exprStr]; ok {
				// Reuse previously compiled expression
				syntheticNames[i] = existing
				continue
			}
			synName := SlotName(SlotAggInput, i)
			// WITH THE OUTER SCOPE, exactly as the SELECT-list projection
			// site compiles its own expressions (see the CompileWith*
			// ladder above). Without it this site asked for none, so a
			// correlated subquery in an AGGREGATE ARGUMENT was compiled as
			// UNCORRELATED and run ONCE against no outer row: `SUM(CASE WHEN
			// EXISTS (SELECT 1 FROM y WHERE y.id = x.id * 2) THEN 1 ELSE 0
			// END)` read a query-wide constant FALSE and answered 0 for
			// PostgreSQL's 4, in silence, until v0.18.16 made the dangling
			// re-run loud (#734, ADR-0021 §1c). The identical expression one
			// level down — in a derived table's SELECT list — has always
			// answered, because that site does ask.
			aggOuterTables := collectTableAliases(node.Children[0])
			aggOuterCols := collectOuterColumns(node.Children[0])
			var compiled expr.Expr
			var compErr error
			if len(aggOuterTables) > 0 {
				compiled, compErr = expr.CompileWithScopeResolver(agg.InputExpr, p.subqueryRunner,
					aggOuterTables, aggOuterCols, p.subqueryInnerColumns(),
					p.subqueryDeclOption(), p.subqueryBudgetOption())
			} else {
				compiled, compErr = expr.CompileWithRunner(agg.InputExpr, p.subqueryRunner,
					p.subqueryDeclOption(), p.subqueryBudgetOption())
			}
			if expr.IsCompileRefusal(compErr) {
				return nil, nil, nil, compErr
			}
			if compErr == nil {
				aggDecl := inferProjectionDeclType(agg.InputExpr, parquet.TypeFloat64, nil, aggInputDecls)
				pc := exec.ProjectColumn{
					Name: synName,
					// Aggregate inputs are usually numeric, so Float64 is the
					// fallback — but MAX(UPPER(c)) is not, and declaring it
					// Float64 handed vecUpper a vector with no BytesData to
					// write into: the same process-killing mismatch as the
					// projection path (#310). MAX(COALESCE(a, b)) needs the
					// input's column types on top of that, or the polymorphic
					// declaration falls back to the same wrong Float64 (#333),
					// and a DECIMAL needs its (p,s) with the TypeID or the
					// materialized vector truncates at scale 0 (ADR-0024
					// item 2).
					Type:      aggDecl.ID,
					Precision: aggDecl.Precision,
					Scale:     aggDecl.Scale,
					Expr:      wrapExpr(compiled),
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
				//
				// Gated on the DECLARED type, the way the SELECT-list
				// projection gates its own typed paths (buildProjectOp only
				// attaches them when outType is Float64). aggPreProject picks
				// its write route from WHICH eval is set, not from the
				// column, so a float writer on a non-float column writes into
				// a nil Float64Data — reached the moment a ROW field path
				// started taking this route, since a bare column reference
				// implements every one of these interfaces (#568).
				//
				// The gate applies to EVERY derived aggregate input, not only
				// to field paths, and that is deliberate: the mismatch it
				// prevents was always possible here — any expression whose
				// declared type is not Float64 while its compiled form
				// implements VecFloat64Expr had the same nil-slice write
				// available to it — and the two sites now state the same
				// rule. Nothing that was reaching a typed path with a
				// matching declared type loses it.
				if pc.Type == parquet.TypeFloat64 {
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
					}
				}
				if pc.Type == parquet.TypeInt64 {
					if ie, ok := compiled.(expr.Int64Expr); ok {
						pc.Int64Eval = ie.EvalInt64
					}
				}
				// Exact fixed-point arithmetic into a DECIMAL aggregate
				// input, gated on the DECLARED type exactly like the paths
				// above and like the SELECT-list projection builder.
				//
				// The kernel existed and was reachable — BinOpNumeric
				// implements DecimalVecExpr and writes carriers straight
				// into out.DecimalData.Data — and it was attached in
				// exactly ONE place in the tree, the SELECT-list builder.
				// Every DECIMAL aggregate input therefore took the boxed
				// checked writer: 4.00 allocations per computed cell, at
				// SF1 48,026,572 for Q01's two computed columns over
				// 6,001,215 rows, 1804x the FLOAT64 arm's object count.
				// The mechanism is one round trip — Int128 →
				// FormatDecimal → FormatUint → any box →
				// SetComputedChecked → DecimalTextParts re-parse — so the
				// value is rendered to decimal TEXT and parsed back
				// between two exact kernels (#705). The BYTE ratio is a
				// different thing and is NOT a defect: the Int128 carrier
				// is 16 B against float64's 8, which is ADR-0024's
				// predicted cost.
				if pc.Type == parquet.TypeDecimal {
					if dv, ok := compiled.(expr.DecimalVecExpr); ok {
						pc.VecDecimalEval = dv.EvalDecimalVec
					}
				}
				// A ROW FIELD PATH declares the FIELD, wholesale: its (p,s),
				// its dimension, its nested shape. colRefDeclaredType above
				// declines every parameterized type — it can only answer a
				// TypeID — so MIN over a DECIMAL field fell back to the
				// Float64 default and a container field to Float64 outright
				// (#568).
				meta := parquet.Column{Name: synName, Type: pc.Type, Nullable: true,
					Precision: pc.Precision, Scale: pc.Scale}
				if fc, ok := aggInputDecls.field(fieldPathColRef(agg.InputExpr)); ok {
					meta = fc
					meta.Name, meta.Nullable = synName, true
					pc.Type = fc.Type
					pc.Dimension = fc.Dimension
					// The boxed route is the only null-correct one for a
					// field path: aggPreProject picks its writer from WHICH
					// eval is set, and the typed writers have no way to mark
					// a NULL field — MIN over a float field read the 0 they
					// leave behind instead of skipping the row.
					pc.VecEval, pc.VecFloat64Eval, pc.VecFloat64Clone = nil, nil, nil
					pc.Float64Eval, pc.Int64Eval = nil, nil
				}
				preProjectCols = append(preProjectCols, pc)
				preProjectMeta = append(preProjectMeta, meta)
				syntheticNames[i] = synName
				exprDedup[exprStr] = synName
				synDecl[synName] = meta
			}
		}
	}

	var aggCols []exec.AggColumn
	for i, agg := range node.AggExprs {
		fn := parseAggFunc(agg.Func)
		if agg.Distinct && fn == exec.AggCount {
			fn = exec.AggCountDistinct
		}
		// Preserve the table qualifier — NormalizeIdentRef only strips the
		// quotes off a delimited identifier, the way the GROUP BY keys below
		// are normalized and the way the stage-DAG AggSpec carries
		// agg.InputCol verbatim. cleanExpr here would drop the qualifier
		// ("t2.c2" -> "c2"), and a bare name binds to the FIRST column of
		// that name in the input schema. Over a join whose two sides share a
		// bare column name, that is the wrong table's column: BOOL_OR(t2.c2)
		// over `t5, t1 FULL OUTER JOIN t2 ON t1.c7` read t5.c2 (all NULL) and
		// answered NULL, and an always-true WHERE that reorders the cross
		// join flipped which wrong column it read (t1.c2, FALSE) — a
		// TLP-Aggregate self-consistency violation (#622). exec.HashAggregate's
		// columnIndexFallback still resolves the qualified name against a
		// scan that emits the column bare via its qualified->bare fallback.
		inputCol := plansql.NormalizeIdentRef(strings.TrimSpace(agg.InputCol))
		// Use synthetic column name if the input was an expression
		if synName, ok := syntheticNames[i]; ok {
			inputCol = synName
		}
		// Prefer the resolved declaration (MIN/MAX carry their input
		// column's type) so this pipeline and the stage DAG declare the
		// same thing for the same query. exec.HashAggregate overrides
		// MIN/MAX from the vector it observes anyway, so this only decides
		// the type of the identity row an empty input produces — which is
		// exactly where the two paths would otherwise disagree.
		outType, outTypeKnown := aggSpecOutputType(node, agg)
		if !outTypeKnown {
			outType = aggOutputType(agg.Func, agg.Distinct)
		}
		// The arguments past the first arrive as their own fields
		// (logical/agg_extra_args.go). They used to be recovered by
		// splitting InputCol on a comma, which only worked if something
		// had packed them there — nothing had, because the SELECT parser
		// dropped every argument after the first (#353).
		ac := exec.AggColumn{
			Func:       fn,
			InputCol:   inputCol,
			InputCol2:  agg.InputCol2,
			InputCol3:  agg.InputCol3,
			Separator:  agg.Separator,
			Percentile: agg.Percentile,
			OutputCol:  agg.OutputCol,
			OutputType: outType,
			// DISTINCT for every aggregate PostgreSQL accepts it for, not
			// only COUNT (#703). COUNT spells it as its own AggFunc above —
			// its state IS the set — so the flag would be redundant there;
			// MIN/MAX are unaffected by de-duplication and carry it only so
			// the two paths declare the same spec.
			Distinct: agg.Distinct && fn != exec.AggCountDistinct,
		}
		// And the (p,s) a bare TypeID cannot carry, for the same reason: it
		// decides what the identity row of an EMPTY input declares, which is
		// the one output no observed vector can type. On the DAG that row is
		// a whole partial task's .wshf file (#685); here it is the zero-row
		// result's schema, and the two paths declare the same thing only if
		// both read this function.
		if m, known := aggSpecOutputDecimal(node, agg); known {
			ac.OutputPrecision, ac.OutputScale = m.Precision, m.Scale
		}
		// A ROW-valued aggregate's FIELDS, which a bare TypeID cannot carry
		// either. Same function the stage spec uses, so the two paths declare
		// one bar (#965).
		if fields, ok := aggOhlcvOutputFields(node, agg); ok {
			ac.OutputFields = fields
		}
		// A COMPUTED argument is declared from the projection this path
		// materializes it under, which is the DAG's rule read off the local
		// equivalent of AggSpec.InputType/Precision/Scale. Without it a
		// zero-row SUM(a * (1 - b)) declared float64 here and DECIMAL there.
		if synName, ok := syntheticNames[i]; ok {
			if d, ok := synDecl[synName]; ok {
				if t, prec, sc, known := aggOutputFromInputDecl(
					agg.Func, agg.Distinct, d.Type, d.Precision, d.Scale,
					aggInputIsWideInteger(agg.InputExpr, aggInputDecls)); known {
					ac.OutputType = t
					ac.OutputPrecision, ac.OutputScale = prec, sc
				}
			}
		}
		aggCols = append(aggCols, ac)
	}

	// Catalog types of the aggregate's input, for typing derived GROUP BY
	// key expressions (see nodeDeclaredType, #333). Resolved once.
	aggChildStrictInt := strictIntArithCols(node.Children[0])
	aggChildColTypes := inputColDecls(node.Children[0])

	groupByCols := make([]string, len(node.GroupBy))
	for i, gb := range node.GroupBy {
		// Preserve table qualifiers for self-join disambiguation (e.g., n1.n_name vs n2.n_name).
		// The aggregate operator resolves qualified names with fallback to unqualified.
		// Delimited identifiers lose their quotes here: the operator matches
		// the batch column name itself (Zeek's flat id.orig_h).
		groupByCols[i] = plansql.NormalizeIdentRef(strings.TrimSpace(gb))
	}

	// Literal group keys (GROUP BY 1, URL — the positional ref resolves to
	// the literal select item) are constant per row: they cannot affect
	// grouping, but as synthetic key columns they widen every serialized
	// key and force the multi-column generic path over the single-column
	// fast paths (ClickBench Q35 vs Q34). Elide them from the key set and
	// re-attach the constant as a post-aggregate column under the same
	// synthetic name the downstream projection expects. Kept out of
	// grouping-sets plans (set indices reference key positions), and only
	// when a non-literal key remains — GROUP BY over literals alone must
	// still emit zero rows on empty input, which one retained key
	// preserves.
	var litPostOps []exec.UnaryOperator
	litElided := map[int]bool{}
	// The names this aggregate publishes its keys under, resolved once. The
	// literal elision below, the derived-key materialization further down,
	// aggregateOutputNames and the projection above all read them from here —
	// one rule, so the two engines' aggregate output schemas cannot drift
	// apart (#723).
	keyOuts := groupKeyOutputs(node)
	if len(node.GroupByExprs) == len(node.GroupBy) && len(node.GroupingSets) == 0 {
		nonLit := 0
		for _, gbExpr := range node.GroupByExprs {
			if gbExpr == nil {
				nonLit++
				continue
			}
			if _, isLit := gbExpr.(*plansql.Lit); !isLit {
				nonLit++
			}
		}
		if nonLit > 0 {
			for i, gbExpr := range node.GroupByExprs {
				if gbExpr == nil {
					continue
				}
				if _, isLit := gbExpr.(*plansql.Lit); !isLit {
					continue
				}
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
				if expr.IsCompileRefusal(compErr) {
					return nil, nil, nil, compErr
				}
				if compErr != nil {
					continue
				}
				litDecl := inferProjectionDeclType(gbExpr, parquet.TypeString, aggChildStrictInt, aggChildColTypes)
				litPostOps = append(litPostOps, &aggPreProject{computed: []exec.ProjectColumn{{
					Name:      keyOuts[i].Name,
					Type:      litDecl.ID,
					Precision: litDecl.Precision,
					Scale:     litDecl.Scale,
					Expr:      wrapExpr(compiled),
				}}})
				litElided[i] = true
			}
		}
	}

	// Handle GROUP BY expressions (e.g., SUBSTR(c_phone, 1, 2)).
	// Compile expression-valued GROUP BY entries into pre-aggregate projections
	// so the aggregate can group by the computed result.
	if len(node.GroupByExprs) == len(node.GroupBy) {
		for i, gbExpr := range node.GroupByExprs {
			if litElided[i] {
				continue
			}
			// keyOuts is the single answer to "is this key derived", shared
			// with aggregateOutputNames and with the projection above, so
			// the schema this materialization produces and the schema they
			// describe cannot disagree (ADR-0026).
			if gbExpr != nil && i < len(keyOuts) && keyOuts[i].Derived {
				// The HIDDEN SLOT, not the key's own text. The
				// pre-aggregate projection APPENDS this column to the input
				// batch and every consumer resolves by name, so a slot
				// spelled like an input column the query already carries is
				// shadowed BY it — `GROUP BY g + 1` over a table that also
				// has a column called "g + 1" grouped by the column. The
				// key is PUBLISHED under its canonical text by
				// GroupByOutNames below, which is what keeps the two
				// engines' output schemas equal (#720, ADR-0026).
				synName := keyOuts[i].Slot
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
				if expr.IsCompileRefusal(compErr) {
					return nil, nil, nil, compErr
				}
				if compErr == nil {
					gbDecl := derivedGroupKeyDecl(node.GroupBy[i], gbExpr, node.Children[0])
					pc := exec.ProjectColumn{
						Name: synName,
						// Numeric expressions (abs(x), x-1, …) must get a
						// numeric synthetic column: SetValue on a String
						// vector mangles float group keys.
						Type: gbDecl.ID,
						// And a DECIMAL key needs its (p,s) with the type:
						// a scale-0 vector TRUNCATES every value on the way
						// in, so `GROUP BY COALESCE(a, b)` collapsed 12.75
						// and 12.7501 into one group holding 12 (ADR-0024
						// item 2).
						Precision: gbDecl.Precision,
						Scale:     gbDecl.Scale,
						Expr:      wrapExpr(compiled),
					}
					// Batched evaluation when available — beyond the vec
					// kernels themselves, FuncCall.EvalVec is where the
					// per-batch input memo for expensive scalar functions
					// (regexp family, ClickBench Q29's GROUP BY key) lives;
					// the per-row Expr path bypasses it.
					if ve, ok := compiled.(expr.VecExpr); ok {
						pc.VecEval = ve.EvalVec
					}
					// Same rule the aggregate INPUT takes above: a ROW field
					// path declares the whole field, and its value is
					// written through the boxed route so a NULL field stays
					// NULL (#568).
					meta := parquet.Column{Name: synName, Type: pc.Type, Nullable: true,
						Precision: pc.Precision, Scale: pc.Scale}
					if fc, ok := aggChildColTypes.field(fieldPathColRef(gbExpr)); ok {
						meta = fc
						meta.Name, meta.Nullable = synName, true
						pc.Type = fc.Type
						pc.Dimension = fc.Dimension
						pc.VecEval = nil
					}
					preProjectCols = append(preProjectCols, pc)
					preProjectMeta = append(preProjectMeta, meta)
					groupByCols[i] = synName
				}
			}
		}
	}

	// If we have expression inputs or GROUP BY expressions, build a
	// pass-through projection that keeps all input columns and adds
	// the computed ones.
	if len(preProjectCols) > 0 {
		childOps = append(childOps, &aggPreProject{computed: preProjectCols, meta: preProjectMeta})
	}

	// Compact literal-elided entries out of the key set.
	if len(litElided) > 0 {
		kept := groupByCols[:0:0]
		for i, c := range groupByCols {
			if !litElided[i] {
				kept = append(kept, c)
			}
		}
		groupByCols = kept
	}

	hashAgg := exec.NewHashAggregate(groupByCols, aggCols)
	// A DERIVED key resolves by its hidden slot and publishes under its own
	// canonical text. Only then do the two engines' aggregate output schemas
	// match, which is what lets one HAVING predicate — rewritten once, in the
	// logical plan — be evaluable on both (ADR-0026). Set only when a key
	// really is derived, so every other shape keeps exec's own naming rule
	// (the qualifier strip, and the ambiguity exception for `GROUP BY
	// n1.n_name, n2.n_name`).
	if outNames, derived := publishedGroupKeyNames(keyOuts, litElided); derived {
		hashAgg.GroupByOutNames = outNames
	}
	if est := findScanRowEstimate(node.Children[0]); est > 0 {
		hashAgg.InputRowHint = est
	}
	if ndv := groupKeyNDVEstimate(node.Children[0], groupByCols); ndv > 0 {
		hashAgg.GroupNDVHint = ndv
	}
	if sm := p.getSpillManager(); sm != nil {
		hashAgg.Spill = sm
	}

	// For GROUPING SETS: single-pass mode — convert the sets' terms to key
	// POSITIONS.
	//
	// A term is looked up against `node.GroupBy`, the key list as the query
	// wrote it, and NOT against `groupByCols`, which is what the aggregate
	// actually groups on: a DERIVED key's entry there is its hidden
	// `__gb_expr_N` slot (ADR-0026 §2), which no grouping set can be spelled
	// with. Keyed on the materialized name, `ROLLUP (g + 1)` found nothing,
	// produced an EMPTY set, and every set collapsed to the grand total.
	var postOps []exec.UnaryOperator
	if len(node.GroupingSets) > 0 || len(node.GroupingCalls) > 0 {
		keyIndex := make(map[string]int, len(node.GroupBy))
		for i, c := range node.GroupBy {
			// FIRST wins. The key list is deduped upstream, so a repeat should
			// not arrive — but last-wins is the wrong reading if one ever does,
			// and it is what pointed both sets of `ROLLUP (g, g)` at position 1
			// and left position 0 grouped on nothing.
			if _, taken := keyIndex[strings.ToLower(strings.TrimSpace(c))]; !taken {
				keyIndex[strings.ToLower(strings.TrimSpace(c))] = i
			}
		}
		for i, c := range groupByCols {
			// The materialized spelling too, so a set written against a name
			// the pre-projection did not move still resolves.
			if _, taken := keyIndex[strings.ToLower(c)]; !taken {
				keyIndex[strings.ToLower(c)] = i
			}
		}
		if len(node.GroupingSets) > 0 {
			sets := make([][]int, len(node.GroupingSets))
			for i, set := range node.GroupingSets {
				indices := make([]int, 0, len(set))
				for _, col := range set {
					if idx, ok := keyIndex[strings.ToLower(strings.TrimSpace(col))]; ok {
						indices = append(indices, idx)
					}
				}
				sets[i] = indices
			}
			hashAgg.GroupingSets = sets
		}

		// GROUPING(a[, b, ...]): the same name→key-position map, but the
		// ARGUMENT ORDER is preserved and an unresolvable argument is an
		// error rather than a silently dropped bit. Dropping one would shift
		// every bit below it and answer a different number (#804); the
		// grouping-set loop above can drop a term because a set is a SET.
		for _, call := range node.GroupingCalls {
			positions := make([]int, 0, len(call.Args))
			for _, arg := range call.Args {
				idx, ok := keyIndex[strings.ToLower(strings.TrimSpace(arg))]
				if !ok {
					return nil, nil, nil, sqlerr.New("42803",
						"arguments to GROUPING must be grouping expressions of the associated query level: %q is not a group key of this aggregate", arg)
				}
				positions = append(positions, idx)
			}
			hashAgg.GroupingCalls = append(hashAgg.GroupingCalls, positions)
			hashAgg.GroupingCallNames = append(hashAgg.GroupingCallNames, call.OutputCol)
		}
	}
	if len(node.GroupingSets) == 0 && len(node.GroupingSetNulls) > 0 {
		hashAgg.NullGroupCols = node.GroupingSetNulls
	}

	// Elided literal keys re-attach as constant columns on the aggregate's
	// output, under the synthetic names the projection maps to.
	postOps = append(postOps, litPostOps...)

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

func (p *Planner) buildSetOp(ctx context.Context, node *logical.Node, op string) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) < 2 {
		return nil, nil, nil, fmt.Errorf("%s requires two children", op)
	}
	// Arms with NO COMMON TYPE are refused HERE, at plan time, with
	// PostgreSQL's 42804 — the same refusal the stage DAG takes, from the same
	// walk, so one query has one answer (#648). Left to the runtime, this path
	// let the arms meet under the FIRST arm's box: `SELECT s FROM t UNION ALL
	// SELECT d FROM t` came back as a STRING column holding rendered decimals,
	// and the same pair the other way round failed mid-execution with 22P02 on
	// the first row of text that is not a number.
	if err := setOpArmTypeConflict(node); err != nil {
		return nil, nil, nil, err
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
		leftSource:   leftSource,
		leftOps:      leftOps,
		rightSource:  rightSource,
		rightOps:     rightOps,
		all:          node.UnionAll,
		op:           op,
		leftLits:     setOpArmLiterals(node.Children[0]),
		rightLits:    setOpArmLiterals(node.Children[1]),
		leftUnknown:  setOpArmUnknownLits(node.Children[0]),
		rightUnknown: setOpArmUnknownLits(node.Children[1]),
	}

	return src, nil, &exec.CollectSink{}, nil
}

// setOpArmLiterals reads one arm's SELECT list and records, per OUTPUT
// POSITION, the exact DECIMAL a numeric LITERAL there names — the same
// `setOpLitArm` answer the stage DAG builds its arm projection from. A nil
// entry means "not a bare numeric literal", which is every other select item.
//
// PostgreSQL types a numeric constant `numeric` whenever it carries a decimal
// point or an exponent, so `SELECT d FROM t UNION ALL SELECT 1.23456` is a
// numeric union there (#665). The stage DAG resolves that; the single-process
// path built the literal arm's vector from the declared-type layer, which
// still answers float8 for a fractional literal everywhere, and the two paths
// then answered one query two ways: 1234567890123456.78 came back exact from
// the DAG and 1.2345678901234568e+15 from this one, and a join on the union's
// column matched nothing here because a float8 column met a DECIMAL key (#683).
func setOpArmLiterals(arm *logical.Node) []*setOpLitDecimal {
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) == 0 {
		return nil
	}
	out := make([]*setOpLitDecimal, len(proj.Projections))
	any := false
	for i, pr := range proj.Projections {
		if pr.ASTExpr == nil {
			continue
		}
		if d, ok := setOpLitArm(pr.ASTExpr); ok {
			lit := d
			out[i] = &lit
			any = true
		}
	}
	if !any {
		return nil
	}
	return out
}

// setOpArmUnknownLits is setOpUnknownLiteralArms for the single-process path,
// whose arms are a NESTED tree rather than the DAG's flattened list: the width
// comes from this arm's own select list, and a nested set-operation arm has no
// output projection of its own, so it contributes no mask (its columns are
// already resolved by its own adapter).
func setOpArmUnknownLits(arm *logical.Node) []bool {
	proj := findOutputProjectionNode(arm)
	if proj == nil {
		return nil
	}
	return setOpUnknownLiteralArms(arm, len(proj.Projections))
}

// setOpApplyLiteralDecls restates a literal arm's column as the DECIMAL its
// SPELLING names, so the arms reconcile through the ordinary ladder.
//
// Applied only when the arm's runtime schema has one column per select item:
// anything else means the pipeline emitted columns this walk did not count,
// and a position is only an address while the two lists line up.
func setOpApplyLiteralDecls(schema []parquet.Column, lits []*setOpLitDecimal) []parquet.Column {
	if len(lits) == 0 || len(lits) != len(schema) {
		return schema
	}
	out := append([]parquet.Column(nil), schema...)
	for i, lit := range lits {
		if lit == nil {
			continue
		}
		out[i].Type = parquet.TypeDecimal
		out[i].Precision, out[i].Scale = lit.decl.Precision, lit.decl.Scale
	}
	return out
}

// setOpLiteralRows replaces a literal column's boxed value with the literal's
// plain decimal TEXT, on every row.
//
// The text is not decoration: the evaluator folds a numeric literal into a
// float64 box, so `1234567890123456.78` is already 1234567890123456.8 by the
// time it reaches this adapter and declaring DECIMAL over that box would put
// an exact type on a rounded number. A DECIMAL arrives here as its rendered
// text anyway (Vector.GetValue), so the literal's own text is the shape every
// reader below already expects — batch.FromRowsChecked parses it at the
// resolved scale and setOpCheckedDecimalText range-checks it, which is what
// gives this path the same 22003 the stage DAG raises for a literal the
// union's own type cannot hold (ADR-0024 item 7).
func setOpLiteralRows(rows []map[string]any, lits []*setOpLitDecimal) []map[string]any {
	if len(lits) == 0 {
		return rows
	}
	for i, lit := range lits {
		if lit == nil {
			continue
		}
		slot := setOpSlotName(i)
		for _, row := range rows {
			if _, ok := row[slot]; ok {
				row[slot] = lit.text
			}
		}
	}
	return rows
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
	// leftLits / rightLits carry the exact DECIMAL a numeric LITERAL select
	// item names, per output position; nil where the arm has none. See
	// setOpArmLiterals.
	leftLits  []*setOpLitDecimal
	rightLits []*setOpLitDecimal
	// leftUnknown / rightUnknown mark, per output position, the select items
	// that are UNKNOWN-typed literals — a quoted string or a bare NULL, which
	// PostgreSQL types from the OTHER arm. See setOpResolveUnknownLiteralArms.
	leftUnknown  []bool
	rightUnknown []bool

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

		// SQL says the arms of a set operation correspond BY POSITION and
		// the result takes the FIRST arm's column names. These rows are
		// keyed maps, so an arm whose columns are spelled differently has
		// to be re-keyed before anything compares or concatenates them —
		// `SELECT n_regionkey FROM nation UNION SELECT r_regionkey FROM
		// region` deduped nothing (every row of one arm was a distinct map
		// from every row of the other) and batch.FromRows then read the
		// right arm's values under names it does not carry and wrote NULLs.
		//
		// Schema() instead of Batches()[0].Schema — ToRows below releases the
		// sinks' batches as it boxes them — and the arms' two schemas
		// UNIFIED rather than the first one alone. Under the first arm's
		// schema the arm ORDER decided the answer: FromRows re-reads each
		// row's rendered decimal text at the schema's scale, so the first
		// arm's scale truncated the second arm's values (#532); an INTEGER
		// arm was read raw as an unscaled carrier (#547); and a DECIMAL arm
		// under a FLOAT64 first arm failed the store outright while the same
		// pair the other way round silently kept the DECIMAL type (#541).
		// unifySetOpSchemas resolves the common type through the same
		// setOpWiden / setOpDecimalTarget the stage DAG uses, so the two
		// paths cannot answer with different types for the same query.
		//
		// The type is resolved HERE rather than at the FromRows call below
		// because the DEDUP KEY needs it too: a set operation decides
		// membership by equality, so two values the comparator calls equal
		// have to produce one key — which their BOXES alone cannot say, a
		// DECIMAL being rendered text (#499).
		leftSchema := setOpApplyLiteralDecls(leftSink.Schema(), u.leftLits)
		rightSchema := setOpApplyLiteralDecls(rightSink.Schema(), u.rightLits)
		leftSchema, rightSchema = setOpResolveUnknownLiteralArms(
			leftSchema, rightSchema, u.leftUnknown, u.rightUnknown)
		schema := unifySetOpSchemas(leftSchema, rightSchema)

		// The boxes are not uniform across types — a DECIMAL is its rendered
		// TEXT, an integer a raw int64, a float a float64 — so a widened
		// column needs each arm's box MOVED into the shape the unified column
		// reads, not merely relabelled. coerceSetOpArmRows does that for every
		// rung of the ladder before the arms meet, so both the dedup key and
		// FromRows read one shape per column — and ERRORS on a value that does
		// not fit the unified DECIMAL, the same overflow the stage DAG raises
		// (exec.coerceDecimalVector), rather than saturating silently. The
		// right arm is coerced against its OWN schema, before alignSetOpRows
		// re-keys it to the result names.
		//
		// POSITIONALLY, from here to the batch. SQL says the arms of a set
		// operation correspond by POSITION and a result may legally carry two
		// output columns of the same NAME — `SELECT n_name AS u, n_comment AS
		// u FROM nation UNION ALL …` is two columns called `u` in PostgreSQL
		// too. A map keyed by name holds ONE of them, so both output columns
		// came back carrying the SECOND source column's value: every row
		// wrong, no error, and only on this path — the stage DAG answers it
		// correctly, which is what isolated the collapse as the cause (#556,
		// and #844's UNION ALL branch, which is the same map).
		//
		// The rows keep their map form — every helper below reads it, and the
		// DECIMAL, dedup and overflow rules those helpers encode are not what
		// is wrong here — but their KEYS become slot positions, which are
		// addresses. The schemas are renamed to match for the duration and
		// the result batch is renamed back at the end, so nothing outside
		// this function sees a slot name.
		posResult := setOpSlotSchema(schema)
		leftRows, err := coerceSetOpArmRows(
			setOpLiteralRows(setOpArmRows(leftSink, leftSchema), u.leftLits),
			setOpSlotSchema(leftSchema), posResult)
		if err != nil {
			return nil, fmt.Errorf("executing %s left side: %w", u.op, err)
		}
		rightRows, err := coerceSetOpArmRows(
			setOpLiteralRows(setOpArmRows(rightSink, rightSchema), u.rightLits),
			setOpSlotSchema(rightSchema), posResult)
		if err != nil {
			return nil, fmt.Errorf("executing %s right side: %w", u.op, err)
		}

		keyer := newSetOpKeyer(posResult)

		var resultRows []map[string]any

		switch u.op {
		case "intersect":
			resultRows = intersectRows(keyer, leftRows, rightRows, u.all)
		case "except":
			resultRows = exceptRows(keyer, leftRows, rightRows, u.all)
		default: // "union"
			resultRows = append(leftRows, rightRows...)
			if !u.all {
				resultRows = deduplicateRows(keyer, resultRows)
			}
		}

		if len(resultRows) > 0 {
			if schema != nil {
				// FromRowsChecked, not FromRows: this is where the operation's
				// VALUES are materialized, and the unchecked writer answered a
				// DECIMAL with no carrier at the unified scale with the
				// SATURATED end of the Int128 range — a DECIMAL(38,0) arm's
				// 10^30 came back as 17014118346046923173168730371.5884105727
				// under a DECIMAL(38,10) union, silently (#553). ADR-0024
				// item 4: at a value-producing site, no exact carrier is a
				// 22003 error, never the nearest storable number.
				b, err := batch.FromRowsChecked(posResult, resultRows)
				if err != nil {
					return nil, fmt.Errorf("building the %s result: %w", u.op, err)
				}
				// Back to the names the query publishes. The slots were an
				// internal addressing scheme for the rows above; the result
				// takes the FIRST arm's column names, duplicates included.
				for i := range b.Schema {
					if i < len(schema) {
						b.Schema[i].Name = schema[i].Name
					}
				}
				u.batches = []*batch.RecordBatch{b}
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
func intersectRows(k *setOpKeyer, left, right []map[string]any, all bool) []map[string]any {
	rightSet := make(map[string]int, len(right))
	for _, row := range right {
		rightSet[k.key(row)]++
	}

	if all {
		result := make([]map[string]any, 0)
		for _, row := range left {
			key := k.key(row)
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
		key := k.key(row)
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
func exceptRows(k *setOpKeyer, left, right []map[string]any, all bool) []map[string]any {
	rightSet := make(map[string]int, len(right))
	for _, row := range right {
		rightSet[k.key(row)]++
	}

	if all {
		result := make([]map[string]any, 0)
		for _, row := range left {
			key := k.key(row)
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
		key := k.key(row)
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

// alignSetOpRows re-keys one set-operation arm's rows onto the column names
// of the arm that decides the result schema (the first one). Arms correspond
// by POSITION in SQL, but these rows are name-keyed maps, so an arm selecting
// differently-spelled columns is invisible to rowHashKey and to
// batch.FromRows unless its keys are rewritten first.
//
// Returns rows unchanged when the schemas already agree, when either is
// unknown (an arm that produced nothing has no schema), or when the widths
// differ — a width mismatch is a malformed set operation, not something to
// paper over here.
// setOpSlotName is the internal address of one output column of a set
// operation. It is in the reserved hidden-slot namespace, so no query can
// spell it and it cannot collide with a column of either arm.
func setOpSlotName(i int) string { return "__setop_" + strconv.Itoa(i) }

// setOpSlotSchema is cols with every column renamed to its slot. Types,
// precision and scale are untouched — only the ADDRESS changes.
func setOpSlotSchema(cols []parquet.Column) []parquet.Column {
	out := make([]parquet.Column, len(cols))
	for i, c := range cols {
		c.Name = setOpSlotName(i)
		out[i] = c
	}
	return out
}

// setOpArmRows boxes one arm's result with its columns keyed by POSITION.
//
// CollectSink.ToRowValues is the positional form and it is non-nil exactly
// when the map form would lose a column — that is, when two of the arm's
// output columns share a name, which is the shape this exists for. When it is
// nil the map IS the positional form and the arm's own schema order supplies
// the addresses.
func setOpArmRows(sink *exec.CollectSink, schema []parquet.Column) []map[string]any {
	if vals := sink.ToRowValues(); vals != nil {
		out := make([]map[string]any, len(vals))
		for i, cells := range vals {
			m := make(map[string]any, len(schema))
			for j := range schema {
				if j < len(cells) {
					m[setOpSlotName(j)] = cells[j]
				}
			}
			out[i] = m
		}
		return out
	}
	rows := sink.ToRows()
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		m := make(map[string]any, len(schema))
		for j, c := range schema {
			m[setOpSlotName(j)] = row[c.Name]
		}
		out[i] = m
	}
	return out
}

// alignSetOpRows re-keys an arm's rows to the result's column names.
//
// It is no longer reached from the set-operation adapter, which addresses its
// arms by POSITION (setOpArmRows) and therefore needs no re-keying at all.
// Kept for the other caller and because it states the rule the slots enforce:
// the arms correspond by position, and the result takes the first arm's names.
func alignSetOpRows(want, have []parquet.Column, rows []map[string]any) []map[string]any {
	if len(want) == 0 || len(want) != len(have) {
		return rows
	}
	aligned := false
	for i := range want {
		if want[i].Name != have[i].Name {
			aligned = true
			break
		}
	}
	if !aligned {
		return rows
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		re := make(map[string]any, len(want))
		for j := range want {
			re[want[j].Name] = row[have[j].Name]
		}
		out[i] = re
	}
	return out
}

// deduplicateRows removes duplicate rows from a slice of row maps.
func deduplicateRows(k *setOpKeyer, rows []map[string]any) []map[string]any {
	seen := make(map[string]struct{}, len(rows))
	result := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		key := k.key(row)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// rowHashKey generates a string key from a row's column values, with no types
// to consult: names sorted for determinism, values rendered with %v.
//
// It is the FALLBACK now, for a set operation whose schema cannot type the
// rows — an arm that produced nothing has none. setOpKeyer.key is the typed
// path and is what every schema-carrying set operation uses, because %v alone
// cannot say that a DECIMAL's "12.75" and "12.7500" are one value (#499).
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
		catalog:          p.catalog,
		tableName:        tableName,
		partitionFilter:  partFilter,
		requiredCols:     requiredCols,
		scanPreds:        scanPreds,
		manifestSnapshot: p.ManifestSnapshot,
	}
	// Attach scan cache if this table is scanned multiple times in this query.
	if p.scanCache != nil {
		if cached, ok := p.scanCache[tableName]; ok {
			src.cache = cached
		}
	}
	// Wire per-query memory tracker so parquet pooled buffers are accounted for.
	if sm := p.getSpillManager(); sm != nil {
		src.memTracker = sm.Tracker()
		src.spillMgr = sm
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
	cache           *scanCached      // non-nil when this table is scanned multiple times
	replayIdx       atomic.Int64     // position in cache replay (atomic for parallel pipeline)
	projOnce        sync.Once        // guards projIdx/projSchema init (replay Next is concurrent)
	projIdx         []int            // cache-batch column indices for this consumer; nil = no projection
	projSchema      []parquet.Column // this consumer's projected schema
	isReplay        bool             // true when reading from cache instead of scanning; written once in Init before runParallel starts, so no synchronization needed
	// claimedCache is true when THIS source created cache.ready, i.e. it owes
	// every other consumer a release. Written once in Init, before any worker
	// goroutine exists, for the same reason isReplay is.
	claimedCache     bool
	bloomFilter      *exec.BloomScanFilter // bloom filter pushdown from hash join build side
	dynamicFilter    []exec.DynamicRange   // dynamic min/max range filter from hash join build side
	rowLimit         int64                 // LIMIT pushdown: enables lazy file downloading (0 = eager)
	memTracker       *memory.Tracker       // per-query memory tracker; wired at construction when budget>0
	spillMgr         *memory.SpillManager  // for pre-emptive relief on file-load reservations; nil-safe
	emitRowLoc       bool                  // top-N late materialization: stamp __row_loc on scan batches
	rowPreds         []scan.RowPred        // scan-level filter conjuncts (scan_filter_pushdown.go)
	shapeOnlyCols    map[string]bool       // byte-array columns decoded as lengths only (logical/shape_only_columns.go)
	manifestSnapshot *ManifestSnapshot     // pins this table's manifest to one read per statement (#502); nil-safe
}

// RefetchRows re-reads the full-width rows named by __row_loc values (see
// topn_late_mat.go), in locs order. Only valid on an emitRowLoc scan whose
// narrow phase has completed and whose source has not been closed.
func (s *catalogScanSource) RefetchRows(ctx context.Context, locs []int64) (*batch.RecordBatch, error) {
	ses, ok := s.inner.(*scannerExecSource)
	if !ok || ses.scanner == nil {
		return nil, fmt.Errorf("refetch: scan source is not a row-loc scan")
	}
	return ses.scanner.RefetchRows(ctx, locs)
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
			// The claim is released on EVERY exit, not only on success, so
			// waking up says the claiming scan is FINISHED — not that it
			// filled the cache. Replaying an abandoned cache would answer
			// from a truncated table, so this fails loudly with the reason
			// the claiming scan stopped.
			s.cache.mu.Lock()
			if !s.cache.done {
				err := s.cache.err
				s.cache.mu.Unlock()
				if err == nil {
					err = errors.New("scan ended before the table did")
				}
				return &abandonedClaimError{table: s.tableName, cause: err}
			}
			s.cache.mu.Unlock()
			s.isReplay = true
			s.replayIdx.Store(0)
			return nil
		}
		// First scan claims the cache. Whoever claims it OWES every other
		// consumer a release — see abandonCache.
		s.cache.ready = make(chan struct{})
		s.claimedCache = true
		s.cache.mu.Unlock()
	}
	// First scan (or no cache) — scan from storage. A cache-populating
	// scan reads the UNION of all consumers' columns so the cache can
	// serve every consumer; each consumer (this one included) projects
	// back down to its own columns in Next.
	scanCols := s.requiredCols
	if s.cache != nil {
		scanCols = s.cache.unionCols
	}
	sc := newScannerSource(s.catalog, s.tableName, s.partitionFilter, scanCols, s.scanPreds, s.manifestSnapshot)
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
		if s.memTracker != nil {
			ses.memTracker = s.memTracker
			ses.spillMgr = s.spillMgr
		}
		ses.emitRowLoc = s.emitRowLoc
		ses.rowPreds = s.rowPreds
		ses.shapeOnlyCols = s.shapeOnlyCols
	}
	s.inner = sc
	if err := s.inner.Init(ctx); err != nil {
		s.abandonCache(err)
		return err
	}
	return nil
}

// abandonedClaimError is what a waiter gets when the scan that claimed the
// shared cache finished without filling it.
//
// It is never a ROOT CAUSE. The claiming scan stopped because something else
// went wrong — its own read failed, or the query was already being torn down —
// so this error is always downstream of the reason the client actually needs.
// It carries that reason where the claiming scan knew it (Unwrap), and callers
// that hold BOTH this and the real failure prefer the real one; see the probe
// side of buildJoin.
type abandonedClaimError struct {
	table string
	cause error
}

func (e *abandonedClaimError) Error() string {
	return fmt.Sprintf("shared scan of %s did not complete: %v", e.table, e.cause)
}

func (e *abandonedClaimError) Unwrap() error { return e.cause }

// isAbandonedClaim reports whether err is (or wraps) a waiter's abandoned-claim
// failure — that is, whether it is a CONSEQUENCE of some other failure rather
// than a reason of its own.
func isAbandonedClaim(err error) bool {
	var a *abandonedClaimError
	return errors.As(err, &a)
}

// abandonCache releases a claim this source took but will not fill, so the
// consumers waiting on it fail with err instead of blocking forever.
//
// The claim used to be released in exactly one place — Next's end-of-table
// branch — which made every other exit a permanent block: a scan that FAILED,
// or was closed before the end of the table, left `ready` open and every other
// reader of that table's cache waited on a channel nobody would ever close.
// That is the deadlock #616 reports and it is reachable from any plan with two
// readers of one table where the first one fails (measured: two LATERALs over
// the same table, the second carrying a residual the scan cannot compile —
// 6 ms to the error on the single-process arm, an unbounded block on both DAG
// arms).
//
// Releasing is not enough on its own: a waiter that wakes on an abandoned
// claim must NOT replay, because the cache holds only what was read before the
// scan stopped. Init returns the error instead.
func (s *catalogScanSource) abandonCache(err error) {
	if s.cache == nil || !s.claimedCache {
		return
	}
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	s.abandonCacheLocked(err)
}

// abandonCacheLocked is abandonCache for callers already holding cache.mu.
func (s *catalogScanSource) abandonCacheLocked(err error) {
	if s.cache.done || s.cache.abandoned {
		return
	}
	s.cache.abandoned = true
	s.cache.err = err
	if s.cache.ready != nil {
		close(s.cache.ready)
	}
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
		// Sel mutation. Sel itself is carried over (slice header copy —
		// downstream filters replace Sel rather than mutating in place):
		// dropping it, as this used to, resurrected delete-marker-filtered
		// rows on replay.
		clone := &batch.RecordBatch{
			Schema:  cached.Schema,
			Columns: make([]*batch.Vector, len(cached.Columns)),
			Len:     cached.Len,
			Sel:     cached.Sel,
		}
		copy(clone.Columns, cached.Columns)
		return s.projectForConsumer(clone), nil
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
			// This scan owns the claim and is not going to fill the cache.
			s.abandonCacheLocked(err)
			return nil, err
		}
		if b == nil {
			// An ABANDONED claim is already released and its batches are a
			// partial read. Pipeline.runParallel calls Next from every worker
			// on this same source, so "worker A failed, worker B then reached
			// the end of the table" is a real interleaving — and running this
			// branch after it would close an already-closed channel (a panic)
			// and, worse, mark a TRUNCATED cache done for the next waiter to
			// replay as if it were the whole table.
			if s.cache.abandoned {
				return nil, s.cache.err
			}
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
		// corrupt cached data by setting Sel in-place. Sel is preserved
		// (delete markers arrive from the scan as Sel) — see the replay
		// branch.
		cached := &batch.RecordBatch{
			Schema:  b.Schema,
			Columns: make([]*batch.Vector, len(b.Columns)),
			Len:     b.Len,
			Sel:     b.Sel,
		}
		copy(cached.Columns, b.Columns)
		// NOT charged to the memory tracker, deliberately. The cache's
		// vectors are SHARED with its consumers — hash-join builds
		// Reserve hashBuildBytes for these same vectors, and the scan
		// source charges them transiently in flight. Reserving them
		// again here (tried 2026-07-06) triple-counted the same physical
		// memory: the ledger hit the budget while RSS was fine, every
		// append stalled in ReserveOrForce's relief wait, and the forced
		// build spills turned SF10 Q21 from 1m28s into 8m35s on EC2
		// (CPU profile: 4.76% utilization — pure stall). Honest cache
		// accounting needs the cache to OWN spillable bytes
		// (SpillableBatchCollector, like the CTE cache) — not a second
		// charge for memory the ledger already sees.
		s.cache.batches = append(s.cache.batches, cached)
		return s.projectForConsumer(b), nil
	}

	// No cache: inner.Next() is thread-safe for channel-based scan sources.
	return s.inner.Next(ctx)
}

// projectForConsumer narrows a union-column cache batch down to this
// consumer's RequiredColumns. Shallow: shares vectors, no copies. The
// no-cache path, SELECT-* consumers (empty requiredCols), and batches
// already matching the consumer's set pass through untouched. Also
// defensive: any required column missing from the batch schema (e.g.,
// synthetic columns) disables projection rather than dropping data.
func (s *catalogScanSource) projectForConsumer(b *batch.RecordBatch) *batch.RecordBatch {
	if s.cache == nil || len(s.requiredCols) == 0 || b == nil {
		return b
	}
	s.projOnce.Do(func() {
		if len(s.requiredCols) >= len(b.Schema) {
			return
		}
		want := make(map[string]bool, len(s.requiredCols))
		for _, name := range s.requiredCols {
			want[name] = true
		}
		// Keep BATCH-SCHEMA (table) order, matching what a standalone
		// scan of this node would emit via buildReadSchema — downstream
		// operators may have bound positions against that shape.
		idx := make([]int, 0, len(s.requiredCols))
		schema := make([]parquet.Column, 0, len(s.requiredCols))
		found := 0
		for i, col := range b.Schema {
			if want[col.Name] || batch.NameSetNames(want, col.Name) {
				idx = append(idx, i)
				schema = append(schema, col)
				found++
			}
		}
		if found < len(want) {
			return // some required column missing — pass through unprojected
		}
		s.projIdx = idx
		s.projSchema = schema
	})
	if s.projIdx == nil {
		return b
	}
	nb := &batch.RecordBatch{
		Schema:  s.projSchema,
		Columns: make([]*batch.Vector, len(s.projIdx)),
		Len:     b.Len,
		Sel:     b.Sel,
	}
	for i, ci := range s.projIdx {
		nb.Columns[i] = b.Columns[ci]
	}
	return nb
}

func (s *catalogScanSource) Close() error {
	if s.isReplay {
		return nil
	}
	// A claiming scan torn down before the end of the table (a LIMIT upstream,
	// a cancelled query, an operator that failed) owes the release just as
	// much as one that errored — otherwise every other reader of this table
	// waits on a channel nobody will close.
	s.abandonCache(errors.New("scan closed before the end of the table"))
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

// buildFilterOp compiles one predicate into a filter operator. It returns an
// error only for a predicate naming a function that does not exist: every other
// compile failure falls through to the raw-string and column-compare paths
// below, which is what makes those fallbacks useful. An unknown function has
// nothing to fall through TO — the string parser would not recognize it either,
// so the predicate would quietly become nil and the filter would vanish,
// admitting every row (#341).
func (p *Planner) buildFilterOp(pred logical.Predicate, outerTables map[string]bool, outerCols map[string]string) (exec.UnaryOperator, error) {
	// Try to compile from AST expression first (full expression engine)
	if pred.ASTExpr != nil {
		var compiled expr.Expr
		var err error
		if len(outerTables) > 0 {
			if len(outerCols) > 0 {
				compiled, err = expr.CompileWithScopeResolver(pred.ASTExpr, p.subqueryRunner, outerTables, outerCols, p.subqueryInnerColumns(), p.subqueryDeclOption(), p.subqueryBudgetOption())
			} else {
				compiled, err = expr.CompileWithScope(pred.ASTExpr, p.subqueryRunner, outerTables, p.subqueryDeclOption(), p.subqueryBudgetOption())
			}
		} else {
			compiled, err = expr.CompileWithRunner(pred.ASTExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
		}
		if expr.IsCompileRefusal(err) {
			return nil, err
		}
		if err == nil {
			// Try to extract vectorized filter for simple comparison patterns.
			// First try full vectorization, then partial (vectorize what we can
			// from AND chains, keep the rest as row-at-a-time predicates).
			if vf := tryVectorizeFilter(compiled); vf != nil {
				return vf, nil
			}
			// The #147 guard for the ROW evaluator, which KernelFilter has
			// had since and this path did not: a predicate naming a column
			// the input does not carry is UNKNOWN on every row, and a WHERE
			// admits only TRUE, so it answers zero rows in silence (#653).
			// Declined for a CORRELATED predicate, whose outer references
			// resolve outside this batch by design.
			check := rowFilterColumnCheck(pred.ASTExpr, outerTables)
			if vf := tryPartialVectorize(compiled, check); vf != nil {
				return vf, nil
			}
			f := exec.NewFilter(wrapPredicate(compiled))
			f.Check = check
			return f, nil
		}
	}

	// Fall back to raw string parsing
	if pred.Raw != "" {
		p := parseSimplePredicate(pred.Raw)
		if p != nil {
			return p, nil
		}
	}

	if pred.Column != "" && pred.Op != "" {
		op := parseCompareOp(pred.Op)
		return exec.NewFilter(exec.ColumnCompareLit(pred.Column, op, pred.Value, pred.ValueText)), nil
	}

	return nil, nil
}

// tryVectorizeFilter inspects a compiled expression tree and returns a vectorized
// filter operator when the pattern is a simple comparison (col op col, col op const)
// or an AND chain of such comparisons. Returns nil for complex expressions.
func tryVectorizeFilter(e expr.Expr) exec.UnaryOperator {
	ops := extractFilterOps(e, false)
	if len(ops) == 0 {
		return nil
	}
	if len(ops) == 1 {
		return ops[0]
	}
	return exec.NewChainFilter(ops)
}

// rowFilterColumnCheck returns the first-batch column-existence check for a
// row-evaluated predicate, or nil where the guard cannot apply: a correlated
// predicate (its outer references resolve outside the batch by design) or one
// carrying a node FilterColumnRefs declines to enumerate.
func rowFilterColumnCheck(ast plansql.Node, outerTables map[string]bool) func(*batch.RecordBatch) error {
	if ast == nil || len(outerTables) > 0 {
		return nil
	}
	refs, ok := expr.FilterColumnRefs(ast)
	if !ok || len(refs) == 0 {
		return nil
	}
	return func(b *batch.RecordBatch) error { return expr.CheckFilterColumns(b, refs) }
}

// tryPartialVectorize handles AND chains where some operands are vectorizable and
// some are not. Vectorized operands run first (narrowing the selection vector),
// followed by row-at-a-time predicates for the rest. This is better than falling
// back entirely to row-at-a-time when any part of an AND chain isn't vectorizable.
func tryPartialVectorize(e expr.Expr, check func(*batch.RecordBatch) error) exec.UnaryOperator {
	parts := flattenAnds(e)
	if len(parts) < 2 {
		return nil // not an AND chain
	}
	var vectorized []exec.UnaryOperator
	var nonVectorized []expr.Expr
	for _, part := range parts {
		ops := extractFilterOps(part, false)
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
		f := exec.NewFilter(wrapPredicate(e))
		// The whole predicate's references, not this conjunct's: every op in
		// the chain reads ONE batch, so its schema answers for all of them.
		f.Check = check
		check = nil // once is enough
		allOps = append(allOps, f)
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
// kernelFilterWithRowFallback builds a typed kernel filter; for dotted
// column names ("attrs.score") it attaches the compiled comparison as a
// row-at-a-time fallback so ROW-field access works (the kernel resolves
// qualified table refs by stripping the prefix, but cannot reach into ROW
// children — issue #147).
// colColFilterWithRowFallback builds a col-col kernel filter carrying the
// compiled comparison as a row-at-a-time fallback. The kernel requires both
// columns to share a storage type; when they differ (e.g. FLOAT64 <>
// INT32), the fallback evaluates the comparison with SQL numeric coercion
// instead of the kernel indexing the wrong typed slice (issue #375).
func colColFilterWithRowFallback(left, right string, op exec.CompareOp, cmp expr.Expr) *exec.ColColFilter {
	f := exec.NewColColFilter(left, right, op)
	f.RowFallback = wrapPredicate(cmp)
	return f
}

// fieldPathColRef returns node as a *plansql.ColRef when it is one, seeing
// through parentheses — the shape colDecls.field resolves against. A nil
// answer simply resolves to no field.
func fieldPathColRef(node plansql.Node) *plansql.ColRef {
	for {
		switch n := node.(type) {
		case *plansql.ColRef:
			return n
		case *plansql.ParenNode:
			node = n.Inner
		default:
			return nil
		}
	}
}

// nullCheckWithRowFallback and likeFilterWithRowFallback are
// kernelFilterWithRowFallback for the two vectorized filters that had no
// fallback at all. Both resolved a dotted name by stripping the qualifier and
// then, finding nothing, matched NO ROWS silently — so `WHERE rw.f IS NULL`
// and `WHERE rw.s LIKE 'x%'` over a ROW field answered an empty result
// indistinguishable from real data (#568). The comparison filters have had
// this delegation since #147.
func nullCheckWithRowFallback(name string, checkNull bool, e expr.Expr) exec.UnaryOperator {
	f := exec.NewNullCheckFilter(name, checkNull)
	if strings.Contains(name, ".") {
		f.RowFallback = wrapPredicate(e)
	}
	return f
}

func likeFilterWithRowFallback(name, pattern string, negate bool, e expr.Expr) exec.UnaryOperator {
	f := exec.NewLikeFilter(name, pattern, negate)
	if strings.Contains(name, ".") {
		f.RowFallback = wrapPredicate(e)
	}
	return f
}

func kernelFilterWithRowFallback(name string, op exec.CompareOp, lit *expr.Lit, cmp expr.Expr) exec.UnaryOperator {
	if lit.Val == nil {
		// A comparison against a NULL literal is UNKNOWN for every row, so no
		// row qualifies. It cannot be lowered to a value comparison at all:
		// the kernel takes its constant as a box and every typed coercion
		// reads nil as that type's ZERO, which answered `WHERE c_i64 = NULL`
		// with the rows where the column is 0 (#450).
		return exec.NewMatchNothingFilter()
	}
	// The literal's own TEXT travels with its box. A DECIMAL column's kernel
	// converts the text at the column's scale, which is the only way a
	// literal past a float64's ~15-16 significant digits reaches the
	// comparison as the number that was written (#452).
	kf := exec.NewKernelFilterLit(name, op, lit.Val, lit.Text)
	if strings.Contains(name, ".") {
		kf.RowFallback = wrapPredicate(cmp)
	}
	return kf
}

// kernelOrNothing is the `col <op> constant` operator for a constant that did
// not arrive inside an expr.Lit — a BETWEEN bound, or a value parsed out of
// raw predicate text. Same NULL rule as kernelFilterWithRowFallback.
func kernelOrNothing(col string, op exec.CompareOp, val any, text string) exec.UnaryOperator {
	return kernelOrNothingRef(nil, col, op, val, text)
}

// kernelOrNothingRef is kernelOrNothing carrying the reference the constant is
// compared against, so a dotted name can be given the row-at-a-time fallback
// kernelFilterWithRowFallback gives the `col <op> lit` shape. Without it a
// BETWEEN over a ROW field path failed with `filter column "rw.f" does not
// exist in the input schema` — loud, but a query PostgreSQL answers (#568).
//
// Each HALF of a BETWEEN gets its own fallback comparison rather than the
// whole predicate: the two halves are separate operators, and handing both
// the same conjunction would evaluate it twice.
func kernelOrNothingRef(ref *expr.ColRef, col string, op exec.CompareOp, val any, text string) exec.UnaryOperator {
	if val == nil {
		return exec.NewMatchNothingFilter()
	}
	kf := exec.NewKernelFilterLit(col, op, val, text)
	if ref != nil && strings.Contains(col, ".") {
		kf.RowFallback = wrapPredicate(expr.NewCmp(ref, &expr.Lit{Val: val, Text: text}, execToCmpOp(op)))
	}
	return kf
}

// execToCmpOp is cmpToExecOp's inverse, for the sites that lower a comparison
// to a kernel and then have to rebuild the equivalent expression as a
// fallback.
func execToCmpOp(op exec.CompareOp) expr.CmpOp {
	switch op {
	case exec.OpEq:
		return expr.CmpEq
	case exec.OpNe:
		return expr.CmpNe
	case exec.OpLt:
		return expr.CmpLt
	case exec.OpLe:
		return expr.CmpLe
	case exec.OpGt:
		return expr.CmpGt
	default:
		return expr.CmpGe
	}
}

// inFilterForList builds the IN / NOT IN operator for a list of literals,
// applying SQL's NULL rule to the LIST — which is not the same rule as for a
// scalar comparison, and is the one that surprises people:
//
//	`x IN (a, NULL)` is TRUE where x = a and UNKNOWN everywhere else, because
//	TRUE dominates the disjunction. A NULL member therefore drops out; with
//	nothing else left the whole test is UNKNOWN and nothing qualifies.
//
//	`x NOT IN (a, NULL)` is `x <> a AND x <> NULL`, and the second conjunct is
//	UNKNOWN for every row: the result is FALSE or UNKNOWN, never TRUE. A NULL
//	anywhere in a NOT IN list empties the answer (#450).
//
// An empty list with no NULL in it is left alone — that is a different shape
// and the set kernel already answers it.
//
// RESIDUAL (real NOT IN + NULL + over-range literal only): `real NOT IN (1e40,
// NULL)` short-circuits to MatchNothing below on the NULL rule (#450) before any
// literal is examined, so PostgreSQL's 22003 for the over-range 1e40 in the
// real[] cast is not raised — wadjet answers empty. The positive `IN (1e40,
// NULL)` is NOT affected: it keeps the over-range literal, carries the syntactic
// arity of 2 (SetSyntacticLen below), narrows to real[], and raises 22003 like
// PostgreSQL. Surfacing the error on the NOT-IN path would mean checking the
// over-range literal before the MatchNothing short-circuit; left as a documented
// residual (obscure — a NULL in a NOT IN already empties the answer).
func inFilterForList(col string, values []any, texts []string, negate bool) exec.UnaryOperator {
	kept := make([]any, 0, len(values))
	keptTexts := make([]string, 0, len(texts))
	hadNull := false
	for i, v := range values {
		if v == nil {
			hadNull = true
			continue
		}
		kept = append(kept, v)
		if i < len(texts) {
			keptTexts = append(keptTexts, texts[i])
		}
	}
	if hadNull && (negate || len(kept) == 0) {
		return exec.NewMatchNothingFilter()
	}
	inf := exec.NewInFilterLit(col, kept, keptTexts, negate)
	// The comparison WIDTH of a FLOAT32 IN list is decided by the SYNTACTIC
	// element count, not the count that survives the NULL strip above: PostgreSQL
	// casts the whole `{...}` array literal — NULLs included — to real[] whenever
	// there is more than one element, so `real IN (0.1, NULL)` narrows to real
	// and matches, while `real IN (0.1)` widens to double and does not (#549).
	inf.SetSyntacticLen(len(values))
	return inf
}

// negateCmpOp inverts a comparison for an enclosing NOT. Under SQL's
// three-valued logic NOT (a = b) is TRUE exactly where a <> b is TRUE — both
// are UNKNOWN when either side is NULL, and every filter kernel already skips
// NULL rows — so the inverted operator is the whole of the negation for a
// WHERE, which admits only TRUE. The second result is false for an operator
// with no inverse in this set: the signal to leave the predicate to the row
// evaluator rather than lower it wrongly.
func negateCmpOp(op exec.CompareOp) (exec.CompareOp, bool) {
	switch op {
	case exec.OpEq:
		return exec.OpNe, true
	case exec.OpNe:
		return exec.OpEq, true
	case exec.OpLt:
		return exec.OpGe, true
	case exec.OpLe:
		return exec.OpGt, true
	case exec.OpGt:
		return exec.OpLe, true
	case exec.OpGe:
		return exec.OpLt, true
	default:
		return op, false
	}
}

// maybeNegate applies an enclosing NOT to an already-mapped comparison.
func maybeNegate(op exec.CompareOp, neg bool) (exec.CompareOp, bool) {
	if !neg {
		return op, true
	}
	return negateCmpOp(op)
}

// negatedExpr is the expression a lowered operator's row-at-a-time fallback
// must evaluate. That fallback is the ORIGINAL comparison, so under a NOT it
// has to be wrapped: handing it the un-negated node is the same dropped
// negation one layer down (#461).
func negatedExpr(e expr.Expr, neg bool) expr.Expr {
	if !neg {
		return e
	}
	return &expr.Not{Operand: e}
}

// orOfOps unions two extracted operand lists into one OR filter. Nil on
// either side means that side is not vectorizable, and an OR is only as
// vectorizable as both of its arms.
func orOfOps(leftOps, rightOps []exec.UnaryOperator) []exec.UnaryOperator {
	if leftOps == nil || rightOps == nil {
		return nil
	}
	one := func(ops []exec.UnaryOperator) exec.UnaryOperator {
		if len(ops) == 1 {
			return ops[0]
		}
		return exec.NewChainFilter(ops)
	}
	return []exec.UnaryOperator{exec.NewOrFilter(one(leftOps), one(rightOps))}
}

// extractFilterOps recursively extracts vectorizable filter ops from AND
// combinations.
//
// neg carries an enclosing NOT: what comes back is the negation of e. That is
// what the *expr.Not case used to drop — it returned the operand's own
// operators, so `WHERE NOT (k = 131)` was executed as `WHERE k = 131`, the
// complement of the answer, silently and on both engines (#461). Anything
// that cannot be negated returns nil instead, and the caller's residual
// row-at-a-time filter evaluates the NOT itself, where three-valued logic
// survives (expr.Not.EvalBoolNull).
func extractFilterOps(e expr.Expr, neg bool) []exec.UnaryOperator {
	switch v := e.(type) {
	case *expr.Cmp:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		// col op col
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					// Possible ROW-field access — the col-col kernel can't
					// evaluate it; leave this comparison row-at-a-time.
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
		}
		// col op const
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
		// const op col → flip
		if lit, lok := v.Left.(*expr.Lit); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(rc.Name, flipOp(op), lit, fb)}
			}
		}
	case *expr.CmpNetworkLit:
		// Bare column vs. a string literal compileCmp pre-parsed as an IPv4
		// or MAC address (tryNetworkLit/CmpNetworkLit in expr/compile.go).
		// This case was missing entirely, so every `ipv4_col <op> 'lit'` /
		// `mac_col <op> 'lit'` predicate fell through to nil here and ran
		// row-at-a-time, losing the vectorized kernel a plain *expr.Cmp node
		// got on this exact shape before compileCmp started emitting
		// CmpNetworkLit (measured +43% on 400k rows).
		//
		// v.Col's type isn't known here — extractFilterOps has no schema,
		// same as the *expr.Cmp arm above — so this builds the identical
		// "col op const" kernel filter that arm would have built for the
		// original `col op 'lit'`/`'lit' op col`, from v.Lit (the literal's
		// original text) rather than the pre-parsed ipv4/mac int64s on the
		// node: ResolveFilterKernel (exec/kernel/compare.go) dispatches
		// purely on the column's REAL runtime type, parsing v.Lit itself via
		// parseIPv4ToInt64/parseMACToInt64 for an actual network column and
		// falling to compareFilterString for anything else. That is also
		// why tryNetworkLit does not need to be, and cannot be, restricted
		// to network-typed columns at compile time: a STRING column whose
		// literal happens to parse as an address (`s = '10.1.2.3'`) rides
		// this same case and gets exactly its normal compareFilterString
		// kernel — using the pre-parsed int64s directly here, bypassing
		// that dispatch, would misinterpret a STRING vector as encoded
		// IPv4/MAC int64 data.
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		kOp := op
		if v.Flip {
			kOp = flipOp(op)
		}
		return []exec.UnaryOperator{kernelFilterWithRowFallback(v.Col.Name, kOp, &expr.Lit{Val: v.Lit}, fb)}
	case *expr.CmpInt64:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
	case *expr.CmpFloat64:
		op, ok := maybeNegate(cmpToExecOp(v.Op), neg)
		if !ok {
			return nil
		}
		fb := negatedExpr(v, neg)
		if lc, lok := v.Left.(*expr.ColRef); lok {
			if rc, rok := v.Right.(*expr.ColRef); rok {
				if strings.Contains(lc.Name, ".") || strings.Contains(rc.Name, ".") {
					return nil
				}
				return []exec.UnaryOperator{colColFilterWithRowFallback(lc.Name, rc.Name, op, fb)}
			}
			if lit, rok := v.Right.(*expr.Lit); rok {
				return []exec.UnaryOperator{kernelFilterWithRowFallback(lc.Name, op, lit, fb)}
			}
		}
	case *expr.And:
		if neg {
			// De Morgan: NOT (a AND b) is NOT a OR NOT b, which holds in
			// Kleene logic as well as Boolean.
			return orOfOps(extractFilterOps(v.Left, true), extractFilterOps(v.Right, true))
		}
		leftOps := extractFilterOps(v.Left, false)
		if leftOps == nil {
			return nil
		}
		rightOps := extractFilterOps(v.Right, false)
		if rightOps == nil {
			return nil
		}
		return append(leftOps, rightOps...)
	case *expr.Or:
		if neg {
			// De Morgan the other way: NOT (a OR b) is NOT a AND NOT b, and
			// an AND is the chained intersection of the two selections.
			leftOps := extractFilterOps(v.Left, true)
			if leftOps == nil {
				return nil
			}
			rightOps := extractFilterOps(v.Right, true)
			if rightOps == nil {
				return nil
			}
			return append(leftOps, rightOps...)
		}
		return orOfOps(extractFilterOps(v.Left, false), extractFilterOps(v.Right, false))
	case *expr.Between:
		// col BETWEEN low AND high → two kernel filters: col >= low AND col <= high
		// col NOT BETWEEN low AND high → col < low OR col > high
		if col, ok := v.Expr.(*expr.ColRef); ok {
			if lo, lok := v.Low.(*expr.Lit); lok {
				if hi, hok := v.Hi.(*expr.Lit); hok {
					// SQL defines `x NOT BETWEEN a AND b` as `NOT (x BETWEEN
					// a AND b)`, so an enclosing NOT is the same flag.
					// A NULL bound makes its own half UNKNOWN and leaves the
					// other half standing. BETWEEN then admits nothing, and
					// NOT BETWEEN reduces to the surviving comparison —
					// `x NOT BETWEEN NULL AND h` is TRUE exactly where
					// x > h, because a FALSE conjunct makes the conjunction
					// FALSE whatever the UNKNOWN one says (#450).
					if v.Not != neg {
						return []exec.UnaryOperator{exec.NewOrFilter(
							kernelOrNothingRef(col, col.Name, exec.OpLt, lo.Val, lo.Text),
							kernelOrNothingRef(col, col.Name, exec.OpGt, hi.Val, hi.Text),
						)}
					}
					return []exec.UnaryOperator{
						kernelOrNothingRef(col, col.Name, exec.OpGe, lo.Val, lo.Text),
						kernelOrNothingRef(col, col.Name, exec.OpLe, hi.Val, hi.Text),
					}
				}
			}
		}
	case *expr.In:
		// col IN (lit, lit, ...) or col NOT IN (lit, lit, ...)
		if col, ok := v.Expr.(*expr.ColRef); ok {
			values := make([]any, 0, len(v.Values))
			texts := make([]string, 0, len(v.Values))
			for _, val := range v.Values {
				if lit, ok := val.(*expr.Lit); ok {
					values = append(values, lit.Val)
					texts = append(texts, lit.Text)
				} else {
					return nil // non-literal in IN list
				}
			}
			f := inFilterForList(col.Name, values, texts, v.Not != neg)
			if inf, ok := f.(*exec.InFilter); ok && strings.Contains(col.Name, ".") {
				inf.RowFallback = wrapPredicate(negatedExpr(v, neg))
			}
			return []exec.UnaryOperator{f}
		}
	case *expr.Like:
		// col LIKE 'pattern' or col NOT LIKE 'pattern'
		if col, ok := v.Expr.(*expr.ColRef); ok {
			if pat, ok := v.Pattern.(*expr.Lit); ok {
				if pat.Val == nil {
					// `col LIKE NULL` is UNKNOWN for every row, negated or
					// not — there is no pattern to match against (#450).
					return []exec.UnaryOperator{exec.NewMatchNothingFilter()}
				}
				if s, ok := pat.Val.(string); ok {
					return []exec.UnaryOperator{likeFilterWithRowFallback(col.Name, s, v.Not != neg, negatedExpr(v, neg))}
				}
			}
		}
	case *expr.IsNull:
		// col IS NULL / col IS NOT NULL — vectorized null bitmap scan
		if col, ok := v.Operand.(*expr.ColRef); ok {
			return []exec.UnaryOperator{nullCheckWithRowFallback(col.Name, v.Not == neg, negatedExpr(v, neg))}
		}
	case *expr.ColIsNull:
		// Offsets-shape rewrite of `col IS [NOT] NULL` (expr/shape_funcs.go).
		// Same kernel the *expr.IsNull case builds — without this the filter
		// would silently drop to row-at-a-time evaluation.
		return []exec.UnaryOperator{nullCheckWithRowFallback(v.Col.Name, v.Not == neg, negatedExpr(v, neg))}
	case *expr.ColEmptyStr:
		// Offsets-shape rewrite of a column compared against the empty
		// string literal (expr/shape_funcs.go). Reproduces exactly what the
		// *expr.Cmp "col op const" branch built for the pre-rewrite node.
		op := exec.OpEq
		if v.Not != neg {
			op = exec.OpNe
		}
		return []exec.UnaryOperator{kernelFilterWithRowFallback(v.Col.Name, op, &expr.Lit{Val: ""}, negatedExpr(v, neg))}
	case *expr.Not:
		// NOT (expr) — vectorize the NEGATION of the inner expression. This
		// case used to return the inner expression's own operators, which
		// applied the predicate positively and answered the complement (#461).
		return extractFilterOps(v.Operand, !neg)
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
			// Derived-table aliases count: this is the outer scope a
			// correlated subquery's references are resolved against (#489).
			for _, name := range n.ScopeNames() {
				aliases[strings.ToLower(name)] = true
			}
		}
		// So does a CTE reference. It records its scope on the SUBTREE ROOT
		// rather than on the scans below (subtreeNamesRelation says why), so
		// a walk that reads only NodeScan never sees it and `WHERE EXISTS
		// (… WHERE t.k = u.did)` over a CTE `u` was not recognized as
		// correlated at all (#535).
		if n.CTEName != "" {
			aliases[strings.ToLower(n.CTEName)] = true
		}
		if n.CTERefAlias != "" {
			aliases[strings.ToLower(n.CTERefAlias)] = true
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
// groupKeyNDVEstimate resolves a GROUP-KEY cardinality estimate from the
// scan's merged-HLL column stats: the per-column NDVs' product (single
// column: the NDV itself), capped by the scan row estimate — the true
// group count can exceed neither. Returns 0 (no hint) when any key column
// lacks stats (synthetic __gb_expr keys, expression keys, missing HLL) or
// the input shape hides the scan (joins, subqueries): sizing then falls
// back to organic growth, which is never wrong, just slower.
func groupKeyNDVEstimate(child *logical.Node, groupByCols []string) int64 {
	if len(groupByCols) == 0 {
		return 0
	}
	n := child
	for n != nil && n.Type != logical.NodeScan {
		switch n.Type {
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort:
			if len(n.Children) != 1 {
				return 0
			}
			n = n.Children[0]
		default:
			return 0
		}
	}
	if n == nil || n.ScanColStats == nil {
		return 0
	}
	est := int64(1)
	for _, c := range groupByCols {
		cs, ok := n.ScanColStats[c]
		if !ok {
			// Stats keys carry catalog casing; group cols may be
			// SQL-normalized.
			for name, v := range n.ScanColStats {
				if strings.EqualFold(name, c) {
					cs, ok = v, true
					break
				}
			}
		}
		if !ok || cs.NDV <= 0 {
			return 0
		}
		// Overflow-safe product; anything past the row estimate is capped
		// below anyway.
		if est > (1<<62)/cs.NDV {
			est = 1 << 62
			break
		}
		est *= cs.NDV
	}
	if n.ScanRowEstimate > 0 && est > n.ScanRowEstimate {
		est = n.ScanRowEstimate
	}
	return est
}

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
			return kernelOrNothing(col, o.op, val, numericLitText(valStr))
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
			loStr := strings.TrimSpace(rest[:andIdx])
			hiStr := strings.TrimSpace(rest[andIdx+len(" AND "):])
			lo, hi := parseValue(loStr), parseValue(hiStr)
			return exec.NewChainFilter([]exec.UnaryOperator{
				kernelOrNothing(col, exec.OpGe, lo, numericLitText(loStr)),
				kernelOrNothing(col, exec.OpLe, hi, numericLitText(hiStr)),
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
		texts := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			values = append(values, parseValue(part))
			texts = append(texts, numericLitText(part))
		}
		if len(values) > 0 {
			return inFilterForList(col, values, texts, false)
		}
	}

	return nil
}

// numericLitText returns a raw predicate operand's text when it is a plain
// decimal number, and "" otherwise. It is what lets a DECIMAL comparison
// built from raw SQL text keep the digits the float64 box drops (#452);
// exponent forms are deliberately not included, so they keep the box.
func numericLitText(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	i := 0
	if s[0] == '+' || s[0] == '-' {
		i++
	}
	digits, dot := false, false
	for ; i < len(s); i++ {
		switch {
		case s[i] >= '0' && s[i] <= '9':
			digits = true
		case s[i] == '.' && !dot:
			dot = true
		default:
			return ""
		}
	}
	if !digits {
		return ""
	}
	return s
}

func parseValue(s string) any {
	s = strings.TrimSpace(s)
	// Remove quotes
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	// An UNQUOTED null is the NULL literal. Falling through returned the
	// four-character string "null", so `WHERE c = NULL` reaching this path
	// compared the column against that text instead of answering UNKNOWN;
	// the callers turn nil into a match-nothing operator (#450).
	if strings.EqualFold(s, "null") {
		return nil
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
	case "percentile_cont", "quantile_cont":
		return exec.AggPercentileCont
	case "percentile_disc", "quantile_disc":
		return exec.AggPercentileDisc
	case "mode":
		return exec.AggMode
	case "ohlcv":
		return exec.AggOhlcv
	case exec.OhlcvStateFunc:
		return exec.AggOhlcvState
	case exec.OhlcvStateMergeFunc:
		return exec.AggOhlcvStateMerge
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

// parseWindowFunc maps a SQL window function name onto its operator constant.
// A name exec has no window form for still resolves to ROW_NUMBER — the zero
// value — but no plan reaches the operator with one, because refuseUnwindowable
// runs first at every site that builds a window operator.
func parseWindowFunc(s string) exec.WindowFunc {
	fn, _ := exec.ParseWindowFunc(s)
	return fn
}

// refuseUnwindowable fails the plan for a window expression whose function has
// no window form, with the SAME sentence the worker's fragment builder raises
// (exec.RefuseUnsupportedWindowFunc). Before it, such a plan reached
// exec.Window as ROW_NUMBER with a mis-typed output vector and PANICKED —
// 23 of the 28 known aggregates did, reported as "internal error in pipeline"
// with no SQLSTATE (#965's census; see exec.RefuseUnsupportedWindowFunc).
func refuseUnwindowable(exprs []logical.WindowExpr) error {
	for _, we := range exprs {
		if _, ok := exec.ParseWindowFunc(we.Func); !ok {
			return exec.RefuseUnsupportedWindowFunc(we.Func)
		}
	}
	return nil
}

// resolveNullsLast determines whether nulls should sort last for a given order
// expression. An explicit NULLS FIRST / NULLS LAST always wins; otherwise the
// engine default applies: NULLS LAST for ASC, NULLS FIRST for DESC.
//
// That is PostgreSQL's rule, chosen deliberately. SQL leaves the default
// implementation-defined and DuckDB picks NULLS LAST in both directions, but
// wadjet speaks the PostgreSQL wire protocol, so a psql/DataGrip/Superset user
// writing ORDER BY x DESC expects PostgreSQL's placement. The DuckDB gate is
// held to the same rule by setting default_null_order in the oracle rather
// than by exempting entries, so the comparison keeps its full strength.
//
// See distributed.SortKeySpec.PlaceNullsLast, which has to agree with this
// function key for key or the two execution paths sort differently.
func resolveNullsLast(ob logical.OrderExpr) bool {
	if ob.NullsFirst != nil {
		return !*ob.NullsFirst // NullsFirst=true => NullsLast=false, and vice versa
	}
	return !ob.Desc
}

// isComputedProjection reports whether a SELECT item's value is COMPUTED
// rather than read straight from an input column. Only a bare column
// reference — optionally parenthesised — reads an input column; everything
// else (function call, arithmetic, CASE, CAST, concatenation) produces a new
// value whose type comes from the expression, not from whatever input column
// happens to share the output's alias (#327).
//
// A nil AST expression is the pre-AST projection form, which is always a
// plain column.
func isComputedProjection(e plansql.Node) bool {
	for {
		switch n := e.(type) {
		case nil:
			return false
		case *plansql.ColRef:
			return false
		case *plansql.ParenNode:
			e = n.Inner
		default:
			return true
		}
	}
}

// cleanExpr drops the table qualifier from a COLUMN REFERENCE, and leaves
// everything else exactly as written.
//
// The distinction is the whole of the function. Its callers hand it text that
// is usually `t.col` and sometimes an arbitrary expression, and the second
// kind has no qualifier to strip: the first dot in `concat(t0.c0, t0.c1)`
// separates a table from a column only if you already know the text is a
// column reference. A naive SplitN on '.' does not, so it returned
// `c0, t0.c1)` — a fragment of the expression, parentheses and commas
// included, which then became the OUTPUT COLUMN NAME a client binds by
// (#513).
//
// plansql.SplitIdentRef is the test, because it is the lexer: it accepts
// `col`, `t.col` and the delimited spellings (`"id.orig_h"` is ONE name, a
// flat Zeek JSON column with no qualifier — #304) and rejects anything that
// does not end after the identifier, which is every function call, operator
// expression and literal.
func cleanExpr(s string) string {
	s = strings.TrimSpace(s)
	if _, name, ok := plansql.SplitIdentRef(s); ok {
		return name
	}
	return s
}

// scanParallelism returns the worker count for scan/decode/pipeline
// parallelism, honoring WADJET_SCAN_WORKERS when set (>0). Default is
// runtime.NumCPU(), the historical behavior — but a 2026-08-17 profiling
// pass measured the fast-query tier DOUBLING its wall time from decode
// over-subscription past the memory-bandwidth knee (24 workers 87.6ms vs
// 12 workers 43.8ms on a 12-core box, every profile symbol inflating
// uniformly with zero contention symbols). The env knob exists to A/B a
// lower default on the benchmark metal before changing it for everyone.
func scanParallelism() int {
	if v := os.Getenv("WADJET_SCAN_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return runtime.NumCPU()
}

// innerPipelineWorkers returns the number of parallel workers for an inner
// pipeline (aggregate/sort child). Returns 0 (serial) unless the source is
// a concurrent-safe scan source.
func innerPipelineWorkers(src exec.Source) int {
	switch src.(type) {
	case *catalogScanSource, *scannerExecSource, *deferredJoinBridge:
		return scanParallelism()
	}
	return 0
}

// aggSourceAdapter wraps a child pipeline + hash aggregate into a Source.
type aggSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	agg         *exec.HashAggregate
	initialized bool
	// pipe is the inner pipeline this adapter runs. It is HELD, not
	// discarded: it owns the child ops and the morsel-parallel clone
	// sinks, and only its Close reaches them. Discarding it left a
	// cancelled GROUP BY's agg-spill-*.bin files on disk for the process
	// lifetime (#625 M2).
	pipe *exec.Pipeline
}

// ServesHeldState marks the adapter's output phase as a held-state drain —
// exempt from heap-backpressure pauses (exec.HeldStateSource).
func (a *aggSourceAdapter) ServesHeldState() bool { return true }

func (a *aggSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (a *aggSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !a.initialized {
		a.initialized = true
		// Run child pipeline into aggregate
		a.pipe = &exec.Pipeline{
			Source:  a.childSource,
			Ops:     a.childOps,
			Sink:    a.agg,
			Workers: innerPipelineWorkers(a.childSource),
		}
		if err := a.pipe.Run(ctx); err != nil {
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
	if a.pipe != nil {
		// Reaches the child ops and the clone sinks as well as the
		// aggregate and the source.
		return a.pipe.Close()
	}
	a.agg.Close()
	return a.childSource.Close()
}

// sortSourceAdapter wraps a child pipeline + sort into a Source.
// When sort.Limit >= 0, it truncates results to the top N rows after
// sorting (Top-K optimization: avoids materializing the full sorted
// result). The bound lives only on sort.Limit — no separate limitN field to
// keep in sync — so a real LIMIT 0 (sort.Limit == 0) truncates correctly
// instead of colliding with sort.Limit's own "no limit" sentinel (#481).
type sortSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	sort        *exec.Sort
	initialized bool
	pipe        *exec.Pipeline // held so Close reaches the child ops and clones (#625 M2)
}

// ServesHeldState marks the adapter's output phase as a held-state drain —
// exempt from heap-backpressure pauses (exec.HeldStateSource).
func (s *sortSourceAdapter) ServesHeldState() bool { return true }

func (s *sortSourceAdapter) Init(ctx context.Context) error {
	return nil
}

func (s *sortSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !s.initialized {
		s.initialized = true
		s.pipe = &exec.Pipeline{
			Source:  s.childSource,
			Ops:     s.childOps,
			Sink:    s.sort,
			Workers: innerPipelineWorkers(s.childSource),
		}
		if err := s.pipe.Run(ctx); err != nil {
			return nil, err
		}
		// Top-K truncation: discard everything beyond sort.Limit rows. >= 0,
		// not > 0 — a real LIMIT 0 must truncate to zero rows too (#481).
		if s.sort.Limit >= 0 {
			s.sort.Truncate(s.sort.Limit)
		}
	}
	return s.sort.Next(ctx)
}

func (s *sortSourceAdapter) Close() error {
	if s.pipe != nil {
		return s.pipe.Close()
	}
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
// ServesHeldState — see aggSourceAdapter (exec.HeldStateSource).
func (w *windowSourceAdapter) ServesHeldState() bool { return true }

type windowSourceAdapter struct {
	childSource exec.Source
	childOps    []exec.UnaryOperator
	win         *exec.Window
	initialized bool
	pipe        *exec.Pipeline // held so Close reaches the child ops and clones (#625 M2)
}

func (w *windowSourceAdapter) Init(_ context.Context) error { return nil }

func (w *windowSourceAdapter) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if !w.initialized {
		w.initialized = true
		w.pipe = &exec.Pipeline{
			Source: w.childSource,
			Ops:    w.childOps,
			Sink:   w.win,
		}
		if err := w.pipe.Run(ctx); err != nil {
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
	if w.pipe != nil {
		return w.pipe.Close()
	}
	w.win.Close()
	return w.childSource.Close()
}

// newScannerSource creates a scanner exec.Source from the catalog. snap may
// be nil (falls back to an ordinary catalog.GetManifest call in Init).
func newScannerSource(cat *catalog.Catalog, tableName string, partFilter map[string]string, requiredCols []string, scanPreds []logical.Predicate, snap *ManifestSnapshot) exec.Source {
	return &scannerExecSource{
		catalog:          cat,
		tableName:        tableName,
		partitionFilter:  partFilter,
		requiredCols:     requiredCols,
		scanPreds:        scanPreds,
		manifestSnapshot: snap,
	}
}

type scannerExecSource struct {
	catalog          *catalog.Catalog
	tableName        string
	partitionFilter  map[string]string
	requiredCols     []string
	scanPreds        []logical.Predicate
	allowedFiles     []string // probe-split: only scan these files (nil = all)
	scanner          *scanSourceInner
	bloomFilter      *exec.BloomScanFilter
	dynamicFilter    []exec.DynamicRange
	rowLimit         int64                // LIMIT pushdown: enables lazy file downloading (0 = eager)
	memTracker       *memory.Tracker      // per-query memory tracker; passed to scanSourceInner at Init
	spillMgr         *memory.SpillManager // for pre-emptive relief on file-load reservations; nil-safe
	emitRowLoc       bool                 // top-N late materialization: stamp __row_loc on scan batches
	rowPreds         []scan.RowPred       // scan-level filter conjuncts
	shapeOnlyCols    map[string]bool      // byte-array columns decoded as lengths only
	manifestSnapshot *ManifestSnapshot    // pins this table's manifest to one read per statement (#502); nil-safe
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
	rgUnits       []rgUnit        // flat list of row group work units
	rgIdx         int64           // atomic index for parallel RG workers
	emitRowLoc    bool            // stamp __row_loc (rgUnit ordinal, row) on every scan batch; disables batch pooling
	eqProbes      []scan.EqProbe  // "=" conjuncts for dictionary-probe row-group pruning (dict_prune.go)
	rowPreds      []scan.RowPred  // scan-level filter conjuncts, evaluated per row group in readRG
	shapeOnlyCols map[string]bool // lowercased names of columns decoded as lengths only (lengths_decode.go)
	countOnlyScan bool            // requiredCols is exactly the row-count sentinel: batches carry Len/Sel only
	useNative     bool            // true if native page decoder can be used (no Decimal/Array/Map)
	loadGate      *loadGate       // byte-budgeted admission for in-flight file LOADs (data, not metadata)

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

	// fatalScanErr is a failure the scan must NOT tolerate, however many
	// other files succeeded. failedFiles is deliberately forgiving — it only
	// fails the scan when EVERY file failed, because a since-deleted object
	// is a survivable degradation. A recovered panic is not in that class: a
	// footer decoder that panicked has no idea how many row groups it should
	// have produced, so tolerating it drops that file's rows and answers a
	// wrong number (#511).
	fatalScanErr error

	// pooledBufs tracks []byte buffers obtained from readBufPool during
	// buildRGUnits. These are returned to the pool when the scan source
	// is closed, enabling cross-query buffer reuse.
	pooledBufsMu sync.Mutex
	pooledBufs   [][]byte

	// memTracker accounts for pooled buffers (parquet file []byte loads).
	// nil-safe: when nil, tracking is a no-op. Wired by the planner at
	// scan-source construction when a per-query spill manager is available.
	memTracker *memory.Tracker

	// trackedBufBytes is the cumulative bytes currently reported to memTracker
	// from pooledBufs. Released atomically in releasePooledBufs to avoid
	// double-release on idempotent close.
	trackedBufBytes atomic.Int64

	// spillMgr lets file-load reservations request operator relief before
	// waiting on the budget (memory.ReserveOrForce). nil-safe.
	spillMgr *memory.SpillManager

	// residentSlabs counts the row-group buffers this scan source is holding
	// across every file (scan_rowgroup_load.go). It is the deadlock-freedom
	// floor: a loader with nothing resident admits its next row group without
	// waiting, because there is nothing decoding that could free room for it.
	residentSlabs atomic.Int64

	// batchCharges maps decoded batches currently held by the scan source
	// (decode in progress, prefetched, or queued in batchCh) to the bytes
	// charged against memTracker when they were decoded. Released when the
	// batch leaves through next(), is dropped on a filter path, or is
	// drained at Close. LoadAndDelete makes every release idempotent.
	batchCharges sync.Map
}

// trackScanBatch charges a freshly decoded batch's footprint to the memory
// tracker until the batch leaves the scan source. No-op without a tracker.
func (inner *scanSourceInner) trackScanBatch(b *batch.RecordBatch) {
	if inner.memTracker == nil || b == nil {
		return
	}
	n := b.MemBytes()
	if n <= 0 {
		return
	}
	inner.batchCharges.Store(b, n)
	inner.memTracker.ForceReserveFor(n, memory.ForceScanDecodedBatch)
}

// releaseScanBatch releases the charge recorded by trackScanBatch.
// Idempotent: a second release for the same batch is a no-op.
func (inner *scanSourceInner) releaseScanBatch(b *batch.RecordBatch) {
	if inner.memTracker == nil || b == nil {
		return
	}
	if n, ok := inner.batchCharges.LoadAndDelete(b); ok {
		inner.memTracker.ReleaseForced(n.(int64), memory.ForceScanDecodedBatch)
	}
}

// drainSlotCharges releases the lazy fileSlot state of every slot whose row
// groups were not fully consumed — buffers, load-gate bytes and shared-tracker
// charges abandoned by an early Close (LIMIT, cancel, error). Must run after
// wg.Wait (no rg worker may still be loading) and BEFORE releasePooledBufs,
// which nils rgUnits (the only reference to the slots).
func (inner *scanSourceInner) drainSlotCharges() {
	seen := make(map[*fileSlot]bool)
	for _, u := range inner.rgUnits {
		if u.slot == nil || seen[u.slot] {
			continue
		}
		seen[u.slot] = true
		if u.slot.rgRemaining.Load() > 0 {
			u.slot.drainAbandoned(inner)
		}
	}
}

// drainBatchCharges releases every outstanding decoded-batch charge —
// batches stranded in batchCh by cancellation or never sent by an exiting
// worker. Callers must ensure the rg/scan workers have exited first (the
// charges live on a shared worker-level tracker; a racing Store here would
// leak its bytes for the worker's lifetime).
func (inner *scanSourceInner) drainBatchCharges() {
	if inner.memTracker == nil {
		return
	}
	inner.batchCharges.Range(func(k, _ any) bool {
		if n, ok := inner.batchCharges.LoadAndDelete(k); ok {
			inner.memTracker.Release(n.(int64))
		}
		return true
	})
}

// trackPooledBuf records a buffer obtained from readBufPool so it can be
// returned when the scan source is closed. Thread-safe for parallel readers.
func (inner *scanSourceInner) trackPooledBuf(buf []byte) {
	inner.pooledBufsMu.Lock()
	inner.pooledBufs = append(inner.pooledBufs, buf)
	inner.pooledBufsMu.Unlock()

	if inner.memTracker != nil {
		n := int64(cap(buf))
		inner.memTracker.ForceReserveFor(n, memory.ForceScanPooledBuffer)
		inner.trackedBufBytes.Add(n)
	}
}

// releasePooledBufs returns all tracked buffers to readBufPool.
// Safe to call multiple times — subsequent calls are no-ops.
func (inner *scanSourceInner) releasePooledBufs() {
	inner.pooledBufsMu.Lock()
	bufs := inner.pooledBufs
	inner.pooledBufs = nil
	inner.pooledBufsMu.Unlock()

	if inner.memTracker != nil {
		released := inner.trackedBufBytes.Swap(0)
		if released > 0 {
			inner.memTracker.ReleaseForced(released, memory.ForceScanPooledBuffer)
		}
	}

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
	manifest, err := getManifestWith(ctx, s.manifestSnapshot, s.catalog, s.tableName)
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
	// Convert logical predicates to scan predicates for row-group pruning.
	//
	// The literal is in the ENGINE's domain and the row group's statistics
	// and dictionary are in the FILE's, and for several types those are not
	// the same thing: a DATE is a day number against a text literal, a
	// DECIMAL's bounds are the unscaled integer against a float, an IPV6's
	// are the raw sixteen bytes against an address in text. The prune layer
	// compares two `any` values by their Go kind and cannot tell — so the
	// conversion happens HERE, the one place that still holds the column's
	// type and scale, and a predicate with no conversion is WITHHELD rather
	// than pushed down raw (#442, #438). kernel.StatsDomainValue is the same
	// conversion the filter kernel applies to the literal, so the prune and
	// the filter cannot disagree about what the predicate means.
	var sp []scanPredicate
	var eqProbes []scan.EqProbe
	for _, pred := range s.scanPreds {
		if pred.Column == "" || pred.Op == "" || pred.Value == nil {
			continue
		}
		// The predicate's column arrives as a REFERENCE — an unquoted
		// identifier folds to lower case at the lexer (#731) — while the
		// schema keeps the spelling the parquet file gave it, and CamelCase
		// column names are ordinary there (ClickBench's `hits` has
		// `EventDate`, `UserAgent`, `ResolutionWidth`). A byte-exact lookup
		// missed every column of every such table, so neither the row-group
		// statistics prune nor the dictionary probe was ever built for it:
		// the answer stayed right and the whole table was read. Resolve the
		// way the engine resolves every other reference, and carry the
		// SCHEMA's spelling forward — that name keys the row group's
		// per-column statistics (`scan.CanPruneRowGroup`) and matches the
		// file's own leaves (`scan.CanDictPruneRowGroup`), neither of which
		// has ever seen the folded spelling.
		ci := batch.ResolveSchemaIndex(tableMeta.Schema.Columns, pred.Column)
		if ci < 0 {
			continue
		}
		col := tableMeta.Schema.Columns[ci]
		// A DECIMAL bound is converted from the literal's TEXT: the float64
		// box has already dropped the digits past a double, and a bound that
		// is off by a fraction of the last place prunes the row group the
		// answer is in (#452).
		lit := pred.Value
		if col.Type == parquet.TypeDecimal && pred.ValueText != "" {
			lit = pred.ValueText
		}
		val, ok := kernel.StatsDomainValue(col.Type, int(col.Scale), lit)
		if !ok {
			continue
		}
		sp = append(sp, scanPredicate{Column: col.Name, Op: pred.Op, Value: val})
		// Equality conjuncts also feed the dictionary probe — the
		// precise prune where zonemaps are blind (point filters on
		// high-cardinality columns). Dictionary entries are raw file
		// values too, so they take the same converted literal.
		if pred.Op == "=" && scan.DictPrune.On() {
			// A DECIMAL probe's carrier is at the CATALOG scale; the file's
			// dictionary is at the file's own scale. Carry the catalog
			// declaration so the probe layer can reconcile the two before
			// comparing (dictProbeDecimalAbsent), the dictionary twin of the
			// stats-path reconcile (#707/#916). Non-DECIMAL probes leave these
			// zero and the probe layer never reads them.
			ep := scan.EqProbe{ColName: col.Name, Value: val}
			if col.Type == parquet.TypeDecimal {
				ep.Scale, ep.Precision = int(col.Scale), int(col.Precision)
			}
			eqProbes = append(eqProbes, ep)
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
	batchChSize := scanParallelism()
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
		memTracker:    s.memTracker,
		spillMgr:      s.spillMgr,
		emitRowLoc:    s.emitRowLoc,
		eqProbes:      eqProbes,
		rowPreds:      s.rowPreds,
		shapeOnlyCols: s.shapeOnlyCols,
		countOnlyScan: len(s.requiredCols) == 1 && s.requiredCols[0] == logical.RowCountOnlyColumn,
	}
	// Nested (ARRAY/MAP/ROW) schemas must take the file-level scan whose
	// readBatchDirect falls back to the row-based reader. This MUST be
	// decided HERE: the eager branch only learned about nested types
	// inside buildRGUnits, which runs after the branch was already taken —
	// the early return left zero rgUnits and every query against a nested
	// table returned 0 rows with no error (issue #144 suite finding).
	// Decided on the columns this scan READS, matching readBatchDirect's own
	// test: one ARRAY/ROW/MAP column in a table used to put every query on
	// that table onto the row reader, which mints unpooled batches and reads
	// every column of every row group (#393).
	innerSchema := parquet.Schema{Columns: buildReadSchema(inner.schema, inner.requiredCols)}
	inner.hasNestedTypes = innerSchema.HasNestedColumns()
	s.scanner = inner

	// Row-loc stamping needs the row-group-parallel path: rgUnit ordinals
	// are the row identity. The planner's rewrite only engages on shapes
	// that take the eager branch; this is the belt-and-braces check.
	if inner.emitRowLoc && (inner.rowLimit > 0 || inner.hasNestedTypes) {
		cancel()
		return fmt.Errorf("scan %s: row-loc emission requires the row-group-parallel scan path", s.tableName)
	}
	// Same for pushed scan filters: the lazy/nested path never evaluates
	// them, and a silently dropped filter is wrong results. The planner
	// gates both conditions; fail loudly if they ever meet anyway.
	if len(inner.rowPreds) > 0 && (inner.rowLimit > 0 || inner.hasNestedTypes) {
		cancel()
		return fmt.Errorf("scan %s: pushed scan filters require the row-group-parallel scan path", s.tableName)
	}

	if inner.rowLimit > 0 || inner.hasNestedTypes {
		// Lazy file-level scan: download files on-demand, one at a time per worker.
		// Used for LIMIT pushdown (avoids downloading all files upfront) and
		// nested types (which need row-level reading).
		// Workers stop when context is cancelled (pipeline cancels after LIMIT satisfied).
		workers := scanParallelism()
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

		// A panic on a footer reader is fatal to the scan regardless of how
		// many other files parsed, because the rows it would have
		// contributed are simply missing from the answer.
		if inner.fatalScanErr != nil {
			cancel()
			return fmt.Errorf("scan %s: %w", s.tableName, inner.fatalScanErr)
		}

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

		// Initialize batch pool from the LARGEST row group: GetForSize
		// falls back to a fresh unpooled allocation for any request above
		// the pool's batch size, so sizing from rgUnits[0] meant every
		// row group bigger than the first bypassed the pool entirely —
		// full vector allocation + zeroing per row group (13% of the
		// 100-part floor probe's makeslice profile).
		if len(inner.rgUnits) > 0 {
			rgSize := 0
			for _, u := range inner.rgUnits {
				if int(u.numRows) > rgSize {
					rgSize = int(u.numRows)
				}
			}
			readSchema := inner.readSchema()
			// Row-loc stamping appends a column after decode, which would
			// poison the fixed-schema pool on release — skip pooling (the
			// narrow late-mat scan allocates little anyway).
			if rgSize > 0 && len(readSchema) > 0 && !inner.emitRowLoc {
				inner.pool = batch.NewBatchPool(readSchema, rgSize)
				inner.pool.PreWarm(runtime.NumCPU())
			}
			// Pre-compute whether native page decoding can be used.
			inner.useNative = !scan.HasUnsupportedColumnarTypes(readSchema)
		}

		// Byte-aware decode parallelism: CPU-count workers with a CPU-count
		// queue are blind to batch WIDTH. On a 105-column SELECT * scan each
		// decoded row-group batch is hundreds of MB; 16 decoders + 16 queued
		// batches held multiple GB of live wide batches (plus the GC target
		// doubling that live set), which OOM-killed the c6a on ClickBench
		// Q24 even with the sort side bounded. Clamp workers + queue so
		// estimated in-flight decoded bytes stay within a budget slice;
		// narrow scans (TPC-H) still get full CPU-count parallelism.
		workers := scanParallelism()
		if len(inner.rgUnits) > 0 {
			rgRows := int(inner.rgUnits[0].numRows)
			perBatch := estimateDecodedBatchBytes(inner.readSchema(), rgRows)
			inflightCap := int64(8 << 30)
			if inner.memTracker != nil {
				if b := inner.memTracker.Budget(); b > 0 && b/3 < inflightCap {
					inflightCap = b / 3
				}
			}
			if perBatch > 0 {
				maxInflight := int(inflightCap / perBatch)
				if maxInflight < 2 {
					maxInflight = 2
				}
				if workers > maxInflight-1 {
					workers = maxInflight - 1
				}
				queue := maxInflight - workers
				if queue < 1 {
					queue = 1
				}
				if queue < cap(inner.batchCh) {
					inner.batchCh = make(chan *batch.RecordBatch, queue)
				}
			}
		}
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
		defer inner.recoverWorkerPanic(ctx, "scan batch-channel closer")
		inner.wg.Wait()
		close(inner.batchCh)
	}()

	return nil
}

// estimateDecodedBatchBytes estimates the decoded in-memory size of one
// row-group batch for the projected schema. Fixed types use their storage
// width; variable-length types assume 48 B/row — a deliberate overestimate
// for short strings (safe direction: it only reduces decode parallelism).
func estimateDecodedBatchBytes(schema []parquet.Column, rows int) int64 {
	if rows <= 0 {
		return 0
	}
	perRow := 0
	for _, c := range schema {
		switch c.Type {
		case parquet.TypeBool:
			perRow += 1
		case parquet.TypeInt32, parquet.TypeDate, parquet.TypePort, parquet.TypeProtocol, parquet.TypeFloat32:
			perRow += 4
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeFloat64, parquet.TypeIPv4,
			parquet.TypeMAC, parquet.TypeDuration:
			perRow += 8
		case parquet.TypeDecimal, parquet.TypeUUID, parquet.TypeIPv6:
			perRow += 16
		default: // strings/bytes/nested
			perRow += 48
		}
	}
	return int64(perRow) * int64(rows)
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
		// Wait for the scan workers to exit before draining: a worker racing
		// drainBatchCharges could charge a batch after the drain, leaking the
		// bytes on the shared worker-level tracker for the worker's lifetime.
		// Workers observe the cancel at the loop head and in every blocking
		// select, so this wait is bounded by one in-flight row-group decode.
		s.scanner.wg.Wait()
		s.scanner.drainBatchCharges()
		s.scanner.drainSlotCharges()
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

// recoverWorkerPanic converts a panic raised on a scan goroutine into the
// scan's error instead of letting it take the process down.
//
// These goroutines are not the caller's: Pipeline.Run recovers on ITS
// goroutine, so a *batch.TypeMismatchError raised by Vector.SetValue — whose
// whole design (#361) is "a query error, never the server" — killed the
// process here, and with it every other client's query (#400, and #393 as the
// query that reaches it). Since #511 it converts ANY panic, not only the
// FatalEvalPanic class: a decoder bug on a scan worker is still one query's
// failure, not the server's.
//
// errCh is buffered, and next() selects on it, so a non-blocking send is
// enough; the cancel stops the sibling workers.
func (inner *scanSourceInner) recoverWorkerPanic(ctx context.Context, what string) {
	r := recover()
	if r == nil {
		return
	}
	err := exec.RecoverQueryPanic(ctx, what, r)
	select {
	case inner.errCh <- fmt.Errorf("%s: %w", what, err):
	default:
	}
	if inner.cancel != nil {
		inner.cancel()
	}
}

// scanWorker reads files in parallel, writing decoded batches to batchCh.
func (inner *scanSourceInner) scanWorker(ctx context.Context) {
	defer inner.wg.Done()
	defer inner.recoverWorkerPanic(ctx, "scan worker")

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
			reader, err = parquet.NewReaderFromBytesCached(data,
				footerCacheIdentity(inner.cat, file, int64(len(data))))
			if err != nil {
				continue
			}
		}

		b, err := readBatchDirect(reader, inner.schema, inner.requiredCols, inner.scanPreds...)
		if err != nil {
			// Surface the first decode error so the scan FAILS instead of
			// dropping this file's rows: a swallowed error here is
			// indistinguishable from a file that legitimately contributed
			// nothing (the same silent-partial class readRG guards).
			select {
			case inner.errCh <- fmt.Errorf("reading %s: %w", file.Path, err):
			default:
			}
			return
		}
		if b == nil || b.Len == 0 {
			continue
		}
		inner.trackScanBatch(b)

		// Apply delete markers: skip rows marked for deletion
		if delSet := inner.deleteMarkers[file.Path]; len(delSet) > 0 {
			sel := make([]uint32, 0, b.Len)
			for i := 0; i < b.Len; i++ {
				if !delSet[int64(i)] {
					sel = append(sel, uint32(i))
				}
			}
			if len(sel) == 0 {
				inner.releaseScanBatch(b)
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
		inner.cachedReadSchema = buildReadSchema(inner.schema, inner.requiredCols)
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
		// The batch leaves the scan source here — downstream operators that
		// retain it account for it themselves (TrackBatch et al).
		inner.releaseScanBatch(b)
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

// wrapPredicate adapts an expr.Expr into an exec.Predicate function. The
// typed protocol and its two-valued collapse are chosen once, in
// expr.FilterPredicate, so the row loop neither boxes nor re-dispatches.
func wrapPredicate(e expr.Expr) exec.Predicate {
	return expr.FilterPredicate(e)
}

// expandStarProjections runs logical star expansion on a plan that reached the
// physical planner without it — logical.Optimize expands stars before column
// pruning, so this only fires for plans built and planned without optimizing.
// The rewrite reads the scan's annotated schema, so annotate first; that costs
// a catalog walk, which is why it is gated on a star actually being present.
func (p *Planner) expandStarProjections(ctx context.Context, node, child *logical.Node) {
	if p.catalog == nil || !logical.HasStarProjection(node) {
		return
	}
	p.AnnotateScanColumns(ctx, child)
	logical.ExpandStarProjections(node)
	logical.ResolveOrdinalSortKeys(node)
}

// limitPushdownSafe reports whether a LIMIT may be applied independently by
// each task under it.
//
// It may when every node between the LIMIT and its scans passes rows through
// one at a time: Project and Filter qualify (a filtered task simply reaches n
// later, or never), and a scan is the base case. Anything that derives rows
// from more than one input row — join, aggregate, distinct, sort, window, set
// operation — does not: bounding its INPUT changes its OUTPUT, which would
// silently produce wrong answers rather than merely fewer rows.
//
// Multiple scans under a UNION ALL are fine: each bounds itself, and the
// coordinator trims the union to n.
func limitPushdownSafe(node *logical.Node) bool {
	if node == nil {
		return false
	}
	sawScan := false
	var walk func(n *logical.Node) bool
	walk = func(n *logical.Node) bool {
		if n == nil {
			return false
		}
		switch n.Type {
		case logical.NodeScan:
			// A table function's row count is not bounded by its input, but
			// stopping early still yields a prefix of what it would produce.
			sawScan = true
			return true
		case logical.NodeProject, logical.NodeFilter, logical.NodeLimit:
			// A nested LIMIT is at most as permissive as this one.
		default:
			return false
		}
		for _, c := range n.Children {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	if !walk(node) {
		return false
	}
	return sawScan
}
