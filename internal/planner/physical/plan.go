// Package physical converts logical plans to physical execution plans.
package physical

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// PhysicalPlan represents an executable query plan.
type PhysicalPlan struct {
	Pipeline *exec.Pipeline
	Stages   []Stage // for distributed execution
	Cleanup  func()  // optional: called after pipeline finishes to clean up spill files
	// OutputSchema is the PLAN-DERIVED output schema: the SELECT list's
	// column names with the types the catalog says they carry. It answers
	// the question a zero-row result leaves open, since every other source
	// of a result schema in this engine reads it off a batch that never
	// arrived (#416, declaredOutputSchema). Advisory: a consumed batch
	// always wins.
	OutputSchema []parquet.Column
}

// Stage represents a unit of distributed work with metadata for task creation.
type Stage struct {
	ID           string
	Type         string // see exchange.go constants (scan, aggregate, sort, hash_join, broadcast_join, window, pipeline, exchange-repartition, exchange-replicate, exchange-gather)
	ClusterID    string // target cluster for routing ("" = local/coordinator's cluster)
	Dependencies []string
	Tasks        int

	// Scan metadata
	TableName string
	ScanAlias string // unique scan identity: "table" or "table:N" for Nth duplicate
	Columns   []string
	// ScanSchema is the CATALOG's declared schema for TableName — the whole
	// table's columns, with the parameters a bare TypeID does not carry
	// (DECIMAL precision/scale, VECTOR dimension).
	//
	// The DAG worker's scan otherwise takes its column TYPES from the FILE,
	// and a parquet file cannot express nine of this engine's types. Files
	// written from v0.18.0 on stamp their declared types into their own
	// footer; files written before it do not, and on those the DAG answered
	// an IPv4 as 167772165 where the single-process engine (which reads the
	// catalog) answered 10.0.0.5 — #396's symptom, on existing data (#423).
	//
	// Declared at PLAN time rather than looked up by the worker for the same
	// reason as AggSpec.OutputType and the join-side schemas: one catalog
	// read, one revision, one answer for every task of the query. A worker
	// resolving it itself could see a different revision from its peers, and
	// two tasks of one stage would type the same column differently.
	//
	// The WHOLE table, not the read set: the worker's projection guard
	// reverts to full width whenever a requested name is missing from the
	// file, and a column read under that fallback still has to be typed.
	ScanSchema []parquet.Column
	// ScanDeletes is the table's merge-on-read DELETE state at plan time:
	// data-file path → the file-absolute row indices a DELETE removed and
	// compaction has not yet folded in (catalog.PartitionManifest.
	// DeleteMarkers). Read from the SAME manifest that produced ScanFiles,
	// which is not an accident: markers only ever grow until a compaction
	// replaces the file they name, so a marker set read AFTER the file list
	// can be missing the markers of a file the list still holds, and those
	// deleted rows come back. Reading both from one manifest object makes
	// the pair a snapshot.
	//
	// Rides to the worker as Task.DeleteMarkers, stamped per task from the
	// files that task actually reads. Declared at plan time for the reason
	// ScanSchema is: one catalog revision for every task of the query, so
	// two tasks of one stage cannot disagree about which rows exist (#491).
	// Nil for every table with no deletes.
	ScanDeletes map[string][]int64
	// OutputColumns, when non-empty, narrows the stage's EMITTED columns
	// to this set (worker inserts a zero-copy ColumnPrune before the
	// sink). Columns stays the READ set — a scan must read its pushed
	// filter columns but must not ship them: Q13's orders scan read
	// o_comment for the NOT LIKE and then materialized+shuffled it,
	// 68.8 B/row where 16 were consumed (~16 GB excess at SF100).
	// Set by pruneScanOutputColumns from consumer declarations.
	OutputColumns   []string
	PartitionFilter map[string]string
	ScanFiles       []string // files to distribute across scan tasks
	// ScanFileSizes aligns 1:1 with ScanFiles (catalog SizeBytes). Feeds
	// byte-balanced affinity fan-outs (coordinator scan_affinity.go);
	// empty/misaligned degrades to count-based splitting.
	ScanFileSizes []int64
	FilterExprs   []string // SQL filter expressions pushed down to scan

	// Aggregate metadata
	GroupByCols []string
	// GroupByTypes is the plan-time output type of each DERIVED (non-bare)
	// GROUP BY key expression, keyed by the exact GroupByCols text — the
	// same inferProjectionTypeCols answer the single-process pre-aggregate
	// projection types its synthetic key columns with. Dispatch ships it as
	// OpSpec.GroupByTypes so the worker's buildAggInputProjection declares
	// the same vector type instead of inferring from the expression text
	// with no catalog (#379: COALESCE(l_extendedprice, 0) inferred Int64
	// from the literal and truncated every float group key). Bare column
	// keys are absent — their vectors come from the input schema.
	GroupByTypes map[string]parquet.TypeID
	AggSpecs     []AggSpec
	// GroupByAll marks a keys-only hash aggregate over EVERY input column —
	// the DISTINCT shape. The key set is resolved at runtime from the input
	// schema (no plan-time column list), matching exec.HashAggregate.GroupByAll
	// and the single-process buildDistinct path.
	GroupByAll bool

	// Sort metadata
	SortKeys []SortKeySpec
	// Limit is the row bound this stage's Sort/TopN carries — meaningful
	// only when HasLimit is true. A companion bool rather than a -1
	// sentinel on Limit itself: Stage is built via dozens of `Stage{...}`
	// literals across the planner/coordinator that never touch Limit, and
	// every one of them must keep meaning "unbounded" by leaving both
	// fields at their zero value. Before HasLimit existed, Limit's own 0
	// doubled as that same "unbounded" sentinel, so a stage carrying a
	// real `ORDER BY ... LIMIT 0` was indistinguishable from one with no
	// limit at all (#481) — every reader of Limit below must consult
	// HasLimit, never `Limit > 0`/`Limit == 0` alone.
	// A StageLimit carries its LIMIT here too — same meaning, same
	// HasLimit guard — paired with Offset below.
	Limit    int
	HasLimit bool
	// Offset is the rows a StageLimit SKIPS before it starts emitting.
	// Meaningful only on that stage type: everywhere else the OFFSET is
	// applied once by the coordinator over the gathered result, and a
	// stage that skipped rows on its own would skip them twice. No
	// companion bool — 0 rows skipped and no OFFSET are the same thing,
	// unlike LIMIT, where 0 rows kept and no LIMIT are opposites.
	Offset int

	// RowLimit bounds how many rows this stage's tasks EMIT, for a LIMIT with
	// no ORDER BY. Distinct from Limit, which is a top-N applied after a sort:
	// this one lets a scan stop pulling batches once satisfied.
	//
	// Set only when nothing between the scan and the LIMIT can change
	// cardinality (no join, aggregate, distinct or sort), so each task may
	// stop at n independently. k tasks then emit up to k*n rows and the
	// coordinator's gather limit trims to n — which is well-defined precisely
	// because a bare LIMIT does not specify WHICH rows it returns. An
	// `ORDER BY ... LIMIT` must never use this path; it goes through the
	// sort/TopN stages, where Limit above applies.
	RowLimit int
	// SortShardLocal marks a grouped final_aggregate whose SortKeys/Limit
	// are SHARD-LOCAL: the stage fans out across disjoint group-key shards
	// (each computes exact aggregates for its groups, then sorts and
	// applies Limit locally) and a surviving downstream Singleton sort
	// stage merges the N sorted ≤Limit-row outputs. Distribution rules
	// treat such a stage like a sort-free grouped final (input clustered
	// on GroupByCols, output mirrors the input partitioning) — without
	// this flag SortKeys/Limit force the Singleton collapse. Set only by
	// fuseSortIntoPredecessor's shard-local fold.
	SortShardLocal bool

	// Join metadata
	JoinType        string // inner, left, right, full, cross
	JoinLeftKeys    []string
	JoinRightKeys   []string
	LeftDepStage    string // stage providing probe (left) side
	RightDepStage   string // stage providing build (right) side
	BuildTableAlias string // build-side table alias for column disambiguation in self-joins
	// BuildColOrigins maps each bare build-output column (lowercased) to the
	// scan alias that owns it. Only set when the build subtree spans multiple
	// tables (bushy shapes) — nil for single-scan builds, where
	// BuildTableAlias is already exact. The join executor qualifies duplicate
	// build columns with the OWNING alias instead of BuildTableAlias.
	BuildColOrigins map[string]string
	JoinFilter      string // semi/anti join inequality filter (e.g., "l2.l_suppkey != l1.l_suppkey")
	// NullAwareAnti carries logical.Node.NullAwareAnti to the worker: this
	// anti join came from a NOT IN and owes its three-valued rule, not the
	// two-valued "did nothing match" an anti join asks on its own (#507).
	NullAwareAnti bool
	// BuildFilterExprs are row predicates applied to the BUILD input before
	// hash-table insertion. Set by dedupeSubsumedScanExchanges when this
	// join's build was rewired from a filtered exchange to a subsuming raw
	// exchange: the dropped exchange's scan filter (or its computed
	// __subsume flag) must now run at build-read time. Semantically
	// identical to filtering at the dropped scan.
	BuildFilterExprs []string

	// JoinProbeSchema / JoinBuildSchema are the plan-declared columns of each
	// join side (physical.declaredJoinSchema). The worker reads them only for
	// the side that turns out to be empty, where there is no batch to learn a
	// schema from and an outer join still owes the rows that side shapes
	// (#348/#352).
	JoinProbeSchema []parquet.Column
	JoinBuildSchema []parquet.Column

	// Fused broadcast joins absorbed into this stage (avoids separate
	// shuffle+join stages for small dimension tables like nation, region).
	FusedJoins []FusedJoinSpec

	// ChainedJoins are 1:1 downstream joins absorbed into this stage by
	// fuseStageChains (docs/design/stage-chain-fusion.md). Unlike
	// FusedJoins (broadcast probes applied BEFORE the primary join), these
	// run AFTER it, in order — the fragment pipes the primary's output
	// through each chained probe in-process, eliding the per-link
	// materialization the separate stages paid.
	ChainedJoins []ChainedJoinSpec

	// ChainedAgg* describe a downstream PARTIAL aggregate absorbed as the
	// chain's terminal step (fuseStageChains step 2): the fragment runs
	// OpHashAggregate (raw mode) after the chained joins, so the join
	// output collapses to partials in-process instead of materializing
	// for a separate round-robin aggregate stage. Partial aggregation is
	// partition-agnostic, so the fused stage keeps ITS OWN distribution
	// and task count — it just emits N partial outputs instead of the
	// dropped stage's fan-out count; finals merge either way.
	ChainedAggGroupBy []string
	ChainedAggSpecs   []AggSpec

	// UnionArms describes each arm of a StageUnion, in SQL order. Arm i is
	// produced by Dependencies[i] and dispatched as task i; Projections is
	// the OpProject that normalizes that arm's output onto the result
	// column names. Empty on every other stage type.
	UnionArms []UnionArm

	// Window metadata
	WindowCols []WindowColSpec

	// JoinPartitionCount is the number of partitions for a hash-join stage
	// that was preceded by repartition exchanges. Zero means the join is
	// not partitioned (broadcast or single-partition). Exchange stages carry
	// their partition count on Exchange.Count instead.
	JoinPartitionCount int

	// Fused scan-aggregate: partial aggregation is performed at the scan
	// level, eliminating the scan→aggregate S3 round-trip. Workers produce
	// partial aggregate results instead of raw rows.
	FusedAggGroupBy []string
	FusedAggSpecs   []AggSpec

	// RawInputAggregate marks a final_aggregate whose input is RAW rows
	// from an exchange hash-partitioned on the group keys, not partial
	// aggregates — set by rewireAggOverRawExchange when it rewires the
	// final from a duplicate fused scan-agg leg onto a sibling raw
	// exchange, and by emitSetOpCountingStage (whose input is the raw
	// tagged concatenation). Partition-disjoint keys make per-partition
	// raw aggregation exact, so the dispatcher builds the fragment with
	// MergeMode=false (no InputCol→OutputCol remap, no COUNT→SUM
	// rewrite). AggSpecs carry the raw form (the dropped scan's
	// FusedAggSpecs).
	RawInputAggregate bool

	// SetOp marks a final_aggregate that computes an INTERSECT or EXCEPT
	// (#346): the stage GROUP BYs the full result row and SUMs the two
	// per-arm tag columns (SetOpLeftCountCol / SetOpRightCountCol), and
	// its fragment appends an emit operator that turns each distinct
	// row's (countA, countB) into the operation's answer — one copy when
	// the distinct form's membership rule holds, min(countA, countB) /
	// max(0, countA−countB) copies for the ALL forms — and drops the tag
	// columns. Values: "intersect", "except"; "" on every other stage.
	SetOp string
	// SetOpAll distinguishes the multiset (ALL) form. Meaningful only
	// when SetOp is set.
	SetOpAll bool

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

	// PreComputedAggregates holds signatures + cache paths for derived-
	// aggregate builds that were computed once by the coordinator before
	// dispatch. Each probe-split task carries the same list; the worker's
	// plan-rewrite pass matches logical Aggregate subtrees against the
	// signatures and replaces them with synthetic scans of the cache files.
	// Spec: 2026-04-18-shuffle-distributed-aggregate.md.
	PreComputedAggregates []PreComputedAggregateMeta

	// Multi-level merge: partitions upstream results among parallel merge groups.
	// When MergeGroupCount > 0, this stage processes only the MergeGroup-th
	// fraction of its dependency results. Independent merge groups run on
	// different workers for parallel merging.
	MergeGroup      int // 0-based index of this merge group
	MergeGroupCount int // total groups (0 = not grouped, process all results)

	// Cost estimation (populated at plan time from manifest metadata)
	EstimatedBytes int64
	EstimatedRows  int64

	// Distribution describes how this stage's output is partitioned.
	// Default zero value is {Kind: DistSingleton} which is correct for
	// most existing stages (single-worker output). Shuffle stages set this
	// to DistHashPartitioned with Keys and Count populated. Broadcast pre-scans
	// (build cache) set Kind: DistBroadcast.
	Distribution Distribution

	// Exchange carries per-variant metadata for StageExchange* stages.
	// nil for non-Exchange stages.
	Exchange *ExchangeStage

	// ScalarDependencies maps placeholder names (e.g. ":scalar_1") to
	// producer stage IDs that emit a single-row, single-column output. The
	// native-DAG coordinator awaits each producer, extracts the scalar from
	// its stage output, and string-substitutes the placeholder in this
	// stage's FilterExprs / AggSpecs.InputExpr before dispatching tasks.
	// This lets CTE-referencing scalar subqueries share the distributed
	// float-accumulation path with the filter-carrying stage's upstream,
	// eliminating the single-process vs distributed bit-pattern divergence
	// that caused Q15 to return 0 rows at SF0.1.
	ScalarDependencies map[string]string

	// OutputRenames is the SELECT-list alias map applied by the coordinator
	// to the Gather stage's result schema. walkStages currently passes
	// NodeProject through without applying its projections, so without this
	// the final result schema carries raw worker column names ("n1.n_name",
	// "substr(l_shipdate, 1, 4)") instead of the user's aliases
	// ("supp_nation", "l_year"). Only populated on the Gather stage.
	OutputRenames []OutputRename

	// OutputSchema is the PLAN-DERIVED result schema — the same column list
	// OutputRenames names, with the types the catalog says they carry. Only
	// populated on the Gather stage, and read only when the gathered batches
	// cannot answer: a zero-row result (#416, declaredOutputSchema).
	OutputSchema []parquet.Column

	// OutputWireUnconstrainedDecimal names the DECIMAL columns in
	// OutputSchema whose PostgreSQL wire typmod must say "unconstrained"
	// (-1) even though OutputSchema itself (and the executed result, when
	// there is one) carries their real (p,s) — an aggregate function call
	// never keeps its argument's typmod on live PostgreSQL. Only populated
	// on the Gather stage, and unlike OutputSchema's zero-row-only role,
	// consulted for every result (FIX 2, #457/#458 fold-in; see
	// declaredWireUnconstrainedDecimal).
	OutputWireUnconstrainedDecimal map[string]bool

	// ProjectExprs, set on a leaf scan stage whose output feeds the gather
	// directly, makes the scan fragment compute the SELECT list (worker-side
	// exec.Project after scan+filter). Without it a bare expression SELECT
	// over a scan reaches the gather as raw scan columns — the gather's
	// applyOutputRenames can rename/drop but not evaluate (#169). Expression
	// entries are named by the lowercased expression text, matching the
	// convention extractOutputRenames already expects for worker-computed
	// expressions; bare columns are passthrough entries so the fragment
	// output is exactly the SELECT-list inputs.
	ProjectExprs []ProjectExprSpec

	// SecurityProjectExprs is the ABAC security barrier absorbed from a
	// SecurityBarrier logical Project wrapping this scan
	// (absorbSecurityBarrier): visible columns pass through, masked columns
	// are literal expressions, denied columns are absent. Applied as the
	// FIRST projection in the scan fragment — before ProjectExprs and
	// before any aggregate — so restricted values never leave the worker.
	SecurityProjectExprs []ProjectExprSpec

	// Dynamic filter (Trino-style semi-join pushdown) annotations.
	// EmitDynamicFilters is set on a build-side leaf scan stage; each task
	// computes a partial KeyRange+Bloom and uploads as a sideband artifact.
	// ConsumeDynamicFilters is set on a probe-side leaf scan stage; the
	// coordinator unions the upstream partials and injects the result into
	// each scan task's OpSpec.DynamicFilters.
	//
	// The stat-dep edge (emit stage ID appended to the consume stage's
	// Dependencies) makes execute_stage_dag.go serialize them via the
	// existing dependency mechanism — no new edge type or async broker.
	EmitDynamicFilters    []DynamicFilterEmit
	ConsumeDynamicFilters []DynamicFilterConsume

	// QualifyAllBuildCols, when true, instructs the join executor to always
	// emit build-side columns under their qualified name
	// ("BuildTableAlias.col_name") instead of the default behavior of
	// qualifying only on probe-collision. Set by the planner when the same
	// source table is scanned more than once and the scans co-path into the
	// same join chain (Q07's "nation n1" + "nation n2"). Without this flag
	// the FIRST self-join leaves its column unqualified, the SECOND qualifies
	// only its own copy, and references to the FIRST alias resolve to NULL
	// downstream.
	QualifyAllBuildCols bool
}

// OutputRename pairs a worker-emitted column name with the SELECT-list alias
// the user wrote. The coordinator rewrites batch and result schemas after
// Gather using these pairs. When Expr is non-nil, the coordinator compiles
// and evaluates the expression per row instead of doing a name rename — used
// for wrapped aggregates ("SUM(x)/7.0 AS y" emits Expr=BinaryOp{ColRef("__agg_0"), /, 7.0}
// because the logical planner replaces nested aggregates with refs to their
// synthetic OutputCol). From is the primary input column (used as the
// existence check); other column refs in Expr resolve via ColumnIndexFallback.
type OutputRename struct {
	From string
	To   string
	Expr plansql.Node
}

// ProjectExprSpec is one SELECT-list item a scan fragment must emit: Name is
// the output column, Expr the SQL text the worker compiles and evaluates
// (bare column references become passthrough copies). Type is the plan-time
// inferred output type for computed expressions (inferProjectionTypeCols) —
// the worker cannot resolve it from the input schema because the output
// column doesn't exist there. A bare passthrough leaves Type at its zero
// value and the worker never consults it there (a ColRef resolves by
// DirectCopy instead).
type ProjectExprSpec struct {
	Expr string
	Name string
	Type parquet.TypeID
	// TypeKnown distinguishes a DECLARED Type from the zero value, which
	// TypeBool shares — the same shape as AggSpec.OutputTypeKnown (#354,
	// #371). A computed BOOLEAN expression (a comparison, LIKE, IS NULL, a
	// boolean literal — anything inferProjectionTypeCols resolves to
	// TypeBool) otherwise reads as "not set": projectOpFromSpecs drops it
	// off the wire, and the worker's buildSelectProjection then guesses
	// STRING for a column that IS a bool, so a pgwire client asking for the
	// true OID gets a boxed "true"/"false" string instead (#445).
	TypeKnown bool
}

// UnionArm is one arm of a StageUnion: the stage producing it, and the
// projection that puts its output under the set operation's result column
// names. The projection is what makes the arms concatenable — without it
// each arm reaches the union under its own names (and a raw-parquet
// pass-through scan arm reaches it carrying every column of its table).
type UnionArm struct {
	DepStage    string
	Projections []ProjectExprSpec
	// DecimalCoercions names the result columns this arm must MOVE into the
	// set operation's output DECIMAL(p,s) — after the projection above has
	// put them under the result names — before its rows join the union
	// stream. Empty for an arm that already carries the output type.
	//
	// It is a separate list rather than a field on ProjectExprSpec because
	// it is not a projection at all: a DECIMAL value is an unscaled integer
	// plus a declared scale, and making two arms agree means multiplying the
	// integer, which no CAST expression in this engine does exactly (the
	// cast evaluator's DECIMAL destination produces a float64). See
	// exec.DecimalCoerce and issue #533.
	DecimalCoercions []DecimalCoercion
}

// DecimalCoercion is one column that must arrive as DECIMAL(Precision, Scale).
type DecimalCoercion struct {
	Name      string
	Precision int
	Scale     int
}

// WindowColSpec defines a window function column in a stage. Every field is
// resolved by windowExecColumn — the same resolution the single-process
// pipeline compiles into exec.WindowColumn — so the stage carries a spec the
// worker can execute without a catalog or a logical plan.
type WindowColSpec struct {
	Func string
	// InputCol is the column the function reads, alone: the offset, default
	// and N that share a SQL argument list are parsed out into the fields
	// below at plan time (logical.WindowExpr.InputCol keeps the raw list).
	InputCol   string
	OutputCol  string
	OutputType parquet.TypeID
	// PartitionBy is what makes a window distributable: rows of one
	// partition can be windowed without seeing any other partition, so a
	// hash exchange on these keys turns the stage into N independent tasks.
	// Empty = a global window, which needs every row in one place.
	PartitionBy []string
	OrderBy     []SortKeySpec
	Frame       *logical.WindowFrameSpec
	// Function-specific arguments (see InputCol).
	LagLeadOffset  int
	LagLeadDefault any
	NtileBuckets   int
	NthValueN      int
}

// FusedJoinSpec describes a broadcast join absorbed into a parent join stage.
type FusedJoinSpec struct {
	JoinType        string
	JoinLeftKeys    []string
	JoinRightKeys   []string
	BuildDepStage   string // stage providing build-side data
	BuildTableAlias string
	BuildColOrigins map[string]string // bare build col → owning scan alias (multi-table builds only)
	JoinFilter      string
	FilterExprs     []string
	// JoinBuildSchema is the absorbed join's declared build columns, read
	// only when that build turns out to be empty — an absorbed LEFT join
	// owes the same NULL-padded columns a standalone one does (#348).
	JoinBuildSchema []parquet.Column
}

// ChainedJoinSpec describes a 1:1 downstream join absorbed into an upstream
// hash_join stage by fuseStageChains. It carries everything the dispatcher
// needs to emit the join as a post-primary probe op in the fused fragment.
type ChainedJoinSpec struct {
	JoinType        string
	JoinLeftKeys    []string
	JoinRightKeys   []string
	BuildDepStage   string // stage providing build-side data
	BuildTableAlias string
	BuildColOrigins map[string]string
	JoinFilter      string
	// FilterExprs are the absorbed stage's residual post-join filters —
	// emitted as an OpFilter immediately after this chained probe.
	FilterExprs []string
	// BuildFilterExprs filter the build input rows before hash-table
	// construction (exchange-subsume flag filters on the absorbed stage).
	BuildFilterExprs    []string
	QualifyAllBuildCols bool
	// Columns is the absorbed stage's output projection; applied as the
	// chained probe's OutputFilter so the fused stage emits exactly what
	// the absorbed stage emitted.
	Columns []string
	// JoinBuildSchema is the absorbed join's declared build columns, read
	// only when that build turns out to be empty (#348).
	JoinBuildSchema []parquet.Column
	// Partitioned marks a hash-partitioned 1:1 build input (the absorbed
	// stage was a hash_join): task i reads build partition i. False means
	// a replicated broadcast build read whole by every task.
	Partitioned bool
}

// AggSpec defines an aggregation in a stage.
type AggSpec struct {
	Func      string
	InputCol  string
	OutputCol string
	// InputExpr is the SQL text of a derived input expression, e.g.
	// "l_extendedprice * (1 - l_discount)". Empty when InputCol is a
	// bare column reference. Distributed workers compile this into a
	// Project operator before the aggregate so HashAggregate sees a
	// column whose name matches InputCol.
	InputExpr string
	// OutputType is the plan-time output type of this aggregate, mirrored
	// onto distributed.AggSpec at dispatch. Undeclared — OutputTypeKnown
	// false — is only produced for a MIN/MAX-family aggregate whose input
	// column does not resolve to a catalog type; see aggSpecOutputType.
	OutputType parquet.TypeID
	// OutputTypeKnown distinguishes a DECLARED OutputType from the zero
	// value, which TypeBool shares: BOOL_AND/BOOL_OR always declare BOOL,
	// and since #392 so does MIN_BY over a BOOL column. Reading that zero
	// as "undeclared" is the #354/#371 shape — a declaration dropped on
	// one dispatch path and re-guessed by the worker.
	OutputTypeKnown bool
	// InputType is the plan-time type of the vector InputExpr evaluates
	// into, mirrored onto distributed.AggSpec at dispatch. Zero when
	// there is no derived input. The worker hardcoded Float64 here, which
	// is the projection-typing defect of #310/#333 living in a second
	// place: MAX(COALESCE(a, b)) over two string columns wrote strings
	// into a Float64 vector and the aggregate saw zeros.
	InputType parquet.TypeID
	// InputCol2, Separator and Percentile carry the aggregate arguments
	// past the first one — the second column of CORR/COVAR_*/MIN_BY/MAX_BY,
	// STRING_AGG's delimiter, PERCENTILE_CONT/DISC's fraction. They are
	// mirrored onto distributed.AggSpec at dispatch. Before #353 nothing
	// carried them at all: the parser kept only Args[0], so MIN_BY had no
	// ordering column and answered NULL, STRING_AGG ignored the separator
	// the query asked for, and PERCENTILE_CONT read its fraction as 0.
	InputCol2  string
	Separator  string
	Percentile float64
}

// SortKeySpec defines a sort key in a stage.
type SortKeySpec struct {
	Column    string
	Desc      bool
	NullsLast bool

	// SourceExpr, SourceColumn and SourceType describe what MATERIALIZES a
	// synthetic ORDER BY key — a term the SELECT list does not carry, which
	// logical.resolveOrderBy named __sortkey_N. Nothing on the DAG computes
	// a Project, so unless some pass puts that name on the producing stage
	// the sort has no such column to key on (#424). They ride the key
	// itself rather than the stage because every pass that moves a sort's
	// ordering somewhere else — fuseSortIntoPredecessor's fold onto a
	// join/aggregate, emitMergeSortTree, the gather's Exchange.Ordering —
	// copies the SortKeySpec slice wholesale, so the definition travels with
	// the key for free.
	//
	// SourceExpr is the term's expression text; SourceColumn is non-empty
	// only when the term is a plain column reference, in which case the
	// producer already emits that column under its own name and the key can
	// simply be renamed to it. SourceType is the declared type a computed
	// term's materialized column carries; SourceTypeKnown distinguishes a
	// DECLARED SourceType from the zero value TypeBool shares (the same
	// ProjectExprSpec.TypeKnown shape, #445/#472) — without it a genuinely
	// BOOL sort key reads as "not set" and the materialized projection drops
	// its type off the wire.
	//
	// Empty on every ordinary key, and read by exactly one pass
	// (resolveHiddenSortKeys) — sortKeysEqual compares ORDERING, so these
	// are deliberately outside that comparison.
	SourceExpr      string
	SourceColumn    string
	SourceType      parquet.TypeID
	SourceTypeKnown bool

	// AliasSource is the column the producing stream carries for a key that
	// names a DERIVED TABLE's SELECT-list alias — the non-synthetic sibling
	// of SourceColumn above (#467, #468).
	//
	// `SELECT k FROM (SELECT s_suppkey AS k FROM supplier ORDER BY
	// s_suppkey DESC) x` sorts on "k", because logical.resolveOrderBy binds
	// an ORDER BY term to the SELECT list's OUTPUT name — which is also what
	// PostgreSQL does, and is the whole point when the alias SHADOWS a base
	// column of the same relation (`s_acctbal AS s_suppkey ... ORDER BY
	// s_suppkey` means the alias). On the DAG that name exists nowhere
	// unless attachScanSelectProjections materialized it, and only the
	// OUTERMOST SELECT list gets that treatment: a derived table's sort
	// either failed loud (`sort: key column "k" does not exist in the input
	// schema`) or — with a shadowing alias — silently keyed on the WRONG
	// column, ordering by base s_suppkey where PostgreSQL orders by
	// s_acctbal.
	//
	// Set by annotateDerivedAliasSortKey at stage-emission time, where the
	// logical Projects are still in hand, and consumed by
	// resolveDerivedAliasSortKeys once planning can see whether the alias
	// was materialized after all. Empty on every ordinary key, and outside
	// SameOrdering for the same reason the fields above are.
	AliasSource string
}

// SameOrdering reports whether two keys impose the same order. It compares
// what the sort actually does — column, direction, NULL placement — and
// ignores the materialization fields, which say where a synthetic key comes
// from rather than how it sorts.
func (k SortKeySpec) SameOrdering(o SortKeySpec) bool {
	return k.Column == o.Column && k.Desc == o.Desc && k.NullsLast == o.NullsLast
}

// DynamicFilterEmit is the planner-side spec attached to a build-side leaf
// scan stage. Mirrors distributed.DynamicFilterEmit (separate copy keeps
// the physical package free of the wire-format dependency direction; the
// dispatcher converts at the boundary).
type DynamicFilterEmit struct {
	FilterID  string
	KeyColumn string
	KeyType   string // "int32" | "int64" | "date"
	BloomBits int    // total bloom-bitset size; identical across all tasks so union = bitwise OR
	// AtOutput accumulates over the stage's OUTPUT stream (pre-sink) rather
	// than the scan source — required when the emitting stage is a join or
	// filtered scan whose output, not input, defines the key set
	// (markSemiAntiBuildFilters).
	AtOutput bool
	// LateAttach forces the coordinator to stage the merged filter to its
	// deterministic S3 key regardless of inline size — an attach-on-arrival
	// consumer polls that key, so it must exist even for tiny blooms
	// (applyAttachOnArrival; docs/design/attach-on-arrival-dynamic-filters.md).
	LateAttach bool
	// GuardConsumes lists the FilterIDs of this stage's OWN attach-mode
	// consumes that must retro-filter this emit's buffered head rows at
	// finalize (guarded re-emit — applyAttachOnArrival rule-1 relaxation).
	// The worker's emit op buffers (emit-key, guard-column) pairs for rows
	// scanned before those blooms install and drops non-matching pairs
	// before the partial uploads, keeping the emitted bloom exactly as
	// tight as under the start barrier.
	GuardConsumes []string
	// InFlow marks an emitter whose tasks must ride NORMAL scheduling
	// instead of the priority lane. The lane's contract is planner-bounded
	// tiny tasks (extra slots above MaxConcurrent are memory-safe only
	// because dimension scans are tiny) and its purpose is overtaking bulk
	// work that is ALREADY consuming the filter attach-mode. An in-flow
	// cascade mid (e.g. 15M-row customer) violates the first and doesn't
	// need the second: its consumer is WAIT-blocked on a stat-dep, so
	// ordinary slots serve it correctly (docs/design/dimension-cascade.md
	// §In-flow mid emitters).
	InFlow bool
}

// DynamicFilterConsume is the planner-side spec attached to a probe-side
// leaf scan stage. SourceStageID names the build-scan stage that emits the
// corresponding stats; the planner also appends SourceStageID to this
// stage's Dependencies so the stage DAG serializes them.
type DynamicFilterConsume struct {
	FilterID      string
	SourceStageID string
	TargetColumn  string
	KeyType       string
	// AttachOnArrival marks this consume as non-blocking: the stat-dep edge
	// is removed, the consumer dispatches immediately, and its tasks install
	// the bloom mid-scan when the emitter's merged artifact lands at the
	// deterministic staged key. Set only by applyAttachOnArrival under its
	// structural rules; drop-only bloom semantics keep results identical.
	AttachOnArrival bool
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
	planCtx        context.Context  // context from the current Plan() call, used by subquery runner
	ctes           []plansql.CTEDef // CTE definitions from the current query, for subquery resolution
	MemoryBudget   int64            // per-query memory budget in bytes (0 = unlimited)
	SpillDir       string           // directory for spill files (empty = os temp dir)

	// ManifestSnapshot pins each table's manifest to one catalog read for
	// this statement (#502). NewPlanner sets a fresh one; forSubquery's
	// shallow copy shares it with every child/subquery planner. A caller
	// that builds several Planner instances for one statement must assign
	// the SAME snapshot to each — see ManifestSnapshot's doc.
	ManifestSnapshot *ManifestSnapshot

	// SharedTracker / SharedSpillMgr (if set) are used in place of per-query
	// Tracker+SpillManager creation. Workers set these to point at the
	// executor-level pool so concurrent tasks on the same worker compete for
	// ONE budget and spill cooperatively under pool pressure, matching the
	// Trino/Spark unified memory manager model. When nil, getSpillManager()
	// falls back to creating a per-query pool as before.
	SharedTracker  *memory.Tracker
	SharedSpillMgr *memory.SpillManager
	QueryLimits    *config.QueryLimits         // cost-based query guard (nil = no limits)
	cteCache       map[string]*cteMaterialized // materialized CTE results
	scanCache      map[string]*scanCached      // cached scan results for duplicate table scans
	res            *queryResources             // per-query spill manager + memory tracker (lazy, shared with child planners)
	WorkerCount    int                         // number of distributed workers (for shuffle partitioning)

	// BroadcastBytesThreshold is the maximum estimated build-side size for
	// a join to be planned as broadcast_join. Builds above this become
	// hash_join (hash-shuffle), eliminating the N× build-cache duplication
	// every worker pays under broadcast.
	//
	// Zero = use absolute default (100 MB), preserving legacy behavior for
	// embedded callers that don't set this. Distributed callers should set
	// this from per-worker pool budget — e.g., budget * 0.3 capped at
	// 200 MB — so the broadcast/shuffle decision adapts to cluster memory
	// instead of trusting an absolute constant. Negative = never broadcast
	// (force every join to hash_join).
	//
	// Architectural rule the threshold encodes: broadcast is sound when
	// every worker can comfortably hold the full build state in memory
	// alongside concurrent hash tables. As cluster width or per-worker
	// budget shrinks, the threshold shrinks too — without changing the
	// planner's join-type logic.
	BroadcastBytesThreshold int64

	// SortMergeJoinBytes gates the sort-merge join path for big-vs-big inner
	// equi-joins (docs/design/sort-merge-join.md). A join takes SMJ only when
	// BOTH sides' estimated post-selectivity bytes reach this threshold —
	// small builds keep the strictly-better hash/broadcast paths. Zero (the
	// shipped default) disables SMJ entirely; the planner behaves exactly as
	// before. Distributed callers should derive it from per-worker pool
	// budget (the broadcast-threshold pattern) so "too big to sit resident"
	// tracks cluster memory.
	SortMergeJoinBytes int64

	// LateMaterialization emits inner/left hash-join output as view
	// (dictionary) columns over the probe input and build batches; the
	// gather is deferred to the first consumer needing owned storage
	// (docs/design/late-materialization.md). Off by default.
	LateMaterialization bool

	// DynamicFiltersEnabled gates the Trino-style dynamic-filter planner
	// pass. When true, applyDynamicFilters annotates eligible hash_join
	// build/probe leaf scans with Emit/Consume specs and adds the stat-dep
	// edge from build-scan to probe-scan. Off by default for v1 rollout;
	// distributed callers flip this on after the local-harness gate passes.
	DynamicFiltersEnabled bool

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

	// scanCounter tracks the N-th scan of each table for alias resolution.
	// It is per-BUILD scratch, not query state: it numbers the scans of one
	// pipeline build so MaterializedInputs / StreamingSources / ScanFileFilter
	// lookups hit the right "table" / "table:N" key. forSubquery gives every
	// subquery build a fresh one.
	scanCounter map[string]int

	// scalarPlaceholderSeq allocates unique ":scalar_N" placeholder names
	// across a single query when resolveFilterSubqueries defers CTE-
	// referencing subqueries for late coordinator-side substitution.
	scalarPlaceholderSeq int

	// ctePlannedTerminal caches the terminal stage ID emitted while walking
	// a CTE's subtree, keyed by `cteName + "|" + structuralHash(subtree)`.
	// On a second walk of a structurally-identical CTE clone, walkStages
	// links the parent's deps to the cached terminal and skips re-emitting
	// duplicate stages. Eliminates the dual-chain float drift that fails
	// Q15 ~50% under multi-file scans (project_q15_dual_chain_float_drift).
	// Reset at the start of generateStages so each query gets a fresh map.
	ctePlannedTerminal map[string]string

	// scanDeletes caches the merge-on-read DELETE state walkStages read for
	// each base table, table name → (file path → file-absolute deleted row
	// indices). Captured from the SAME manifest object that produced the
	// stage's ScanFiles, so the pair is a snapshot; annotateScanDeletes
	// then replays it onto the FINAL stage list, which is where a fused or
	// rewritten stage that ends up owning those files can be reached.
	// Re-reading the manifest in that late pass instead would reintroduce
	// exactly the skew the snapshot exists to avoid (see Stage.ScanDeletes).
	// Reset at the start of generateStages.
	scanDeletes map[string]map[string][]int64

	// limitStageRoot is the node generateStages was entered with — the one
	// node whose LIMIT the coordinator's post-gather pass can see, because
	// logical.ExtractMergeInfo reads the plan root and nothing below it.
	// needsLimitStage compares against it to decide which LIMITs still need
	// a stage of their own (#478). Set at the start of generateStages.
	limitStageRoot *logical.Node

	// setOpErr records a set operation walkStages cannot lower to stages.
	// walkStages has no error return (it is recursive over ~20 node kinds
	// and every other case is total), so the refusal is parked here and
	// PlanDistributed turns it into a planning error. Refusing is the
	// point: the alternative this replaces was emitting each arm's stages
	// and no merge, which returned ONE arm's raw output as the query's
	// answer (#346). Reset at the start of generateStages.
	setOpErr error

	// joinCondErr records an ON clause parseJoinKeys cannot represent as a
	// key pair, parked for the same reason setOpErr is: walkStages has no
	// error return. Refusing is the point — the alternative it replaces was
	// passing the unrepresentable operand to the executor AS A COLUMN NAME,
	// which resolves to nothing and silently matches either no rows or every
	// row (#351). Reset at the start of generateStages.
	joinCondErr error

	// correlatedErr records a per-row correlated subquery found during stage
	// generation (#359), parked for the same reason the two above are. The
	// primary detection is refuseCorrelatedSubqueries, a pre-pass over the
	// logical plan; this field is its structural backstop at the deferral
	// seam — resolveSubqueryAST refuses to defer or eagerly execute a
	// subquery that is not self-contained (plansql.DanglingTableRefs),
	// because a producer stage executing it standalone evaluates the
	// dangling outer reference to NULL and the query silently answers 0.
	// Reset at the start of generateStages.
	correlatedErr error
	// inSubqueryErr records an IN-subquery the planner could not materialize
	// into a literal set, for the same reason and by the same mechanism as
	// correlatedErr. See in_subquery_set.go.
	inSubqueryErr error

	// aggStageRenames maps a name an Aggregate node reads in the LOGICAL plan
	// to the name the aggregate STAGE emits for it, for every group key
	// walkStages had to resolve through a subquery's rename (#355). The
	// gather's output renames are rewritten through it, or a query grouping
	// on a renamed column answers under the source column's name while the
	// single-process path answers under the alias. Reset at the start of
	// generateStages.
	aggStageRenames map[string]string
}

// refuseJoin parks the first refusal; PlanDistributed returns it. First one
// wins so a nested join's specific message is not overwritten by an outer
// one's.
func (p *Planner) refuseJoin(err error) {
	if p.joinCondErr == nil {
		p.joinCondErr = err
	}
}

// queryResources holds the spill manager and memory tracker for one query.
// They belong to the QUERY, not to a single pipeline build: the root planner
// and every child planner spawned by forSubquery share one instance, so a
// query has exactly one budget and exactly one spill directory for Plan's
// Cleanup to sweep.
//
// mu guards the lazy construction. A correlated subquery builds its pipeline
// from the execution goroutines (exec.Pipeline.runParallel), so two builds can
// otherwise race to create the manager — and each would create its own spill
// directory that nothing releases.
type queryResources struct {
	mu         sync.Mutex
	spillMgr   *memory.SpillManager
	memTracker *memory.Tracker
}

// resources returns this planner's shared per-query resources, allocating the
// holder if a caller constructed the Planner as a bare struct literal rather
// than through NewPlanner. Both NewPlanner and Plan set it, so on every real
// path the holder exists well before any goroutine fan-out.
func (p *Planner) resources() *queryResources {
	if p.res == nil {
		p.res = &queryResources{}
	}
	return p.res
}

// getSpillManager returns a shared per-query spill manager, creating it on
// first call. Uses MemoryBudget if set, otherwise auto-detects from system
// memory (cgroup or physical). Returns nil if no memory limit can be determined.
func (p *Planner) getSpillManager() *memory.SpillManager {
	// Shared pool (worker-level, injected by Executor) takes precedence so
	// all concurrent tasks spill against one budget.
	if p.SharedSpillMgr != nil {
		return p.SharedSpillMgr
	}
	r := p.resources()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.spillMgr != nil {
		return r.spillMgr
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
	tracker := memory.NewTracker("query", budget)
	dir := p.SpillDir
	if dir == "" {
		dir = os.TempDir()
	}
	sm, err := memory.NewSpillManager(dir, tracker)
	if err != nil {
		return nil
	}
	r.memTracker = tracker
	r.spillMgr = sm
	return sm
}

// getMemTracker returns the shared per-query memory tracker.
// Must be called after getSpillManager().
func (p *Planner) getMemTracker() *memory.Tracker {
	// Shared pool takes precedence (worker-level pool across tasks).
	if p.SharedTracker != nil {
		return p.SharedTracker
	}
	r := p.resources()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.memTracker
}

// spillManagerIfSet returns the per-query spill manager without creating one.
func (p *Planner) spillManagerIfSet() *memory.SpillManager {
	r := p.resources()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spillMgr
}

// cteMaterialized stores the pre-computed results of a CTE in one of two
// forms. The columnar form (coll) is the production shape: batches held in
// a tracker-charged, spill-backed collector and replayed without consuming
// it, so a multi-gigabyte CTE never sits boxed in heap (sweep finding #13 /
// issue #127). The boxed form (rows) remains for the recursive work table —
// one iteration's delta, re-seeded into the cache each fixed-point step.
type cteMaterialized struct {
	schema []parquet.Column
	rows   []map[string]any              // boxed form; nil when coll is set
	coll   *exec.SpillableBatchCollector // columnar form; nil when rows is set
}

// scanCached stores columnar scan results for a table that is scanned multiple
// times within a single query (e.g., decorrelated subqueries). The first scan
// populates the cache, and subsequent scans replay from it. Thread-safe for
// concurrent builds: subsequent scans block on ready until the first completes.
//
// The cache holds the UNION of all consumers' columns (unionCols) so one
// scan serves everyone; each consumer projects back down to its own
// columns in catalogScanSource.Next. Without the projection, every
// consumer — including hash-join BUILD sides, which store their input
// batches — carried the widest consumer's columns: Q21's two 60M-row
// lineitem semi/anti builds each stored l1's 4-column set instead of
// the 2 columns they need, and partition-on-arrival evictions spilled
// the dead columns to disk (the SF10 cold-S3 Q21 churn, 2026-07-06).
//
// The cache's vectors are shared with its consumers and are therefore
// NOT charged to the memory tracker here — see the comment at the
// append site in catalogScanSource.Next for the 2026-07-06 stall
// incident that rule prevents. Batches are dropped by
// Planner.releaseScanCache on every Plan exit path.
type scanCached struct {
	mu      sync.Mutex
	batches []*batch.RecordBatch
	schema  []parquet.Column
	done    bool          // true when the first scan is complete
	ready   chan struct{} // closed when first scan completes; nil until first scan claims the cache
	// unionCols is the union of every consumer's RequiredColumns, in
	// first-seen order. nil = full schema (some consumer needs all).
	unionCols []string
}

// NewPlanner creates a new physical planner.
func NewPlanner(cat *catalog.Catalog) *Planner {
	p := &Planner{catalog: cat, res: &queryResources{}, ManifestSnapshot: NewManifestSnapshot()}
	// Create a subquery runner that re-uses this planner for nested queries
	p.subqueryRunner = p.makeSubqueryRunner()
	return p
}

// makeSubqueryRunner creates a SubqueryRunner that executes SQL via this planner.
// Uses the planCtx stored during Plan() so subqueries respect the parent context's
// cancellation and timeout.
//
// Each call gets its own child planner. This is the runner baked into every
// compiled expression, so a correlated subquery reaches it once per row from
// every parallel pipeline goroutine — see forSubquery for why sharing the
// parent's build scratch across those goroutines is not an option.
func (p *Planner) makeSubqueryRunner() expr.SubqueryRunner {
	return func(sql string) ([]map[string]any, error) {
		ctx := p.planCtx
		if ctx == nil {
			ctx = context.Background()
		}
		return p.forSubquery().executeSubquery(ctx, sql)
	}
}

// forSubquery returns a child planner for building and running ONE subquery
// pipeline.
//
// A correlated subquery is executed per row from the pipeline's parallel
// worker goroutines (exec.Pipeline.runParallel), and each execution runs a
// full physical build. Those builds must not share the parent's per-build
// scratch, for two independent reasons:
//
//   - It is a data race. scanCounter is a plain map written by buildScan, so
//     concurrent builds crash the process with "fatal error: concurrent map
//     writes" — an unrecoverable throw that takes down every other connection
//     in server mode (issue #334).
//   - It is wrong even when serialized. scanCounter numbers the scans of one
//     build; letting the outer build's count leak in makes a subquery's first
//     scan of customer resolve as alias "customer:1", so the ScanFileFilter /
//     MaterializedInputs / StreamingSources lookups keyed by that alias miss.
//     A mutex would hide the crash and keep the mis-keying.
//
// So the child gets fresh build scratch, and shares everything that genuinely
// belongs to the query: the catalog, the CTE definitions and their
// materialized cache, the scan cache, the memory/spill resources, and all
// configuration. Sharing those is what keeps one budget, one spill directory,
// and one materialization of each CTE per query; a per-goroutine copy would
// leak spill directories that Plan's Cleanup never sees.
//
// The shared maps (cteCache, scanCache) are populated by Plan before execution
// begins and are read-only from here on; scanCached carries its own mutex for
// the concurrent-replay case.
//
// The scan-alias injections (MaterializedInputs, StreamingSources,
// ScanFileFilter) are dropped. They describe the ENCLOSING fragment's scans —
// a worker's probe-split file slice, a scan-split pre-scan — keyed by that
// plan's aliases. A subquery is its own query over the catalog and must see
// the whole table; binding it to the fragment's file slice would answer it
// from one worker's shard. Today they are missed only because the shared
// counter happens to push the subquery's aliases past the injected keys, so
// dropping them makes the existing behavior explicit rather than incidental.
func (p *Planner) forSubquery() *Planner {
	sub := *p
	sub.scanCounter = nil
	sub.ctePlannedTerminal = nil
	sub.scanDeletes = nil
	sub.scalarPlaceholderSeq = 0
	sub.MaterializedInputs = nil
	sub.StreamingSources = nil
	sub.ScanFileFilter = nil
	sub.res = p.resources()
	// Nested subqueries inside this one recurse through the same rule.
	sub.subqueryRunner = sub.makeSubqueryRunner()
	return &sub
}

// buildSubqueryPipeline parses, plans, and builds (but does not run) the
// physical pipeline for a SQL subquery, merging the enclosing WITH clause's
// CTEs so the subquery can reference them. Shared by executeSubquery (boxed
// results) and materializeCTEColumnar (columnar collection).
func (p *Planner) buildSubqueryPipeline(ctx context.Context, sql string) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// Parse using our SQL parser
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("subquery parse error: %w", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("subquery extract error: %w", err)
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
		return nil, nil, nil, fmt.Errorf("subquery plan error: %w", err)
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
		return nil, nil, nil, fmt.Errorf("subquery execution plan error: %w", err)
	}
	return source, ops, sink, nil
}

// executeSubquery parses and executes a SQL subquery, returning result rows.
func (p *Planner) executeSubquery(ctx context.Context, sql string) ([]map[string]any, error) {
	source, ops, sink, err := p.buildSubqueryPipeline(ctx, sql)
	if err != nil {
		return nil, err
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

// scalarDeferAll gates deferring ALL uncorrelated scalar filter subqueries
// to distributed producer stages (not only CTE-referencing ones).
// Kill switch: WADJET_SCALAR_DEFER=0 reverts non-CTE subqueries to eager
// plan-time execution on the coordinator's single-process pipeline
// (mirrors WADJET_EXCHANGE_ELIDE / WADJET_SCAN_COL_SANITIZE).
var scalarDeferAll = os.Getenv("WADJET_SCALAR_DEFER") != "0"

// deferredScalar carries a single CTE-referencing subquery whose resolution
// has been deferred until coordinator dispatch. The placeholder is a colon-
// prefixed identifier rendered into the serialized filter expression; the
// coordinator substitutes it after the producer stage completes.
type deferredScalar struct {
	Placeholder string // e.g. "scalar_1" (no leading colon)
	SubquerySQL string // the subquery to execute as a producer stage
}

// resolveFilterSubqueries finds embedded SQL subqueries in a filter expression
// string, executes them using the planner's standalone pipeline, and substitutes
// the scalar results as literals. This is needed for distributed mode where
// workers don't have catalog access to execute subqueries themselves.
//
// Subqueries whose FROM clause references a CTE are rewritten with
// :scalar_N placeholders instead of being pre-computed. The returned deferredScalar list describes the producer stages
// the caller must emit, and the filter-carrying stage's ScalarDependencies
// should point to those producer stage IDs. This eliminates the float-precision
// divergence between single-process cteCache evaluation and the distributed
// pipeline's accumulation order (root cause of Q15 SF0.1 0-row bug).
//
// Non-native-DAG mode keeps the legacy behavior: CTE-referencing subqueries
// are left unresolved (worker re-executes via SubqueryRunner), others are
// pre-computed and substituted in place.
func (p *Planner) resolveFilterSubqueries(exprStr string) (string, []deferredScalar) {
	// Quick check: no subquery to resolve
	if !strings.Contains(strings.ToUpper(exprStr), "SELECT") {
		return exprStr, nil
	}

	ctx := p.planCtx
	if ctx == nil {
		return exprStr, nil
	}

	// Parse the expression to find SubqueryNode elements
	ast, err := plansql.ParseExpression(exprStr)
	if err != nil {
		return exprStr, nil
	}

	var deferred []deferredScalar
	resolved := p.resolveSubqueryAST(ctx, ast, &deferred)
	if resolved != nil {
		return resolved.String(), deferred
	}
	return exprStr, deferred
}

// allocScalarPlaceholder returns the next unused placeholder name (no leading
// colon) for this planner. Names are unique per Planner instance so that
// multiple deferred subqueries in the same query can coexist.
func (p *Planner) allocScalarPlaceholder() string {
	p.scalarPlaceholderSeq++
	return fmt.Sprintf("scalar_%d", p.scalarPlaceholderSeq)
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
// elements with either literal values obtained by executing the subquery, or
// (when the subquery references a CTE under native-DAG) a LiteralPlaceholder
// whose concrete value will be substituted by the coordinator. Any deferred
// subqueries are appended to *deferred.
func (p *Planner) resolveSubqueryAST(ctx context.Context, node plansql.Node, deferred *[]deferredScalar) plansql.Node {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *plansql.SubqueryNode:
		// A subquery that is not self-contained must NOT be deferred to a
		// producer stage or eagerly executed: standalone, its dangling outer
		// reference resolves to no column, evaluates NULL, and the query
		// silently answers 0 (#359). refuseCorrelatedSubqueries catches
		// correlation before stage generation with full scope; this is the
		// structural backstop for any expression that reaches deferral
		// without having passed through that pre-pass (e.g. inside a scalar
		// producer's own re-walk). Parked, not returned — walkStages has no
		// error path — and PlanDistributed turns it into the typed refusal
		// the coordinator routes on.
		if dangling := plansql.DanglingTableRefs(n.SQL); len(dangling) > 0 {
			p.refuseCorrelated(fmt.Errorf("%w: a scalar subquery references outer %s"+
				" and cannot execute as a standalone producer stage",
				ErrCorrelatedSubqueryDistributed, describeOuterRefs(dangling)))
			return node
		}
		// Defer scalar subqueries to producer stages so they share the
		// distributed accumulation path instead of running as a silent
		// single-process pipeline on the coordinator at plan time (Q11's
		// partsupp⨝supplier⨝nation subquery cost ~39s/query at SF100 this
		// way, Q22's customer avg ~10s). CTE-referencing subqueries MUST
		// defer regardless of the kill switch — eager evaluation over the
		// cteCache floats-drifts vs the outer query's distributed
		// aggregate (the Q15 SF0.1 0-row bug).
		if scalarDeferAll || p.subqueryReferencesCTE(n.SQL) {
			name := p.allocScalarPlaceholder()
			*deferred = append(*deferred, deferredScalar{Placeholder: name, SubquerySQL: n.SQL})
			return &plansql.LiteralPlaceholder{Name: name}
		}
		start := time.Now()
		rows, err := p.executeSubquery(ctx, n.SQL)
		slog.Info("plan-time scalar subquery executed on coordinator",
			"duration", time.Since(start).Round(time.Millisecond),
			"rows", len(rows), "error", err != nil)
		if err != nil || len(rows) == 0 {
			return node
		}
		// Extract scalar value from first row, first column
		for _, v := range rows[0] {
			return scalarToLiteral(v)
		}
		return node

	case *plansql.InExpr:
		// `x IN (SELECT …)` that reached here did NOT decorrelate into a
		// semi/anti join, and the worker has no SubqueryRunner to execute it
		// with — the filter used to ship verbatim and fail (#524). An
		// uncorrelated IN-subquery is a SET, so it is materialized here and
		// the predicate becomes the literal list the expression layer already
		// evaluates. See in_subquery_set.go for the two bounds and the
		// refusal that routes past them.
		if subq := findInSubqueryValue(n); subq != nil {
			if rewritten, ok := p.materializeInSubquery(ctx, n, subq); ok {
				return rewritten
			}
			return node
		}
		vals := make([]plansql.Node, len(n.Values))
		for i, v := range n.Values {
			vals[i] = p.resolveSubqueryAST(ctx, v, deferred)
		}
		return &plansql.InExpr{
			Left:   p.resolveSubqueryAST(ctx, n.Left, deferred),
			Not:    n.Not,
			Values: vals,
		}

	case *plansql.CmpExpr:
		return &plansql.CmpExpr{
			Left:  p.resolveSubqueryAST(ctx, n.Left, deferred),
			Op:    n.Op,
			Right: p.resolveSubqueryAST(ctx, n.Right, deferred),
		}

	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  p.resolveSubqueryAST(ctx, n.Left, deferred),
			Op:    n.Op,
			Right: p.resolveSubqueryAST(ctx, n.Right, deferred),
		}

	case *plansql.UnaryOp:
		return &plansql.UnaryOp{
			Op:    n.Op,
			Inner: p.resolveSubqueryAST(ctx, n.Inner, deferred),
		}

	case *plansql.ParenNode:
		inner := p.resolveSubqueryAST(ctx, n.Inner, deferred)
		if inner != nil {
			return &plansql.ParenNode{Inner: inner}
		}
		return node

	default:
		return node
	}
}

// emitScalarProducerStages parses subquerySQL, walks its logical plan, and
// appends the resulting distributed stages to *stages. Returns the terminal
// stage's ID (the one whose single-row, single-column output holds the
// scalar). The coordinator awaits this producer at dispatch time, extracts
// the value, and substitutes it into the filter-carrying stage's expression.
//
// CTE definitions from the enclosing query are merged so the subquery can
// resolve :CTE references. The terminal stage is forced to Tasks=1 so its
// output is a single unpartitioned WSHF file suitable for scalar extraction.
func (p *Planner) emitScalarProducerStages(stages *[]Stage, subquerySQL string) (string, error) {
	pq, err := plansql.Parse(subquerySQL)
	if err != nil {
		return "", fmt.Errorf("parse subquery: %w", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return "", fmt.Errorf("extract subquery: %w", err)
	}
	var logicalPlan *logical.Node
	if len(p.ctes) > 0 {
		merged := append([]plansql.CTEDef(nil), p.ctes...)
		merged = append(merged, info.CTEs...)
		logicalPlan, err = logical.BuildFromSelectWithCTEs(info, merged)
	} else {
		logicalPlan, err = logical.BuildFromSelect(info)
	}
	if err != nil {
		return "", fmt.Errorf("build subquery plan: %w", err)
	}
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	p.AnnotateScanColumns(ctx, logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		p.AnnotateScanColumns(ctx, plan)
	})

	before := len(*stages)
	p.walkStages(logicalPlan, stages, nil)
	if len(*stages) == before {
		return "", fmt.Errorf("subquery emitted no stages")
	}
	terminal := &(*stages)[len(*stages)-1]
	// Force Singleton: a scalar producer emits exactly one row. Tasks>1
	// here would fan out into partitioned WSHF output that the coordinator's
	// scalar extractor can't read.
	terminal.Tasks = 1
	// Pin the subquery's projection on the terminal so the scalar extractor
	// can apply post-aggregate wrappers like Q11's "SUM(...) * 0.0001". The
	// producer chain only emits the raw aggregate (e.g. __agg_0 = SUM); the
	// SELECT-level multiplier needs to be applied by the coordinator after
	// reading the producer output. Reuses Stage.OutputRenames the same way
	// Gather does — at extract time we'll detect the producer-vs-Gather
	// case via context.
	if renames := extractOutputRenames(logicalPlan); len(renames) > 0 {
		terminal.OutputRenames = renames
	}
	return terminal.ID, nil
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
			intCols := make(map[string]bool, len(table.Schema.Columns))
			colTypes := make(map[string]parquet.TypeID, len(table.Schema.Columns))
			colDecimal := make(map[string]logical.DecimalMeta)
			for i, c := range table.Schema.Columns {
				cols[i] = c.Name
				colTypes[strings.ToLower(c.Name)] = c.Type
				if c.Type == parquet.TypeDecimal {
					colDecimal[strings.ToLower(c.Name)] = logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale}
				}
				switch c.Type {
				case parquet.TypeInt64, parquet.TypeInt32, parquet.TypeTimestamp,
					parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration,
					parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
					intCols[strings.ToLower(c.Name)] = true
				}
			}
			strictInt := make(map[string]bool, len(table.Schema.Columns))
			for _, c := range table.Schema.Columns {
				switch c.Type {
				case parquet.TypeInt64, parquet.TypeInt32:
					strictInt[strings.ToLower(c.Name)] = true
				}
			}
			node.ScanColumns = cols
			node.ScanIntCols = intCols
			node.ScanStrictIntCols = strictInt
			node.ScanColTypes = colTypes
			node.ScanColDecimal = colDecimal
		}
		// Estimate row count from manifest for join reordering
		if manifest, err := p.getManifest(ctx, node.TableName); err == nil {
			var total int64
			for _, part := range manifest.Partitions {
				for _, f := range part.Files {
					total += f.NumRows
				}
			}
			node.ScanRowEstimate = total

			// Aggregate per-column stats for CBO selectivity estimation
			if colStats, err := p.getAggregateColumnStats(ctx, node.TableName); err == nil && colStats != nil {
				scanStats := make(map[string]logical.ScanColumnStats, len(colStats))
				for col, cs := range colStats {
					var hist any
					if cs.Histogram != nil {
						hist = cs.Histogram
					}
					scanStats[col] = logical.ScanColumnStats{
						MinValue:  cs.MinValue,
						MaxValue:  cs.MaxValue,
						NullCount: cs.NullCount,
						TotalRows: cs.TotalRows,
						NDV:       cs.NDV,
						Histogram: hist,
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
		// Materialize columnar into a tracker-charged, spill-backed
		// collector. The previous shape boxed the whole result via
		// CollectSink.ToRows (one map[string]any per row, entirely outside
		// the budget/spill machinery) — `WITH x AS (SELECT * FROM lineitem)`
		// held the full table in coordinator-process heap.
		coll, schema, err := p.materializeCTEColumnar(ctx, cte.SQL)
		if err != nil {
			continue // fall back to inline expansion
		}
		if schema == nil {
			coll.Release()
			continue
		}
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
	}
}

// materializeCTEColumnar runs a CTE body into a spill-backed collector,
// applying the same first-row string→numeric coercion the boxed path did
// (see cteCoercingSink). Returns the collector and the (possibly coerced)
// output schema; the caller owns the collector and must Release it —
// normally via PhysicalPlan.Cleanup through releaseCTECache.
func (p *Planner) materializeCTEColumnar(ctx context.Context, sql string) (*exec.SpillableBatchCollector, []parquet.Column, error) {
	source, ops, _, err := p.buildSubqueryPipeline(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	coll := &exec.SpillableBatchCollector{Spill: p.getSpillManager()}
	sink := &cteCoercingSink{coll: coll}
	pipeline := &exec.Pipeline{Source: source, Ops: ops, Sink: sink}
	if err := pipeline.Run(ctx); err != nil {
		coll.Release()
		return nil, nil, err
	}
	schema := sink.schema
	if schema == nil {
		// Empty result: no batch ever arrived, so derive column names from
		// the SQL like the boxed path did — downstream projection still
		// needs the names to resolve.
		schema = p.inferCTESchema(sql, nil)
	}
	return coll, schema, nil
}

// releaseScanCache drops every duplicate-scan cache entry so cached
// batches don't outlive their query. Idempotent; wired into
// PhysicalPlan.Cleanup, Plan's error paths, and Plan's per-query reset.
func (p *Planner) releaseScanCache() {
	for _, c := range p.scanCache {
		c.mu.Lock()
		c.batches = nil
		c.mu.Unlock()
	}
	p.scanCache = nil
}

// releaseCTECache frees every columnar CTE collector (tracker charge +
// spill scratch). Idempotent; wired into PhysicalPlan.Cleanup and Plan's
// error paths.
func (p *Planner) releaseCTECache() {
	for _, mat := range p.cteCache {
		if mat.coll != nil {
			mat.coll.Release()
		}
	}
	p.cteCache = nil
}

// cteCacheHasCollectors reports whether any cached CTE holds spill-backed
// state that requires an explicit release at query end.
func (p *Planner) cteCacheHasCollectors() bool {
	for _, mat := range p.cteCache {
		if mat.coll != nil {
			return true
		}
	}
	return false
}

// cteCoercingSink wraps the CTE collector with the columnar equivalent of
// inferCTESchema's first-row coercion: the expression evaluator emits
// numeric literals as strings ("SELECT 1" produces "1"), and the boxed CTE
// path coerced such columns to Int64/Float64 by inspecting the first row.
// The plan is decided once, from the first non-empty batch, and every batch
// is transformed BEFORE it reaches the collector — so spilled runs already
// hold coerced data and replays need no per-reference transform. Columns
// whose first value does not parse stay strings (real string data is never
// touched: "CEO" doesn't parse).
type cteCoercingSink struct {
	coll    *exec.SpillableBatchCollector
	planned bool
	coerce  []parquet.TypeID // per-column target; TypeString = leave as-is
	any     bool             // true when at least one column coerces
	schema  []parquet.Column // output schema (coerced types); nil until first non-empty batch
}

func (s *cteCoercingSink) Init(ctx context.Context) error { return s.coll.Init(ctx) }

func (s *cteCoercingSink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	if !s.planned && b.ActiveLen() > 0 {
		s.planBatch(b)
	}
	if s.any {
		b = s.applyCoercions(b)
	}
	return s.coll.Consume(ctx, b)
}

func (s *cteCoercingSink) Finalize(ctx context.Context) error { return s.coll.Finalize(ctx) }
func (s *cteCoercingSink) Close() error                       { return s.coll.Close() }

// planBatch decides per-column coercions from the first active row,
// mirroring inferCTESchema's rule: int64 first, then float64.
func (s *cteCoercingSink) planBatch(b *batch.RecordBatch) {
	s.planned = true
	row0 := 0
	if b.Sel != nil {
		row0 = int(b.Sel[0])
	}
	s.coerce = make([]parquet.TypeID, len(b.Schema))
	s.schema = append([]parquet.Column(nil), b.Schema...)
	for ci, col := range b.Schema {
		s.coerce[ci] = parquet.TypeString // sentinel: no coercion
		if col.Type != parquet.TypeString {
			continue
		}
		vec := b.Columns[ci]
		if vec.Nulls.IsNullFast(row0) {
			continue
		}
		v := string(vec.BytesData.Value(row0))
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			s.coerce[ci] = parquet.TypeInt64
		} else if _, err := strconv.ParseFloat(v, 64); err == nil {
			s.coerce[ci] = parquet.TypeFloat64
		}
		if s.coerce[ci] != parquet.TypeString {
			s.any = true
			s.schema[ci].Type = s.coerce[ci]
			s.schema[ci].Nullable = true
		}
	}
}

// applyCoercions rebuilds the coerced columns as typed vectors. All Len
// slots are converted (Sel-agnostic — inactive slots become null, which is
// harmless because Sel guards every read downstream); unparseable values
// become null, matching what FromRows produced when the boxed path's
// coerced schema met a non-numeric string.
func (s *cteCoercingSink) applyCoercions(b *batch.RecordBatch) *batch.RecordBatch {
	cols := append([]*batch.Vector(nil), b.Columns...)
	for ci, target := range s.coerce {
		if target == parquet.TypeString || ci >= len(cols) {
			continue
		}
		src := cols[ci]
		dst := batch.NewVector(target, b.Len)
		for i := 0; i < b.Len; i++ {
			if src.Nulls.IsNullFast(i) {
				dst.Nulls.SetNull(i)
				continue
			}
			v := string(src.BytesData.Value(i))
			switch target {
			case parquet.TypeInt64:
				iv, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					dst.Nulls.SetNull(i)
					continue
				}
				dst.Int64Data[i] = iv
			case parquet.TypeFloat64:
				fv, err := strconv.ParseFloat(v, 64)
				if err != nil {
					dst.Nulls.SetNull(i)
					continue
				}
				dst.Float64Data[i] = fv
			}
			dst.Nulls.SetValid(i)
		}
		cols[ci] = dst
	}
	return &batch.RecordBatch{
		Schema:  s.schema,
		Columns: cols,
		Len:     b.Len,
		Sel:     b.Sel,
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
		// No UNION ALL found — fall back to non-recursive (columnar)
		// materialization.
		coll, schema, err := p.materializeCTEColumnar(ctx, cte.SQL)
		if err != nil {
			return
		}
		if schema == nil {
			coll.Release()
			return
		}
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
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

	// Accumulate iteration results columnar into a tracker-charged,
	// spill-backed collector instead of an unbounded boxed slice — the
	// iteration count was bounded (1000) but the row count was not, and
	// every accumulated row lived as a map[string]any until Plan returned.
	// The per-iteration work table stays boxed: it is one iteration's
	// delta (inherent to the fixed-point algorithm) and is re-seeded into
	// the cache each step for the recursive query's self-reference.
	coll := &exec.SpillableBatchCollector{Spill: p.getSpillManager()}
	appendRowsColumnar := func(rs []map[string]any) error {
		for off := 0; off < len(rs); off += batch.DefaultBatchSize {
			end := off + batch.DefaultBatchSize
			if end > len(rs) {
				end = len(rs)
			}
			if err := coll.Consume(ctx, batch.FromRows(schema, rs[off:end])); err != nil {
				return err
			}
		}
		return nil
	}
	if err := appendRowsColumnar(anchorRows); err != nil {
		coll.Release()
		return
	}

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

		if err := appendRowsColumnar(newRows); err != nil {
			// Spill scratch failure mid-iteration: abandon materialization.
			// Without a cache entry the recursive reference cannot resolve
			// and the query errors — same failure mode as an anchor error.
			coll.Release()
			delete(p.cteCache, cte.Name)
			return
		}
		workTable = newRows
	}

	// Store final accumulated results (columnar; replayed per reference).
	p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
}

// stageTypeCTEAlias marks a phantom stage that walkStages emits in place of
// a re-computed CTE subtree when ctePlannedTerminal already has a cached
// terminal stage ID. flattenCTEAliases removes these stages from the final
// plan and rewrites every dependency edge that targets an alias to target
// the alias's underlying CTE terminal instead. The alias never reaches
// dispatch — it exists purely to give parent walkStages cases something to
// pick up via leafStages without changing every parent's child-resolution
// logic.
const stageTypeCTEAlias = "cte-alias"

// flattenCTEAliases collapses cte-alias stages: replaces every Dependencies
// reference to an alias with its target, recursing through chains of aliases,
// then drops alias stages from the slice. Idempotent on slices that contain
// no aliases.
func flattenCTEAliases(stages []Stage) []Stage {
	// Build alias → target map. Aliases have exactly one Dependencies entry
	// pointing at the cached CTE terminal (or another alias, in pathological
	// chain cases — recurse to flatten).
	aliasTarget := map[string]string{}
	for _, s := range stages {
		if s.Type == stageTypeCTEAlias && len(s.Dependencies) == 1 {
			aliasTarget[s.ID] = s.Dependencies[0]
		}
	}
	if len(aliasTarget) == 0 {
		return stages
	}
	// Resolve transitively: follow alias→alias chains until we hit a real
	// stage. Caps at len(aliasTarget) hops to defend against any cycle.
	resolve := func(id string) string {
		for i := 0; i <= len(aliasTarget); i++ {
			next, ok := aliasTarget[id]
			if !ok {
				return id
			}
			id = next
		}
		return id
	}
	// Rewrite every Dependencies / LeftDepStage / RightDepStage / FusedJoin
	// build dep that points at an alias.
	for i := range stages {
		s := &stages[i]
		for j, dep := range s.Dependencies {
			s.Dependencies[j] = resolve(dep)
		}
		if t, ok := aliasTarget[s.LeftDepStage]; ok {
			s.LeftDepStage = resolve(t)
		}
		if t, ok := aliasTarget[s.RightDepStage]; ok {
			s.RightDepStage = resolve(t)
		}
		for j, fj := range s.FusedJoins {
			if t, ok := aliasTarget[fj.BuildDepStage]; ok {
				s.FusedJoins[j].BuildDepStage = resolve(t)
			}
		}
		for ph, prod := range s.ScalarDependencies {
			if t, ok := aliasTarget[prod]; ok {
				s.ScalarDependencies[ph] = resolve(t)
			}
		}
	}
	// Drop alias stages.
	out := make([]Stage, 0, len(stages))
	for _, s := range stages {
		if s.Type == stageTypeCTEAlias {
			continue
		}
		out = append(out, s)
	}
	return out
}

// cteSubtreeHash returns a hex SHA-256 over a structural projection of the
// logical subtree rooted at n. Two CTE clones with the same hash are safe
// to dedupe in walkStages: identical scan tables, identical pushed-down
// predicates, identical projections/aggregates, identical column-pruning
// outputs, identical child shapes. A clone where the optimizer pushed
// different filters or columns has a different hash and is NOT deduped.
//
// The hash is intentionally over the *post-optimization* logical shape —
// we want bit-identical execution paths, not source-text equality.
func cteSubtreeHash(n *logical.Node) string {
	h := sha256.New()
	hashLogicalNode(h, n)
	return hex.EncodeToString(h.Sum(nil))
}

func hashLogicalNode(h io.Writer, n *logical.Node) {
	if n == nil {
		_, _ = io.WriteString(h, "<nil>|")
		return
	}
	// Order matters and so does separator — Sprint with a delimiter so a
	// field containing the same characters as another can't collide. The
	// fields chosen are the ones the physical planner actually reads from
	// when emitting stages; if a future planner change reads a new field
	// during walkStages, add it here.
	//
	// IMPORTANT: RequiredColumns is INTENTIONALLY excluded. The optimizer's
	// column-pruning analysis pushes columns referenced anywhere in the
	// outer query into the CTE's inner scan rc list — including columns
	// that don't even belong to the CTE's tables (e.g., supplier_no
	// projected by the Project ABOVE the CTE body, or s_suppkey from the
	// JOIN's other side). Two clones of the same CTE will therefore
	// disagree on RequiredColumns even though they compute byte-identical
	// data; downstream scan code already over-approximates and prunes to
	// real schema columns at execution time. Hashing RC would defeat the
	// dedup whenever a CTE is consumed by two consumers with different
	// outer column needs (i.e., always).
	fmt.Fprintf(h, "T:%v|TBL:%s|PF:%v|SP:%v|", n.Type, n.TableName, n.PartitionFilter, n.ScanPredicates)
	fmt.Fprintf(h, "Pred:%v|Proj:%v|", n.Predicates, n.Projections)
	fmt.Fprintf(h, "GB:%v|GBE:%v|Agg:%v|", n.GroupBy, n.GroupByExprs, n.AggExprs)
	fmt.Fprintf(h, "OB:%v|Lim:%d|Off:%d|", n.OrderBy, n.LimitVal, n.OffsetVal)
	fmt.Fprintf(h, "JT:%s|JC:%s|JF:%s|LK:%v|RK:%v|", n.JoinType, n.JoinCond, n.JoinFilter, n.LeftKeys, n.RightKeys)
	fmt.Fprintf(h, "Win:%v|UA:%v|", n.WindowExprs, n.UnionAll)
	// Don't fold n.CTEName into the hash — two clones of the same CTE
	// SHARE that name, that's the whole point. The cache key in walkStages
	// already uses CTEName as a separate dimension.
	_, _ = io.WriteString(h, "C:[")
	for i, c := range n.Children {
		if i > 0 {
			_, _ = io.WriteString(h, ",")
		}
		hashLogicalNode(h, c)
	}
	_, _ = io.WriteString(h, "]|")
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
		// Compute the union of required columns in first-seen order. The
		// union lives on the CACHE entry only — each scan node keeps its
		// own RequiredColumns and catalogScanSource projects the cached
		// (union-wide) batches back down per consumer. Rewriting the scan
		// nodes to the union, as this used to do, silently widened every
		// consumer: hash-join build sides stored the union's columns and
		// spilled them on eviction (Q21's semi/anti lineitem builds).
		// A scan with no RequiredColumns needs every column: the union
		// degrades to nil (full schema).
		var merged []string
		colSet := map[string]bool{}
		needAll := false
		for _, s := range scans {
			if len(s.RequiredColumns) == 0 {
				needAll = true
				break
			}
			for _, col := range s.RequiredColumns {
				if !colSet[col] {
					colSet[col] = true
					merged = append(merged, col)
				}
			}
		}
		if needAll {
			merged = nil
		}
		// Initialize the scan cache entry.
		if p.scanCache == nil {
			p.scanCache = make(map[string]*scanCached)
		}
		p.scanCache[table] = &scanCached{unionCols: merged}
	}
}

// Plan converts a logical plan to a physical plan for local execution.
func (p *Planner) Plan(ctx context.Context, node *logical.Node) (*PhysicalPlan, error) {
	p.planCtx = ctx           // store for subquery runner context propagation
	p.releaseScanCache()      // reset per-query scan cache (drops tracker reservation)
	p.res = &queryResources{} // reset per-query spill manager + memory tracker
	p.releaseCTECache()       // reset per-query CTE cache (frees stale spill scratch)
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
		p.releaseCTECache()  // free CTE spill scratch on the no-Cleanup path
		p.releaseScanCache() // drop the scan cache's tracker reservation
		return nil, err
	}

	// Drop the columns the logical builder materialized for its own use so the
	// client sees exactly the columns it selected (#320).
	if trim := hiddenSortTrimOp(node); trim != nil {
		ops = append(ops, trim)
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
		pipelineWorkers = scanParallelism()
	}

	plan := &PhysicalPlan{
		Pipeline: &exec.Pipeline{
			Source:  source,
			Ops:     ops,
			Sink:    sink,
			Workers: pipelineWorkers,
		},
		OutputSchema: declaredOutputSchema(node),
	}
	// Hand the sink the plan's answer for the case where no batch will ever
	// tell it: a zero-row result. It is consulted only then (#416).
	if cs, ok := sink.(*exec.CollectSink); ok {
		cs.SchemaHint = plan.OutputSchema
		// Unlike SchemaHint, this is consulted on EVERY result, zero-row or
		// not: which DECIMAL columns are aggregate output is a property of
		// the PLAN, not of whether a batch arrived (FIX 2, #457/#458 fold-in).
		cs.SchemaHintWireUnconstrainedDecimal = declaredWireUnconstrainedDecimal(node)
	}

	// Attach spill file cleanup. CTE collectors and the scan cache
	// release first (tracker charge + their scratch files) so the
	// SpillManager sweep that follows never races their removal. The
	// scan cache release matters most on the shared-tracker path: its
	// reservation would otherwise outlive the query as a permanent
	// phantom on the worker-lifetime tracker.
	if sm := p.spillManagerIfSet(); sm != nil {
		plan.Cleanup = func() {
			p.releaseCTECache()
			p.releaseScanCache()
			sm.Cleanup()
		}
	} else if p.cteCacheHasCollectors() || p.scanCache != nil {
		// Shared (worker-injected) spill manager: its dir outlives this
		// query, so the collectors' scratch must be released explicitly.
		plan.Cleanup = func() {
			p.releaseCTECache()
			p.releaseScanCache()
		}
	}

	// Generate distributed stages for coordinator dispatch
	plan.Stages = p.generateStages(node)

	if err := p.enforceQueryLimits(plan.Stages, node); err != nil {
		p.releaseCTECache()
		p.releaseScanCache()
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
	// Ensure scan nodes have column metadata — needed by assignJoinKeySides
	// to assign shuffle keys to the correct child side.
	p.AnnotateScanColumns(ctx, node)
	// Per-row correlated subqueries have no distributed lowering: refuse
	// with a typed error BEFORE stage generation so the coordinator can
	// route the query onto its local single-process engine instead of the
	// scalar-deferral path silently answering 0 (#359).
	if err := p.refuseCorrelatedSubqueries(node); err != nil {
		return nil, err
	}
	// A DISTINCT with no stage and no coordinator dedup is a DROPPED
	// DISTINCT — the raw row set, returned confidently (#466). Refuse it
	// here for the same reason: loud beats silently different.
	if err := refuseUnstageableDistinct(node); err != nil {
		return nil, err
	}
	stages := p.generateStages(node)
	if p.setOpErr != nil {
		return nil, p.setOpErr
	}
	if p.joinCondErr != nil {
		return nil, p.joinCondErr
	}
	if p.correlatedErr != nil {
		return nil, p.correlatedErr
	}
	if p.inSubqueryErr != nil {
		return nil, p.inSubqueryErr
	}
	if err := p.enforceQueryLimits(stages, node); err != nil {
		return nil, err
	}
	// Phase 1 distribution-property pass: populate Stage.Distribution for
	// every stage. Phase 2 then runs EnsureDistribution to insert Exchange
	// stages where child output doesn't satisfy parent input, and asserts
	// consistency strictly.
	assignStageDistributions(stages, p.WorkerCount)
	// Collapse multi-level merge_aggregate/merge_sort trees before the
	// Exchange pass so the emitted Exchange stages are placed against
	// the final merger, not the intermediate tree levels. The tree
	// shape is a single-pipeline optimization that becomes a SF10-
	// killing N-round-trip fan-out under native-DAG dispatch.
	stages = collapseMergeTreesForNativeDAG(stages)
	// Drop redundant trailing merge_sort Singleton stages whose sole
	// dep is a Singleton sort — the merge_sort is a no-op in that
	// shape and costs a full worker round-trip per query.
	stages = collapseRedundantFinalMergeSort(stages)
	// Fuse Singleton sort into its Singleton predecessor (aggregate /
	// hash_join / broadcast_join / final_aggregate) so the worker
	// applies the sort in-process rather than serializing the
	// pre-sort output and letting a separate sort task pick it up.
	stages = fuseSortIntoPredecessor(stages, p.WorkerCount)
	var ensureErr error
	stages, ensureErr = EnsureDistribution(stages, p.WorkerCount)
	if ensureErr != nil {
		return nil, fmt.Errorf("ensure distribution: %w", ensureErr)
	}
	// Re-resolve distributions after EnsureDistribution: stages whose
	// inputs got rewritten to exchange outputs need their Distribution
	// recomputed against the new dep distributions. Without this, e.g.,
	// a grouped final_aggregate whose dep was upgraded from Singleton
	// to HashPartitioned (via an inserted exchange-repartition) would
	// keep its initial Singleton label, and dispatchComputeStage would
	// run it as one task instead of the N parallel tasks the exchange
	// is feeding (Q18 SF10 OOM trigger).
	assignStageDistributions(stages, p.WorkerCount)
	// Drop identity re-shuffles (input already hash-partitioned on the
	// exchange's exact keys/count — Q18's 40 GB repartition-15 at
	// SF100). Must run after distributions are final; consumers keep
	// valid labels because the elided exchange's output distribution
	// was by definition its input's. Kill switch WADJET_EXCHANGE_ELIDE=0.
	stages = elideCoPartitionedExchanges(stages)
	// Drop filtered scan-exchanges fully subsumed by a raw sibling over
	// the same table (Q21 l3 ⊂ l2): the raw exchange ships the filter
	// as a computed flag column and the dropped exchange's consumer
	// filters its build input on it. Kill switch
	// WADJET_EXCHANGE_SUBSUME=0.
	stages = dedupeSubsumedScanExchanges(stages)
	// Feed a grouped final_aggregate from a sibling RAW exchange
	// hash-partitioned on its exact group keys, dropping the duplicate
	// fused scan-agg leg (Q18's 2nd full lineitem scan). The rewired
	// final mirrors the raw exchange's partitioning, which typically
	// turns its downstream re-shuffle into an identity exchange — run
	// the elide pass again to collect it. Kill switch
	// WADJET_AGG_OVER_EXCHANGE=0.
	if rewired := rewireAggOverRawExchange(stages); len(rewired) != len(stages) {
		stages = elideCoPartitionedExchanges(rewired)
	}
	// Drop join subtrees that duplicate a sibling subtree (Q11's
	// scalar-subquery leg clones its main leg stage-for-stage; Q17's
	// semi lineitem⋈part ≡ its inner sibling), rewiring the clone's
	// consumers onto the survivor — stage outputs already support
	// multiple consumers. MUST run before fuseStageChains: chain
	// fusion absorbs consumers into the legs, breaking the clones'
	// structural symmetry. Kill switch WADJET_SHARED_SUBPLAN=0.
	stages = dedupeSharedSubplans(stages)
	// Fuse 1:1 same-distribution join chains (consumer task i reads
	// exactly producer task i's output) into single fragments, eliding
	// the per-link materialization — Q18's join-class 48.9 GB at
	// SF100. Runs when distributions are final so the count-equality
	// gate sees real values. No file-count amplification (task count
	// unchanged), unlike the disabled fuseScanShuffle/fuseJoinShuffle
	// below. Kill switch WADJET_STAGE_FUSION=0.
	stages = fuseStageChains(stages)
	// Narrow scan-stage OUTPUT to what consumers declare (Columns
	// stays the read set — pushed filter columns are read, applied,
	// then dropped from the payload). Runs after every stage-rewiring
	// pass so the consumer set is final. Kill switch
	// WADJET_SCAN_OUTPUT_PRUNE=0.
	pruneScanOutputColumns(stages)
	// Fuse scan→exchange-repartition pairs whose consumers all
	// partition-bind (hash/sort-merge joins, grouped finals): the scan
	// task hash-partitions its filtered output directly, deleting the
	// full write+read of the unpartitioned intermediate (2026-08-02
	// SF100 accounting: ~20 GB duplicated per cold suite run on scan
	// legs — Q03 10.0 GB, Q21 6.8 GB, Q13 2.4 GB). Re-enabled
	// 2026-08-02: the 2026-05 regressions that kept this disabled
	// (Q07 +85%, Q03 +30% — file-count amplification at broadcast
	// caches and flattening exchange readers) are excluded
	// structurally by the consumer-shape gate, and the old
	// consolidation argument no longer holds — scan and shuffle
	// fan-out are both capacity-bound now, so the fused layout's
	// per-partition file count matches the unfused two-step for
	// partition-binding consumers. Kill switch
	// WADJET_FUSE_SCAN_SHUFFLE=0.
	stages = fuseScanShuffle(stages)
	// Same treatment for join→exchange pairs (Q18 join-4→rp-6 7.6 GB,
	// Q05 join-4→rp-8 2.96 GB duplicated per SF100 run in the 08-02
	// fusion-ab pair): hash_join outputs partition directly via the
	// fragment runner's OpExchangeSender terminal, gated identically
	// (partition-binding consumers only, no computed cols; hash_join
	// only — broadcast_join fusion stays off per the 2026-05-03 Q02
	// wrong-rows history). Kill switch WADJET_FUSE_JOIN_SHUFFLE=0.
	stages = fuseJoinShuffle(stages)
	//
	// fuseScanAggregateShuffle IS enabled. Pattern: scan(FusedAgg) →
	// exchange-repartition → final_aggregate/merge_aggregate. Aggregate
	// collapses input cardinality to K rows per task, so fused output
	// scan-task-count × numPartitions matches the unfused output's
	// post-exchange-repartition file count. No amplification at
	// downstream consumers. Enables Q01/Q02/Q15/Q17/Q18/Q20-style
	// aggregation queries to skip the standalone exchange-repartition
	// stage. Gated on collapsing-consumer (final_aggregate /
	// merge_aggregate) only.
	stages = fuseScanAggregateShuffle(stages)
	// Sender-side partial aggregation on surviving exchanges (SF100 Q18's
	// 600M-row raw (l_orderkey, l_quantity) rp leg → ~4× reduction). Runs
	// after every stage-rewiring pass so the consumer set it validates is
	// final. Kill switch WADJET_EXCHANGE_PARTIAL_AGG=0.
	markExchangePartialAgg(stages)
	// Dynamic-filter pass: must run AFTER fuseScanAggregateShuffle (which
	// may absorb an exchange-repartition into a fused scan-aggregate) but
	// BEFORE AssertExchangeConsistency / ValidateNativeDAGShape so any
	// stat-dep edges we add are visible to the validators.
	stages = p.applyDynamicFilters(ctx, stages)
	// Probe-sourced build filters for semi/anti joins (Q21's raw-lineitem
	// EXISTS/NOT-EXISTS builds, Q04, Q22): the probe dep's output key set
	// prunes the build exchange before shuffle + hash build. Runs after
	// every rewiring pass (shapes final) and independent of the legacy
	// DynamicFiltersEnabled flag. Kill switch WADJET_SEMIANTI_BUILD_FILTER=0.
	stages = p.markSemiAntiBuildFilters(ctx, stages)
	// Two-hop dimension bloom cascade (nation→supplier→lineitem class):
	// transitive semijoin reduction of a fact probe scan via a tiny
	// filtered dimension riding a chained/fused build. Cardinality-capped
	// L2-resident blooms. Kill switch WADJET_DIMENSION_CASCADE=0.
	stages = p.markDimensionCascade(ctx, stages)
	// Attach-on-arrival normalization: converts consume edges whose emitter
	// chain is scan-only and whose consumer is a terminal dispatched scan
	// into non-blocking consumes (stat-dep removed; bloom installs
	// mid-scan). Must see the FINAL emit/consume state, so it runs after
	// every marking pass. Kill switch WADJET_DF_ATTACH_ON_ARRIVAL=0.
	stages = applyAttachOnArrival(stages)
	prev := BehaviorPreservingMode
	BehaviorPreservingMode = false
	defer func() { BehaviorPreservingMode = prev }()
	if err := AssertExchangeConsistency(stages); err != nil {
		// In strict mode (Phase 2 onward, or test override) this is a
		// hard failure. BehaviorPreservingMode swallows the error inside
		// AssertExchangeConsistency, so reaching this branch implies the
		// caller flipped the var.
		return nil, fmt.Errorf("exchange consistency: %w", err)
	}
	// Attach SELECT-list aliases to the Gather stage so the coordinator can
	// rename the final result schema. walkStages currently treats NodeProject
	// as a passthrough — this surfaces the user's aliases that would otherwise
	// be lost (e.g., "n1.n_name" -> "supp_nation", "substr(l_shipdate, 1, 4)"
	// -> "l_year"). Gather drives the result schema under native-DAG.
	if renames := extractOutputRenames(node); len(renames) > 0 {
		// A group key walkStages had to resolve through a subquery's rename
		// is emitted under the SOURCE column, so the rename that names the
		// result has to read from there (#355). The general case of the same
		// passthrough (#385): a source naming a NESTED Project's alias —
		// the outer SELECT merely forwarding a subquery's rename — is chased
		// to the column the streams actually carry, because no stage ever
		// applies the rename itself.
		var renameChild *logical.Node
		if pn := findOutputProjectionNode(node); pn != nil && len(pn.Children) == 1 {
			renameChild = pn.Children[0]
		}
		for i := range renames {
			if src, ok := p.aggStageRenames[strings.ToLower(renames[i].From)]; ok {
				renames[i].From = src
				continue
			}
			if renames[i].Expr == nil {
				renames[i].From = resolveOutputRenameSource(renames[i].From, renameChild)
			}
		}
		for i := range stages {
			if stages[i].Type == StageExchangeGather {
				stages[i].OutputRenames = renames
				break
			}
		}
	}
	// The gather also carries the PLAN's answer for the output schema, which
	// is what a zero-row DAG result has instead of a batch to read it off:
	// OutputRenames already gave such a result its column NAMES, and this
	// gives it their TYPES, so pgwire declares the same OIDs for an empty
	// result as for a full one (#416).
	if outSchema := declaredOutputSchema(node); len(outSchema) > 0 {
		for i := range stages {
			if stages[i].Type == StageExchangeGather {
				stages[i].OutputSchema = outSchema
				break
			}
		}
	}
	// Same PLAN-time answer as the single-process path's
	// SchemaHintWireUnconstrainedDecimal (FIX 2, #457/#458 fold-in):
	// unlike OutputSchema above, consulted on every result, not only a
	// zero-row one.
	if wireUnconstrained := declaredWireUnconstrainedDecimal(node); len(wireUnconstrained) > 0 {
		for i := range stages {
			if stages[i].Type == StageExchangeGather {
				stages[i].OutputWireUnconstrainedDecimal = wireUnconstrained
				break
			}
		}
	}
	// #169: when the SELECT list carries scalar expressions and the gather
	// reads a bare leaf scan, the expressions would never be computed —
	// applyOutputRenames can rename/drop but not evaluate. Attach the
	// SELECT list to the scan so its fragment projects it worker-side.
	attachScanSelectProjections(node, stages)
	// #424: a synthetic ORDER BY key (__sortkey_N) that the pass above did
	// not materialize — every sort but the outermost query's — still names
	// no column on the DAG. Point it at one. Runs last because the repair
	// depends on what attachScanSelectProjections did.
	resolveHiddenSortKeys(stages)
	// #467/#468: and the same repair for a key naming a DERIVED table's
	// SELECT-list alias. Runs after attachScanSelectProjections for the same
	// reason — the alias is real exactly where that pass materialized it,
	// and names the wrong column (a shadowing alias) or none at all
	// everywhere else.
	resolveDerivedAliasSortKeys(stages)
	// #423: the worker's scan reads column TYPES from the FILE, and a
	// parquet file cannot express nine of ours. Declare the catalog's
	// schema for every table this plan scans so a file written before the
	// footer key existed is still typed the way the catalog says.
	p.annotateScanSchemas(ctx, stages)
	// #491: and the same declare-on-the-plan treatment for the table's
	// merge-on-read DELETE state, replayed from the snapshot walkStages
	// took rather than a second manifest read.
	p.annotateScanDeletes(stages)
	return stages, nil
}

// GatherOutputSchema returns the plan-declared output schema carried on a
// stage DAG's terminal gather, or nil when the plan could not declare one.
//
// The coordinator calls it for the case its own answer cannot cover: a
// zero-row result has no batch to read a schema off, so `gatherSchema` over
// the gathered batches returns nil and pgwire falls back to declaring OID 25
// (text) for every column. Names already survive that case through
// OutputRenames; this is the other half (#416).
func GatherOutputSchema(stages []Stage) []parquet.Column {
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			return stages[i].OutputSchema
		}
	}
	return nil
}

// GatherOutputWireUnconstrainedDecimal is GatherOutputSchema's companion for
// the DECIMAL output columns whose PostgreSQL wire typmod must say
// "unconstrained" (-1) regardless of whether the result has rows — an
// aggregate function call, unlike a bare column reference (FIX 2,
// #457/#458 fold-in; see declaredWireUnconstrainedDecimal).
func GatherOutputWireUnconstrainedDecimal(stages []Stage) map[string]bool {
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			return stages[i].OutputWireUnconstrainedDecimal
		}
	}
	return nil
}

// attachScanSelectProjections sets ProjectExprs on a leaf scan stage when
// (a) the terminal gather's sole dependency is that scan (nothing computes
// between scan and gather) and (b) the outermost SELECT list contains at
// least one scalar expression (non-column, non-aggregate, not a wrapped
// synthetic aggregate). Expression outputs are named by their lowercased
// text — exactly the source name extractOutputRenames maps to the user's
// alias — and bare columns become passthrough entries so the fragment emits
// the full SELECT-list input set.
//
// (b) has a second trigger: a SORT KEY that names a SELECT alias the producer
// does not emit. `SELECT o_orderpriority AS p FROM orders ORDER BY p` has no
// expression at all, so the pass used to decline — the scan emitted
// "o_orderpriority", the sort keyed on "p" matched no column and silently did
// nothing, and only the gather's rename made the output *look* right (#316).
// Adding any expression to the SELECT list fixed it by accident, because that
// flipped hasExpr and the alias got materialized on the way past. The alias
// naming is decided here rather than in resolveSortKeyColumn precisely because
// this pass owns it: it runs last, and only it knows whether the producing
// fragment will carry an alias-naming OpProject.
func attachScanSelectProjections(root *logical.Node, stages []Stage) {
	projNode := findOutputProjectionNode(root)
	if projNode == nil {
		return
	}
	proj := projNode.Projections
	if len(proj) == 0 {
		return
	}
	// The types these specs carry are the DAG's only answer for a computed
	// output column — the worker's buildSelectProjection copies them straight
	// onto exec.ProjectColumn.Type. Resolving bare column references against
	// the catalog has to happen here too, or COALESCE(n_name, n_comment)
	// stays Float64 on arm B alone (#333).
	var colTypes map[string]parquet.TypeID
	var strictInt map[string]bool
	if len(projNode.Children) == 1 {
		colTypes = inputColTypes(projNode.Children[0])
		// The same integer-preserving-arithmetic hint the single-process
		// path resolves via emittedColTypes/declaredProjectionType (#297):
		// without it, `id + 1` over a strict-int column declares (and
		// COMPUTES) FLOAT64 here, where the single-process engine answers
		// INT64 for the identical SQL (#443, #445).
		strictInt = strictIntArithCols(projNode.Children[0])
	}
	hasExpr := false
	specs := make([]ProjectExprSpec, 0, len(proj))
	for _, p := range proj {
		if p.IsAgg {
			return // aggregates compute in their own fragments
		}
		expr := p.Expr
		if expr == "" {
			expr = p.Column
		}
		if expr == "" {
			return
		}
		name := strings.ToLower(expr)
		var typ parquet.TypeID
		var typeKnown bool
		if p.ASTExpr != nil && !isSimpleColRefForRename(p.ASTExpr) {
			if referencesSyntheticAgg(p.ASTExpr) {
				return // wrapped aggregate — evaluated at the gather
			}
			hasExpr = true
			typ = inferProjectionTypeCols(p.ASTExpr, parquet.TypeString, strictInt, colTypes)
			typeKnown = true
		}
		specs = append(specs, ProjectExprSpec{Expr: expr, Name: name, Type: typ, TypeKnown: typeKnown})
	}
	// #386: a NESTED subquery rename never trips anyRenamed — the outer list
	// merely forwards the alias (`SELECT k FROM (SELECT r_regionkey AS k FROM
	// region) t ORDER BY k DESC`), so the pass declined, the sort keyed on a
	// column no stage emits, and the ORDER BY silently no-oped (ASC spellings
	// passed only by scan-order luck; an alias shadowing a real column sorted
	// by the WRONG one). Resolve each simple column reference through nested
	// rename-only Projects (the #385 walk): the spec's Expr becomes the
	// SOURCE column the streams actually carry, its Name keeps the outer
	// spelling, and the substitution itself is a trigger for the pass.
	var renameChild *logical.Node
	if len(projNode.Children) == 1 {
		renameChild = projNode.Children[0]
	}
	anyNestedRename := false
	for j := range specs {
		if proj[j].ASTExpr != nil && !isSimpleColRefForRename(proj[j].ASTExpr) {
			// #387: an EXPRESSION referencing a nested rename (`k + 1` over
			// `r_regionkey AS k`) was attached verbatim, so the fragment
			// compiled it against a schema with no `k` and the task
			// hard-failed. Substitute the references in the AST (a name
			// swap on the string cannot see them), regenerate the compiled
			// text, and re-infer the type against the SOURCE schema the
			// rewritten expression now reads — the alias was invisible to
			// inputColTypes, so the spec fell back to Float64 (#333's
			// symptom one level down). The spec's NAME keeps the outer
			// text: the gather's renames and the sort's alias keys are
			// written against it. A declined rewrite (subquery/window
			// bearing, unknown node) leaves the spec untouched, keeping
			// today's loud failure over a silently different expression.
			if rewritten, ok := substituteNestedRenameRefs(proj[j].ASTExpr, renameChild); ok && rewritten != proj[j].ASTExpr {
				specs[j].Expr = rewritten.String()
				// strictIntArithColsThroughRenames mirrors the colTypes call
				// just below it: the rewritten expression names only SOURCE
				// columns, so the strict-int set to check it against is the
				// one visible BELOW the rename chain, same as #445 above.
				specs[j].Type = inferProjectionTypeCols(rewritten, parquet.TypeString,
					strictIntArithColsThroughRenames(renameChild),
					sourceColTypesThroughRenames(renameChild))
				specs[j].TypeKnown = true
				anyNestedRename = true
			}
			continue
		}
		src := resolveOutputRenameSource(specs[j].Name, renameChild)
		if strings.EqualFold(src, specs[j].Name) && strings.Contains(specs[j].Name, ".") {
			// Qualified spelling: the nested Project's alias is bare — the
			// same qualified↔bare fallback the gather applies.
			if bare := specs[j].Name[strings.LastIndexByte(specs[j].Name, '.')+1:]; bare != "" {
				if r := resolveOutputRenameSource(bare, renameChild); !strings.EqualFold(r, bare) {
					src = r
				}
			}
		}
		if !strings.EqualFold(src, specs[j].Name) {
			specs[j].Expr = src
			anyNestedRename = true
		}
	}
	// The other trigger is a sort key naming an alias, which needs the target
	// stage's keys — decided below, once the target is known.
	if !hasExpr && !anyRenamed(proj, specs) && !anyNestedRename {
		return
	}
	var gather *Stage
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			gather = &stages[i]
			break
		}
	}
	if gather == nil || len(gather.Dependencies) != 1 {
		return
	}
	// Resolve the compute target through at most one standalone sort hop:
	// scan→sort→gather (ORDER BY over a bare expression SELECT, #288 seeds
	// 231/246) needs the projection on the SCAN so the sort can resolve an
	// expression alias in its keys — the sort stage itself computes
	// nothing. The sort's keys join the coverage check below.
	targetID := gather.Dependencies[0]
	var viaSort *Stage
	for i := range stages {
		s := &stages[i]
		if s.ID == targetID && (s.Type == "sort" || s.Type == "merge_sort") && len(s.Dependencies) == 1 {
			viaSort = s
			targetID = s.Dependencies[0]
			break
		}
	}
	for i := range stages {
		s := &stages[i]
		if s.ID != targetID {
			continue
		}
		// A scan already carrying a projection (a computed subquery column
		// materialized by absorbComputedSubqueryProjection, #383) keeps it:
		// overwriting would drop the computed column the sort keys on, and
		// these SELECT-list specs are written against the subquery's
		// OUTPUT, not the scan's schema.
		if len(s.ProjectExprs) > 0 {
			return
		}
		isPlainScan := s.Type == StageScan && len(s.FusedAggGroupBy) == 0 && len(s.FusedAggSpecs) == 0
		isJoin := (s.Type == StageHashJoin || s.Type == StageBroadcastJoin || s.Type == StageSortMergeJoin) &&
			len(s.GroupByCols) == 0
		if !isPlainScan && !isJoin {
			// Something else computes between here and the gather (fused
			// scan-aggregates project via their aggregate machinery).
			return
		}
		// Direct scan→gather keeps the original #169 convention: outputs
		// named by lowercased expression text, which the gather's
		// project-mode rename maps to the user's alias. Nothing sorts here,
		// so a rename alone is no reason to project: the gather does it.
		if isPlainScan && viaSort == nil {
			if !hasExpr {
				return
			}
			s.ProjectExprs = specs
			// #387: with a nested rename substituted into the specs, the
			// fragment emits the outer SELECT's names ("k", "k + 1") — but
			// the #385 resolution already pointed the gather's From at the
			// SOURCE names the stream would have carried without this
			// projection (r_regionkey), so the rename would miss and fall
			// back to full width. Re-point each From at the name the
			// fragment now emits, exactly as the aliased path below does.
			if anyNestedRename && len(gather.OutputRenames) <= len(specs) {
				for j := range gather.OutputRenames {
					if gather.OutputRenames[j].Expr == nil {
						gather.OutputRenames[j].From = specs[j].Name
					}
				}
			}
			return
		}
		// Join feeding the gather (the #169 class on the join path), or a
		// scan/join under a standalone sort (ORDER BY over a bare
		// expression SELECT): nothing computes the SELECT expressions —
		// the gather renamed-by-expression-text, missed, and passed raw
		// columns through. Attach the SELECT list so the producing
		// fragment projects worker-side (join fragments and the scan's
		// filter-fragment path both append the OpProject).
		//
		// Unlike the direct-scan case, outputs are named by the user's
		// ALIAS when one exists: a sort — standalone, or fused into the
		// join by fuseSortIntoPredecessor — may key on the alias, and the
		// projection must emit it under that name for the sort to resolve.
		// The gather rename then finds columns already carrying final
		// names and leaves them alone (rename-only keeps exactly the
		// projected set).
		aliased := make([]ProjectExprSpec, len(specs))
		for j, sp := range specs {
			aliased[j] = sp
			if a := proj[j].Alias; a != "" {
				aliased[j].Name = strings.ToLower(a)
			}
		}
		// Every sort key — the fused sort's on a join, and the standalone
		// sort stage's — must resolve among the projection's outputs:
		// OpProject narrows the schema to exactly its projections. Bail
		// (keep old behavior) when uncovered.
		sortKeys := append([]SortKeySpec(nil), s.SortKeys...)
		if viaSort != nil {
			sortKeys = append(sortKeys, viaSort.SortKeys...)
		}
		// With no expression to compute, the projection only earns its place
		// when a sort key names an alias this stage does not emit under that
		// name — otherwise the gather's rename already covers the query and
		// narrowing the schema here would be pure cost (#316).
		if !hasExpr && !sortKeysNeedAlias(sortKeys, specs, aliased) {
			return
		}
		for _, k := range sortKeys {
			covered := false
			for _, sp := range aliased {
				if strings.EqualFold(sp.Name, k.Column) {
					covered = true
					break
				}
			}
			if !covered {
				return
			}
		}
		s.ProjectExprs = aliased
		// This fragment now emits the SELECT list under its FINAL names, so
		// the gather's source→alias pairs are stale. Not merely redundant:
		// when one item's alias shadows another item's source column
		// ("n_name AS n_comment, n_comment AS c"), the stale pair matches the
		// column this projection already renamed and renames it a second time
		// — both outputs came back named "c". Point each source at the name
		// the stage emits; the gather still projects to exactly the
		// SELECT-list set, in order, and now resolves every source instead of
		// relying on all of them missing.
		// The rename list carries only the VISIBLE select items, so it can be
		// shorter than the projection when the plan materialized an ORDER BY
		// term (#320). Hidden columns are appended last precisely so the
		// leading indices still line up — and leaving them unnamed here is
		// what drops them: the gather projects to exactly the names it lists.
		if len(gather.OutputRenames) <= len(aliased) {
			for j := range gather.OutputRenames {
				if gather.OutputRenames[j].Expr == nil {
					gather.OutputRenames[j].From = aliased[j].Name
				}
			}
		}
		return
	}
}

// anyRenamed reports whether any SELECT-list item carries an alias that
// differs from the name its producing stage would emit — i.e. whether an
// alias-naming projection could make any difference at all. Cheap pre-gate
// for attachScanSelectProjections: with no rename and no expression there is
// nothing for it to do, and it can decline before looking at any stage.
func anyRenamed(proj []logical.Projection, specs []ProjectExprSpec) bool {
	for j, p := range proj {
		if p.Alias != "" && !strings.EqualFold(p.Alias, specs[j].Name) {
			return true
		}
	}
	return false
}

// sortKeysNeedAlias reports whether any sort key names a SELECT-list alias
// that the producing stage does not emit under that name — the #316
// condition. specs[j].Expr carries what the stage emits without an
// alias-naming projection (the source column after nested-rename resolution,
// or the expression text); aliased[j] carries the user's alias. A key
// matching an alias whose source is spelled differently would find no column
// at all, or — when the alias shadows another column of the input — the
// WRONG one, so the sort must be given the projection that materializes the
// alias. Comparing the key against Expr rather than Name is what lets a
// NESTED rename trip the condition (#386): there the outer list has no alias
// of its own, so Name equals the key, but the stream carries the resolved
// source column.
func sortKeysNeedAlias(sortKeys []SortKeySpec, specs, aliased []ProjectExprSpec) bool {
	for _, k := range sortKeys {
		for j := range aliased {
			if strings.EqualFold(aliased[j].Name, k.Column) &&
				!strings.EqualFold(specs[j].Expr, k.Column) {
				return true
			}
		}
	}
	return false
}

// extractOutputRenames inspects the logical plan tree's outermost projection
// node and returns one (source-column → alias) pair per SELECT-list item — in
// SELECT-list order — describing the final output schema. The coordinator
// uses this list both to RENAME columns AND to DROP columns the worker
// emitted but the user didn't ask for (e.g., Q15's join output carries
// supplier/lineitem internals that the SELECT list doesn't project).
//
// For aggregate columns, the source equals the alias (planner sets
// AggSpec.OutputCol to the alias). Wrapped aggregates ("SUM(x)/7.0 AS x")
// still aren't handled here — the worker emits the raw aggregate, and
// applying the divisor needs a post-aggregate Project. Wrapped-aggregate
// projections are passed through with their source pointing at the wrapped
// expression text so that at least the rename is attempted (it'll miss
// gracefully and the column drops, surfacing the bug clearly in tests).
//
// Returns nil when the outermost emitting node isn't a projection (e.g.,
// top-level scan or aggregate without a SELECT-list rename layer).
func extractOutputRenames(root *logical.Node) []OutputRename {
	// Hidden projections are the planner's own — a materialized ORDER BY term
	// the SELECT list does not carry (#320). Leaving them out of the rename
	// list is what drops them from the client's result: the gather projects to
	// exactly the columns named here.
	proj := logical.VisibleProjections(findOutputProjectionsForRename(root))
	if len(proj) == 0 {
		return nil
	}
	renames := make([]OutputRename, 0, len(proj))
	for _, p := range proj {
		var src, target string
		var astExpr plansql.Node
		switch {
		case p.IsAgg:
			// AggSpec.OutputCol == alias; if no alias, fall back to expr.
			target = p.Alias
			if target == "" {
				target = strings.ToLower(p.Expr)
			}
			src = target
		case p.ASTExpr != nil && !isSimpleColRefForRename(p.ASTExpr) && referencesSyntheticAgg(p.ASTExpr):
			// Wrapped aggregate — the logical layer replaced aggregate calls
			// with ColRefs to their __agg_N synthetic columns. Compile+eval
			// at gather time so the divisor (e.g. "/7.0" in Q17's avg_yearly)
			// gets applied. We restrict this to expressions that reference
			// __agg_N because pure scalar expressions like SUBSTR(o_orderdate,
			// 1, 4) are computed by the worker's GROUP BY / project pipeline
			// and surface as a column under the expression's lowercased text
			// — those need a plain rename, not eval (and eval would mistype
			// SUBSTR's string output as float64).
			target = p.Alias
			if target == "" {
				target = strings.ToLower(p.Expr)
			}
			astExpr = p.ASTExpr
			src = firstColRefName(p.ASTExpr)
		case p.Column != "" && p.Alias != "":
			// Bare column reference. Worker may emit qualified ("n1.n_name")
			// or unqualified ("n_name") depending on the upstream join chain.
			// Prefer Expr (qualified-preserving) when it's a colref, else Column.
			src = p.Column
			if p.Expr != "" {
				src = strings.ToLower(p.Expr)
			}
			target = p.Alias
		case p.Expr != "":
			src = strings.ToLower(p.Expr)
			target = p.Alias
			if target == "" {
				target = src
				// No AS on a plain column reference: SQL names the output
				// column after the COLUMN, not the qualified reference the
				// user typed — `SELECT d.label` is a column named "label",
				// which is what the single-process path returns. Projection
				// .Column is the parser's unqualified name and is set only
				// for a colref, so this branch is exactly that shape. The
				// SOURCE stays the qualified Expr; the gather resolves it
				// through the same qualified↔bare fallback everything else
				// uses.
				if p.Column != "" {
					target = p.Column
				}
			}
		case p.Column != "":
			src = p.Column
			target = src
		default:
			continue
		}
		renames = append(renames, OutputRename{From: src, To: target, Expr: astExpr})
	}
	return renames
}

// isSimpleColRefForRename is the OutputRenames-specific variant of
// isSimpleColRef. The base helper at line 4308 also returns true for Lit
// nodes; here we only want to skip the eval path when the projection is
// strictly a column reference (or a parenthesized one).
func isSimpleColRefForRename(n plansql.Node) bool {
	if n == nil {
		return false
	}
	if _, ok := n.(*plansql.ColRef); ok {
		return true
	}
	if p, ok := n.(*plansql.ParenNode); ok {
		return isSimpleColRefForRename(p.Inner)
	}
	return false
}

// referencesSyntheticAgg reports whether an AST contains any ColRef whose
// name starts with "__agg_" — the marker the logical layer's nested-
// aggregate rewrite uses for synthetic column names. Lets the gather rewrite
// distinguish "SUM(x)/7.0" (rewritten to "__agg_0/7.0", needs eval) from
// "SUBSTR(o_orderdate, 1, 4)" (worker-computed, needs rename).
func referencesSyntheticAgg(n plansql.Node) bool {
	if n == nil {
		return false
	}
	switch x := n.(type) {
	case *plansql.ColRef:
		return strings.HasPrefix(x.Column, "__agg_")
	case *plansql.BinaryOp:
		return referencesSyntheticAgg(x.Left) || referencesSyntheticAgg(x.Right)
	case *plansql.UnaryOp:
		return referencesSyntheticAgg(x.Inner)
	case *plansql.CmpExpr:
		return referencesSyntheticAgg(x.Left) || referencesSyntheticAgg(x.Right)
	case *plansql.ParenNode:
		return referencesSyntheticAgg(x.Inner)
	case *plansql.FuncCallNode:
		for _, a := range x.Args {
			if referencesSyntheticAgg(a) {
				return true
			}
		}
	case *plansql.CastNode:
		return referencesSyntheticAgg(x.Inner)
	case *plansql.CaseNode:
		if referencesSyntheticAgg(x.Subject) {
			return true
		}
		for _, w := range x.Whens {
			if referencesSyntheticAgg(w.Cond) || referencesSyntheticAgg(w.Result) {
				return true
			}
		}
		return referencesSyntheticAgg(x.Else)
	}
	return false
}

// firstColRefName returns the first column reference name in an AST, used as
// the existence anchor for the gather rewrite. Returns "" when no ColRef is
// found (very rare — pure-literal projections).
func firstColRefName(n plansql.Node) string {
	if n == nil {
		return ""
	}
	switch x := n.(type) {
	case *plansql.ColRef:
		if x.Table != "" {
			return x.Table + "." + x.Column
		}
		return x.Column
	case *plansql.BinaryOp:
		if v := firstColRefName(x.Left); v != "" {
			return v
		}
		return firstColRefName(x.Right)
	case *plansql.UnaryOp:
		return firstColRefName(x.Inner)
	case *plansql.CmpExpr:
		if v := firstColRefName(x.Left); v != "" {
			return v
		}
		return firstColRefName(x.Right)
	case *plansql.ParenNode:
		return firstColRefName(x.Inner)
	case *plansql.FuncCallNode:
		for _, a := range x.Args {
			if v := firstColRefName(a); v != "" {
				return v
			}
		}
	case *plansql.CastNode:
		return firstColRefName(x.Inner)
	}
	return ""
}

// findOutputProjectionsForRename walks down through Sort/Limit/Filter wrappers
// to the outermost NodeProject and returns its projections. Returns nil when
// the outermost emitting node is not a projection (e.g., a top-level scan or
// aggregate without a SELECT-list rename layer).
func findOutputProjectionsForRename(n *logical.Node) []logical.Projection {
	if p := findOutputProjectionNode(n); p != nil {
		return p.Projections
	}
	return nil
}

// findOutputProjectionNode is findOutputProjectionsForRename returning the
// Project node itself, for callers that also need what feeds it — typing a
// projection expression takes the input's column types (inputColTypes).
func findOutputProjectionNode(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeProject:
			return n
		case logical.NodeSort, logical.NodeLimit, logical.NodeFilter, logical.NodeDistinct:
			// Descend through Distinct too: the gather must project to the
			// SELECT-list columns (the Project under the Distinct) so the
			// coordinator's distinct dedup runs over the output columns, not
			// the full upstream schema (#163).
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
			return nil
		default:
			return nil
		}
	}
	return nil
}

// hiddenSortTrimOp returns the projection that drops a materialized ORDER BY
// term from the single-process pipeline's output, or nil when the plan carries
// none.
//
// The DAG drops these at the gather: extractOutputRenames lists only the
// visible select items and the gather projects to exactly that set. The
// single-process pipeline has no equivalent stage — its result columns are the
// sink's schema — so without this the Sort would hand __sortkey_N straight to
// the client alongside the columns the query asked for. The projection below
// the Sort is never elided when it carries a hidden column (its alias always
// differs from its source, which is what buildProject's needsProject test
// looks for), so the names resolved here are the ones the pipeline emits.
func hiddenSortTrimOp(root *logical.Node) exec.UnaryOperator {
	projs := findOutputProjectionsForRename(root)
	if !logical.HasHiddenProjection(projs) {
		return nil
	}
	visible := logical.VisibleProjections(projs)
	cols := make([]exec.ProjectColumn, 0, len(visible))
	for i, p := range visible {
		// Same output naming buildProject applies: the alias, else the column
		// reference, else the expression text.
		name := p.Alias
		if name == "" {
			name = p.Column
		}
		if name == "" {
			name = cleanExpr(p.Expr)
		}
		if name == "" || name == "*" || strings.HasSuffix(name, ".*") {
			// An unexpanded star (no catalog to resolve it against) has no
			// column list to trim to. Leave the plan alone: an extra column in
			// the result beats projecting every row to nulls.
			return nil
		}
		cols = append(cols, exec.ProjectColumn{
			Name: name,
			// POSITIONAL, not by name. The trim is a narrowing: hidden
			// columns go LAST and stay last (logical.resolveOrderBy keeps
			// that invariant for the gather's benefit too), so visible
			// output i IS input column i. Copying by name instead gave two
			// same-named outputs — which `SELECT abs(a), abs(b)` now
			// legitimately produces, PostgreSQL calling both `abs` — the
			// SAME input column, so the second carried the first's values.
			SourceIdx:    i,
			SourceIdxSet: true,
			// The name-based fields stay as the fallback for an input whose
			// column count does not match (nothing produces one today, and
			// an extra column beats projecting every row to nulls).
			DirectCopy: name,
			SourceCol:  name,
			// DirectCopy resolution can still miss on a qualified/bare
			// mismatch; ColumnRef resolves lazily and keeps Project.Execute
			// from invoking a nil Expr.
			Expr: exec.ColumnRef(name),
		})
	}
	return exec.NewProject(cols)
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

// ShuffleCandidate describes a join in the plan whose build side is large
// enough to warrant the shuffle execution path instead of broadcast.
type ShuffleCandidate struct {
	JoinStageID string   // the join stage to be served by shuffled inputs
	BuildAlias  string   // which scan stage produces the build side
	ProbeAlias  string   // which scan stage produces the probe side (the largest scan)
	BuildKeys   []string // build-side join keys (the join's JoinRightKeys)
	ProbeKeys   []string // probe-side join keys (the join's JoinLeftKeys)
	JoinKeys    []string // canonical (build-side) join key names for partitioning
	BuildBytes  int64    // EstimatedBytes of the build scan (for logging)
}

// PickShuffleCandidate identifies the largest non-probe scan above
// thresholdBytes as the shuffle candidate — the table that would otherwise be
// broadcast-duplicated as the runtime build side — and returns the join stage
// that connects it to the probe.
//
// The approach deliberately does NOT read BuildTableAlias on the join stage
// because in probe-split mode the planner's logical build/probe assignment is
// inverted at runtime: the planner labels the largest scan as the build (e.g.
// "lineitem"), but probe-split partitions that scan across workers, making the
// second-largest scan (e.g. "orders") the actual broadcast hash table.
// Shuffling orders instead of broadcasting it is the correction.
//
// Algorithm:
//  1. probeAlias = largest scan (matches CanProbeSplit's heuristic).
//  2. candidate = largest non-probe scan above thresholdBytes.
//  3. Walk join stages to find one that directly references the candidate
//     scan (via LeftDepStage or RightDepStage) or via a FusedJoin entry
//     whose BuildTableAlias matches the candidate alias.
//  4. Extract build/probe keys from the matching join or fused-join entry.
//
// Phase 1: returns the single best candidate. Phase 2 (chained shuffles)
// will return all candidates.
func PickShuffleCandidate(stages []Stage, thresholdBytes int64) (ShuffleCandidate, bool) {
	// stage-id → stage lookup.
	byID := map[string]Stage{}
	for _, s := range stages {
		byID[s.ID] = s
	}

	// Step 1: identify probe alias (largest scan, mirrors CanProbeSplit).
	var probeAlias string
	var probeBytes int64
	for _, s := range stages {
		if s.Type == "scan" && s.EstimatedBytes > probeBytes {
			probeAlias = s.ScanAlias
			probeBytes = s.EstimatedBytes
		}
	}

	// Step 2: find the largest non-probe scan above the threshold.
	var candidateScan Stage
	var candidateScanID string
	var candidateFound bool
	for _, s := range stages {
		if s.Type != "scan" || s.ScanAlias == probeAlias {
			continue
		}
		if s.EstimatedBytes <= thresholdBytes {
			continue
		}
		if !candidateFound || s.EstimatedBytes > candidateScan.EstimatedBytes {
			candidateScan = s
			candidateScanID = s.ID
			candidateFound = true
		}
	}
	if !candidateFound {
		return ShuffleCandidate{}, false
	}

	// candidateCols is the set of column names the candidate scan exposes in its
	// raw Parquet files. Used to validate that join keys are actually rooted in
	// the candidate scan and not in a fused-join output (e.g. Q10: orders is the
	// left dep of join-4, but join-4's left key c_nationkey comes from the fused
	// customer join, not from orders' Parquet files).
	// If Columns is empty (not annotated), validation is skipped and the original
	// dep-match logic applies unchanged.
	candidateCols := make(map[string]bool, len(candidateScan.Columns))
	for _, c := range candidateScan.Columns {
		candidateCols[c] = true
	}
	// keysInCols returns true iff all keys are present in cols.
	// Returns true when cols is empty (no annotation = skip validation).
	keysInCols := func(keys []string, cols map[string]bool) bool {
		if len(cols) == 0 {
			// No column annotation: cannot validate, assume valid.
			return true
		}
		if len(keys) == 0 {
			return false
		}
		for _, k := range keys {
			if !cols[k] {
				return false
			}
		}
		return true
	}

	// probeCols is the set of column names the probe scan exposes in its raw
	// Parquet files. Used to validate that probe-side keys are directly readable
	// from the probe table, not from an intermediate join output.
	probeCols := make(map[string]bool)
	for _, s := range stages {
		if s.Type == "scan" && s.ScanAlias == probeAlias {
			for _, c := range s.Columns {
				probeCols[c] = true
			}
			break
		}
	}

	// Step 3: walk join stages to find the one referencing the candidate.
	for _, j := range stages {
		if j.Type != "hash_join" && j.Type != "broadcast_join" {
			continue
		}

		// Direct left-dep match: candidate is on the left (probe) side of the join.
		// Guard: the join's left keys must actually live in the candidate scan's raw
		// Parquet columns. If they don't (e.g. Q10: join-4 has LeftDepStage=orders
		// but JoinLeftKeys=[c_nationkey] which originates from the fused customer
		// join, not from orders files), skip this match and let the column-anchored
		// fallback below find the correct join.
		if j.LeftDepStage == candidateScanID && keysInCols(j.JoinLeftKeys, candidateCols) {
			return ShuffleCandidate{
				JoinStageID: j.ID,
				BuildAlias:  candidateScan.ScanAlias,
				ProbeAlias:  probeAlias,
				BuildKeys:   append([]string(nil), j.JoinLeftKeys...),
				ProbeKeys:   append([]string(nil), j.JoinRightKeys...),
				JoinKeys:    append([]string(nil), j.JoinLeftKeys...),
				BuildBytes:  candidateScan.EstimatedBytes,
			}, true
		}

		// Direct right-dep match: candidate is on the right (build) side of the join.
		// The left dep must be the probe scan directly (not an intermediate join)
		// to ensure JoinLeftKeys map to raw probe Parquet columns.
		if j.RightDepStage == candidateScanID {
			probeScanID := ""
			for _, s := range stages {
				if s.Type == "scan" && s.ScanAlias == probeAlias {
					probeScanID = s.ID
					break
				}
			}
			if j.LeftDepStage != probeScanID {
				continue
			}
			return ShuffleCandidate{
				JoinStageID: j.ID,
				BuildAlias:  candidateScan.ScanAlias,
				ProbeAlias:  probeAlias,
				BuildKeys:   append([]string(nil), j.JoinRightKeys...),
				ProbeKeys:   append([]string(nil), j.JoinLeftKeys...),
				JoinKeys:    append([]string(nil), j.JoinRightKeys...),
				BuildBytes:  candidateScan.EstimatedBytes,
			}, true
		}

		// FusedJoin match: candidate is the build of a secondary join fused into
		// this join stage. This covers Q03's shape where orders is not the top-level
		// join's direct dep but is referenced as a fused build.
		for _, fj := range j.FusedJoins {
			if fj.BuildTableAlias == candidateScan.ScanAlias {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), fj.JoinRightKeys...),
					ProbeKeys:   append([]string(nil), fj.JoinLeftKeys...),
					JoinKeys:    append([]string(nil), fj.JoinRightKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
		}
	}

	// Column-anchored fallback: the candidate appears as a left dep somewhere in
	// the join chain, but the matching join's keys were not valid for the raw
	// candidate scan (e.g. Q10: orders appears as the left dep of join-4 but with
	// c_nationkey keys from a fused join output). Find any join where:
	//   - JoinLeftKeys are all present in the candidate scan's raw columns, AND
	//   - JoinRightKeys are all present in the probe scan's raw columns.
	// This anchors both sides to their actual Parquet files regardless of the
	// dep-chain topology.
	if len(candidateCols) > 0 && len(probeCols) > 0 {
		for _, j := range stages {
			if j.Type != "hash_join" && j.Type != "broadcast_join" {
				continue
			}
			if keysInCols(j.JoinLeftKeys, candidateCols) && keysInCols(j.JoinRightKeys, probeCols) {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), j.JoinLeftKeys...),
					ProbeKeys:   append([]string(nil), j.JoinRightKeys...),
					JoinKeys:    append([]string(nil), j.JoinLeftKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
			// Also check if the keys are swapped: candidate provides the right
			// keys and probe provides the left keys.
			if keysInCols(j.JoinRightKeys, candidateCols) && keysInCols(j.JoinLeftKeys, probeCols) {
				return ShuffleCandidate{
					JoinStageID: j.ID,
					BuildAlias:  candidateScan.ScanAlias,
					ProbeAlias:  probeAlias,
					BuildKeys:   append([]string(nil), j.JoinRightKeys...),
					ProbeKeys:   append([]string(nil), j.JoinLeftKeys...),
					JoinKeys:    append([]string(nil), j.JoinRightKeys...),
					BuildBytes:  candidateScan.EstimatedBytes,
				}, true
			}
		}
	}

	// No join stage references the candidate.
	return ShuffleCandidate{}, false
}

// LargeBuildScans returns scan stages that are build-side (not the probe alias)
// and whose estimated size exceeds the given threshold. These are candidates for
// the build-side broadcast cache: the coordinator pre-scans them once, caches the
// result in S3, and each worker loads the shared cache instead of independently
// scanning the large source table N times.
//
// The cache provides two wins for queries with selective build-side filters
// or wide build tables:
//
//  1. Avoids decoding the source parquet on every worker (parquet decode is
//     CPU-expensive; the cached WSHF format is essentially raw typed bytes
//     and reads in a fraction of the time).
//  2. Lets the planner overlap the slow source scan with the rest of the
//     query once instead of N times.
//
// We previously gated this on len(large) >= 2 ("only cache when multiple
// large builds would compound a worker's hash table footprint"), reasoning
// that single-large-build queries can fit one hash table in memory and the
// cache only adds spill+upload latency. SF100 deploy disproved that: Q07's
// historical 3m13s was caching orders, and skipping it pushed the same
// query past 19 minutes (workers stuck spilling/scanning parquet 3 times).
// The win from caching orders comes mostly from amortising parquet decode,
// not from memory deduplication.
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
		if s.Type == "hash_join" || s.Type == "broadcast_join" || s.Type == StageSortMergeJoin {
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

// needsLimitStage reports whether this NodeLimit needs a StageLimit of its
// own, given whether walkStages just handed its bound to a sort stage.
//
// Exactly three things can bound a stream on the DAG, and only one of them
// applies to any given LIMIT:
//
//   - The coordinator's post-gather pass (`mi.Limit`/`mi.Offset` in
//     ExecuteSQL). It reads `logical.ExtractMergeInfo`, which inspects the
//     PLAN ROOT and nothing else — so it reaches a top-level LIMIT and no
//     other.
//   - A sort stage's top-N. It needs an ORDER BY below the LIMIT, and it
//     truncates to limit+OFFSET rather than skipping, because the OFFSET is
//     the coordinator's job in the shape it was written for. So it covers a
//     sorted LIMIT with no OFFSET that NO LOWER LIMIT has already claimed,
//     and only that.
//   - This stage.
//
// "Disjoint" is a property of the OWNERSHIP RULE, not of the shapes: two
// LIMITs in one query can both want the same sort stage, and until #525 the
// outer one took it — overwriting the inner's bound and then suppressing its
// own stage because it had just found a sort. walkStages' backwards scan
// stops at a claimed sort for that reason, so `sorted` here means "a sort
// stage carries THIS limit", never "there is a sort somewhere below".
//
// A LIMIT the first two miss bounded NOTHING before #478: `SELECT COUNT(*)
// FROM (SELECT DISTINCT k FROM t LIMIT 2) u` counted every distinct k, and
// its plain and explicit-GROUP-BY twins did the same. Silent, deterministic,
// and dependent only on how much data sits behind the query.
//
// The root case is left exactly as it was rather than moved onto this stage:
// the coordinator's pass is correct there, and emitting a stage as well would
// apply the OFFSET twice.
func (p *Planner) needsLimitStage(node *logical.Node, sorted bool) bool {
	if node == p.limitStageRoot {
		return false // the coordinator's post-gather pass owns this one
	}
	if sorted && node.OffsetVal == 0 {
		return false // the sort stage's top-N is already the global bound
	}
	// An OFFSET alone still needs a stage: nothing below skips rows.
	return node.LimitVal != logical.NoLimit || node.OffsetVal > 0
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
				var fileSizes []int64
				for _, part := range manifest.Partitions {
					if len(stage.PartitionFilter) > 0 && len(part.Values) > 0 {
						if !matchesPartitionFilter(part.Values, stage.PartitionFilter) {
							continue
						}
					}
					for _, f := range part.Files {
						files = append(files, f.Path)
						fileSizes = append(fileSizes, f.SizeBytes)
					}
				}
				newStage.ScanFiles = files
				newStage.ScanFileSizes = fileSizes
				// The remote cluster's own delete state — the local one
				// names files this stage will never read.
				newStage.ScanDeletes = deleteMarkerMap(manifest.DeleteMarkers)
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
	// Fresh CTE dedup cache per query — walkStages populates it as it
	// encounters CTE-named subtrees and consults it on subsequent walks
	// (typically when emitScalarProducerStages re-walks a CTE-referencing
	// scalar subquery, which would otherwise double-compute the CTE).
	p.ctePlannedTerminal = make(map[string]string)
	p.scanDeletes = nil
	p.limitStageRoot = node
	p.setOpErr = nil
	p.joinCondErr = nil
	p.correlatedErr = nil
	p.inSubqueryErr = nil
	p.aggStageRenames = nil
	p.walkStages(node, &stages, nil)
	// Resolve cte-alias phantoms emitted by walkStages dedup. Must happen
	// before fuseJoinStages so fusion sees real stage IDs everywhere.
	stages = flattenCTEAliases(stages)
	// Broadcast-join fusion absorbs a leaf broadcast_join into its consumer
	// join's FusedJoins list, so a chain of N broadcast joins runs as ONE
	// task that builds N hash tables and pipelines probes batch-by-batch
	// through them — instead of N separate stages with an S3 round-trip
	// between each, single-tasked all the way down because the chain root
	// probe is a small dimension table.
	//
	// Previously gated to only the legacy single-pipeline executor — the
	// native-DAG validator rejected fused shapes because executeStageHashJoin
	// only handled 2 deps and didn't read task.FusedJoins. Both are fixed:
	//   - native_dag_rewrite.go's validator now allows 2+N deps when N
	//     FusedJoins are present.
	//   - executor_stage.go:executeStageHashJoin builds a hash table per
	//     FusedJoin entry and chains their Probe operators in the pipeline.
	//   - dispatchComputeStage translates planner-side FusedJoinSpec
	//     (BuildDepStage) into wire-format FusedJoinSpec (BuildFiles).
	if p.WorkerCount > 1 {
		stages = fuseJoinStages(stages)
	}
	markCoPathingSelfJoinBuilds(stages)
	return stages
}

// markCoPathingSelfJoinBuilds finds joins whose build-side scans target
// the same source table AND whose forward-reachable stage sets intersect
// (one join transitively depends on the other), then sets
// QualifyAllBuildCols on both. This is the planner half of the Q07
// self-join column-disambiguation fix.
//
// The narrower "co-pathing" check is required because counting same-table
// scans across the WHOLE plan also catches scalar-subquery scans (Q02, Q17)
// and CTE-producer scans (Q15) that are independent of the outer join chain
// — qualifying their unrelated joins broke those queries in the first
// attempt.
func markCoPathingSelfJoinBuilds(stages []Stage) {
	stageByID := make(map[string]*Stage, len(stages))
	for i := range stages {
		stageByID[stages[i].ID] = &stages[i]
	}

	// For each join stage, walk its build-side dep chain (transitively
	// through any Exchange wrappers) to the underlying scan and record
	// (joinIdx, scanTableName).
	//
	// The walk deliberately does NOT branch into both sides of a join stage
	// encountered mid-chain (bushy builds). An attempted extension that did
	// (2026-07-09) surfaced same-table scans from Q02/Q20's decorrelated
	// subquery chains and force-qualified joins whose downstream consumers
	// reference bare names — 0 rows. Bushy self-join collisions are instead
	// qualified at the join where the collision occurs, via isDup +
	// BuildColOrigins in joinOutputSchemaWithMapping. If a bushy shape ever
	// needs Q07-style cross-chain force-qualification, decide it from the
	// LOGICAL tree's exact output visibility (subtreeNaming), not from a
	// stage-DAG walk.
	type joinScan struct {
		joinIdx   int
		joinID    string
		tableName string
	}
	var joinScans []joinScan
	for i := range stages {
		s := &stages[i]
		if s.Type != StageHashJoin && s.Type != StageBroadcastJoin && s.Type != StageSortMergeJoin {
			continue
		}
		buildDep := s.RightDepStage
		if buildDep == "" {
			continue
		}
		// Walk through Exchange wrappers (one or two levels) to the scan.
		cur := stageByID[buildDep]
		for hop := 0; cur != nil && cur.Type != StageScan && hop < 3; hop++ {
			if len(cur.Dependencies) == 0 {
				cur = nil
				break
			}
			cur = stageByID[cur.Dependencies[0]]
		}
		if cur == nil || cur.Type != StageScan || cur.TableName == "" {
			continue
		}
		joinScans = append(joinScans, joinScan{joinIdx: i, joinID: s.ID, tableName: cur.TableName})
	}

	if len(joinScans) < 2 {
		return
	}

	// reachable[X] = set of stage IDs reachable forward from X (X's transitive
	// consumers). Computed by running BFS from each join via reverse-deps.
	consumers := make(map[string][]string, len(stages))
	for i := range stages {
		for _, dep := range stages[i].Dependencies {
			consumers[dep] = append(consumers[dep], stages[i].ID)
		}
	}
	reachable := make(map[string]map[string]bool, len(joinScans))
	for _, js := range joinScans {
		reach := map[string]bool{js.joinID: true}
		stack := []string{js.joinID}
		for len(stack) > 0 {
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, c := range consumers[n] {
				if !reach[c] {
					reach[c] = true
					stack = append(stack, c)
				}
			}
		}
		reachable[js.joinID] = reach
	}

	// Mark a join when another join over the SAME table is in its forward
	// reachable set OR vice versa — i.e. the two are in the same join
	// chain.
	//
	// NOTE (bushy, 2026-07-09): generalizing this to "reachable sets
	// intersect" (parallel branches meeting downstream) was tried and
	// REVERTED — it force-qualified Q02's outer/scalar-subquery partsupp
	// pair (parallel branches meeting at the scalar join) whose consumers
	// reference bare names, breaking flag-OFF Q02. Bushy self-join
	// disambiguation is handled where the copies actually collide, via
	// isDup + BuildColOrigins in the join executor.
	for i := range joinScans {
		for j := range joinScans {
			if i == j {
				continue
			}
			a, b := joinScans[i], joinScans[j]
			if a.tableName != b.tableName {
				continue
			}
			if reachable[a.joinID][b.joinID] || reachable[b.joinID][a.joinID] {
				stages[a.joinIdx].QualifyAllBuildCols = true
				stages[b.joinIdx].QualifyAllBuildCols = true
			}
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

// buildSideBytes returns the EstimatedBytes of the build-side scan reachable
// from a broadcast_join's RightDepStage. Walks one level (the typical shape:
// join → exchange-replicate → scan) and accepts the direct case
// (join → scan). Returns 0 when the build subtree shape is unknown — callers
// should treat 0 as "no info; allow fusion" so fuseJoinStages doesn't
// over-restrict on edges its heuristic doesn't model.
func buildSideBytes(stages []Stage, joinStage *Stage) int64 {
	if joinStage == nil {
		return 0
	}
	buildDep := joinStage.RightDepStage
	if buildDep == "" {
		return 0
	}
	for _, s := range stages {
		if s.ID != buildDep {
			continue
		}
		if s.Type == "scan" {
			return s.EstimatedBytes
		}
		// Walk through one level of exchange-replicate to its underlying scan.
		if s.Type == StageExchangeReplicate && len(s.Dependencies) == 1 {
			for _, t := range stages {
				if t.ID == s.Dependencies[0] && t.Type == "scan" {
					return t.EstimatedBytes
				}
			}
		}
		return 0
	}
	return 0
}

// fuseJoinStages absorbs broadcast join stages into their downstream consumer
// join stage when the broadcast join's output feeds directly as the probe or
// build side of another join. This avoids materializing the intermediate result
// to S3 and re-reading it — the worker chains probes batch-by-batch instead.
//
// Example: join-A (lineitem ⨝ part) → join-B (broadcast_join with nation)
// becomes: join-A with FusedJoins=[nation join spec], join-B removed.
//
// # FUSION DEPTH AND BYTE BUDGET
//
// Multi-level fusion is allowed when the cumulative build-side bytes on the
// consumer (its primary build, if broadcast_join, plus every existing fused
// build, plus the candidate's primary build, plus the candidate's existing
// fused builds) stays below maxFusedBuildBytes. The check exists because
// under probe-split each shard task loads ALL caches the fused stage
// references — a chain of M fused builds means each shard reads M cache
// files, so the cluster-wide S3/store amplification is workerCount × M ×
// per-build-bytes. Capping the cumulative byte total caps the amplification
// regardless of M.
//
// Pre-2026-04-30 the implementation hard-capped at depth 1 (a single fused
// entry per consumer) by skipping any candidate that already had FusedJoins.
// That excluded star-schema queries with multiple tiny dimension tables
// (nation, region, supplier) where the cumulative build size is well under
// the budget but the dispatch overhead of N separate broadcast stages is the
// dominant cost. The new cumulative-bytes check fails closed: when
// EstimatedBytes is unknown for any participant we treat it as conservative
// and do NOT extend the fusion past depth 1.
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

	// cumulativeBuildBytes is the sum of EstimatedBytes for every cache the
	// given stage will load: its primary build (only for broadcast_join — a
	// hash_join shuffles its build side), plus the build-side scan behind
	// each entry in FusedJoins. Returns -1 when ANY participant has unknown
	// (zero) EstimatedBytes — forces the conservative path so we don't
	// silently approve a chain that might exceed the budget.
	cumulativeBuildBytes := func(s *Stage) int64 {
		var total int64
		if s.Type == "broadcast_join" {
			pb := buildSideBytes(stages, s)
			if pb <= 0 {
				return -1
			}
			total += pb
		}
		for _, fj := range s.FusedJoins {
			b := fusedSpecBuildBytes(stages, fj)
			if b <= 0 {
				return -1
			}
			total += b
		}
		return total
	}

	for i := range stages {
		s := &stages[i]
		if s.Type != "broadcast_join" {
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
		// A null-aware anti join carries a semantics flag (#507) that the
		// absorbed-join spec has no field for, and absorbing it would answer
		// the two-valued question instead. FusedJoinSpec can grow the field
		// the day a shape needs it; until then, not fusing is the honest
		// alternative to carrying it silently.
		if s.NullAwareAnti || consumer.NullAwareAnti {
			continue
		}
		// The candidate's output must feed the consumer's PROBE side.
		// Fusion replays the absorbed join as a broadcast-probe step on the
		// consumer's probe STREAM — valid only when the absorbed output IS
		// that stream. A candidate feeding the consumer's BUILD side (bushy
		// composite builds) would replay its probe steps against the wrong
		// stream: Q07/Q08-class chains returned 0 rows (2026-07-09).
		if consumer.LeftDepStage != s.ID {
			continue
		}

		// Cumulative byte budget: cap total cache amplification across the
		// fused chain. Required because each probe-split shard loads every
		// cache the consumer references — see comment on fuseJoinStages.
		// Backwards-compat fallback: when the candidate has no existing
		// FusedJoins (the historical depth-1 case) we still allow the fuse
		// based only on the candidate's primary build size, even if the
		// consumer's existing chain has unknown bytes — preserves the
		// historical fusion shape in the absence of cardinality estimates.
		candidateBytes := buildSideBytes(stages, s) + sumFusedBytes(stages, s.FusedJoins)
		if candidateBytes <= 0 {
			continue
		}
		if candidateBytes > maxFusedBuildBytes {
			continue
		}
		if len(s.FusedJoins) > 0 || len(consumer.FusedJoins) > 0 {
			// Multi-level fusion path — require known cumulative bytes for
			// both consumer + candidate and check the sum.
			cb := cumulativeBuildBytes(consumer)
			if cb < 0 {
				continue
			}
			if cb+candidateBytes > maxFusedBuildBytes {
				continue
			}
		}

		// Absorb: PREPEND the candidate's chain (its FusedJoins followed by
		// its primary spec) to the consumer's FusedJoins. The runtime walks
		// FusedJoins in order, then the primary join — so the resulting
		// runtime chain is:
		//
		//   probe_candidate → s.FusedJoins[0..n-1] → s.primary → consumer.original_FusedJoins → consumer.primary
		//
		// which matches what the un-fused DAG would compute: the candidate's
		// stage produced (its-probe ⨝ its-fused-builds ⨝ its-primary-build)
		// and that result was the consumer's probe input, so consumer's own
		// chain runs after the candidate's. Pre-fix this code APPENDED
		// (placing s.primary AFTER consumer.original_FusedJoins) which
		// silently re-ordered joins and caused empty result sets on Q02 + Q05
		// at SF1.
		spec := FusedJoinSpec{
			JoinType:        s.JoinType,
			JoinLeftKeys:    s.JoinLeftKeys,
			JoinRightKeys:   s.JoinRightKeys,
			BuildDepStage:   s.RightDepStage,
			BuildTableAlias: s.BuildTableAlias,
			BuildColOrigins: s.BuildColOrigins,
			JoinFilter:      s.JoinFilter,
			FilterExprs:     s.FilterExprs,
			JoinBuildSchema: s.JoinBuildSchema,
		}
		merged := make([]FusedJoinSpec, 0, len(s.FusedJoins)+1+len(consumer.FusedJoins))
		merged = append(merged, s.FusedJoins...)
		merged = append(merged, spec)
		merged = append(merged, consumer.FusedJoins...)
		consumer.FusedJoins = merged

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

		// Add the candidate's build-side dependency to consumer.Dependencies,
		// plus any deps that came from the candidate's existing fused chain.
		// Without these, the coordinator can't see the build-side stages as
		// upstream dependencies of the fused consumer and may schedule it
		// before its caches are ready.
		if s.RightDepStage != "" {
			consumer.Dependencies = append(consumer.Dependencies, s.RightDepStage)
		}
		for _, fj := range s.FusedJoins {
			if fj.BuildDepStage != "" {
				consumer.Dependencies = append(consumer.Dependencies, fj.BuildDepStage)
			}
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

// fusedSpecBuildBytes is buildSideBytes for a FusedJoinSpec — walks the
// stages slice to find the BuildDepStage scan and returns its EstimatedBytes.
// Returns 0 (unknown) when the build subtree shape doesn't match the
// expected join → [exchange-replicate] → scan pattern, matching
// buildSideBytes' fail-closed convention.
func fusedSpecBuildBytes(stages []Stage, fj FusedJoinSpec) int64 {
	if fj.BuildDepStage == "" {
		return 0
	}
	for _, s := range stages {
		if s.ID != fj.BuildDepStage {
			continue
		}
		if s.Type == "scan" {
			return s.EstimatedBytes
		}
		if s.Type == StageExchangeReplicate && len(s.Dependencies) == 1 {
			for _, t := range stages {
				if t.ID == s.Dependencies[0] && t.Type == "scan" {
					return t.EstimatedBytes
				}
			}
		}
		return 0
	}
	return 0
}

// sumFusedBytes returns the cumulative build-side bytes across a slice of
// fused specs, using fusedSpecBuildBytes per entry. Returns -1 when any
// participant's bytes are unknown — forces the cumulative-budget check to
// fall back to the conservative depth-1 path.
func sumFusedBytes(stages []Stage, specs []FusedJoinSpec) int64 {
	if len(specs) == 0 {
		return 0
	}
	var total int64
	for _, fj := range specs {
		b := fusedSpecBuildBytes(stages, fj)
		if b <= 0 {
			return -1
		}
		total += b
	}
	return total
}

// resolveShuffleKey resolves a join key name through any Project alias nodes
// in the child subtree. For example, a CTE with `l_suppkey AS supplier_no`
// creates a Project that renames the column — the shuffle key `supplier_no`
// must be mapped back to `l_suppkey` so the executor can find it in the data
// (distributed walkStages treats ordinary Projects as passthrough, so the
// physical columns keep their original names).
//
// Join nodes recurse into their output-visible children: both sides for
// inner/outer joins, probe side only for semi/anti. First resolution wins.
//
// A key qualified by the derived table's own alias (`ON x.k = n_nationkey`)
// resolves through derivedScopeBareName, which drops the qualifier only
// inside the scope that owns it — without that the key reached the worker as
// `x.k`, a broadcast join's probe matched nothing and the query returned 0
// rows where the single-process path returned 24, and a hash join's shuffle
// failed loud with `partitioned shuffle: key "x.a" not in schema` (#467,
// #480).
//
// Renames CHAIN: `SELECT k AS j FROM (SELECT s_nationkey AS k FROM supplier)`
// has to walk j → k → s_nationkey, mirroring resolveAggInputName and
// resolveOutputRenameSource. Each Project substitutes at most once (a
// projection list is simultaneous, so `b AS a, a AS b` must not chase
// itself) and the walk only ever descends, so it terminates.
func resolveShuffleKey(key string, child *logical.Node) string {
	if child == nil {
		return key
	}
	resolved := key
	for n := child; n != nil; {
		if n.Type == logical.NodeProject {
			bare := derivedScopeBareName(resolved, n)
			if proj := projectionForName(n.Projections, resolved, bare); proj != nil &&
				proj.Column != "" && !strings.EqualFold(proj.Column, resolved) {
				resolved = proj.Column
			}
		}
		if n.Type == logical.NodeJoin && len(n.Children) == 2 {
			if r := resolveShuffleKey(resolved, n.Children[0]); r != resolved {
				return r
			}
			jt := strings.ToLower(n.JoinType)
			if jt != "semi" && jt != "anti" {
				return resolveShuffleKey(resolved, n.Children[1])
			}
			return resolved
		}
		// Continue down to single-child nodes (Filter, Sort, Limit, Project, Aggregate)
		if len(n.Children) == 1 {
			n = n.Children[0]
		} else {
			break
		}
	}
	return resolved
}

// resolveAggInputName maps a name an aggregate stage READS — an aggregate
// argument, or a GROUP BY key — back to what the stage below it actually
// emits, following the SELECT-list renames of any Project in between.
//
// walkStages treats an ordinary Project as a passthrough: it emits no stage,
// so a subquery's rename never happens on the DAG. `SELECT MAX(n) FROM
// (SELECT o_custkey AS n FROM orders)` therefore dispatched a scan reading
// o_custkey and an aggregate asking for `n`, and exec.HashAggregate answers a
// column it cannot resolve with NULL — 1499 on the single-process path and on
// DuckDB, NULL on the DAG (#355). A renamed GROUP BY key is the louder half of
// the same defect: an unresolvable key serializes as a NULL key, so every row
// collapses into one NULL group.
//
// This is the aggregate's version of what resolveShuffleKey does for join keys
// and resolveSortKeyColumn for ORDER BY terms — the same root cause, patched
// per consumer because the passthrough is what all three share.
//
// Three outcomes:
//
//	name unchanged, alias false — not a rename; the name is whatever the
//	  child already emits, which is the overwhelmingly common case.
//	name rewritten, alias true — the Project renamed a plain column; the
//	  aggregate reads the source column instead.
//	expr non-nil, alias true — the Project computed an EXPRESSION under this
//	  name (`SELECT o_custkey * 2 AS n`). There is no column to read; the
//	  caller attaches it as the aggregate's derived InputExpr, which the
//	  worker projects before aggregating. exprInput is then the node that
//	  Project reads, which is what the expression's column references are
//	  written against — the caller types the expression there, because the
//	  Project's OWN output does not carry them and a polymorphic declaration
//	  (COALESCE, NULLIF, GREATEST, LEAST) falls back to Float64 without them
//	  and drops every string (#333).
//
// It stops at an Aggregate: that node's outputs are its own GroupBy and
// OutputCol names, which the parent reads directly, and descending past it
// would resolve a name against the wrong schema.
func resolveAggInputName(name string, child *logical.Node) (resolved string, expr plansql.Node, exprInput *logical.Node, alias bool) {
	resolved = name
	if child == nil || name == "" {
		return resolved, nil, nil, false
	}
	// A projection list is simultaneous, so `b AS a, a AS b` must not chase
	// itself: each Project substitutes at most once, and the walk is finite
	// because it only ever descends.
	for n := child; n != nil; {
		switch {
		case n.Type == logical.NodeProject:
			// A name qualified by the derived table's own alias (`GROUP BY
			// u.k`) is looked up bare inside that table's scope — see
			// derivedScopeBareName. Without it the key reached the worker
			// as `u.k` and the task failed loud: `hash aggregate: GROUP BY
			// key "u.k" is not a column of its input` (#467).
			bare := derivedScopeBareName(resolved, n)
			if proj := projectionForName(n.Projections, resolved, bare); proj != nil {
				switch {
				case proj.Column != "" && !strings.EqualFold(projSourceName(proj), resolved):
					// The QUALIFIER-PRESERVING spelling, for the reason
					// resolveSortKeyColumn and annotateDerivedAliasSortKey
					// both prefer it: a self-joined table gives both arms the
					// same bare column name, so `n2.n_name AS b` carries
					// Column "n_name" — which names n1's column just as well
					// as n2's, and the aggregate bound the wrong one. GROUP BY
					// over that derived table answered 25 groups where
					// PostgreSQL 17 answers 5 (#489). Only "n2.n_name" says
					// which arm, and the worker's own lookup applies the
					// qualified↔bare fallback where the stream spells it the
					// other way.
					resolved, alias = projSourceName(proj), true
				case proj.Column == "" && !proj.IsAgg && proj.ASTExpr != nil:
					var below *logical.Node
					if len(n.Children) == 1 {
						below = n.Children[0]
					}
					return resolved, proj.ASTExpr, below, true
				}
			}
		case n.Type == logical.NodeAggregate:
			return resolved, nil, nil, alias
		case n.Type == logical.NodeJoin && len(n.Children) == 2:
			// Mirror resolveShuffleKey: a rename can sit under either arm.
			left, lexpr, lin, lok := resolveAggInputName(resolved, n.Children[0])
			if lok {
				return left, lexpr, lin, true
			}
			jt := strings.ToLower(n.JoinType)
			if jt == "semi" || jt == "anti" {
				return resolved, nil, nil, alias
			}
			right, rexpr, rin, rok := resolveAggInputName(resolved, n.Children[1])
			if rok {
				return right, rexpr, rin, true
			}
			return resolved, nil, nil, alias
		}
		if len(n.Children) == 1 {
			n = n.Children[0]
			continue
		}
		break
	}
	return resolved, expr, exprInput, alias
}

// aggStageGroupKey reports the name the aggregate STAGE will emit for a
// logical GROUP BY key, and whether that differs from the key as written.
//
// A key naming a subquery's rename is dispatched under the source column; a
// key naming a subquery's computed alias is dispatched under the EXPRESSION
// TEXT, which is the spelling the worker's pre-aggregate projection already
// compiles and emits (buildAggInputProjection treats a group-by entry that
// does not parse as a bare column reference as derived). Both walkStages and
// the sort's aggregateOutputName have to agree on it, so both call this.
func aggStageGroupKey(key string, child *logical.Node) (string, bool) {
	resolved, expr, _, renamed := resolveAggInputName(key, child)
	if !renamed {
		return key, false
	}
	if expr != nil {
		return expr.String(), true
	}
	return resolved, true
}

// derivedGroupKeyTypes types every DERIVED (non-bare) GROUP BY key for the
// wire (Stage.GroupByTypes): the same inferProjectionTypeCols call, over the
// same input column types, that the single-process pre-aggregate projection
// uses for its synthetic key columns — so both engines store one key
// expression in one vector type. Keys that parse as bare column references
// (or not at all) are omitted; the worker passes those through untyped.
//
// The map is keyed by the exact dispatched key text (post-aggStageGroupKey),
// because that text is what the worker parses and looks up (#379).
func derivedGroupKeyTypes(groupBy []string, child *logical.Node) map[string]parquet.TypeID {
	var out map[string]parquet.TypeID
	var colTypes map[string]parquet.TypeID
	var strictInt map[string]bool
	resolved := false
	for _, key := range groupBy {
		if key == "" {
			continue
		}
		node, err := plansql.ParseExpression(key)
		if err != nil {
			continue
		}
		if _, bare := node.(*plansql.ColRef); bare {
			continue
		}
		if !resolved {
			colTypes = inputColTypes(child)
			strictInt = strictIntArithCols(child)
			resolved = true
		}
		if out == nil {
			out = make(map[string]parquet.TypeID)
		}
		out[key] = inferProjectionTypeCols(node, parquet.TypeString, strictInt, colTypes)
	}
	return out
}

// resolveSortKeyColumn maps an ORDER BY key that names a SELECT-list alias
// back to the name the aggregate stage below it actually emits.
//
// The logical builder resolves ORDER BY to the SELECT list's OUTPUT name
// (logical.resolveOrderByColumn), which is exactly what the single-process
// pipeline needs — there the Project really does run below the Sort, so the
// alias exists by the time the sort reads a row. Distributed walkStages
// instead treats an ordinary Project as a passthrough (the same reason
// resolveShuffleKey exists for join keys), so an aggregate's output keeps the
// GroupBy spelling. A sort keyed on `p` from `o_orderpriority AS p` then
// matches no column, the sort is a no-op, and the ORDER BY is silently lost —
// while the same query without the rename sorts correctly (#313, and TPC-H
// Q09 via `n_name AS nation` / `SUBSTR(o_orderdate,1,4) AS o_year`).
//
// Scope is deliberately the aggregate: a final_aggregate names its output
// from GroupByCols and AggSpec.OutputCol, which walkStages copies verbatim
// from this node, so the mapping is exact and decidable here. Sorts over a
// bare scan or join are left alone — attachScanSelectProjections may attach
// an alias-naming OpProject to the producing fragment later in
// PlanDistributed, so the correct spelling for those is not yet known at
// this point (it declines every plan carrying an aggregate, so the two
// never overlap).
//
// Each Project substitutes at most once: a projection list is simultaneous,
// so `b AS a, a AS b` must not chase itself.
func resolveSortKeyColumn(key string, child *logical.Node) string {
	// resolved is the preferred candidate, alt a second one to try when the
	// first names no output of the aggregate below.
	resolved, alt := key, ""
	for n := child; n != nil; {
		switch n.Type {
		case logical.NodeProject:
			for _, proj := range n.Projections {
				if !strings.EqualFold(proj.Alias, resolved) &&
					(alt == "" || !strings.EqualFold(proj.Alias, alt)) {
					continue
				}
				// Expression text first, because it keeps the table
				// qualifier: a self-joined table gives both aliases the same
				// bare column name, so `n1.n_name AS supp_nation` carries
				// column "n_name" — which matches neither group key and
				// cannot, since n2 shares it. Only "n1.n_name" identifies
				// which alias, and a GROUP BY that spells its key bare is
				// covered by the Column fallback below (#314/#313).
				resolved, alt = proj.Expr, proj.Column
				if resolved == "" {
					resolved, alt = proj.Column, ""
				}
				if alt == resolved {
					alt = ""
				}
				break
			}
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
			// Order-preserving passthroughs: keep descending.
		case logical.NodeAggregate:
			if out, ok := aggregateOutputName(n, resolved); ok {
				return out
			}
			if alt != "" {
				if out, ok := aggregateOutputName(n, alt); ok {
					return out
				}
			}
			return key
		default:
			return key
		}
		if len(n.Children) != 1 {
			return key
		}
		n = n.Children[0]
	}
	return key
}

// aggregateOutputName reports the name an Aggregate node emits for col — its
// own spelling of the matching group key or aggregate output — so a sort key
// resolved through a rename lands on a column the stage really produces.
//
// A group key is reported the way walkStages will EMIT it, which is through
// resolveAggInputName: a key naming a subquery's rename is dispatched under
// the source column, because the Project that would have created the alias
// emits no stage (#355).
func aggregateOutputName(n *logical.Node, col string) (string, bool) {
	var child *logical.Node
	if len(n.Children) == 1 {
		child = n.Children[0]
	}
	emit := func(g string) (string, bool) {
		if child != nil {
			if resolved, renamed := aggStageGroupKey(g, child); renamed {
				return resolved, true
			}
		}
		return g, true
	}
	for _, g := range n.GroupBy {
		if strings.EqualFold(g, col) {
			return emit(g)
		}
	}
	// The two spellings of one key: `GROUP BY u.k` names the same output as
	// the `k` the SELECT list and the ORDER BY use, and either side may be
	// the qualified one. Both are dropped to their bare form only inside the
	// derived scope that owns the qualifier — see derivedScopeBareName
	// (#467). A bare spelling that matches TWO group keys is a self-join's
	// `n1.n_name`/`n2.n_name`: naming one of them would order by an
	// arbitrary side, so the key is left for the caller to give up on, the
	// same call lookupEmittedColumn makes on the same ambiguity.
	bare := func(name string) string {
		if b := derivedScopeBareName(name, child); b != "" {
			return b
		}
		return name
	}
	cb := bare(col)
	match, count := "", 0
	for _, g := range n.GroupBy {
		if strings.EqualFold(bare(g), cb) {
			match, count = g, count+1
		}
	}
	if count == 1 {
		return emit(match)
	}
	for _, a := range n.AggExprs {
		if strings.EqualFold(a.OutputCol, col) {
			return a.OutputCol, true
		}
	}
	return "", false
}

func (p *Planner) walkStages(node *logical.Node, stages *[]Stage, parentID *string) {
	// CTE deduplication: when this subtree's root is a CTE reference and
	// a structurally-identical clone has already been planned, link the
	// parent's deps to the cached terminal stage and skip re-walking.
	// Eliminates the dual-chain float drift that fails Q15 under multi-
	// file scans: the JOIN's right side and the MAX-subquery producer
	// chain previously emitted independent stages computing the same
	// CTE, producing 1-ULP drift between their float SUMs.
	//
	// The structural hash guards correctness — a CTE clone with different
	// pushed-down filters or column projections has a different hash and
	// is NOT deduped, falling back to the historical compute-twice path.
	if node.CTEName != "" {
		hash := cteSubtreeHash(node)
		cacheKey := node.CTEName + "|" + hash
		if termID, ok := p.ctePlannedTerminal[cacheKey]; ok {
			// Emit a phantom "cte-alias" stage that points at the cached
			// terminal. Parent walkStages cases compute their dependencies
			// via leafStages over [preCount:], which naturally picks up
			// this alias as a leaf — so the parent's deps reference the
			// alias's ID. A post-pass (flattenCTEAliases) rewrites every
			// dep that points to an alias into the alias's target and
			// drops the alias stages, leaving the parent reading directly
			// from the cached CTE terminal. Surgical: avoids modifying
			// every parent case to consult a Planner-level "deduped child"
			// list; the bookkeeping lives entirely in the alias stage and
			// the post-pass.
			aliasID := fmt.Sprintf("cte-alias-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:           aliasID,
				Type:         stageTypeCTEAlias,
				Dependencies: []string{termID},
			})
			if parentID != nil {
				for i := range *stages {
					if (*stages)[i].ID == *parentID {
						(*stages)[i].Dependencies = append((*stages)[i].Dependencies, aliasID)
						break
					}
				}
			}
			return
		}
		// Defer recording: after walkStages returns, the last stage
		// in *stages is the terminal of this CTE's subtree (walkStages
		// emits children first, then the node's own stage last for
		// every node type except Filter — and Filter at a CTE root is
		// degenerate because Filter doesn't emit its own stage).
		before := len(*stages)
		defer func() {
			if len(*stages) > before {
				p.ctePlannedTerminal[cacheKey] = (*stages)[len(*stages)-1].ID
			}
		}()
	}

	switch node.Type {
	case logical.NodeScan:
		stageID := fmt.Sprintf("scan-%d", len(*stages))
		tasks := 1
		var scanFiles []string
		var scanFileSizes []int64
		var estBytes, estRows int64
		partFilter := node.PartitionFilter
		var scanDeletes map[string][]int64
		if meta, err := p.getManifest(context.Background(), node.TableName); err == nil {
			for _, part := range meta.Partitions {
				if len(partFilter) > 0 && len(part.Values) > 0 {
					if !matchesPartitionFilter(part.Values, partFilter) {
						continue
					}
				}
				for _, f := range part.Files {
					scanFiles = append(scanFiles, f.Path)
					scanFileSizes = append(scanFileSizes, f.SizeBytes)
					estBytes += f.SizeBytes
					estRows += f.NumRows
				}
			}
			if len(scanFiles) > 0 {
				tasks = len(scanFiles)
			}
			// Merge-on-read deletes, from THIS manifest object — the same
			// snapshot the file list came from (see Stage.ScanDeletes).
			scanDeletes = deleteMarkerMap(meta.DeleteMarkers)
			p.rememberScanDeletes(node.TableName, scanDeletes)
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
			ScanFileSizes:   scanFileSizes,
			ScanDeletes:     scanDeletes,
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
		// A Project below an aggregate emits no stage (walkStages treats it
		// as a passthrough), so its SELECT-list renames never happen on the
		// DAG. Every name this aggregate reads is therefore resolved back to
		// what the stage below it emits, or the aggregate asks for a column
		// the batch does not have and HashAggregate answers NULL (#355).
		var aggChild *logical.Node
		if len(node.Children) > 0 {
			aggChild = node.Children[0]
		}
		var aggSpecs []AggSpec
		hasDistinctAgg := false
		for _, agg := range node.AggExprs {
			// Resolved before the spec is built so aggSpecOutputType and the
			// derived-expression branch below both see the real column: the
			// type lookup misses on an alias too, and an undeclared type
			// makes MAX come back float64 where the column is INT64.
			inputExpr := agg.InputExpr
			exprCols := aggChild
			if resolved, expr, exprInput, renamed := resolveAggInputName(agg.InputCol, aggChild); renamed {
				if expr != nil {
					// The alias named an EXPRESSION, not a column. There is
					// nothing to read; hand the worker the expression and let
					// it project the value under the alias before
					// aggregating — the same route a derived aggregate
					// argument (`SUM(a * (1 - b))`) already takes, and it
					// names its projected column InputCol, so the alias has
					// to stay. Its column references are written against the
					// Project's INPUT, which is where the type has to be
					// resolved.
					inputExpr, exprCols = expr, exprInput
				} else {
					agg.InputCol = resolved
					if _, bare := inputExpr.(*plansql.ColRef); bare || inputExpr == nil {
						inputExpr = &plansql.ColRef{Column: resolved}
					}
				}
			}
			if resolved, expr, _, renamed := resolveAggInputName(agg.InputCol2, aggChild); renamed && expr == nil {
				agg.InputCol2 = resolved
			}
			outType, outTypeKnown := aggSpecOutputType(node, agg)
			spec := AggSpec{
				Func:            agg.Func,
				InputCol:        agg.InputCol,
				OutputCol:       agg.OutputCol,
				OutputType:      outType,
				OutputTypeKnown: outTypeKnown,
				InputCol2:       agg.InputCol2,
				Separator:       agg.Separator,
				Percentile:      agg.Percentile,
			}
			agg.InputExpr = inputExpr
			// DISTINCT rides the canonical Func string the worker already
			// maps to exec.AggCountDistinct (#291: the flag used to be
			// dropped here, so distributed COUNT(DISTINCT x) degenerated
			// to COUNT(x) on every path).
			if agg.Distinct && strings.EqualFold(agg.Func, "count") {
				spec.Func = "count_distinct"
			}
			if agg.Distinct {
				hasDistinctAgg = true
			}
			// Capture derived expression text when the aggregate argument
			// is not a bare column reference (e.g.
			// SUM(l_extendedprice * (1 - l_discount))). Downstream
			// native-DAG workers need this to project the derived column
			// before running HashAggregate.
			if agg.InputExpr != nil {
				if _, bare := agg.InputExpr.(*plansql.ColRef); !bare {
					spec.InputExpr = agg.InputExpr.String()
					// And the type that expression evaluates into, since
					// the worker builds the pre-aggregate projection from
					// the text alone and has no catalog to consult.
					spec.InputType = inferProjectionTypeCols(agg.InputExpr, parquet.TypeFloat64, nil, inputColTypes(exprCols))
				}
			}
			aggSpecs = append(aggSpecs, spec)
		}
		groupBy := make([]string, len(node.GroupBy))
		copy(groupBy, node.GroupBy)
		// Same resolution for the GROUP BY keys, and the same defect without
		// it: an unresolvable key serializes as NULL rather than failing, so
		// `GROUP BY k` over `SELECT o_orderstatus AS k` collapsed 3 groups
		// into one NULL group of every row.
		for i, key := range groupBy {
			if resolved, renamed := aggStageGroupKey(key, aggChild); renamed {
				groupBy[i] = resolved
				if p.aggStageRenames == nil {
					p.aggStageRenames = make(map[string]string)
				}
				p.aggStageRenames[strings.ToLower(key)] = resolved
				// The gather's rename reads this map by the name the outer
				// SELECT list uses, which for a key written through the
				// derived table's alias (`GROUP BY u.k`) is the BARE one
				// (`SELECT k`). Record both spellings or the lookup misses
				// and the result comes back at full upstream width (#467).
				if bare := derivedScopeBareName(key, aggChild); bare != "" {
					if _, taken := p.aggStageRenames[strings.ToLower(bare)]; !taken {
						p.aggStageRenames[strings.ToLower(bare)] = resolved
					}
				}
			}
		}
		// Plan-time types for the derived keys, computed here where the
		// aggregate's input schema is still known (#379); every stage
		// shape below carries the same map.
		groupByTypes := derivedGroupKeyTypes(groupBy, aggChild)

		// Optimization: fuse aggregation into scan when the only child
		// stages are scans (no joins or sorts in between). This eliminates
		// the scan→aggregate S3 round-trip by doing partial aggregation at
		// the scan level. Each scan task produces partial aggregate results
		// instead of raw rows, massively reducing data volume.
		childStages := (*stages)[preCount:]
		// Distinct aggregates cannot ride the two-phase partial/merge
		// shape: a per-task partial COUNT(DISTINCT) merged by the final's
		// COUNT→SUM rewrite double-counts values that appear in more than
		// one task (#291 — observed as COUNT(DISTINCT)=COUNT(*)). They
		// dispatch instead as one RawInputAggregate final over raw rows:
		// exact by construction (grouped finals declare clustering on the
		// group keys, so the distribution pass hash-partitions raw input
		// into disjoint groups; ungrouped finals collapse to Singleton).
		//
		// MEDIAN, PERCENTILE_*, MODE, MIN_BY/MAX_BY and STRING_AGG take the
		// same route for the same reason and at the same cost — none of
		// them is a valid input to itself, and none has a bounded summary
		// that merges (agg_whole_input.go, #353).
		if hasDistinctAgg || anyAggNeedsWholeInput(aggSpecs) {
			finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:                finalStageID,
				Type:              "final_aggregate",
				Tasks:             1,
				GroupByCols:       groupBy,
				GroupByTypes:      groupByTypes,
				AggSpecs:          aggSpecs,
				RawInputAggregate: true,
				Dependencies:      leafStages(childStages),
			})
		} else if canFuseScanAggregate(childStages) {
			for i := range *stages {
				if i < preCount {
					continue
				}
				if (*stages)[i].Type == "scan" {
					(*stages)[i].FusedAggGroupBy = groupBy
					(*stages)[i].GroupByTypes = groupByTypes
					(*stages)[i].FusedAggSpecs = aggSpecs
					// The scan's RequiredColumns carry the aggregate OUTPUT
					// names (e.g. __having_0) because ancestors reference
					// them. On a fused scan-aggregate those are produced by
					// the fragment's HashAggregate, not read from parquet —
					// but the worker's all-or-nothing projection guard can't
					// know that: one unknown name silently reverts the whole
					// scan to full width (Q18's fused lineitem leg measured
					// 143 B/row vs the ~25 B/row its 2-column read set
					// needs). Strip pure outputs from the read set; an
					// output that aliases a real input (SUM(x) AS x) stays.
					(*stages)[i].Columns = pruneFusedAggOutputCols(
						(*stages)[i].Columns, groupBy, aggSpecs, (*stages)[i].FilterExprs)
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
				ID:           stageID,
				Type:         "aggregate",
				Tasks:        1,
				GroupByCols:  groupBy,
				GroupByTypes: groupByTypes,
				AggSpecs:     aggSpecs,
			}
			stage.Dependencies = leafStages(childStages)
			*stages = append(*stages, stage)

			finalStageID := fmt.Sprintf("final_aggregate-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:           finalStageID,
				Type:         "final_aggregate",
				Tasks:        1,
				GroupByCols:  groupBy,
				GroupByTypes: groupByTypes,
				AggSpecs:     aggSpecs,
				Dependencies: []string{stageID},
			})
		}

	case logical.NodeSort:
		preCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, nil)
		}
		// Same materialization for a sort over a computed subquery column
		// (#383): the sort keys on the alias, which otherwise names no
		// column anywhere on the DAG and the ORDER BY is silently lost.
		if len(node.Children) == 1 {
			absorbComputedSubqueryProjection(node.Children[0], (*stages)[preCount:], true)
		}
		sortStageID := fmt.Sprintf("sort-%d", len(*stages))
		var sortKeys []SortKeySpec
		var sortChild *logical.Node
		if len(node.Children) == 1 {
			sortChild = node.Children[0]
		}
		for _, ob := range node.OrderBy {
			key := SortKeySpec{
				Column:    resolveSortKeyColumn(ob.Column, sortChild),
				Desc:      ob.Desc,
				NullsLast: resolveNullsLast(ob),
			}
			// A key still spelled __sortkey_N names a column the logical
			// Project materializes and no stage does. Record what defines
			// it; resolveHiddenSortKeys settles it at the end of planning,
			// once it can see whether some other pass already put the name
			// on the producing stage (#424).
			annotateHiddenSortSource(&key, sortChild)
			// And the non-synthetic sibling: a key naming a DERIVED table's
			// SELECT-list alias, which resolveSortKeyColumn above leaves
			// alone over a scan/join producer because it cannot yet know
			// whether attachScanSelectProjections will materialize the name
			// (#467, #468). Record the source column;
			// resolveDerivedAliasSortKeys decides once that is settled.
			annotateDerivedAliasSortKey(&key, sortChild)
			sortKeys = append(sortKeys, key)
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
		preLimitCount := len(*stages)
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		// The bound a stage may truncate to is limit+offset, not limit: the
		// OFFSET is applied once, at the coordinator, after the merge, so the
		// rows it skips have to survive the stage that produced them. A
		// stage told to keep 3 rows for `LIMIT 3 OFFSET 5` kept the first
		// three and the answer was the first page again (#337). No bound
		// when there is no LIMIT — an OFFSET alone bounds nothing. NoLimit
		// (-1), not 0: `node.LimitVal` is itself 0 for a real `LIMIT 0`, and
		// treating that the same as "no LIMIT" silently dropped the bound
		// end-to-end for `ORDER BY ... LIMIT 0` on the DAG path (#481).
		stageBound := logical.NoLimit
		hasStageBound := false
		if node.LimitVal != logical.NoLimit {
			stageBound = node.LimitVal + node.OffsetVal
			hasStageBound = true
		}
		// Propagate limit to both merge_sort and sort stages — but only to a
		// sort NO LOWER LIMIT ALREADY OWNS.
		//
		// The scan is backwards over the whole stage list, so for a nested
		// LIMIT it reaches the INNER one's sort: `(SELECT n FROM nation
		// ORDER BY n LIMIT 3) i LIMIT 5` wrote 5 over the inner's 3 and then
		// suppressed its own stage on the strength of the sort it had just
		// mis-claimed, answering 5 where PostgreSQL answers 3 (#525).
		// Restricting the range to (*stages)[preLimitCount:] does not help —
		// the inner sort is inside this LIMIT's own child walk.
		//
		// Two things say a sort is spoken for. A sort/merge_sort that already
		// carries HasLimit was bounded by a LIMIT below (nothing else writes
		// it during walkStages), and a StageLimit anywhere between here and
		// the sort means a lower LIMIT is applied above that sort — this
		// bound has to compose ON TOP of it, not underneath. Either way the
		// scan stops and reports sorted=false, so needsLimitStage gives this
		// LIMIT a stage of its own, which is the correct composition and the
		// one the all-bare nesting already produced.
		sorted := false
		for i := len(*stages) - 1; i >= 0; i-- {
			st := &(*stages)[i]
			if st.Type == StageLimit {
				break
			}
			if st.Type != "merge_sort" && st.Type != "sort" {
				continue
			}
			if st.HasLimit {
				break
			}
			if hasStageBound {
				st.Limit = stageBound
				st.HasLimit = true
			}
			// else: leave both fields at their zero value — an unbounded
			// stage is (0,false), and the shared-subplan fingerprint hashes
			// Limit unconditionally, so writing NoLimit here would split an
			// OFFSET-only sort from its no-LIMIT twin.
			sorted = true
			if st.Type == "sort" {
				break
			}
		}
		// No sort to carry it: a bare LIMIT. The coordinator bounds the
		// gathered result either way (correctness), but without a per-task
		// bound every task still reads its whole input first — DataGrip
		// opening a 15M-row table read all of it for the 501 rows it wanted.
		// Push the bound into this subtree's own stages so each task stops
		// early. Only when nothing between here and the scan can change
		// cardinality; see limitPushdownSafe.
		if !sorted && stageBound > 0 && limitPushdownSafe(node) {
			for i := preLimitCount; i < len(*stages); i++ {
				// "scan" is what walkStages emits for a leaf read; "pipeline"
				// is the type those stages carry once fragments are built.
				// Exchange stages are left alone — they move rows between
				// stages rather than producing them.
				if t := (*stages)[i].Type; t == "scan" || t == "pipeline" {
					(*stages)[i].RowLimit = stageBound
				}
			}
		}
		// …and the bound itself, for every LIMIT the two existing appliers
		// cannot reach. See needsLimitStage: the coordinator's post-gather
		// pass reads the ROOT node only, and the sort top-N above needs an
		// ORDER BY below the LIMIT and cannot skip an OFFSET. A LIMIT that
		// is neither reached NOTHING (#478) — RowLimit above is a per-task
		// truncation, and k tasks each keeping n rows is not the first n
		// rows of their union.
		if p.needsLimitStage(node, sorted) {
			limitStage := Stage{
				ID:           fmt.Sprintf("limit-%d", len(*stages)),
				Type:         StageLimit,
				Tasks:        1,
				Offset:       node.OffsetVal,
				Dependencies: leafStages((*stages)[preLimitCount:]),
			}
			if node.LimitVal != logical.NoLimit {
				limitStage.Limit = node.LimitVal
				limitStage.HasLimit = true
			}
			*stages = append(*stages, limitStage)
		}

	case logical.NodeJoin:
		// Track leaf stages from each child separately so we get the
		// correct left (probe) and right (build) dependencies — even
		// when a child is itself a multi-stage subtree (e.g., nested join).
		var childLeaves [][]string
		for _, child := range node.Children {
			childStart := len(*stages)
			p.walkStages(child, stages, nil)
			// A join input that is a subquery with a COMPUTED projection
			// must materialize the computed column into its producing scan
			// fragment, or the build/probe files never carry it and every
			// downstream read — the ON residual, the projected output —
			// sees NULL (#383).
			absorbComputedSubqueryProjection(child, (*stages)[childStart:], false)
			childLeaves = append(childLeaves, leafStages((*stages)[childStart:]))
		}
		// Map logical join type to canonical short form. Needed before the
		// broadcast decision: a join that preserves its BUILD side cannot
		// replicate it.
		jt := mapJoinType(node.JoinType)
		// An inner join with no condition at all IS a cross join (#376) —
		// same normalization as buildJoin, or the stage below would carry a
		// keyless hash_join the worker rejects.
		if jt == "inner" && strings.TrimSpace(node.JoinCond) == "" && node.JoinFilter == "" {
			jt = "cross"
		}

		// Broadcast replicates the build side to every task and splits the
		// probe across them. A RIGHT or FULL join emits its UNMATCHED build
		// rows, and no task can tell whether another task matched a given
		// build row — every task would emit all of them, so a 25-row answer
		// came back 75 rows on a 3-worker cluster. Those join types take the
		// hash-shuffle path instead, where both sides are co-partitioned and
		// each task owns a disjoint slice of the build. Same rule
		// planSkewSplitTasks already applies for the same reason.
		isBroadcast := !preservesBuildSide(jt) && p.isBroadcastCandidate(node)
		// A null-aware anti join reads ONE fact off its whole build side —
		// did any row have a NULL key — and answers with no rows at all when
		// the answer is yes (#507). Hash-partitioning the build splits that
		// fact: the task holding the NULL partition emits nothing while every
		// other task emits its probe rows, so `NOT IN` over a NULL-carrying
		// list came back with the rows a two-valued anti join would keep.
		// Replicating the build is what makes the fact whole per task; it is
		// a correctness requirement here, not a size heuristic.
		//
		// It overrides the SIZE decision, including an explicit
		// BroadcastBytesThreshold < 0 ("broadcast disabled"), so it is
		// counted and logged rather than silent: a null-aware anti join whose
		// build the threshold would have refused is replicating N× across the
		// cluster, and #539 is where the shape that removes the trade is
		// tracked.
		if node.NullAwareAnti && !preservesBuildSide(jt) && !isBroadcast {
			isBroadcast = true
			NullAwareAntiForcedBroadcasts.Add(1)
			bytes, known := p.estimateSubtreeBytes(node.Children[1])
			slog.Warn("null-aware anti join: build side FORCED to replicate past the broadcast decision",
				"reason", "NOT IN's three-valued rule reads one fact off the WHOLE build (#507); "+
					"a hash-partitioned build splits it",
				"build_bytes", bytes, "build_bytes_known", known,
				"broadcast_threshold", p.BroadcastBytesThreshold,
				"tracked_in", "#539")
		}
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

		// Extract join keys from condition (cross joins have no ON clause)
		var leftKeys, rightKeys []string
		var buildNaming *subtreeNaming
		if len(node.Children) >= 2 {
			buildNaming = subtreeNamingOf(node.Children[1])
		}
		if jt != "cross" {
			var residual []string
			leftKeys, rightKeys, residual = parseJoinKeys(node.JoinCond)
			if len(residual) > 0 {
				// walkStages has no error return; park the refusal the way
				// a set-operation refusal is parked (#346) and let
				// PlanDistributed raise it. Emitting a join keyed on a name
				// that is not a column is what made this silent.
				p.refuseJoin(refuseJoinCond(jt, node.JoinCond, residual))
			}
			// An outer join's ON residual (#358) rides stage.JoinFilter to the
			// worker, which compiles it there. Compile-check it NOW so an
			// unsupported expression refuses the plan instead of failing every
			// task at run time.
			if node.JoinFilter != "" && (jt == "left" || jt == "right" || jt == "full") {
				alias := ""
				if len(node.Children) >= 2 {
					alias = findScanAlias(node.Children[1])
				}
				if BuildJoinResidualFilter(node.JoinFilter, alias) == nil {
					p.refuseJoin(fmt.Errorf("join ON residual %q on a %s join: "+
						"not evaluable as a probe residual (columns, literals, arithmetic and "+
						"comparisons are; function calls and subqueries are not)",
						node.JoinFilter, jt))
				}
			}
			// parseJoinKeys assigns left/right based on position in the "="
			// expression, not based on which child subtree owns the column.
			// Fix the assignment so leftKeys are from the probe (left) child
			// and rightKeys are from the build (right) child.
			if buildNaming != nil {
				assignJoinKeySides(leftKeys, rightKeys,
					subtreeNamingOf(node.Children[0]), buildNaming)
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

		// Big-vs-big inner equi-joins upgrade the shuffled hash join to a
		// sort-merge join when the SortMergeJoinBytes gate passes (same gate
		// as the local buildJoin path). The exchange children below are
		// IDENTICAL — co-partitioning is all SMJ needs — so only the stage
		// type changes. Broadcast candidates keep the strictly-better
		// broadcast path.
		if joinType == StageHashJoin && jt == "inner" && node.JoinFilter == "" &&
			len(leftKeys) > 0 && p.shouldSortMergeJoin(node) {
			joinType = StageSortMergeJoin
			SortMergeJoinsPlanned.Add(1)
		}

		// Insert shuffle stages for non-broadcast joins when distributed
		numPartitions := 0
		if !isBroadcast && jt != "cross" && len(leftKeys) > 0 && p.WorkerCount > 1 {
			// Use 8x workers as partition count to reduce per-task join memory.
			// Each partition receives 1/numPartitions of the shuffled data, so
			// higher counts reduce peak hash table memory on each worker.
			// At SF100 with 3 workers, 24 partitions halves per-partition
			// memory compared to the previous 12. HashPartitionCount is the
			// same rule EnsureDistribution applies to count-unpinned exchanges
			// (grouped finals, windows) — one width for all hash shuffles.
			numPartitions = HashPartitionCount(p.WorkerCount)

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
			leftShuffleID := fmt.Sprintf("exchange-repartition-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:      leftShuffleID,
				Type:    StageExchangeRepartition,
				Tasks:   1,
				Columns: shuffleCols,
				Exchange: &ExchangeStage{
					Keys:  append([]string(nil), leftKeys...),
					Count: numPartitions,
				},
				Dependencies: []string{leftDep},
			})

			// Right (build) side shuffle
			rightShuffleID := fmt.Sprintf("exchange-repartition-%d", len(*stages))
			*stages = append(*stages, Stage{
				ID:      rightShuffleID,
				Type:    StageExchangeRepartition,
				Tasks:   1,
				Columns: shuffleCols,
				Exchange: &ExchangeStage{
					Keys:  append([]string(nil), rightKeys...),
					Count: numPartitions,
				},
				Dependencies: []string{rightDep},
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
		probeSchema, buildSchema := joinSideSchemas(node, leftKeys, rightKeys)
		stage := Stage{
			ID:                 stageID,
			Type:               joinType,
			Tasks:              joinTasks,
			Columns:            resolveJoinNeededColumns(node),
			JoinType:           jt,
			JoinLeftKeys:       leftKeys,
			JoinRightKeys:      rightKeys,
			LeftDepStage:       leftDep,
			RightDepStage:      rightDep,
			JoinPartitionCount: numPartitions,
			JoinProbeSchema:    probeSchema,
			JoinBuildSchema:    buildSchema,
		}
		// Propagate build-side table alias for column disambiguation in self-joins
		// (e.g., nation n1 JOIN nation n2 — prevents duplicate columns from being dropped).
		if len(node.Children) >= 2 {
			if alias := findScanAlias(node.Children[1]); alias != "" {
				stage.BuildTableAlias = alias
			}
			// Multi-table build subtrees additionally carry per-column origin
			// aliases so the executor qualifies each duplicate with its OWNING
			// scan, not the (arbitrary) first one. Nil for single-scan builds.
			stage.BuildColOrigins = buildNaming.buildColOrigins()
		}
		// Propagate semi/anti join inequality filters
		if node.JoinFilter != "" {
			stage.JoinFilter = node.JoinFilter
		}
		// …and NOT IN's three-valued rule, which is a property of the
		// PREDICATE this anti join came from and unknowable from the stage
		// alone (#507).
		stage.NullAwareAnti = node.NullAwareAnti
		if leftDep != "" {
			stage.Dependencies = append(stage.Dependencies, leftDep)
		}
		if rightDep != "" {
			stage.Dependencies = append(stage.Dependencies, rightDep)
		}
		*stages = append(*stages, stage)

	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		p.emitSetOpStages(node, stages)

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
			// Capture the filter-carrying stage by INDEX (not pointer) because
			// subsequent producer-stage emissions may append to *stages and
			// invalidate any held pointer.
			filterIdx := len(*stages) - 1
			for _, pred := range node.Predicates {
				var exprStr string
				if pred.Raw != "" {
					exprStr = pred.Raw
				} else if pred.ASTExpr != nil {
					exprStr = pred.ASTExpr.String()
				}
				if exprStr == "" {
					continue
				}
				// Resolve scalar subqueries. Under native-DAG, CTE-referencing
				// subqueries are deferred to the coordinator — this call returns
				// placeholders and the SQL for each producer we must emit.
				resolvedExpr, deferred := p.resolveFilterSubqueries(exprStr)
				for _, d := range deferred {
					producerID, err := p.emitScalarProducerStages(stages, d.SubquerySQL)
					if err != nil {
						// Fall back: evaluate the subquery eagerly and splice
						// a literal in place of the placeholder. Loses
						// correctness for CTE-drift cases but keeps the query
						// running rather than failing outright.
						start := time.Now()
						rows, sErr := p.executeSubquery(p.planCtx, d.SubquerySQL)
						slog.Warn("scalar producer emission failed; executed subquery on coordinator",
							"duration", time.Since(start).Round(time.Millisecond),
							"emit_error", err, "exec_error", sErr)
						spliced := false
						if sErr == nil && len(rows) > 0 {
							for _, v := range rows[0] {
								lit := scalarToLiteral(v).String()
								resolvedExpr = strings.ReplaceAll(resolvedExpr, ":"+d.Placeholder, lit)
								spliced = true
								break
							}
						}
						if !spliced {
							// Both paths failed: restore the original subquery
							// text so downstream sees what it saw before
							// deferral existed, not a dangling :scalar_N.
							resolvedExpr = strings.ReplaceAll(resolvedExpr,
								":"+d.Placeholder, "("+d.SubquerySQL+")")
						}
						continue
					}
					fs := &(*stages)[filterIdx]
					if fs.ScalarDependencies == nil {
						fs.ScalarDependencies = make(map[string]string)
					}
					fs.ScalarDependencies[d.Placeholder] = producerID
					// NOTE: producer IDs are deliberately NOT appended to
					// Dependencies because Dependencies models data that
					// flows into the stage as record batches; scalar
					// producers feed into FilterExprs via late-bound
					// string substitution instead. The coordinator's stage
					// goroutine awaits ScalarDependencies separately.
				}
				// Re-index after any appends.
				fs := &(*stages)[filterIdx]
				fs.FilterExprs = append(fs.FilterExprs, resolvedExpr)
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
			// Resolved by the same helper buildWindow uses, so the stage
			// spec and the single-process operator describe one computation
			// — including the output type, which nothing downstream of the
			// worker can correct (#345).
			ec := windowExecColumn(node, we)
			var orderBy []SortKeySpec
			for _, ob := range we.OrderBy {
				orderBy = append(orderBy, SortKeySpec{Column: ob.Column, Desc: ob.Desc, NullsLast: resolveNullsLast(ob)})
			}
			winCols = append(winCols, WindowColSpec{
				Func:           we.Func,
				InputCol:       ec.InputCol,
				OutputCol:      ec.OutputCol,
				OutputType:     ec.OutputType,
				PartitionBy:    ec.PartitionBy,
				OrderBy:        orderBy,
				Frame:          we.Frame,
				LagLeadOffset:  ec.LagLeadOffset,
				LagLeadDefault: ec.LagLeadDefault,
				NtileBuckets:   ec.NtileBuckets,
				NthValueN:      ec.NthValueN,
			})
		}
		stage := Stage{
			ID:         stageID,
			Type:       StageWindow,
			Tasks:      1,
			WindowCols: winCols,
		}
		// Only depend on leaf stages from subtree (not transitive deps like scan).
		stage.Dependencies = leafStages((*stages)[preCount:])
		*stages = append(*stages, stage)

	default:
		// Passthrough nodes (Project, Distinct) — walk children.
		//
		// NOTE (#163/#466): Distinct still emits no stage here. It no longer
		// silently drops the DISTINCT, because nothing that carries semantics
		// reaches this branch: logical.rewriteDistinctAsGroupBy turns every
		// user Distinct(Project) in the tree into a GroupBy aggregate, which
		// stages as an aggregate and solves by construction the problem that
		// blocked a GroupByAll dedup stage (the projection below the Distinct
		// becomes the group keys, so the dedup runs on the output columns
		// rather than over-distinguishing on the scan's full width). What can
		// still arrive is a Distinct the rewrite declined: on the root path
		// the coordinator's post-gather dedup applies it (MergeInfo.
		// HasDistinct); anywhere else refuseUnstageableDistinct has already
		// refused the query. A planner-inserted BuildSideDedup Distinct
		// carries no user-visible semantics and passes through as before.
		for _, child := range node.Children {
			p.walkStages(child, stages, parentID)
		}
		// ABAC security barrier (InjectColumnPolicies wraps the scan in a
		// Project of masked/visible columns). An ordinary Project can pass
		// through — the gather recovers aliases — but a DROPPED barrier
		// leaks raw values, so absorb it into the scan stage it wraps:
		// the scan fragment applies it as an OpProject before anything
		// else consumes rows (filters excepted; they run on pre-barrier
		// columns exactly as the single-process pipeline orders them).
		if node.Type == logical.NodeProject && node.SecurityBarrier && len(node.Children) == 1 {
			// Predicate pushdown may have moved Filters below the barrier
			// (barrier → Filter… → Scan); filters on raw columns below the
			// mask is exactly the single-process pipeline's order, so
			// absorbing across them preserves semantics.
			child := node.Children[0]
			for child != nil && child.Type == logical.NodeFilter && len(child.Children) == 1 {
				child = child.Children[0]
			}
			if child != nil && child.Type == logical.NodeScan {
				absorbSecurityBarrier(node, child, stages)
			}
		}
	}
}

// absorbSecurityBarrier attaches a security-barrier projection to the scan
// stage just emitted for its child. The barrier lists every visible column
// (bare passthrough) with masked columns as literal expressions and denied
// columns absent; output names are the original column names, so downstream
// stages (joins, aggregates, gather renames) resolve unchanged — they just
// see masked values and never see denied ones.
func absorbSecurityBarrier(node, scan *logical.Node, stages *[]Stage) {
	var target *Stage
	for i := len(*stages) - 1; i >= 0; i-- {
		s := &(*stages)[i]
		if s.Type == StageScan && s.TableName == scan.TableName {
			target = s
			break
		}
	}
	if target == nil {
		return
	}
	// Trim to the columns the scan actually reads (parquet pruning hint) —
	// passthroughs of unread columns would emit useless null columns.
	need := make(map[string]bool, len(target.Columns))
	for _, c := range target.Columns {
		need[c] = true
	}
	specs := make([]ProjectExprSpec, 0, len(node.Projections))
	for _, pr := range node.Projections {
		name := pr.Alias
		if name == "" {
			name = pr.Column
		}
		if name == "" {
			continue
		}
		expr := pr.Expr
		if expr == "" {
			expr = pr.Column
		}
		isExpr := pr.ASTExpr != nil && !isSimpleColRefForRename(pr.ASTExpr)
		// Trim passthroughs the scan doesn't read; masks are computed from
		// literals (the scan never reads the raw column), so they always stay.
		if !isExpr && len(need) > 0 && !need[name] {
			continue
		}
		var typ parquet.TypeID
		if isExpr {
			// Same integer-preserving-arithmetic hint as
			// attachScanSelectProjections (#297, #445).
			typ = inferProjectionTypeCols(pr.ASTExpr, parquet.TypeString, strictIntArithCols(scan), scan.ScanColTypes)
		}
		specs = append(specs, ProjectExprSpec{Expr: expr, Name: name, Type: typ, TypeKnown: isExpr})
	}
	if len(specs) > 0 {
		target.SecurityProjectExprs = specs
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
	totalBytes, ok := p.estimateSubtreeBytes(joinNode.Children[1])
	if !ok {
		return false
	}
	// Broadcast threshold: defaults to 100 MB (legacy behavior). Distributed
	// callers override via BroadcastBytesThreshold to adapt the decision to
	// per-worker pool budget so a moderate build (say 500 MB) on a tight
	// cluster falls back to hash-shuffle instead of multiplying memory
	// pressure N× across worker procs.
	threshold := int64(100 * 1024 * 1024)
	if p.BroadcastBytesThreshold > 0 {
		threshold = p.BroadcastBytesThreshold
	} else if p.BroadcastBytesThreshold < 0 {
		return false // broadcast disabled
	}
	return totalBytes <= threshold
}

// estimateSubtreeBytes estimates a join input's post-selectivity size by
// walking through Filter/Project/Limit wrappers to the underlying Scan and
// scaling the table's manifest bytes by the subtree's estimated selectivity.
// Returns ok=false when the subtree has no scan root (e.g. another join) or
// the manifest is unavailable — callers must treat "unknown" conservatively.
//
// Under BushyJoinReorder, join-shaped subtrees (composite build sides) are
// estimated too: output bytes ≈ estimated output rows × the combined
// per-row width of the join's visible inputs. Without this, a 25-row
// nation ⋈ region pre-join is "unknown" → never broadcast-eligible → the
// whole composite pays exchange-repartition for both sides (the Q08 SF10
// regression, 2026-07-09: +135% from shuffling what should replicate).
// Gated on the flag: flag-off keeps semi/anti-leaf builds on their
// SF100-validated shuffle plans.
func (p *Planner) estimateSubtreeBytes(n *logical.Node) (int64, bool) {
	// Distinct(Project[keys]) build sides (IN/EXISTS decorrelation and
	// scalar-agg-semijoin key sources) are sized by distinct KEY count,
	// not table bytes × row selectivity. The row-selectivity path is a
	// trap here: Q04's EXISTS build is Distinct(l_orderkey) over lineitem
	// filtered by the col-vs-col l_commitdate < l_receiptdate — heuristic
	// selectivity shrank 76 GB of lineitem under the broadcast threshold
	// and SF100 replicated a ~200M-key hash build to every worker
	// (observed 2026-07-21: Q04 37s → 81s). Key-count × key-width says
	// ~1.6 GB → correctly stays on the shuffle plan, while Q17's filtered
	// part key source (~20K keys) still broadcasts.
	if n != nil && n.Type == logical.NodeDistinct {
		return p.estimateDistinctKeyBytes(n)
	}
	scan := findScanNode(n)
	if scan == nil {
		if logical.BushyJoinReorder.Load() {
			return p.estimateJoinSubtreeBytes(n)
		}
		return 0, false
	}
	// Estimate size from file count
	manifest, err := p.getManifest(context.Background(), scan.TableName)
	if err != nil {
		return 0, false
	}
	var totalBytes, totalRows int64
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			totalBytes += f.SizeBytes
			totalRows += f.NumRows
		}
	}
	// Apply filter selectivity to the bytes estimate. Q17's
	// `part WHERE p_brand=... AND p_container=...` filters 2M rows to
	// ~13K, dropping the build size from 200 MB raw to ~1.5 MB — well
	// below the broadcast threshold. Without this scaling, the raw
	// 200 MB exceeds the threshold and Q17 falls through to a
	// hash-shuffle that pays exchange-repartition for BOTH lineitem
	// (60M rows ≈ 6 GB at SF10) and part.
	//
	// Source of selectivity: RelStatsOf walks the subtree applying
	// histogram-driven predicate selectivity (CBO Phase 3 work). For
	// tables without HLL/histogram, the existing 0.33/0.1 heuristic
	// still applies so the scaling is at-worst-conservative.
	if totalRows > 0 {
		stats := logical.RelStatsOf(n)
		if stats.Rows > 0 && stats.Rows < float64(totalRows) {
			scale := stats.Rows / float64(totalRows)
			totalBytes = int64(float64(totalBytes) * scale)
		}
	}
	return totalBytes, true
}

// estimateDistinctKeyBytes sizes a Distinct(Project[cols]) subtree as
// distinct-key-count × a fixed per-key width. Distinct rows = min(input
// row estimate, product of the projected columns' NDVs); when any NDV is
// unavailable the input row estimate alone is the (looser) bound. Returns
// unknown for shapes other than Distinct→Project — bytes-based reasoning
// through a bare Distinct has no reliable width to scale by, and unknown
// degrades to the shuffle plan, the safe side.
func (p *Planner) estimateDistinctKeyBytes(d *logical.Node) (int64, bool) {
	if len(d.Children) != 1 {
		return 0, false
	}
	proj := d.Children[0]
	if proj == nil || proj.Type != logical.NodeProject ||
		len(proj.Children) != 1 || len(proj.Projections) == 0 {
		return 0, false
	}
	stats := logical.RelStatsOf(proj.Children[0])
	if stats.Rows <= 0 {
		return 0, false
	}
	for _, pr := range proj.Projections {
		if pr.Column == "" {
			return 0, false // expression projection — no width to reason from
		}
	}
	// Size by INPUT rows, not NDV: walkStages treats Distinct as a
	// passthrough (see the #163 note), so the replicated payload is the
	// UNdeduplicated projected scan output. Q22's anti build is
	// Distinct(o_custkey) over unfiltered orders — ~10M distinct keys but
	// 150M shipped rows; NDV-based sizing called it 160MB and SF100
	// replicated + hashed 150M rows on every worker (observed live
	// 2026-07-21: Q22 18.7s → 2m34s cold). Row-based sizing says 2.4GB →
	// correctly stays on the shuffle plan, while Q17's ~20K-row filtered
	// part key source still broadcasts. If replicate ever materializes
	// the dedup, this can tighten back toward NDV.
	//
	// 16 bytes per key column: int64 key + hash-table overhead. TPC-H
	// (and typical) semi-join keys are ints; a string-keyed build would
	// be underestimated, but the shuffle fallback on overflow is still
	// merely slower, not wrong.
	const bytesPerKeyCol = 16
	return int64(stats.Rows) * bytesPerKeyCol * int64(len(proj.Projections)), true
}

// estimateJoinSubtreeBytes estimates the output size of a join-shaped
// subtree: estimated output rows × the per-row width of the join's visible
// inputs (probe + build for inner/outer, probe only for semi/anti). Width
// derives from each input's own bytes/rows estimate (mutual recursion with
// estimateSubtreeBytes), so filter selectivity scaling composes through
// nesting; the plan tree is finite so the recursion terminates.
func (p *Planner) estimateJoinSubtreeBytes(n *logical.Node) (int64, bool) {
	if n == nil {
		return 0, false
	}
	// Unwrap single-child pass-through nodes to the join.
	for n != nil && n.Type != logical.NodeJoin {
		switch n.Type {
		case logical.NodeFilter, logical.NodeProject, logical.NodeLimit:
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
		}
		return 0, false
	}
	if n == nil || len(n.Children) != 2 {
		return 0, false
	}

	sideWidth := func(child *logical.Node) (float64, bool) {
		bytes, ok := p.estimateSubtreeBytes(child)
		if !ok {
			return 0, false
		}
		rows := logical.RelStatsOf(child).Rows
		if rows < 1 {
			rows = 1
		}
		return float64(bytes) / rows, true
	}

	width, ok := sideWidth(n.Children[0])
	if !ok {
		return 0, false
	}
	jt := strings.ToLower(n.JoinType)
	if jt != "semi" && jt != "anti" {
		buildWidth, ok := sideWidth(n.Children[1])
		if !ok {
			return 0, false
		}
		width += buildWidth
	}
	rows := logical.RelStatsOf(n).Rows
	if rows < 1 {
		rows = 1
	}
	return int64(rows * width), true
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

func (p *Planner) buildJoin(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) < 2 {
		return nil, nil, nil, fmt.Errorf("join requires two children")
	}

	jt := mapJoinType(node.JoinType)
	// An inner join with no condition at all IS a cross join (#376): the
	// join reorderer emits this shape for a comma-joined relation with no
	// edge to the rest of the chain, and reading the absent condition as a
	// failed key extraction refused legal SQL.
	if jt == "inner" && strings.TrimSpace(node.JoinCond) == "" && node.JoinFilter == "" {
		jt = "cross"
	}
	joinType := mapExecJoinType(jt)
	// An outer join may carry an ON residual (#358) — routed there by
	// logical.routeOuterJoinOnResiduals — and with it, zero key pairs.
	outerResidual := node.JoinFilter != "" &&
		(jt == "left" || jt == "right" || jt == "full")

	// Parse join condition to extract key columns (cross joins have no ON clause)
	var leftKeys, rightKeys []string
	if jt != "cross" {
		var residual []string
		leftKeys, rightKeys, residual = parseJoinKeys(node.JoinCond)
		if len(residual) > 0 {
			return nil, nil, nil, refuseJoinCond(jt, node.JoinCond, residual)
		}
		if len(leftKeys) == 0 && !outerResidual {
			return nil, nil, nil, fmt.Errorf("could not extract join keys from: %s", node.JoinCond)
		}
		// Fix key assignment using plan-level column ownership: ensure left keys
		// are probe-side and right keys are build-side. This avoids the expensive
		// post-build FixKeyAssignment hash table rebuild.
		assignJoinKeySides(leftKeys, rightKeys,
			subtreeNamingOf(node.Children[0]), subtreeNamingOf(node.Children[1]))
	}

	// Big-vs-big inner equi-joins route to sort-merge join when BOTH sides'
	// estimated bytes reach SortMergeJoinBytes (0 = disabled, the shipped
	// default — this branch is dormant unless the deploy opts in). Small
	// builds keep the strictly-better hash path below, unchanged.
	if joinType == exec.InnerJoin && node.JoinFilter == "" && len(leftKeys) > 0 &&
		p.shouldSortMergeJoin(node) {
		return p.buildSortMergeJoin(ctx, node, leftKeys, rightKeys)
	}

	hj := exec.NewHashJoin(joinType, leftKeys, rightKeys)

	// Set build-side table alias for column disambiguation in self-joins
	if alias := findScanAlias(node.Children[1]); alias != "" {
		hj.BuildTableAlias = alias
	}
	// Multi-table build subtrees carry per-column origin aliases so each
	// duplicate qualifies under its OWNING scan (nil for single-scan builds).
	hj.BuildColOrigins = subtreeNamingOf(node.Children[1]).buildColOrigins()

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
	// NOT IN's three-valued rule, which the anti join does not ask on its own
	// (#507). The logical rewrite is the only thing that knows this anti join
	// came from a NOT IN rather than a NOT EXISTS — and this is computed
	// BEFORE the swap below, because after it joinType is RightAntiJoin and
	// the flag would silently evaluate to false. It did: the local path
	// dropped the rule whenever the estimator chose the swap, while the DAG
	// (whose worker sets the flag from the spec) kept it — a two-path
	// divergence with PostgreSQL on the DAG's side.
	nullAwareAnti := node.NullAwareAnti && joinType == exec.AntiJoin

	// A filtered semi/anti join must NOT swap: the RightSemi/RightAnti probe
	// (markMatchedBuildEntries) marks every key-chain entry matched and never
	// evaluates SemiAntiFilter, so the non-equality condition would silently
	// vanish from the query — wrong results, not a performance trade.
	//
	// A NULL-AWARE anti join must not swap either, and for the same KIND of
	// reason: its two rules (a NULL probe key never survives; a NULL anywhere
	// in the build empties the answer) are applied on the semi/anti probe
	// path, which RightAntiJoin does not take — it marks build entries during
	// the probe and emits the unmatched ones from the arena afterwards. The
	// rules would vanish exactly as the filter would.
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter == "" && !nullAwareAnti &&
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
		assignJoinKeySides(leftKeys, rightKeys,
			subtreeNamingOf(node.Children[0]), subtreeNamingOf(node.Children[1]))
		// Update build-side alias + origins after swap
		if alias := findScanAlias(node.Children[1]); alias != "" {
			hj.BuildTableAlias = alias
		}
		hj.BuildColOrigins = subtreeNamingOf(node.Children[1]).buildColOrigins()
	}

	// Plan-declared schemas for the two sides, read only when a side delivers
	// no batch at all: an outer join still owes the rows the empty side
	// shapes and cannot name their columns without this (#348/#352). Computed
	// after the semi/anti swap above so the sides are final.
	hj.ProbeSchemaHint, hj.BuildSchemaHint = joinSideSchemas(node, hj.LeftKeys, hj.RightKeys)

	// For semi/anti joins without a filter, enable key-only build:
	// only build the key index and bloom filter, skip batch storage and arena refs.
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter == "" {
		hj.SemiAntiKeyOnly = true
	}

	hj.NullAwareAnti = nullAwareAnti

	// Filtered semi/anti builds must store rows for probe-time SemiAntiFilter
	// evaluation, but only the join keys + filter-referenced columns — narrow
	// the stored batches at arrival (see HashJoin.BuildStoreCols; the post-
	// build PruneBuildColumns below is a no-op for partition-on-arrival
	// builds, i.e. for every spill-eligible build).
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter != "" {
		hj.BuildStoreCols = SemiAntiBuildStoreCols(hj.RightKeys, node.JoinFilter)
	}

	// Pass build-side row estimate to pre-allocate arena and hash table.
	est := findScanRowEstimate(node.Children[1])
	if est > 0 {
		hj.BuildRowHint = est
	}

	// Determine if build should be deferred: large semi/anti builds overlap
	// with the early probe pipeline for better I/O utilization. Applies to
	// key-only builds and to filtered builds (whose arrival-time projection
	// keeps the deferred build's storage narrow); the deferred path's post-
	// build FixKeyAssignment is safe for partitioned builds via the nil'd-
	// entry guard, and PruneBuildColumns self-skips them.
	const deferBuildThreshold int64 = 1_000_000
	deferBuild := est > deferBuildThreshold &&
		(hj.SemiAntiKeyOnly || len(hj.BuildStoreCols) > 0)

	// Reverse bloom: for very large builds (>10M rows), run the probe side
	// first, build a bloom from its join key values, then filter the build
	// scan. Sacrifices I/O overlap for massive scan reduction.
	//
	// For semi/anti joins: bloom has no false negatives, correctness preserved.
	// For inner joins with large builds (>50M rows): each probe-split worker
	// only sees 1/N of the probe keys, so ~(N-1)/N of build rows are filtered
	// out. At SF100 with 3 workers, this reduces orders (150M) to ~50M and
	// partsupp (80M) to ~27M per worker — fitting in 4GB memory budget.
	useReverseBloom := reverseBloomToggle.On() &&
		((est > ReverseBloomThreshold && (joinType == exec.SemiJoin || joinType == exec.AntiJoin)) ||
			(est > ReverseBloomInnerThreshold && joinType == exec.InnerJoin))

	// Pre-compute post-build operations that can run in the build goroutine.
	var keepCols []string
	if joinType == exec.SemiJoin || joinType == exec.AntiJoin {
		keepCols = extractFilterBuildColumns(node.JoinFilter)
	}
	if (joinType == exec.SemiJoin || joinType == exec.AntiJoin) && node.JoinFilter != "" {
		hj.SemiAntiFilter = BuildSemiAntiFilter(node.JoinFilter)
		if pc, bc, ok := ParseSemiAntiNE(node.JoinFilter); ok {
			hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = pc, bc
		}
	}
	if outerResidual {
		hj.Residual = BuildJoinResidualFilter(node.JoinFilter, hj.BuildTableAlias)
		if hj.Residual == nil {
			// Refuse loudly rather than answer with the conjunct dropped —
			// the pre-#358 failure mode this path replaced.
			return nil, nil, nil, fmt.Errorf("join ON residual %q on a %s join: "+
				"not evaluable as a probe residual (columns, literals, arithmetic and "+
				"comparisons are; function calls and subqueries are not)",
				node.JoinFilter, jt)
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
		// The build runs here so it can overlap with the probe side's
		// preparation, which also means nothing above it recovers: this
		// goroutine's whole call stack — pipelineSource.runFrom, the
		// operators pushed onto the build side, every expression they
		// evaluate — had no boundary at all. A filter pushed onto a join's
		// build side that raised the DESIGNED query-error panic (an invalid
		// cast) therefore killed the SERVER, while the same condition on a
		// scan filter or the fast path returned a normal error to the client
		// (#508). Everything else it can raise did the same.
		//
		// buildErr is read by all three consumers after the barrier, so
		// setting it is the whole delivery. Registered after
		// close(buildDone), so it runs FIRST on the way out and the error is
		// in place before the barrier opens.
		defer exec.CatchQueryPanic(ctx, "hash join build", func(err error) {
			buildErr = fmt.Errorf("building hash table: %w", err)
		})
		if buildStart != nil {
			<-buildStart // wait for reverse bloom injection
		}
		if err := hj.Build(ctx, rbBuildSource); err != nil {
			buildErr = fmt.Errorf("building hash table: %w", err)
			return
		}
		if deferBuild || useReverseBloom {
			if hj.FixKeyAssignment() {
				slog.Warn("join key repair fired at runtime — plan-time side assignment missed a pair",
					"left_keys", hj.LeftKeys, "right_keys", hj.RightKeys)
			}
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
		p.applyLateMaterialization(probe)
		if len(node.NeededColumns) > 0 {
			filter := make(map[string]bool, len(node.NeededColumns))
			for _, col := range node.NeededColumns {
				filter[col] = true
			}
			probe.OutputFilter = filter
		}

		bridge := &reverseBloomBridge{
			childSource:   leftSource,
			childOps:      leftOps,
			rbBuildSource: &rbBuildSource,
			buildSource:   buildSource,
			buildStart:    buildStart,
			barrier:       buildDone,
			buildErr:      &buildErr,
			probeKey:      leftKeys[0],
			buildKey:      rightKeys[0],
			workers:       innerPipelineWorkers(leftSource),
			spill:         p.getSpillManager(),
		}
		return bridge, []exec.UnaryOperator{probe}, &exec.CollectSink{}, nil
	}

	if deferBuild {
		// Pipeline break: run early probes (scan → inner joins) as a child
		// pipeline that overlaps with the deferred build. The bridge collects
		// filtered batches, waits for the build barrier, then replays them
		// through the deferred probe operators.
		probe := hj.Probe()
		p.applyLateMaterialization(probe)
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
			spill:       p.getSpillManager(),
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
	// A repair firing here means plan-time assignJoinKeySides missed a pair.
	if hj.FixKeyAssignment() {
		slog.Warn("join key repair fired at runtime — plan-time side assignment missed a pair",
			"left_keys", hj.LeftKeys, "right_keys", hj.RightKeys)
	}

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
	p.applyLateMaterialization(probe)
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
	spill       *memory.SpillManager

	collector *exec.SpillableBatchCollector
}

func (d *deferredJoinBridge) Init(ctx context.Context) error {
	// Run child pipeline (scan → early probes) to collect filtered batches.
	// This overlaps with the deferred build goroutine(s) running in background.
	// The collector charges the tracker and spills past pressure — the raw
	// BatchSink it replaces pinned the entire collected probe side in
	// untracked heap while the deferred build held its hash table (double
	// residency, invisible to SpillManager victim selection).
	d.collector = &exec.SpillableBatchCollector{Spill: d.spill}
	pipe := &exec.Pipeline{
		Source:  d.childSource,
		Ops:     d.childOps,
		Sink:    d.collector,
		Workers: d.workers,
	}
	if err := pipe.Run(ctx); err != nil {
		d.collector.Release()
		// Wait for build goroutine to prevent leak
		select {
		case <-d.barrier:
		default:
		}
		return fmt.Errorf("deferred join child pipeline: %w", err)
	}

	// Wait for deferred build to complete
	select {
	case <-d.barrier:
	case <-ctx.Done():
		d.collector.Release()
		return ctx.Err()
	}
	if *d.buildErr != nil {
		d.collector.Release()
		return *d.buildErr
	}
	return nil
}

func (d *deferredJoinBridge) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return d.collector.NextReplay(ctx)
}

func (d *deferredJoinBridge) Close() error {
	if d.collector != nil {
		d.collector.Release()
	}
	return nil
}

// ReverseBloomsInstalled counts reverse-bloom filters actually pushed onto a
// build-side scan. A gate that means to exercise this path asserts on it:
// without it, a test can only prove the query answered, not that the
// optimization it was written for ever engaged.
var ReverseBloomsInstalled atomic.Int64

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
	spill         *memory.SpillManager

	collector *exec.SpillableBatchCollector
}

func (rb *reverseBloomBridge) Init(ctx context.Context) error {
	// Phase 1: Run child pipeline to collect probe-side batches. The
	// spill-backed collector charges the tracker and degrades to disk past
	// pressure — the raw BatchSink it replaces pinned the full probe side
	// (e.g. a 1/N lineitem split at SF100) in untracked heap while the
	// downstream join held its build. It also stays clear of CollectSink's
	// Finalize→ToRows boxing, which this bridge never needed.
	rb.collector = &exec.SpillableBatchCollector{Spill: rb.spill}
	pipe := &exec.Pipeline{
		Source:  rb.childSource,
		Ops:     rb.childOps,
		Sink:    rb.collector,
		Workers: rb.workers,
	}
	if err := pipe.Run(ctx); err != nil {
		rb.collector.Release()
		close(rb.buildStart)
		<-rb.barrier
		return fmt.Errorf("reverse bloom child pipeline: %w", err)
	}

	// Phase 2: Build bloom from the collected key column — a streaming
	// pass over the collector (reads spilled runs back from disk), so the
	// bloom build adds no resident copy.
	//
	// The builder owns the key encoding for BOTH sides of this filter: it
	// freezes it from the inserted column's own type and hands it to the op
	// it produces. That used to be two independent derivations — the insert
	// side hashed raw bytes while the probe side hashed the join's canonical
	// [null-flag][value] key, and the probe's int-vs-bytes dispatch came from
	// a THIRD reading, of the collector's parquet schema. For any bytes-backed
	// key they never agreed, so the filter rejected every build row and the
	// join answered on an empty build side (#543).
	bb := exec.NewBloomBuilder(rb.collector.Rows())
	if bb != nil {
		// A key column that changes type between batches has no single right
		// encoding, so the builder refuses it. That is a reason to answer with
		// NO filter, never a reason to fail the query: the join is correct
		// without one. Iterate's own errors (a spilled run that will not read
		// back) are real and still propagate.
		var encErr error
		if err := rb.collector.Iterate(func(b *batch.RecordBatch) error {
			if encErr != nil {
				return nil
			}
			encErr = bb.Add(b, rb.probeKey)
			return nil
		}); err != nil {
			rb.collector.Release()
			close(rb.buildStart)
			<-rb.barrier
			return fmt.Errorf("reverse bloom build: %w", err)
		}
		if encErr != nil {
			slog.Warn("reverse bloom key column is not one type across the probe output — filter not installed",
				"probe_key", rb.probeKey, "build_key", rb.buildKey, "err", encErr)
			bb = nil
		}
	}

	// Phase 3: Inject bloom filter into build-side pipeline.
	//
	// Two conditions before it goes in. The column has to have RESOLVED and
	// carried keys: a bloom built from a column no batch had rejects
	// everything, which for an anti-join invents unmatched probe rows. And
	// the filter has to match keys taken from its own insert side — a bloom
	// never has false negatives, so a miss there means the two sides encode
	// differently and every rejection is a lost row. Neither failure is worth
	// failing the query over: the join is correct without the filter, just
	// slower. Both are worth saying out loud.
	if bb != nil && bb.Resolved() && bb.Inserted() > 0 {
		bloomOp := bb.FilterOp(rb.buildKey)
		if err := bloomOp.SelfCheck(); err != nil {
			exec.BloomSelfCheckFailures.Add(1)
			slog.Error("reverse bloom rejects its own probe keys — filter NOT installed",
				"probe_key", rb.probeKey, "build_key", rb.buildKey, "err", err)
		} else {
			*rb.rbBuildSource = &pipelineSource{
				source: rb.buildSource,
				ops:    []exec.UnaryOperator{bloomOp},
			}
			ReverseBloomsInstalled.Add(1)
		}
	} else if bb != nil && !bb.Resolved() {
		slog.Warn("reverse bloom key column not found in the probe output — filter not installed",
			"probe_key", rb.probeKey, "build_key", rb.buildKey)
	}

	// Phase 4: Signal build goroutine to start with the bloom-filtered scan.
	close(rb.buildStart)

	// Wait for build to complete.
	select {
	case <-rb.barrier:
	case <-ctx.Done():
		rb.collector.Release()
		return ctx.Err()
	}
	if *rb.buildErr != nil {
		rb.collector.Release()
		return *rb.buildErr
	}
	return nil
}

func (rb *reverseBloomBridge) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return rb.collector.NextReplay(ctx)
}

func (rb *reverseBloomBridge) Close() error {
	if rb.collector != nil {
		rb.collector.Release()
	}
	return nil
}

// joinFlushSource wraps a join probe pipeline and, after the probe side is
// exhausted, drains the probe's flush phase: the spilled partitions first,
// then the resident unmatched build rows (see Next).
type joinFlushSource struct {
	inner    exec.Source
	innerOps []exec.UnaryOperator
	probe    *exec.HashJoinProbe
	pipeline *pipelineSource
	flushed  bool
	drained  bool
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
			return b, nil
		}
		// Probe exhausted — flush unmatched build-side rows. The probe names
		// the probe half of those rows itself: this source only sees the
		// join's OUTPUT batches, and passing one of those as the probe schema
		// mapped every preserved column onto the NULL side (a RIGHT JOIN's
		// unmatched rows came back with the preserved side blank). A join that
		// emitted no output batch at all — nothing matched — used to skip the
		// flush entirely and lose all of them.
		s.flushed = true
	}
	// NextFlush, not FlushUnmatchedRows: a RIGHT/FULL join whose build
	// EVICTED partitions owes the spilled partitions' joined output and their
	// build-side unmatched rows too, and this source is the only driver for
	// this shape — the probe sits in innerOps here, never in the outer
	// Pipeline's Ops, so exec.Pipeline.flushSpilledOps never sees it. Calling
	// the resident-only flush left every spilled partition unprocessed and
	// then dereferenced the nil'd build slots its own arena still pointed at
	// (#550). NextFlush walks the spilled partitions first and ends with
	// exactly the FlushUnmatchedRows this used to call.
	for !s.drained {
		b, err := s.probe.NextFlush(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			s.drained = true
			break
		}
		if b.ActiveLen() == 0 {
			continue
		}
		return b, nil
	}
	return nil, nil
}

// Close releases the probe pipeline whether or not Init ever ran.
//
// pipeline is assigned in Init, and a source can be constructed and then
// closed without one — a plan whose execution is abandoned between
// buildPipeline and the first Init (an early return, a cancellation, a set
// operation that decides not to pull a branch). Dereferencing the nil
// pipeline there crashed the whole server process (#510), and skipping the
// close instead would leak the source and operators that construction
// already built. Close what exists.
func (s *joinFlushSource) Close() error {
	if s.pipeline == nil {
		s.pipeline = &pipelineSource{source: s.inner, ops: s.innerOps}
	}
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
	drained  bool
	resident bool
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
	}
	// A spilling build evicts partitions, and the arena entries pointing at
	// the evicted rows are skipped by the flushes below — the spilled
	// partitions are replayed from disk here instead, each emitting its own
	// matched/unmatched build rows (#550). Without this drain those rows are
	// dropped and the partition's probe rows are never probed at all.
	for !s.drained {
		b, err := s.probe.NextFlush(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			s.drained = true
			break
		}
		if b.ActiveLen() == 0 {
			continue
		}
		return b, nil
	}
	if s.result == nil && !s.resident {
		s.resident = true
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

// Close releases the probe pipeline whether or not Init ever ran — the same
// contract, and the same #510 crash, as joinFlushSource.Close above.
func (s *rightSemiFlushSource) Close() error {
	if s.pipeline == nil {
		s.pipeline = &pipelineSource{source: s.inner, ops: s.innerOps}
	}
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

// preservesBuildSide reports whether a canonical join kind emits build-side
// rows that found no probe partner — the rows a RIGHT or FULL join exists to
// preserve, produced after probing by HashJoinProbe.FlushUnmatchedRows.
//
// Every distributed layout that REPLICATES the build side across tasks is
// unsound for these: each task holds the whole build and sees only its slice
// of the probe, so each would emit the same unmatched rows. Broadcast
// (walkStages) and skew-split (coordinator.planSkewSplitTasks) both gate on
// this; the hash-shuffle layout is sound because a partition's build and
// probe rows land on the same task.
func preservesBuildSide(jt string) bool {
	return jt == "right" || jt == "full"
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

// ParseSemiAntiNE recognizes a join filter that is EXACTLY one
// column-to-column not-equal condition ("l1.l_suppkey <> l2.l_suppkey").
// That is the decorrelated-EXISTS self-inequality class the distinct-pair
// build serves; anything else (conjunctions, other operators, literals)
// returns ok=false and stays on the generic closure path.
func ParseSemiAntiNE(filter string) (probeCol, buildCol string, ok bool) {
	if !SemiAntiNE.Load() || filter == "" {
		return "", "", false
	}
	parts := strings.Split(strings.ToLower(filter), " and ")
	if len(parts) != 1 {
		return "", "", false
	}
	part := strings.TrimSpace(parts[0])
	var idx int
	var opLen int
	if i := strings.Index(part, " <> "); i >= 0 {
		idx, opLen = i, 4
	} else if i := strings.Index(part, " != "); i >= 0 {
		idx, opLen = i, 4
	} else {
		return "", "", false
	}
	left := strings.TrimSpace(part[:idx])
	right := strings.TrimSpace(part[idx+opLen:])
	if !isBareColumnRef(left) || !isBareColumnRef(right) {
		return "", "", false
	}
	// Qualified: exec resolves both through columnIndexFallback, and the
	// qualifier is what distinguishes a joined build's colliding names.
	return left, right, true
}

// isBareColumnRef accepts identifier-shaped refs (optionally qualified);
// rejects literals, expressions, and anything with quoting or operators.
func isBareColumnRef(s string) bool {
	if s == "" {
		return false
	}
	hasLetter := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			hasLetter = true
		case r >= '0' && r <= '9', r == '.':
		default:
			return false
		}
	}
	return hasLetter
}

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
				// Kept QUALIFIED. A join emits a build column under its
				// relation's qualifier whenever the bare name collides on
				// the probe side, so stripping here resolved the filter to
				// whichever relation reorderJoins put on the probe — the
				// #527 defect one layer down from the logical plan.
				// filterColumnIndex falls back to the bare name for the
				// single-relation builds that emit it that way.
				left := strings.TrimSpace(part[:idx])
				right := strings.TrimSpace(part[idx+len(op)+2:])
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

	// Resolved once on first probe; sync.Once provides happens-before so
	// concurrent workers see the same probeIdxs / buildIdxs after the first
	// call returns. The schemas don't change for the lifetime of the join,
	// so caching the indices forever is safe.
	probeIdxs := make([]int, len(conds))
	buildIdxs := make([]int, len(conds))
	var resolveOnce sync.Once

	return func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		resolveOnce.Do(func() {
			for i, c := range conds {
				probeIdxs[i] = filterColumnIndex(probe, c.probeCol)
				buildIdxs[i] = filterColumnIndex(build, c.buildCol)
			}
		})

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

// filterColumnIndex resolves a semi/anti join filter's column name against a
// batch, mirroring exec.columnIndexFallback: the exact spelling first, then
// the bare name for a qualified reference over a single-relation build, then
// a UNIQUE qualified column for a bare reference. Ambiguity resolves to -1
// rather than to a guess — a filter that silently reads the wrong relation's
// column is what #527 was.
func filterColumnIndex(b *batch.RecordBatch, name string) int {
	if idx := b.ColumnIndex(name); idx >= 0 {
		return idx
	}
	if dot := strings.Index(name, "."); dot >= 0 {
		return b.ColumnIndex(name[dot+1:])
	}
	suffix := "." + name
	match := -1
	for i, c := range b.Schema {
		if strings.HasSuffix(c.Name, suffix) {
			if match >= 0 {
				return -1
			}
			match = i
		}
	}
	return match
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
		b, err := ps.source.Next(ctx)
		if err != nil || b == nil {
			return b, err
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

// parseJoinKeys reads a join condition STRUCTURALLY and returns the equi-join
// key columns, plus every conjunct it cannot represent as a key pair.
//
// The join executor takes a condition as two parallel lists of COLUMN NAMES,
// so the only ON conjunct it can express is an equality between two bare
// column references. Anything else — an expression operand
// (`r.r_regionkey + 3`), a literal operand (`n.n_regionkey = 1`), a non-equi
// operator, a disjunction — comes back in residual, and the caller refuses the
// plan rather than handing the executor a name that is not a column.
//
// This used to split the TEXT on " and " and then on the first "=", passing
// whatever fell either side through as a column name. An unresolvable name
// resolves to index -1 in the executor, which hashes as a constant, so the two
// failure modes were a join that matched NOTHING (one side a real column, the
// other not: `n.n_regionkey = r.r_regionkey + 3` answered 0 for a 10-row
// query) and a join that matched EVERYTHING (neither side real: a silent cross
// product). `a.x <= b.y` split on its own "=" and produced the column name
// "a.x <" — the splitting was lexical where the condition is structural
// (#351). Both are the shape this codebase keeps relearning: a key that does
// not resolve must error or fall back, never silently match nothing.
//
// Table qualifiers are preserved ("n1.n_regionkey") so that probe-side lookups
// against a self-join chain's qualified output schema resolve directly. The
// columnIndexFallback in the join executor strips the qualifier on miss, so
// unqualified scan-source schemas still resolve. Stripping here would force
// the executor to suffix-match a qualified column from {n1.X, n2.X}, which is
// ambiguous and returns -1 → 0 rows from the join.
//
// A conjunct comparing two CONSTANTS is passed through as a key pair
// unchanged. That is the optimizer's `1 = 1` sentinel, written into JoinCond
// when every ON conjunct has been pushed to a child (optimizer.go,
// extractJoinCondPredicates): it means ON TRUE, and a constant on both sides
// puts every row in one hash bucket, which is the cross product it asks for.
func parseJoinKeys(cond string) (leftKeys, rightKeys, residual []string) {
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return nil, nil, nil
	}
	expr := parseJoinCondExpr(cond)
	if expr == nil {
		// Unparseable. Refuse it: the lexical split used to invent column
		// names out of whatever text sat either side of an "=".
		return nil, nil, []string{cond}
	}
	for _, conj := range flattenJoinConjuncts(expr) {
		left, right, ok := joinKeyPair(conj)
		if !ok {
			residual = append(residual, conj.String())
			continue
		}
		leftKeys = append(leftKeys, left)
		rightKeys = append(rightKeys, right)
	}
	return leftKeys, rightKeys, residual
}

// parseJoinCondExpr parses a join condition into an expression AST. It borrows
// the WHERE slot of a dummy SELECT, the same trick logical.tryParseExpr uses.
func parseJoinCondExpr(cond string) plansql.Node {
	parsed, err := plansql.Parse("SELECT 1 FROM _dummy WHERE " + cond)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return nil
	}
	return info.WhereExpr
}

// flattenJoinConjuncts splits an ON expression on its top-level ANDs. Unlike
// the string split it replaces, it cannot be fooled by an " and " inside a
// string literal or under an OR.
func flattenJoinConjuncts(expr plansql.Node) []plansql.Node {
	switch e := expr.(type) {
	case *plansql.ParenNode:
		return flattenJoinConjuncts(e.Inner)
	case *plansql.AndNode:
		return append(flattenJoinConjuncts(e.Left), flattenJoinConjuncts(e.Right)...)
	}
	return []plansql.Node{expr}
}

// joinKeyPair reports the two key column names of one ON conjunct, and whether
// the conjunct is expressible as a key pair at all.
func joinKeyPair(conj plansql.Node) (left, right string, ok bool) {
	if p, isParen := conj.(*plansql.ParenNode); isParen {
		return joinKeyPair(p.Inner)
	}
	cmp, isCmp := conj.(*plansql.CmpExpr)
	if !isCmp || cmp.Op != "=" {
		return "", "", false
	}
	lName, lIsCol := joinKeyName(cmp.Left)
	rName, rIsCol := joinKeyName(cmp.Right)
	switch {
	case lIsCol && rIsCol:
		return lName, rName, true
	case !lIsCol && !rIsCol && lName != "" && rName != "":
		// Neither side names a column: the `1 = 1` ON-TRUE sentinel.
		return lName, rName, true
	}
	return "", "", false
}

// joinKeyName renders an ON operand the way the executor spells a column —
// qualifier kept, lowercased, which is what the text split produced. The bool
// reports whether the operand IS a bare column reference; a constant comes
// back with it false and its literal text as the name.
func joinKeyName(n plansql.Node) (string, bool) {
	switch e := n.(type) {
	case *plansql.ParenNode:
		return joinKeyName(e.Inner)
	case *plansql.ColRef:
		if e.Table != "" {
			return strings.ToLower(e.Table + "." + e.Column), true
		}
		return strings.ToLower(e.Column), true
	case *plansql.Lit:
		return strings.ToLower(e.String()), false
	}
	return "", false
}

// refuseJoinCond is the error a join whose ON clause the key representation
// cannot express. Both planning entry points raise it rather than let an
// unrepresentable conjunct reach the executor as a column name.
func refuseJoinCond(joinType, cond string, residual []string) error {
	return fmt.Errorf("join ON %q: %s cannot be represented as an equi-join key "+
		"(the %s join executor matches on column names, and only an equality between two "+
		"bare columns is one); it must be lifted into a filter above the join, which is legal "+
		"for an inner join only", cond, strings.Join(residual, ", "), joinType)
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
	if css, ok := source.(*catalogScanSource); ok && len(ops) == 0 {
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

	// If the child (or child chain through Filter/HAVING) leads to an Aggregate,
	// skip the projection when possible — the aggregate already produces correctly
	// named output columns (group-by cols + agg output cols).
	// Keep the projection when:
	//   1. Any non-aggregate projection has a complex AST expression (e.g., SUM(x) * 0.0001)
	//   2. Any projection renames a column via alias (e.g., l_suppkey AS supplier_no)
	if hasAggregateAncestor(child) {
		needsProject := false
		for _, proj := range node.Projections {
			// A literal select item is NOT elidable: the aggregate's output
			// carries it as a synthetic __gb_expr_N key column, and only the
			// projection renames it to the select-list name.
			if proj.ASTExpr != nil && !proj.IsAgg && !isPlainGroupKey(proj.ASTExpr) {
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
			if gbExpr != nil && !isPlainGroupKey(gbExpr) {
				gbExprToSyn[gbExpr.String()] = fmt.Sprintf("__gb_expr_%d", i)
			}
		}
	}

	// Catalog types of what feeds these projections, resolved once for the
	// whole list: a bare column reference inside a projection expression
	// decides its type from them (see nodeDeclaredType, #333). The second
	// map is for a SELECT expression that maps to a synthetic group column —
	// a rename of a value computed BELOW the aggregate, so it types against
	// the aggregate's input rather than its output.
	childColTypes := inputColTypes(child)
	var aggInputColTypes map[string]parquet.TypeID
	if isOverAggregate && len(aggNode.Children) > 0 {
		aggInputColTypes = inputColTypes(aggNode.Children[0])
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

		if expression == nil && proj.ASTExpr != nil && !proj.IsAgg {
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
					compiled, compErr = expr.CompileWithScopeResolver(proj.ASTExpr, p.subqueryRunner, outerTables, outerCols, p.subqueryInnerColumns())
				} else {
					compiled, compErr = expr.CompileWithScope(proj.ASTExpr, p.subqueryRunner, outerTables)
				}
			} else {
				compiled, compErr = expr.CompileWithRunner(proj.ASTExpr, p.subqueryRunner)
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
		outType := parquet.TypeString
		if proj.ASTExpr != nil && !proj.IsAgg {
			// A select expression mapped to a synthetic group column is a
			// RENAME of a value computed BELOW the aggregate — type it
			// against the aggregate's input, or the declared Float64
			// coerces the pre-projected int64 keys on the copy (#297).
			strictInt := strictIntArithCols(child)
			colTypes := childColTypes
			if isOverAggregate {
				if _, ok := gbExprToSyn[proj.ASTExpr.String()]; ok {
					strictInt = strictIntArithCols(aggNode.Children[0])
					colTypes = aggInputColTypes
				}
			}
			outType = inferProjectionTypeCols(proj.ASTExpr, outType, strictInt, colTypes)
		}

		pc := exec.ProjectColumn{
			Name: name,
			Type: outType, // Will be resolved at runtime if input column matches
			Expr: expression,
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
			if expr.IsCompileRefusal(compErr) {
				return nil, nil, nil, compErr
			}
			if compErr == nil {
				pc := exec.ProjectColumn{
					Name: synName,
					// Aggregate inputs are usually numeric, so Float64 is the
					// fallback — but MAX(UPPER(c)) is not, and declaring it
					// Float64 handed vecUpper a vector with no BytesData to
					// write into: the same process-killing mismatch as the
					// projection path (#310). MAX(COALESCE(a, b)) needs the
					// input's column types on top of that, or the polymorphic
					// declaration falls back to the same wrong Float64 (#333).
					Type: inferProjectionTypeCols(agg.InputExpr, parquet.TypeFloat64, nil, inputColTypes(node.Children[0])),
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
			Separator:  agg.Separator,
			Percentile: agg.Percentile,
			OutputCol:  agg.OutputCol,
			OutputType: outType,
		}
		aggCols = append(aggCols, ac)
	}

	// Catalog types of the aggregate's input, for typing derived GROUP BY
	// key expressions (see nodeDeclaredType, #333). Resolved once.
	aggChildStrictInt := strictIntArithCols(node.Children[0])
	aggChildColTypes := inputColTypes(node.Children[0])

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
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner)
				if expr.IsCompileRefusal(compErr) {
					return nil, nil, nil, compErr
				}
				if compErr != nil {
					continue
				}
				litPostOps = append(litPostOps, &aggPreProject{computed: []exec.ProjectColumn{{
					Name: fmt.Sprintf("__gb_expr_%d", i),
					Type: inferProjectionTypeCols(gbExpr, parquet.TypeString, aggChildStrictInt, aggChildColTypes),
					Expr: wrapExpr(compiled),
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
			if gbExpr != nil && !isPlainGroupKey(gbExpr) {
				synName := fmt.Sprintf("__gb_expr_%d", i)
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner)
				if expr.IsCompileRefusal(compErr) {
					return nil, nil, nil, compErr
				}
				if compErr == nil {
					pc := exec.ProjectColumn{
						Name: synName,
						// Numeric expressions (abs(x), x-1, …) must get a
						// numeric synthetic column: SetValue on a String
						// vector mangles float group keys.
						Type: inferProjectionTypeCols(gbExpr, parquet.TypeString, aggChildStrictInt, aggChildColTypes),
						Expr: wrapExpr(compiled),
					}
					// Batched evaluation when available — beyond the vec
					// kernels themselves, FuncCall.EvalVec is where the
					// per-batch input memo for expensive scalar functions
					// (regexp family, ClickBench Q29's GROUP BY key) lives;
					// the per-row Expr path bypasses it.
					if ve, ok := compiled.(expr.VecExpr); ok {
						pc.VecEval = ve.EvalVec
					}
					preProjectCols = append(preProjectCols, pc)
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
	if est := findScanRowEstimate(node.Children[0]); est > 0 {
		hashAgg.InputRowHint = est
	}
	if ndv := groupKeyNDVEstimate(node.Children[0], groupByCols); ndv > 0 {
		hashAgg.GroupNDVHint = ndv
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

// inferProjectionTypeCols is inferProjectionType with the input's column types
// in hand, in the two shapes the planner can supply them.
//
// strictInt marks scan columns whose vectors are plain Int64/Int32:
// integer-preserving arithmetic (+, -, *, % over those columns and integer
// literals) declares Int64 instead of the blanket Float64, keeping derived
// GROUP BY keys on the typed-int aggregation paths (#297; ClickBench Q36's
// four ClientIP-derived keys). The rule mirrors expr.BinOpNumeric's runtime
// mode resolution and MUST stay a strict subset of it: a declared-int column
// over a float-mode expression reads NULL through the typed getter. The
// reverse direction (declared float, runtime int) is the pre-existing safe
// coercion.
//
// colTypes (inputColTypes) is the full column→catalog-type map, and it is what
// lets a bare column reference INSIDE the expression decide a type rather than
// leaving the polymorphic declarations to fall back to Float64 (#333).
func inferProjectionTypeCols(node plansql.Node, fallback parquet.TypeID, strictInt map[string]bool, colTypes map[string]parquet.TypeID) parquet.TypeID {
	if strictInt != nil && expr.IntArithOn() {
		inner := node
		for {
			p, ok := inner.(*plansql.ParenNode)
			if !ok {
				break
			}
			inner = p.Inner
		}
		if bo, ok := inner.(*plansql.BinaryOp); ok && intArithAllInt(bo, strictInt) {
			return parquet.TypeInt64
		}
	}
	if !isComputedProjection(node) {
		// A bare column reference is a copy, and exec.Project types that
		// output from the column it copies — the input schema is the
		// authority there, and it sees renames and derived inputs the
		// catalog cannot. Withholding colTypes keeps the answer Undecided,
		// so this projection is typed by the caller's fallback exactly as
		// before. #333 is about the arguments INSIDE an expression, where
		// the output is computed and no input column describes it.
		colTypes = nil
	}
	// A guess is still the answer here: nothing is left to consult, and a
	// polymorphic function's fallback is what types SELECT NULLIF(int_col, 1)
	// numeric. Only expr.Undecided leaves the type to the caller.
	if t, c := nodeDeclaredType(node, colTypes); c != expr.Undecided {
		return t
	}
	return fallback
}

// intArithAllInt mirrors expr.operandIsInt over the AST: int-typed scan
// columns, integer literals, and nested integer arithmetic. Anything
// unrecognized declines (Float64 declaration = today's behavior). `/` is
// integer division over integer operands (#369, ADR-0012), so it declares
// Int64 exactly as +,-,*,% do — mirroring expr.BinOpNumeric's runtime mode,
// of which this must stay a strict subset.
func intArithAllInt(node plansql.Node, strictInt map[string]bool) bool {
	switch n := node.(type) {
	case *plansql.BinaryOp:
		switch n.Op {
		case "+", "-", "*", "%", "/":
		default:
			return false
		}
		return intArithAllInt(n.Left, strictInt) && intArithAllInt(n.Right, strictInt)
	case *plansql.ParenNode:
		return intArithAllInt(n.Inner, strictInt)
	case *plansql.ColRef:
		return strictInt[strings.ToLower(n.Column)]
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return false
		}
		_, err := strconv.ParseInt(n.Value, 10, 64)
		return err == nil
	}
	return false
}

// strictIntArithCols resolves the strictly-int column set feeding a node,
// walking only shape-preserving single-child steps. It deliberately stops
// at Project nodes: a projection may rebind a name to a non-int value,
// and a wrong int claim corrupts (see inferProjectionTypeCols); declining
// just keeps the Float64 declaration.
func strictIntArithCols(n *logical.Node) map[string]bool {
	for n != nil {
		switch n.Type {
		case logical.NodeScan:
			return n.ScanStrictIntCols
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// inputColTypes reports the catalog types of the columns visible at n's
// OUTPUT, keyed by lower-cased name, or nil when they cannot be known. It is
// what lets a bare column reference inside an expression decide a type
// (nodeDeclaredType) instead of leaving the polymorphic declarations —
// coalesce, nullif, greatest, least — to answer with the numeric fallback that
// typed SELECT COALESCE(n_name, n_comment) Float64 and dropped every string
// (#333). The map is logical.Node.ScanColTypes, populated by
// AnnotateScanColumns, and is READ-ONLY: the scan's own map is returned
// directly when there is only one.
//
// The walk is deliberately narrower than scanColumnType's, which searches
// every scan below a node for one name. This describes a node's output, so it
// stops at anything that can rebind a name to a different value — Project,
// Aggregate, Window, the set operators. Descending past one of those would
// answer
//
//	SELECT COALESCE(n_name) FROM (SELECT n_nationkey AS n_name FROM nation) t
//
// with the scan's string type for a value that arrives as an int64, which is
// the same silent-drop corruption pointing the other way. For the same reason
// a scan whose columns were never annotated (a table function, a catalog miss)
// makes the whole answer nil rather than a partial map: a name missing from a
// partial map is indistinguishable from a name that is not a column at all.
func inputColTypes(n *logical.Node) map[string]parquet.TypeID {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		return n.ScanColTypes
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return inputColTypes(n.Children[0])
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		left, right := inputColTypes(n.Children[0]), inputColTypes(n.Children[1])
		if left == nil || right == nil {
			return nil
		}
		merged := make(map[string]parquet.TypeID, len(left)+len(right))
		for c, t := range left {
			merged[c] = t
		}
		for c, t := range right {
			if prev, dup := merged[c]; dup && prev != t {
				// Two sides carry the name at different types — a self-join
				// is not the only way to reach one. Drop it rather than pick
				// a side; the caller's fallback is the honest answer.
				delete(merged, c)
				continue
			}
			merged[c] = t
		}
		return merged
	}
	return nil
}

// inputColDecimal is inputColTypes' companion for DECIMAL precision/scale
// (#458): the same walk, sourced from ScanColDecimal instead of
// ScanColTypes, and holding only entries a DECIMAL column has. A name two
// scans disagree on (different (p,s), same as a type disagreement above) is
// dropped rather than picking a side.
func inputColDecimal(n *logical.Node) map[string]logical.DecimalMeta {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		return n.ScanColDecimal
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return inputColDecimal(n.Children[0])
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		left, right := inputColDecimal(n.Children[0]), inputColDecimal(n.Children[1])
		if left == nil || right == nil {
			return nil
		}
		merged := make(map[string]logical.DecimalMeta, len(left)+len(right))
		for c, m := range left {
			merged[c] = m
		}
		for c, m := range right {
			if prev, dup := merged[c]; dup && prev != m {
				delete(merged, c)
				continue
			}
			merged[c] = m
		}
		return merged
	}
	return nil
}

// sourceColTypesThroughRenames is inputColTypes for an expression whose
// references were substituted through nested rename-only Projects (#387):
// after substitution the expression names only SOURCE columns, so the types
// visible BELOW the rename chain are the right ones — a plain rename rebinds
// names, not values. The walk descends only Projects that are pure
// column-forwarders (every item a plain column reference); a computed or
// aggregate item stops it with nil, because past that point a name may be
// rebound to a different value and inputColTypes' own warning applies.
func sourceColTypesThroughRenames(n *logical.Node) map[string]parquet.TypeID {
	for n != nil && n.Type == logical.NodeProject && len(n.Children) == 1 {
		for _, p := range n.Projections {
			if p.IsAgg || p.Column == "" {
				return nil
			}
		}
		n = n.Children[0]
	}
	return inputColTypes(n)
}

// strictIntArithColsThroughRenames mirrors sourceColTypesThroughRenames for
// the integer-preserving-arithmetic hint (#297): an expression rewritten by
// substituteNestedRenameRefs names only SOURCE columns, so the strict-int set
// visible BELOW the rename chain is the one to check the rewritten expression
// against (#445) — a plain rename forwards the exact int column, it does not
// rebind it to a different value.
func strictIntArithColsThroughRenames(n *logical.Node) map[string]bool {
	for n != nil && n.Type == logical.NodeProject && len(n.Children) == 1 {
		for _, p := range n.Projections {
			if p.IsAgg || p.Column == "" {
				return nil
			}
		}
		n = n.Children[0]
	}
	return strictIntArithCols(n)
}

// colRefDeclaredType resolves a bare column reference against the catalog
// types of its input (inputColTypes). Undecided — today's answer, and the
// caller's fallback with it — for a name no scan carries, a name two scans
// disagree on, and anything that is not a scan column at all: an aggregate
// output, a synthetic sort or group key. #331's machinery propagates a
// decision as fact, so a wrong confident answer here is worse than the guess
// it replaces.
//
// A qualified reference matches on its Column alone: the parser keeps the
// qualifier in Table, and a delimited identifier that contains a dot
// ("id.orig_h", a flat Zeek JSON column) is one name that must not be split.
func colRefDeclaredType(n *plansql.ColRef, colTypes map[string]parquet.TypeID) (parquet.TypeID, expr.Confidence) {
	t, ok := colTypes[strings.ToLower(n.Column)]
	if !ok {
		return 0, expr.Undecided
	}
	switch t {
	case parquet.TypeDecimal, parquet.TypeVector, parquet.TypeArray, parquet.TypeMap, parquet.TypeRow:
		// Parameterized types: the catalog map carries the TypeID and
		// nothing else, and a projection declared DECIMAL without its
		// scale, VECTOR without its dimension, or ARRAY without its element
		// type builds an output vector that reads back wrong. funcReturnType
		// declines the nested types for the same reason.
		return 0, expr.Undecided
	}
	return t, expr.Decided
}

// inferProjectionType infers the output parquet type from an AST expression
// node with nothing known about its input, returning the fallback when
// inference isn't possible.
func inferProjectionType(node plansql.Node, fallback parquet.TypeID) parquet.TypeID {
	return inferProjectionTypeCols(node, fallback, nil, nil)
}

// ProjectionOutputType is inferProjectionType for callers outside this
// package. The worker's pre-aggregate projection compiles a derived GROUP BY
// key from its SQL TEXT and has no catalog to resolve the columns in it, so it
// needs the same rule the planner applies to a SELECT-list expression — the
// same reason distributed.AggSpec.InputType is carried on the spec.
//
// It used to declare every derived key String, which is right only when the
// expression returns one: CAST(l_shipdate AS DATE) evaluates to an epoch-day
// number, and a String vector stored it as the DIGITS of that number, so the
// stage DAG grouped by "8039" where the single-process path grouped by
// 1992-01-05 (#340).
//
// Only a DECIDED type is taken. A polymorphic declaration that answered with
// its own fallback (expr.Guessed) has decided nothing here, because the caller
// holds no column types for it to consult: COALESCE(n_name, n_comment) would
// answer Float64 from coalesce's numeric fallback, and a Float64 vector drops
// every string it is handed — 1 group where there are 25 (#331/#333). The
// caller's fallback stands in those cases, exactly as before.
func ProjectionOutputType(node plansql.Node, fallback parquet.TypeID) parquet.TypeID {
	if t, c := nodeDeclaredType(node, nil); c == expr.Decided {
		return t
	}
	return fallback
}

// nodeDeclaredType reports the type an expression decides on its own, and how
// confidently.
//
// colTypes (inputColTypes) resolves a bare column reference to its catalog
// type. Without it — and for a name it does not carry — a column reference
// decides nothing, which is both what the caller's fallback is for and what a
// polymorphic function declaration needs to know before moving on to its next
// candidate argument. That was the whole answer until #333: nothing in
// COALESCE(n_name, n_comment) decided anything, so coalesce's numeric fallback
// stood, the projection allocated a Float64 vector, and every string write was
// dropped for the integer 0.
//
// The confidence matters only inside a nested call: everything below returns a
// type it decides outright, but a function call may return one it merely
// guessed, and its caller must keep looking (see expr.Confidence, #331).
func nodeDeclaredType(node plansql.Node, colTypes map[string]parquet.TypeID) (parquet.TypeID, expr.Confidence) {
	switch n := node.(type) {
	case *plansql.ColRef:
		return colRefDeclaredType(n, colTypes)
	case *plansql.BinaryOp:
		if n.Op == "||" {
			// String concatenation, not arithmetic. Declaring it Float64
			// handed the concat kernel an output vector with no BytesData,
			// so every row came back NULL (#328).
			return parquet.TypeString, expr.Decided
		}
		if t, c := binOpTemporalType(n, colTypes); c != expr.Undecided {
			return t, c
		}
		if !binOpInvolvesInterval(n) {
			return parquet.TypeFloat64, expr.Decided
		}
	case *plansql.UnaryOp:
		// Unary ± preserves its operand's numeric type (expr.UnaryOp.Eval
		// negates int64 as int64 since #369). Declaring it — instead of the
		// String fallback — is what lets `ORDER BY -col` sort numerically:
		// the hidden key materializes into a typed vector rather than into
		// text, where "-0" vs "0" rendering used to decide the order.
		if n.Op == "-" || n.Op == "+" {
			t, c := nodeDeclaredType(n.Inner, colTypes)
			if c != expr.Undecided {
				switch t {
				case parquet.TypeInt64, parquet.TypeInt32:
					return parquet.TypeInt64, c
				case parquet.TypeFloat64, parquet.TypeFloat32:
					return parquet.TypeFloat64, c
				}
			}
		}
	case *plansql.FuncCallNode:
		return funcReturnType(n, colTypes)
	case *plansql.ParenNode:
		return nodeDeclaredType(n.Inner, colTypes)
	case *plansql.CaseNode:
		return caseDeclaredType(n, colTypes)
	case *plansql.CmpExpr, *plansql.AndNode, *plansql.OrNode, *plansql.NotNode,
		*plansql.IsExpr, *plansql.LikeExpr, *plansql.BetweenExpr,
		*plansql.InExpr, *plansql.ExistsNode, *plansql.AnyAllExpr:
		// Predicates are boolean whatever their operands. Before #371 none
		// of these decided anything, so an aggregate over one — the
		// pre-aggregate projection has no runtime re-typing, unlike
		// exec.Project — fell back to Float64, the comparison kernel's
		// boolean writes were dropped, and BOOL_AND/BOOL_OR read 0 (false)
		// on every row.
		return parquet.TypeBool, expr.Decided
	case *plansql.CastNode:
		return inferCastType(n.TypeName), expr.Decided
	case *plansql.Lit:
		// Literal projections (e.g., SELECT 13, SELECT 'x') need a typed
		// output column so the runtime stores the value in the matching
		// typed slice instead of falling back to String. Without this,
		// `... IN (SELECT 13)` returns the literal as "13" and the IN
		// hash lookup against an int column fails to match.
		switch n.Kind {
		case plansql.LitNumber:
			if _, err := strconv.ParseInt(n.Value, 10, 64); err == nil {
				return parquet.TypeInt64, expr.Decided
			}
			return parquet.TypeFloat64, expr.Decided
		case plansql.LitBool:
			return parquet.TypeBool, expr.Decided
		case plansql.LitString:
			return parquet.TypeString, expr.Decided
		}
		// LitNull: type unknown; let the fallback decide.
	}
	return 0, expr.Undecided
}

// caseDeclaredType types a CASE from its result branches: the THEN
// expressions and the ELSE, which are the values the CASE can evaluate to
// (the WHEN conditions and a simple CASE's subject only steer). SQL requires
// the branches to share a type, so the first branch that decides one answers
// for the expression; a branch that only guesses (a polymorphic call whose
// arguments decided nothing, see expr.Confidence/#331) is kept as the
// fallback answer and reported Guessed, so a caller holding a candidate of
// its own can still prefer it. A missing ELSE is an implicit NULL and
// decides nothing, like LitNull.
//
// Before #372 CaseNode had no arm here at all, so MIN/MAX over a string
// CASE aggregated a Float64-declared projection that dropped every string
// write and answered the integer 0 — while the same CASE projected was
// correct, because exec.Project re-types from its input and the
// pre-aggregate projection does not.
func caseDeclaredType(n *plansql.CaseNode, colTypes map[string]parquet.TypeID) (parquet.TypeID, expr.Confidence) {
	var guess parquet.TypeID
	guessed := false
	consider := func(branch plansql.Node) (parquet.TypeID, bool) {
		if branch == nil {
			return 0, false
		}
		t, c := nodeDeclaredType(branch, colTypes)
		switch c {
		case expr.Decided:
			return t, true
		case expr.Guessed:
			if !guessed {
				guess, guessed = t, true
			}
		}
		return 0, false
	}
	for _, w := range n.Whens {
		if t, ok := consider(w.Result); ok {
			return t, expr.Decided
		}
	}
	if t, ok := consider(n.Else); ok {
		return t, expr.Decided
	}
	if guessed {
		return guess, expr.Guessed
	}
	return 0, expr.Undecided
}

// funcReturnType types a function call from the return type declared where the
// function is registered — the same declaration its vec kernel writes through.
//
// This replaces isNumericFunc, a hand-maintained list of function names that
// had to be remembered separately from the 273+ registrations in
// internal/engine/expr. Four times a function was missing from it, was
// therefore typed String, and its kernel wrote Float64Data or BoolData into a
// Bytes output vector — killing the server process for every connection, not
// just the session that asked: the temporal extractors (ClickBench Q19/Q43),
// the vector distance functions, the length family (`SELECT LENGTH(c) FROM t`),
// and starts_with/contains/ends_with. The list also carried names that are
// registered nowhere (date_part, strlen, ceiling, trunc), which is the same
// drift pointing the other way.
//
// expr.Undecided means the declaration does not decide and the caller keeps its
// own fallback. expr.Guessed means a polymorphic declaration answered with its
// fallback because none of its candidate arguments decided: usable, but a
// CALLING function still holding a candidate of its own must prefer that one —
// which is the whole of #331, where coalesce took a nested nullif's numeric
// fallback for fact and never asked the string literal beside it.
func funcReturnType(n *plansql.FuncCallNode, colTypes map[string]parquet.TypeID) (parquet.TypeID, expr.Confidence) {
	t, c := expr.DefaultRegistry.ReturnType(n.Name).Resolve(len(n.Args), func(i int) (parquet.TypeID, expr.Confidence) {
		return nodeDeclaredType(n.Args[i], colTypes)
	})
	if c == expr.Undecided {
		return 0, expr.Undecided
	}
	switch t {
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow:
		// map_keys() really does return an ARRAY, and the declaration says
		// so, but a projection has no element type to size the child vector
		// with and an ARRAY column built without one reads back empty. Keep
		// the string fallback until a projection can carry a nested type.
		return 0, expr.Undecided
	}
	return t, c
}

// inferCastType maps SQL type names to parquet types for CAST expressions.
//
// DATE and TIMESTAMP name the real column types because expr.Cast now produces
// their real representation — epoch days / epoch milliseconds — rather than
// passing its argument through (#340). The declared type is what turns that
// number back into a date at the output: the projection allocates a DATE
// vector, whose renderer is batch.FormatDate. Declaring String instead would
// print the day NUMBER, which is the mirror image of the bug being fixed.
//
// TIME stays a string: the engine has no time-of-day column type, so
// `TIME '10:00:00'` keeps its text, and so does expr.Cast.
func inferCastType(typeName string) parquet.TypeID {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "INTEGER", "INT", "BIGINT", "INT64":
		return parquet.TypeInt64
	case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "FLOAT64", "NUMERIC", "DECIMAL":
		return parquet.TypeFloat64
	case "BOOLEAN", "BOOL":
		return parquet.TypeBool
	case "DATE":
		return parquet.TypeDate
	case "TIMESTAMP", "DATETIME", "TIMESTAMPTZ":
		return parquet.TypeTimestamp
	default:
		return parquet.TypeString
	}
}

// binOpTemporalType types the two date-arithmetic shapes expr.BinOp evaluates,
// so the projection's output column can hold what the evaluator produces —
// the disagreement #340 is about, in the other direction.
//
//	date - date → BIGINT, a count of days
//	date ± n    → DATE, the day n days away
//
// Everything else declines and the caller's numeric/interval rules stand. In
// particular a TIMESTAMP operand declines: SQL calls that difference an
// INTERVAL and the engine has no interval column, so expr.BinOp.dateArith
// leaves it on the numeric path and this must agree.
func binOpTemporalType(n *plansql.BinaryOp, colTypes map[string]parquet.TypeID) (parquet.TypeID, expr.Confidence) {
	if n.Op != "+" && n.Op != "-" {
		return 0, expr.Undecided
	}
	lk := nodeTemporalKind(n.Left, colTypes)
	rk := nodeTemporalKind(n.Right, colTypes)
	switch {
	case n.Op == "-" && lk == temporalDay && rk == temporalDay:
		return parquet.TypeInt64, expr.Decided
	case lk == temporalDay && nodeIsPlainNumber(n.Right):
		return parquet.TypeDate, expr.Decided
	case rk == temporalDay && n.Op == "+" && nodeIsPlainNumber(n.Left):
		return parquet.TypeDate, expr.Decided
	}
	return 0, expr.Undecided
}

// temporalKind is what an operand of `date ± x` can be.
//
// A column the catalog declares VARCHAR counts as a day: that is how the
// TPC-H fixtures spell every date, and it is how `l_receiptdate - l_shipdate`
// reaches the operator at all. Nothing is lost by assuming it, because the
// only values the runtime can produce for a text column here are a day count
// (when the text parses as a date) and NULL — reading a text column as a
// number, which is what the caller's Float64 rule does, answers NULL either
// way.
type temporalKind int

const (
	temporalNone temporalKind = iota
	temporalDay
	temporalInstant
)

// nodeTemporalKind reports what kind of temporal value an operand carries: a
// CAST names one outright, and a column reference has one in the catalog.
func nodeTemporalKind(node plansql.Node, colTypes map[string]parquet.TypeID) temporalKind {
	var t parquet.TypeID
	switch n := node.(type) {
	case *plansql.ParenNode:
		return nodeTemporalKind(n.Inner, colTypes)
	case *plansql.CastNode:
		t = inferCastType(n.TypeName)
	case *plansql.ColRef:
		var ok bool
		if t, ok = colTypes[strings.ToLower(n.Column)]; !ok {
			return temporalNone
		}
		if t == parquet.TypeString {
			return temporalDay
		}
	default:
		return temporalNone
	}
	switch t {
	case parquet.TypeDate:
		return temporalDay
	case parquet.TypeTimestamp:
		// An instant difference is an INTERVAL in SQL and this engine has no
		// interval column to hold one, so expr.BinOp.dateArith declines it
		// and the caller's numeric rules stand.
		return temporalInstant
	}
	return temporalNone
}

// nodeIsPlainNumber reports whether an operand is a whole number written into
// the query — the `n` of `date ± n`. A column or a computed expression is
// deliberately excluded: its runtime value decides whether expr.BinOp takes
// the date branch at all, and a projection column typed DATE on a guess would
// print an integer difference as a date.
func nodeIsPlainNumber(node plansql.Node) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return nodeIsPlainNumber(n.Inner)
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return false
		}
		_, err := strconv.ParseInt(n.Value, 10, 64)
		return err == nil
	}
	return false
}

// binOpInvolvesInterval reports whether either operand of a BinaryOp is an
// IntervalLit or a date/timestamp function (current_date, current_timestamp).
// Date ± interval produces a date string, not a numeric value.
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

// isPlainGroupKey reports whether a GROUP BY expression is a bare column
// reference the aggregate can resolve by name. A literal is NOT plain here:
// GROUP BY 1 (a positional ref resolved to a literal select item, or an
// actual constant key) has no input column — it needs the synthetic
// pre-projection like any computed expression, or the key silently
// resolves to a nonexistent column and every row lands in one NULL group.
func isPlainGroupKey(node plansql.Node) bool {
	_, ok := node.(*plansql.ColRef)
	return ok
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
	cachedSchema      []parquet.Column   // cached output schema (computed once)
	cachedOutput      *batch.RecordBatch // most recent output (NOT reused — fresh struct each Execute call to avoid clobbering downstream's stored references; only the underlying Vectors are pooled via computedVectors)
	computedVectors   []*batch.Vector    // pooled computed-column vectors (reused across calls, sized to computedCap)
	computedCap       int                // row capacity of cached computed vectors
	canPassSelThrough bool               // true if all computed columns are numeric (no BytesColumn)
	checkedSelPass    bool               // true after first call resolves canPassSelThrough
	matPool           *batch.BatchPool   // pool for materialize buffers (avoids per-call allocation)
	shareOutputs      bool               // per-call vector allocation (partitioned-agg sharing)
}

func (a *aggPreProject) Init(_ context.Context) error { return nil }

// ReusesOutputBuffers marks the pre-projection's computed vectors as reused
// across Execute calls — its batches must not be shared across partition
// owners (exec.BufferReusingOperator) unless shared-output mode is on.
func (a *aggPreProject) ReusesOutputBuffers() bool { return !a.shareOutputs }

// EnableSharedOutputs switches to per-call computed-vector allocation so
// this op's batches can be shared across partition owners
// (exec.OutputSharingAware). Costs the pooled-buffer reuse; only the
// partitioned-aggregation pipeline enables it.
func (a *aggPreProject) EnableSharedOutputs() { a.shareOutputs = true }

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
	return &aggPreProject{computed: clonedComputed, shareOutputs: a.shareOutputs}
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

	// We reuse the COMPUTED vectors across Execute calls (the typed data
	// slices and null bitmaps are sized to a.computedCap) but we MUST NOT
	// reuse the *RecordBatch struct itself, because downstream sinks may
	// store batch pointers across calls (CollectSink does, the reverse-
	// bloom bridge does, etc.). Mutating a previously-returned batch's
	// Len / Columns silently corrupts whatever the sink has — manifesting
	// as Q05's panic in CollectSink.ToRows when the sink iterated a batch
	// whose Len had been bumped past the underlying column data's length
	// by a subsequent Execute call.
	//
	// Allocate a fresh RecordBatch struct every call. The struct itself is
	// tiny (header only); the heavy state (Vectors, BytesColumn buffers)
	// is still pooled via a.computedVectors.
	if a.shareOutputs || a.computedVectors == nil || in.Len > a.computedCap {
		a.computedVectors = make([]*batch.Vector, len(a.computed))
		for k, c := range a.computed {
			a.computedVectors[k] = batch.NewVector(c.Type, in.Len)
		}
		a.computedCap = in.Len
	} else {
		// Reuse computed vectors — reset null bitmaps and bytes columns
		for k := range a.computed {
			col := a.computedVectors[k]
			col.Len = in.Len
			col.Nulls.ResetNonNull(in.Len)
			switch col.Type {
			case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
				col.BytesData.Reset()
			}
		}
	}

	cols := make([]*batch.Vector, len(a.cachedSchema))
	// Pass-through columns share Vector pointers with the input. Detach the
	// input from its pool so prev.Release() becomes a no-op — the shared
	// column data must remain valid while this output is consumed downstream.
	in.Detach()
	for j := 0; j < len(in.Schema); j++ {
		cols[j] = in.Columns[j]
	}
	for k := range a.computed {
		cols[computedOffset+k] = a.computedVectors[k]
	}
	a.cachedOutput = &batch.RecordBatch{
		Schema:  a.cachedSchema,
		Columns: cols,
		Len:     in.Len,
		Sel:     in.Sel,
	}

	// Compute expression columns.
	// When selection vector is present and all computed columns are numeric,
	// write values only at selected indices (avoiding full materialize).
	for k, c := range a.computed {
		col := a.computedVectors[k]
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

	// Top-N late materialization: narrow scan + row-loc top-N + winner
	// refetch for wide-projection ORDER BY ... LIMIT over a plain scan.
	// Falls back to the ordinary top-N build when the shape doesn't
	// qualify (see topn_late_mat.go).
	if child.Type == logical.NodeSort && node.LimitVal > 0 {
		src, ok, err := p.tryBuildTopNLateMat(ctx, child, node.LimitVal+node.OffsetVal)
		if err != nil {
			return nil, nil, nil, err
		}
		if ok {
			var ops []exec.UnaryOperator
			if node.OffsetVal > 0 {
				ops = append(ops, exec.NewLimit(int64(node.LimitVal), int64(node.OffsetVal)))
			}
			return src, ops, &exec.CollectSink{}, nil
		}
	}

	// Optimization: Limit(Sort(...)) → TopN sort (heap-based, keeps only N rows)
	if child.Type == logical.NodeSort && node.OffsetVal == 0 && node.LimitVal != logical.NoLimit {
		return p.buildTopN(ctx, child, node.LimitVal)
	}
	// LIMIT n OFFSET m over a sort: same Top-K machinery with n+m kept
	// rows, plus the Limit operator above to skip the offset. Without
	// this, OFFSET queries (ClickBench Q40-43) fully materialized the
	// sort input.
	if child.Type == logical.NodeSort && node.OffsetVal > 0 && node.LimitVal > 0 {
		source, ops, sink, err := p.buildTopN(ctx, child, node.LimitVal+node.OffsetVal)
		if err != nil {
			return nil, nil, nil, err
		}
		ops = append(ops, exec.NewLimit(int64(node.LimitVal), int64(node.OffsetVal)))
		return source, ops, sink, nil
	}

	source, ops, sink, err := p.buildPipeline(ctx, child)
	if err != nil {
		return nil, nil, nil, err
	}

	// Push LIMIT hint to scan source: enables lazy file downloading instead
	// of the eager "download all files upfront" strategy. Safe when no pipeline
	// breaker (sort/aggregate/join) sits between LIMIT and SCAN — if there is
	// one, the source won't be a catalogScanSource and the assertion fails.
	// OFFSET without LIMIT bounds nothing: every row past the offset is in
	// the answer, so there is no row count at which the scan may stop.
	if cs, ok := source.(*catalogScanSource); ok && len(cs.rowPreds) == 0 && node.LimitVal > 0 {
		// Pushed scan filters need the row-group-parallel path; the lazy
		// LIMIT scan would silently skip them. Filters win — a filtered
		// LIMIT usually needs to scan broadly anyway.
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
	// Parity with buildSort: without the spill manager, the pre-sort input
	// of every ORDER BY ... LIMIT query buffered fully untracked — the
	// top-K heap only runs at finalize, so this was an unbounded,
	// pressure-invisible accumulation on the most common query shape.
	if sm := p.getSpillManager(); sm != nil {
		sortOp.Spill = sm
	}

	return &sortSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		sort:        sortOp,
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
		winCols = append(winCols, windowExecColumn(node, we))
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

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	// DISTINCT is a keys-only hash aggregate over all output columns (the
	// Trino/Spark shape). The previous streaming Distinct operator kept one
	// serialized key per distinct row in an untracked, unspillable map —
	// tens of GB at SF100 cardinalities, invisible to the memory budget.
	// As a HashAggregate it inherits spill, tracker accounting, cooperative
	// relief, and the typed group fast paths. GroupByAll resolves the key
	// set from the first batch's schema, so no plan-time schema knowledge
	// is needed (covers SELECT DISTINCT * and the semi/anti dedup rewrite).
	hashAgg := exec.NewHashAggregate(nil, nil)
	hashAgg.GroupByAll = true
	if est := findScanRowEstimate(node.Children[0]); est > 0 {
		hashAgg.InputRowHint = est
	}
	if sm := p.getSpillManager(); sm != nil {
		hashAgg.Spill = sm
	}

	return &aggSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		agg:         hashAgg,
	}, nil, &exec.CollectSink{}, nil
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

		// SQL says the arms of a set operation correspond BY POSITION and
		// the result takes the FIRST arm's column names. These rows are
		// keyed maps, so an arm whose columns are spelled differently has
		// to be re-keyed before anything compares or concatenates them —
		// `SELECT n_regionkey FROM nation UNION SELECT r_regionkey FROM
		// region` deduped nothing (every row of one arm was a distinct map
		// from every row of the other) and batch.FromRows then read the
		// right arm's values under names it does not carry and wrote NULLs.
		leftRows := leftSink.ToRows()
		rightRows := alignSetOpRows(leftSink.Schema(), rightSink.Schema(), rightSink.ToRows())

		// Schema() instead of Batches()[0].Schema — ToRows above released the
		// sinks' batches as it boxed them — and the arms' two schemas
		// UNIFIED rather than the first one alone, because FromRows re-reads
		// each row's rendered decimal text at the schema's scale and the
		// first arm's scale truncated the second arm's values (#532).
		//
		// It is resolved HERE rather than at the FromRows call below because
		// the DEDUP KEY needs it too: a set operation decides membership by
		// equality, so two values the comparator calls equal have to produce
		// one key — which their BOXES alone cannot say, a DECIMAL being
		// rendered text (#499).
		schema := unifySetOpSchemas(leftSink.Schema(), rightSink.Schema())
		keyer := newSetOpKeyer(schema)

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
	catalog          *catalog.Catalog
	tableName        string
	partitionFilter  map[string]string
	requiredCols     []string
	scanPreds        []logical.Predicate
	allowedFiles     []string // probe-split: only scan these files (nil = all)
	inner            exec.Source
	cache            *scanCached           // non-nil when this table is scanned multiple times
	replayIdx        atomic.Int64          // position in cache replay (atomic for parallel pipeline)
	projOnce         sync.Once             // guards projIdx/projSchema init (replay Next is concurrent)
	projIdx          []int                 // cache-batch column indices for this consumer; nil = no projection
	projSchema       []parquet.Column      // this consumer's projected schema
	isReplay         bool                  // true when reading from cache instead of scanning; written once in Init before runParallel starts, so no synchronization needed
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
			s.isReplay = true
			s.replayIdx.Store(0)
			return nil
		}
		// First scan claims the cache.
		s.cache.ready = make(chan struct{})
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
			if want[col.Name] {
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
				compiled, err = expr.CompileWithScopeResolver(pred.ASTExpr, p.subqueryRunner, outerTables, outerCols, p.subqueryInnerColumns())
			} else {
				compiled, err = expr.CompileWithScope(pred.ASTExpr, p.subqueryRunner, outerTables)
			}
		} else {
			compiled, err = expr.CompileWithRunner(pred.ASTExpr, p.subqueryRunner)
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
			if vf := tryPartialVectorize(compiled); vf != nil {
				return vf, nil
			}
			return exec.NewFilter(wrapPredicate(compiled)), nil
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
	if val == nil {
		return exec.NewMatchNothingFilter()
	}
	return exec.NewKernelFilterLit(col, op, val, text)
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
	return exec.NewInFilterLit(col, kept, keptTexts, negate)
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
							kernelOrNothing(col.Name, exec.OpLt, lo.Val, lo.Text),
							kernelOrNothing(col.Name, exec.OpGt, hi.Val, hi.Text),
						)}
					}
					return []exec.UnaryOperator{
						kernelOrNothing(col.Name, exec.OpGe, lo.Val, lo.Text),
						kernelOrNothing(col.Name, exec.OpLe, hi.Val, hi.Text),
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
			return []exec.UnaryOperator{inFilterForList(col.Name, values, texts, v.Not != neg)}
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
					return []exec.UnaryOperator{exec.NewLikeFilter(col.Name, s, v.Not != neg)}
				}
			}
		}
	case *expr.IsNull:
		// col IS NULL / col IS NOT NULL — vectorized null bitmap scan
		if col, ok := v.Operand.(*expr.ColRef); ok {
			return []exec.UnaryOperator{exec.NewNullCheckFilter(col.Name, v.Not == neg)}
		}
	case *expr.ColIsNull:
		// Offsets-shape rewrite of `col IS [NOT] NULL` (expr/shape_funcs.go).
		// Same kernel the *expr.IsNull case builds — without this the filter
		// would silently drop to row-at-a-time evaluation.
		return []exec.UnaryOperator{exec.NewNullCheckFilter(v.Col.Name, v.Not == neg)}
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
			// What the ENCLOSING query calls this scan, which inside a
			// derived table is the derived alias (#489).
			tableID := strings.ToLower(n.OuterTableID())
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

// subqueryInnerColumns returns a resolver that reports a table's columns from
// the catalog, so correlation analysis can bind an unqualified name inside a
// subquery to the subquery's own FROM before considering the outer query —
// the SQL scoping rule. Without it, a name that also exists in the outer
// scope is claimed by the outer scope unless the outer table's identifier
// happens to be spelled the same as an inner table, which turns an ordinary
// uncorrelated subquery into a per-row correlated one (issue #334).
//
// Unknown tables (CTEs, table functions) resolve to nil, which leaves the
// name to the identifier-comparison fallback rather than silently declaring
// it inner.
func (p *Planner) subqueryInnerColumns() plansql.TableColumns {
	if p.catalog == nil {
		return nil
	}
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	return func(table string) []string {
		t, err := p.catalog.GetTable(ctx, table)
		if err != nil || t == nil {
			return nil
		}
		cols := make([]string, len(t.Schema.Columns))
		for i, c := range t.Schema.Columns {
			cols[i] = c.Name
		}
		return cols
	}
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

// parseWindowFunc maps a SQL window function name onto its operator constant,
// falling back to ROW_NUMBER for a name exec does not implement. The fallback
// is the single-process pipeline's long-standing behavior and is left alone
// here; callers that can afford to refuse an unknown name (the worker's
// fragment builder) use exec.ParseWindowFunc's ok directly.
func parseWindowFunc(s string) exec.WindowFunc {
	fn, _ := exec.ParseWindowFunc(s)
	return fn
}

// windowOutputType declares the output type of an INPUT-INDEPENDENT window
// function — the rank family, whose answer is a position or a ratio computed
// from the frame, plus the aggregate window functions, which finalize to the
// same type whatever they consumed (COUNT to int64, SUM/AVG to float64).
//
// The value functions — lag, lead, first_value, last_value, nth_value — are
// NOT here: they return a value taken from their input column rather than
// computing one, so their output type IS that column's type and no name list
// can know it. Declaring them float64 typed the window's output vector
// numeric while the value path wrote strings, and exec.Window (unlike
// exec.Project) had no runtime correction, so every string write was dropped
// for the integer 0 (#345). windowSpecOutputType resolves them instead.
//
// MIN/MAX over a window were the last input-dependent family answered from
// this list, and landed on the float64 default: MIN(a_string) OVER (...)
// and MIN(int32_col) OVER (...) had #345's symptom for the same reason
// (#361). They resolve from the input column like the value functions, and
// since #569 for EVERY type the engine has — exec.WindowMinMaxType names
// them all, so what still reaches this list from a MIN/MAX is only an input
// type the planner could not resolve at all.
func windowOutputType(funcName string) parquet.TypeID {
	switch strings.ToLower(funcName) {
	case "row_number", "rank", "dense_rank", "count", "ntile":
		return parquet.TypeInt64
	case "percent_rank", "cume_dist":
		return parquet.TypeFloat64
	default:
		return parquet.TypeFloat64
	}
}

// windowValueFunc reports whether fn returns a value lifted out of its input
// column instead of computing one. Kept as its own predicate because the same
// five names decide the type question in the planner and the re-typing
// question in exec.Window.
func windowValueFunc(fn string) bool {
	switch fn {
	case "lag", "lead", "first_value", "last_value", "nth_value":
		return true
	}
	return false
}

// windowSpecOutputType declares the output type of one window expression over
// the subtree rooted at the Window node that owns it. It is to windowOutputType
// what aggSpecOutputType is to aggOutputType (#329, #333): the name list
// answers everything that is input-independent, and the input column answers
// the rest.
//
// The value functions copy a value out of their argument column, so the
// argument's catalog type is the answer. It is resolved through
// inputColTypes — the Window node's own input schema, which stops at anything
// that can rebind a name — and colRefDeclaredType, so a parameterized type
// (DECIMAL without its scale, VECTOR without its dimension, the nested types)
// declines the same way it does for a projection.
//
// UNDECIDABLE cases fall back to windowOutputType's float64, which is exactly
// today's behavior: a computed argument (`FIRST_VALUE(a || b)`), a column no
// scan below annotates, two scans that disagree, or an input the walk cannot
// describe at all. A confidently wrong type here is worse than the fallback —
// nothing downstream corrects a declaration, which is the whole of #345.
func windowSpecOutputType(node *logical.Node, we logical.WindowExpr) parquet.TypeID {
	fn := strings.ToLower(strings.TrimSpace(we.Func))
	minMax := fn == "min" || fn == "max"
	if !windowValueFunc(fn) && !minMax {
		return windowOutputType(fn)
	}
	// The same spelling buildWindow hands exec as the input column, so the
	// declaration always describes the vector the operator will read.
	col := cleanExpr(we.InputColumn())
	if col == "" || len(node.Children) != 1 {
		return windowOutputType(fn)
	}
	// colRefDeclaredType declines every PARAMETERIZED type (DECIMAL without
	// its scale, VECTOR without its dimension, the nested types), so those
	// keep the float64 fallback here and are corrected at runtime instead:
	// exec.Window.retypeValueColumns re-declares from the input vector and
	// exec.windowOutputColumn carries the (p,s)/element/field metadata with
	// it. A ZERO-ROW result has no such vector and is described from this
	// declaration alone, which is why `MIN(dec_col) OVER (...)` matching no
	// row still describes itself float8 while the same query matching rows
	// describes itself numeric — tracked in #587, not fixable by widening
	// colRefDeclaredType, whose decline exists for projections that have no
	// runtime correction at all.
	t, conf := colRefDeclaredType(&plansql.ColRef{Column: col}, inputColTypes(node.Children[0]))
	if conf != expr.Decided {
		return windowOutputType(fn)
	}
	if minMax {
		// MIN/MAX copy an input value through the same GetValue/SetValue
		// route as the value functions, so the declaration is the input's
		// own — for every type, since #569. exec.WindowMinMaxType is asked
		// rather than assumed so the planner and the operator cannot come to
		// different conclusions about a type; !ok keeps the float64
		// fallback, as an unresolvable input type does above.
		out, ok := exec.WindowMinMaxType(t)
		if !ok {
			return windowOutputType(fn)
		}
		return out
	}
	return t
}

// windowExecColumn resolves one logical WindowExpr into the executable
// column spec, over the Window node that owns it.
//
// It is the single place window arguments are read: the column out of the
// argument list, the offset/default/N that share it, the frame, and the
// output type. Both consumers go through it — the single-process pipeline
// (buildWindow) builds exec.WindowColumn directly, and walkStages copies the
// resolved values into the stage spec the DAG ships to workers. A worker has
// no catalog and no logical plan, so a second implementation there would be a
// second answer; the arguments are parsed once, here, where the types resolve
// (#345's shape, and #329/#333's).
func windowExecColumn(node *logical.Node, we logical.WindowExpr) exec.WindowColumn {
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
		Func: parseWindowFunc(we.Func),
		// InputColumn drops the offset/default/N that share the
		// argument string, so cleanExpr sees a column reference and
		// nothing else. Applied the other way round, a float default
		// (LAG(x, 1, 1.5)) looked like a qualified name and cleanExpr
		// returned "5" as the input column.
		InputCol:    cleanExpr(we.InputColumn()),
		OutputCol:   we.OutputCol,
		OutputType:  windowSpecOutputType(node, we),
		PartitionBy: we.PartitionBy,
		OrderBy:     orderKeys,
	}
	if we.Frame != nil {
		wc.Frame = &exec.WindowFrameSpec{
			Mode:  we.Frame.Mode,
			Start: exec.WindowBound{Type: we.Frame.Start.Type, Offset: we.Frame.Start.Offset},
			End:   exec.WindowBound{Type: we.Frame.End.Type, Offset: we.Frame.End.Offset},
		}
	}
	// Parse the function-specific arguments out of the rest of the
	// argument string (WindowExpr.InputCol carries the whole list verbatim).
	fn := strings.ToLower(we.Func)
	if fn == "ntile" {
		if n, err := strconv.Atoi(strings.TrimSpace(we.InputCol)); err == nil {
			wc.NtileBuckets = n
		}
	} else if fn == "nth_value" {
		if parts := strings.SplitN(we.InputCol, ",", 2); len(parts) >= 2 {
			if n, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
				wc.NthValueN = n
			}
		}
	} else if fn == "lag" || fn == "lead" {
		parts := strings.SplitN(we.InputCol, ",", 3)
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
				// A string default arrives as SQL source, quotes and
				// all; passing it through wrote 'none' — with the
				// quotes — into the result column.
				if len(defStr) >= 2 && strings.HasPrefix(defStr, "'") && strings.HasSuffix(defStr, "'") {
					defStr = strings.ReplaceAll(defStr[1:len(defStr)-1], "''", "'")
				}
				wc.LagLeadDefault = defStr
			}
		}
	}
	return wc
}

func aggOutputType(funcName string, distinct bool) parquet.TypeID {
	switch strings.ToLower(funcName) {
	case "count", "count_distinct", "approx_distinct":
		return parquet.TypeInt64
	case "string_agg":
		return parquet.TypeString
	case "bool_and", "every", "bool_or":
		return parquet.TypeBool
	default:
		return parquet.TypeFloat64
	}
}

// aggSpecOutputType declares the output type of one aggregate over the
// subtree rooted at the Aggregate node that owns it.
//
// COUNT is input-independent in this engine, so aggOutputType alone is exact
// for it, and it is the same declaration the single-process pipeline compiles
// into exec.AggColumn.OutputType. SUM and AVG are input-independent for every
// type but DECIMAL, over which they answer in DECIMAL (#455).
//
// MIN/MAX are the exception, and MIN_BY/MAX_BY with them: their output IS
// their (first) input's type, which exec.HashAggregate resolves from the
// vector it observes at Consume. To declare the same thing at plan time the
// input column has to resolve to a catalog type, so this walks the
// aggregate's inputs for it and returns 0 — undeclared — when it cannot: a
// derived-expression argument, a column no scan below carries, or two scans
// carrying it at different types.
// ok=false is the undeclared answer; callers fall back to the
// function-name derivation. It is returned as a second value rather than
// as a zero TypeID because TypeBool IS zero: MIN_BY over a BOOL column
// declares BOOL, and a caller reading that as "undeclared" is how a
// declaration goes missing on exactly one path (#354, #371).
func aggSpecOutputType(node *logical.Node, agg logical.AggExpr) (parquet.TypeID, bool) {
	fn := strings.ToLower(strings.TrimSpace(agg.Func))
	// SUM and AVG join the input-dependent list for ONE input type: over a
	// DECIMAL column they answer in DECIMAL, exactly (#455). Over everything
	// else they are still float64, and an input this cannot resolve — a
	// derived expression, a name no scan below carries, the partial's output
	// column read by a final aggregate — keeps that float64 declaration
	// rather than becoming undeclared, which is what it has always been.
	decimalCapable := false
	switch fn {
	case "min", "max", "min_by", "max_by":
	case "sum", "avg":
		decimalCapable = true
	default:
		return aggOutputType(agg.Func, agg.Distinct), true
	}
	unresolved := func() (parquet.TypeID, bool) {
		if decimalCapable {
			return aggOutputType(agg.Func, agg.Distinct), true
		}
		return 0, false
	}
	if agg.InputExpr != nil {
		if _, bare := agg.InputExpr.(*plansql.ColRef); !bare {
			return unresolved()
		}
	}
	in, ok := scanColumnType(node, agg.InputCol)
	if !ok {
		return unresolved()
	}
	if decimalCapable {
		if in == parquet.TypeDecimal {
			// exec.HashAggregate fills in the precision and scale from the
			// vector it observes (outputSchema); the TYPE is what has to
			// agree between the two paths at plan time.
			return parquet.TypeDecimal, true
		}
		return aggOutputType(agg.Func, agg.Distinct), true
	}
	if fn == "min_by" || fn == "max_by" {
		// The VALUE's type, for every type there is. MIN_BY/MAX_BY hand the
		// output vector the box GetValue produced for the winning row, so
		// the only declaration that can hold it is the input's own — see
		// exec.HashAggregate.outputSchema (#392). No switch here: a switch
		// is what fell through to FLOAT64 for the sixteen types outside it
		// and killed the process on the emit goroutine.
		return in, true
	}
	return minMaxDeclaredType(in), true
}

// aggSpecOutputDecimal is aggSpecOutputType's companion for the one piece a
// bare TypeID cannot carry: MIN/MAX/MIN_BY/MAX_BY of a DECIMAL(p,s) column
// answers in that SAME (p,s) — it hands back a value the column already
// holds, not a computed one — so a zero-row result can declare it exactly
// the way declaredOutputSchema declares the type itself (#458).
//
// SUM/AVG widen or rescale their input rather than keeping it, but the
// WIDENING RULE itself is fixed at plan time, not decided by the
// accumulator at runtime: SUM keeps the input's scale and widens precision
// to the carrier's full width, AVG additionally widens the scale by
// batch.AvgScaleIncrement (batch.AvgScale) — exec.HashAggregate.
// decOutputParams computes the identical (precision, scale) from the
// vector it observes, so mirroring the formula here (rather than leaving
// it "unconstrained, precision 0") makes a zero-row SUM/AVG-over-DECIMAL
// result agree with a non-empty one internally, closing the divergence the
// #416 zero-row-schema regression suite could not see because it compared
// only NAME and TYPE, not (precision, scale) (fold-in to #457/#458, FIX 2).
// The WIRE typmod for these is a separate question, answered unconditionally
// -1 for every aggregate regardless of this function's answer — see
// declaredWireUnconstrainedDecimal.
func aggSpecOutputDecimal(node *logical.Node, agg logical.AggExpr) (logical.DecimalMeta, bool) {
	fn := strings.ToLower(strings.TrimSpace(agg.Func))
	switch fn {
	case "min", "max", "min_by", "max_by", "sum", "avg":
	default:
		return logical.DecimalMeta{}, false
	}
	if agg.InputExpr != nil {
		if _, bare := agg.InputExpr.(*plansql.ColRef); !bare {
			return logical.DecimalMeta{}, false
		}
	}
	in, ok := scanColumnDecimal(node, agg.InputCol)
	if !ok {
		return logical.DecimalMeta{}, false
	}
	switch fn {
	case "sum":
		return logical.DecimalMeta{Precision: batch.MaxDecimalPrecision, Scale: in.Scale}, true
	case "avg":
		return logical.DecimalMeta{Precision: batch.MaxDecimalPrecision, Scale: batch.AvgScale(in.Scale)}, true
	default:
		return in, true
	}
}

// minMaxDeclaredType maps a MIN/MAX input column type to the output type
// exec.HashAggregate emits for it. It mirrors exec.minMaxOutputType, whose
// "0" answer means "keep what the planner declared" — float64 — so that
// case is spelled out here as float64 rather than propagated as undeclared.
func minMaxDeclaredType(in parquet.TypeID) parquet.TypeID {
	switch in {
	case parquet.TypeString:
		return parquet.TypeString
	case parquet.TypeBytes:
		return parquet.TypeBytes
	case parquet.TypeDate:
		return parquet.TypeDate
	case parquet.TypeTimestamp:
		return parquet.TypeTimestamp
	case parquet.TypeIPv4:
		return parquet.TypeIPv4
	case parquet.TypeIPv6:
		return parquet.TypeIPv6
	case parquet.TypeCIDR:
		return parquet.TypeCIDR
	case parquet.TypeUUID:
		return parquet.TypeUUID
	case parquet.TypeMAC:
		return parquet.TypeMAC
	case parquet.TypePort:
		return parquet.TypePort
	case parquet.TypeProtocol:
		return parquet.TypeProtocol
	case parquet.TypeDuration:
		return parquet.TypeDuration
	case parquet.TypeBool:
		return parquet.TypeBool
	case parquet.TypeInt64, parquet.TypeInt32:
		return parquet.TypeInt64
	case parquet.TypeDecimal:
		// MIN/MAX of a DECIMAL is a value the column HOLDS, so it answers in
		// DECIMAL — the accumulator was always an exact Int128 and only the
		// declaration said float64, which is where the digits went (#455).
		return parquet.TypeDecimal
	case parquet.TypeArray, parquet.TypeRow, parquet.TypeMap, parquet.TypeVector:
		// A container MIN/MAX answers with an input VALUE (#426), so its
		// declaration is the input's own — the MIN_BY rule above. A
		// FLOAT64 declaration over one of these is the #392 shape: the
		// output vector cannot hold the box at all.
		return in
	}
	return parquet.TypeFloat64
}

// scanColumnType resolves a column name to its catalog type by searching
// the scans below node (ScanColTypes, populated by AnnotateScanColumns).
// A qualified name matches on its bare suffix, since a scan's schema
// carries unqualified names. Two scans that disagree on the type — a
// self-join is not the only way to reach one — report not-found rather
// than picking a side.
func scanColumnType(node *logical.Node, col string) (parquet.TypeID, bool) {
	if node == nil || col == "" {
		return 0, false
	}
	if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
		col = col[dot+1:]
	}
	col = strings.ToLower(col)
	var found parquet.TypeID
	ok := false
	var walk func(n *logical.Node) bool
	walk = func(n *logical.Node) bool {
		if n == nil {
			return true
		}
		if t, present := n.ScanColTypes[col]; present {
			if ok && t != found {
				return false
			}
			found, ok = t, true
		}
		for _, c := range n.Children {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	if !walk(node) {
		return 0, false
	}
	return found, ok
}

// scanColumnDecimal resolves a DECIMAL column name to its (precision, scale)
// by searching the scans below node (ScanColDecimal, populated by
// AnnotateScanColumns) — the same walk as scanColumnType, for the one piece
// of a DECIMAL column's declaration a bare TypeID cannot carry (#458). Two
// scans that disagree report not-found, same as scanColumnType.
func scanColumnDecimal(node *logical.Node, col string) (logical.DecimalMeta, bool) {
	if node == nil || col == "" {
		return logical.DecimalMeta{}, false
	}
	if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
		col = col[dot+1:]
	}
	col = strings.ToLower(col)
	var found logical.DecimalMeta
	ok := false
	var walk func(n *logical.Node) bool
	walk = func(n *logical.Node) bool {
		if n == nil {
			return true
		}
		if m, present := n.ScanColDecimal[col]; present {
			if ok && m != found {
				return false
			}
			found, ok = m, true
		}
		for _, c := range n.Children {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	if !walk(node) {
		return logical.DecimalMeta{}, false
	}
	return found, ok
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
	// Synthetic finalization projections (two-level AVG) pass every
	// aggregate output through by name, so SELECT-list resolution treats
	// the aggregate below as directly visible.
	if node.Type == logical.NodeProject && node.PreservesAggOutputs && len(node.Children) > 0 {
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
		// Stages referenced via ScalarDependencies (Q15 late-bound scalar
		// producer chain) feed into FilterExprs through coordinator-side
		// substitution, not as record-batch input — but they still
		// SHOULDN'T be treated as plan-level leaves. Without this, the
		// next sort/aggregate's leafStages call picks up the producer's
		// terminal as a dependency and the producer's output gets
		// erroneously routed into the sort's input pipeline.
		for _, pid := range s.ScalarDependencies {
			depended[pid] = true
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

// SemiAntiBuildStoreCols returns the build-side columns a filtered semi/anti
// join must retain in stored build batches: the join keys (required to
// re-index spilled partitions and to survive FixKeyAssignment's rebuild)
// plus the JoinFilter's build-side columns. Returns nil when the filter is
// empty — unfiltered semi/anti builds are key-only and store nothing. Shared
// by the single-process planner and the worker fragment executor so both
// paths narrow their builds identically.
func SemiAntiBuildStoreCols(rightKeys []string, joinFilter string) []string {
	if joinFilter == "" {
		return nil
	}
	filterCols := extractFilterBuildColumns(joinFilter)
	cols := make([]string, 0, len(rightKeys)+len(filterCols))
	seen := make(map[string]bool, len(rightKeys)+len(filterCols))
	for _, c := range rightKeys {
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	for _, c := range filterCols {
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	return cols
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
				// Qualified, for BuildSemiAntiFilter's reason: the stored
				// build batch may carry the column under either spelling and
				// only the qualified one is unambiguous (#527). Both
				// consumers resolve with a bare-name fallback.
				right := strings.TrimSpace(part[idx+len(sep):])
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
		pipe := &exec.Pipeline{
			Source:  a.childSource,
			Ops:     a.childOps,
			Sink:    a.agg,
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
		pipe := &exec.Pipeline{
			Source:  s.childSource,
			Ops:     s.childOps,
			Sink:    s.sort,
			Workers: innerPipelineWorkers(s.childSource),
		}
		if err := pipe.Run(ctx); err != nil {
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
	inner.memTracker.ForceReserve(n)
}

// releaseScanBatch releases the charge recorded by trackScanBatch.
// Idempotent: a second release for the same batch is a no-op.
func (inner *scanSourceInner) releaseScanBatch(b *batch.RecordBatch) {
	if inner.memTracker == nil || b == nil {
		return
	}
	if n, ok := inner.batchCharges.LoadAndDelete(b); ok {
		inner.memTracker.Release(n.(int64))
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
		inner.memTracker.ForceReserve(n)
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
			inner.memTracker.Release(released)
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
	statsCols := make(map[string]parquet.Column, len(tableMeta.Schema.Columns))
	for _, c := range tableMeta.Schema.Columns {
		statsCols[c.Name] = c
	}
	var sp []scanPredicate
	var eqProbes []scan.EqProbe
	for _, pred := range s.scanPreds {
		if pred.Column == "" || pred.Op == "" || pred.Value == nil {
			continue
		}
		col, known := statsCols[pred.Column]
		if !known {
			continue
		}
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
		sp = append(sp, scanPredicate{Column: pred.Column, Op: pred.Op, Value: val})
		// Equality conjuncts also feed the dictionary probe — the
		// precise prune where zonemaps are blind (point filters on
		// high-cardinality columns). Dictionary entries are raw file
		// values too, so they take the same converted literal.
		if pred.Op == "=" && scan.DictPrune.On() {
			eqProbes = append(eqProbes, scan.EqProbe{ColName: pred.Column, Value: val})
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
