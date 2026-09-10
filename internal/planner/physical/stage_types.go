// This file holds stage types for the physical planner, governed by ADR-0010 and ADR-0026.
package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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
	// FilterAliases is FilterExprs' second spelling, index-aligned with it:
	// the predicate as the QUERY wrote it, naming a Project's OUTPUT
	// columns, where FilterExprs[i] holds the same predicate re-spelled into
	// the source columns those Projects read. A zero entry means the two
	// spellings are the same and there is nothing to choose between.
	//
	// Both are needed because which one a stage can evaluate is not known
	// when walkStages attaches the predicate. A Project emits no stage of
	// its own, so the usual answer is the source spelling — but
	// attachScanSelectProjections may later put an alias-naming OpProject on
	// the producing fragment, and then the stream carries the ALIAS and not
	// the source column. resolveFilterAliasSpelling settles it at the end of
	// planning, once that pass has run, exactly as resolveDerivedAliasSortKeys
	// settles the same question for a sort key (#656, #467).
	FilterAliases []FilterAliasSpec

	// Aggregate metadata
	//
	// GroupByCols is what the aggregate PUBLISHES each GROUP BY key as — the
	// name every consumer ABOVE it reads. It is `plansql.GroupKeyName`, the
	// same text the single-process planner feeds `exec.HashAggregate`, so
	// both engines' aggregate output schemas are one schema (ADR-0026 §2b).
	// GroupByResolve beside it is what the fragment that COMPUTES the key
	// resolves it BY, against its own input.
	GroupByCols []string
	// GroupByResolve is GroupByCols' second spelling, index-aligned with it:
	// the name or expression the COMPUTING fragment resolves each key by,
	// against the columns its input carries. A stage whose fragment does not
	// compute the keys — every merge-mode aggregate, whose input is a
	// partial's output where the key is already a column under its published
	// name — carries no list at all, and that is what makes the merge
	// boundary correct by construction rather than by agreement (#794).
	//
	// The two are one field's worth of information only when they are the
	// same string, which is every ordinary `GROUP BY c` and every ordinary
	// `GROUP BY c + 1`. They are different strings whenever the key names a
	// derived table's alias: the join stream carries `w` where the query
	// wrote `x.w`, and the defining expression `a * 3` names a column the
	// join does not carry at all. `Stage.GroupByCols` used to be both at
	// once, and the worker re-derived "is this key derived?" by PARSING it —
	// which is why every shape in that class answered one NULL group
	// (ADR-0026 §2, §4a; #736, #777, #781, #794, #795).
	//
	// Index-aligned with whichever group-key list the stage carries:
	// GroupByCols, FusedAggGroupBy on a fused scan-aggregate, or
	// ChainedAggGroupBy on a join that absorbed one. A stage carries exactly
	// one of the three; `stageGroupKeyList` is that rule, and
	// TestStageCarriesOneGroupKeyList asserts it.
	GroupByResolve []GroupKeyResolution
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
	// GroupByDecimal is GroupByTypes' companion for the (p,s) of its DECIMAL
	// entries — the part a bare TypeID cannot carry, and without which the
	// worker's key vector comes out at scale 0 and truncates every value
	// (ADR-0024 item 2, #379's shape one type over).
	GroupByDecimal map[string]logical.DecimalMeta
	AggSpecs       []AggSpec
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
	JoinType      string // inner, left, right, full, cross
	JoinLeftKeys  []string
	JoinRightKeys []string
	// JoinKeyTypes[i] is the resolved COMMON type of the pair
	// (JoinLeftKeys[i], JoinRightKeys[i]) — resolveJoinKeyTypes, #615. Both
	// sides' key bytes and the exchange's partition hash are built at it.
	// Nil means no pair needs widening, which is every same-type join.
	JoinKeyTypes    []parquet.TypeID
	LeftDepStage    string // stage providing probe (left) side
	RightDepStage   string // stage providing build (right) side
	BuildTableAlias string // build-side table alias for column disambiguation in self-joins
	// BuildColOrigins maps each bare build-output column (lowercased) to the
	// scan alias that owns it. Only set when the build subtree spans multiple
	// tables (bushy shapes) — nil for single-scan builds, where
	// BuildTableAlias is already exact. The join executor qualifies duplicate
	// build columns with the OWNING alias instead of BuildTableAlias.
	BuildColOrigins map[string]string
	// HiddenJoinCols is logical.Node.HiddenJoinCols for this join stage: the
	// columns the join MATERIALIZED for itself — a decorrelated LATERAL's
	// correlation key — which it must not publish, however wide the
	// consumer's ask is. The worker applies it as the probe's OutputExclude,
	// and declaredJoinSchema leaves them out of the declaration, so the
	// stage's empty-side files and its full ones describe the same relation
	// (ADR-0010). Nil on every other join.
	HiddenJoinCols []HiddenJoinCol
	// LateralEmptyDefaults is the empty-input value of each output column of
	// an ungrouped-aggregate lateral, LateralPadMarker the column whose NULL
	// marks the row that needs it, and LateralDropMarker says this stage's
	// default operator is the one that removes the marker. The worker runs
	// exec.LateralEmptyDefault directly above the probe, exactly as the
	// single-process planner does.
	LateralEmptyDefaults []LateralEmptyDefaultSpec
	LateralPadMarker     string
	LateralDropMarker    bool
	JoinFilter           string // semi/anti join inequality filter (e.g., "l2.l_suppkey != l1.l_suppkey")
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

	// WindowKeyExprs are the PARTITION BY / window ORDER BY terms the window
	// fragment must COMPUTE before it can key on them — an expression key
	// (`PARTITION BY id % 3`) names no column any upstream stage emits, and a
	// window that cannot find its key used to answer over one partition
	// (#585). Each spec's Name is the term's own text, which is also the name
	// WindowColSpec.PartitionBy/OrderBy carry, so the worker's projection and
	// the operator agree without a second naming convention.
	//
	// Nothing ABOVE the window reads these columns, which is what keeps them
	// clear of #558: the gather projects to the visible SELECT list and a
	// consumer stage reads the window's own outputs.
	WindowKeyExprs []ProjectExprSpec

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
	// OutputStringLength names the output columns whose declaration carries a
	// string LENGTH, and what it is — the DAG's copy of the single-process
	// path's SchemaHintStringLength (#838).
	OutputStringLength map[string]int

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

	// PostSecurityFilterExprs are predicates the USER wrote that must run
	// ABOVE the security projection, never in FilterExprs beside the
	// policy's own row filter.
	//
	// The scan fragment used to run `OpScan → OpFilter → SecurityProject`,
	// one filter slot for both kinds. A user predicate the logical plan
	// deliberately left above the barrier — one predicate substitution could
	// not push down, which in practice means one carrying a subquery — was
	// lowered into that slot and read the STORED column:
	// `WHERE bal > (SELECT MIN(bal) FROM t)` with `bal` masked to 0 returned
	// exactly the rows whose hidden value is positive, per row, to a client
	// that may not read the column. The in-process pipeline evaluated the
	// same predicate above the projection and returned none.
	//
	// The two kinds are told apart by logical.Node.PolicyFilter, which
	// InjectRowFilter sets: a POLICY predicate reads the row as stored
	// (PostgreSQL's RLS ordering, ADR-0033 decision 6) and stays in
	// FilterExprs; everything else goes here.
	PostSecurityFilterExprs []string

	// PolicyFilterExprs is the text of the FilterExprs entries that came from
	// a POLICY (logical.Node.PolicyFilter), so the invariant check can tell
	// the one predicate that is SUPPOSED to read the stored column from a
	// user predicate that must not. Carried as text rather than as indices
	// because the rewriting passes reorder and merge FilterExprs.
	PolicyFilterExprs []string

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

	// ConsumerScoped marks a stage carrying a Filter or a Project that
	// belongs to ONE of its consumers — a predicate written above a CTE
	// REFERENCE rather than inside the CTE's body. A stage like that must
	// never acquire a second consumer, because the second would read the
	// filtered stream (#656 follow-up); assertNoConsumerScopedFilterOn-
	// SharedStage is the check, and ValidateNativeDAGShape runs it.
	ConsumerScoped bool
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
	// IsAgg marks a rename whose source is an AGGREGATE output column rather
	// than a group key. The producer emits all group keys before all
	// aggregates, so when an aggregate shares a name with a group key their
	// select order is not their output order; the gather uses this to pair
	// each rename with the column of its own class (#575).
	IsAgg bool
	// Type is the PLAN's declared output type for a rename the gather has to
	// EVALUATE (Expr non-nil), with TypeKnown distinguishing a declaration
	// from the zero value the way ProjectExprSpec.TypeKnown does — TypeBool
	// is 0, so a genuinely boolean expression is indistinguishable from "not
	// set" without it.
	//
	// A column the gather computes exists in no catalog, so its declared type
	// IS its runtime type — the same rule ADR-0025 states for a materialized
	// sort key and for every ProjectExprSpec. Without it the gather built
	// EVERY wrapped expression into a float64 vector and nulled any box that
	// is not a number or a bool, so `CAST(MAX(c_ts) AS STRING)` came back NULL
	// on both DAG arms for every type while the single-process path rendered
	// the value (#831, #645).
	Type      parquet.TypeID
	TypeKnown bool
	Precision int
	Scale     int
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
	// Precision and Scale carry a computed DECIMAL's declaration alongside
	// Type, for the reason DecimalCoercion carries the same pair: a DECIMAL
	// is an unscaled integer plus a scale, and a worker that learns only the
	// TypeID builds the output vector at scale 0 and reads every value back
	// a hundredfold out (ADR-0024 item 2; #529, #555).
	Precision int
	Scale     int
	// SourceSlot names the input column by POSITION rather than by name, for
	// a projection whose input publishes the name TWICE. It is
	// exec.ProjectColumn.SourceIdx on the wire, and it exists for the same
	// reason that field does: `batch.RecordBatch.ColumnIndex` answers with the
	// FIRST match, so two specs reading one name read one column and the
	// other's value is unreachable (#575, #785).
	//
	// The producer that can publish a name twice is an AGGREGATE — a group
	// key and an aggregate output may share it (ADR-0026 §3a) — and the class
	// of each spec is what says which of the two it means.
	//
	// SourceSlotSet is required because 0 is a valid slot.
	SourceSlot    int
	SourceSlotSet bool
}

// UnionArm is one arm of a StageUnion: the projection that puts its output
// under the set operation's result column names. The projection is what makes
// the arms concatenable — without it each arm reaches the union under its own
// names (and a raw-parquet pass-through scan arm reaches it carrying every
// column of its table).
//
// The arm's PRODUCER is not stored here. It is `Stage.Dependencies[i]`, read
// through Stage.UnionArmDep, and there is exactly one record of it for the
// reason #715 gives: the arm used to carry a `DepStage` copy written at
// construction while `Dependencies` was rewritten by every later pass, and a
// pass that moved one and not the other left the pair inconsistent.
// ValidateNativeDAGShape then refused the plan — correctly, since nothing
// could say which stream an arm's projection applied to — and the refusal
// reached the client for queries the single-process path answers: a union over
// two identical sorted producers, and a union over a twice-referenced CTE.
// collapseRedundantFinalMergeSort was the pass that did it, and there were six
// writers of the copy to keep in step.
type UnionArm struct {
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

// UnionArmDep is the stage producing arm i of a StageUnion. Arm i is
// dispatched as task i reading Dependencies[i], so the dependency list IS the
// per-arm producer record; see UnionArm.
func (s *Stage) UnionArmDep(i int) string {
	if i < 0 || i >= len(s.Dependencies) {
		return ""
	}
	return s.Dependencies[i]
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
	// InputRefs is AggSpec.InputRefs for a WINDOW argument: the planner-only
	// record of the candidate spellings for a reference naming a derived
	// table's alias, settled at the end of planning by
	// bindConsumersToPublishedIdentity. Never on the wire.
	InputRefs []AggInputRef
}

// FusedJoinSpec describes a broadcast join absorbed into a parent join stage.
type FusedJoinSpec struct {
	JoinType        string
	JoinLeftKeys    []string
	JoinRightKeys   []string
	JoinKeyTypes    []parquet.TypeID // see Stage.JoinKeyTypes (#615)
	BuildDepStage   string           // stage providing build-side data
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
	JoinKeyTypes    []parquet.TypeID // see Stage.JoinKeyTypes (#615)
	BuildDepStage   string           // stage providing build-side data
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
	// HiddenJoinCols is the absorbed stage's own materialized columns (see
	// Stage.HiddenJoinCols): fusing a join into its parent must not turn a
	// column the join hid into one the fused stage publishes.
	HiddenJoinCols []HiddenJoinCol
	// LateralEmptyDefaults / LateralPadMarker / LateralDropMarker are the
	// absorbed join's own empty-input rules (see the Stage fields of the same
	// names). A LATERAL's pad marker and its per-column defaults belong to the
	// JOIN that manufactured the padded row, so a fused fragment owes one
	// `exec.LateralEmptyDefault` per absorbed lateral join, not one per
	// fragment: with these fields missing, `SELECT * FROM o JOIN LATERAL (…) s
	// ON true JOIN LATERAL (…) s2 ON true` published the second lowering's
	// minted slot `__key_1` to the client and left its empty-input default
	// unapplied — `COUNT(*) + 1` came back NULL for an outer row the lateral
	// matched nothing for, where PostgreSQL answers 1 (#988).
	LateralEmptyDefaults []LateralEmptyDefaultSpec
	LateralPadMarker     string
	LateralDropMarker    bool
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
	// OutputPrecision/OutputScale carry a DECIMAL OutputType's (p,s), for
	// InputPrecision/InputScale's reason one direction over: the .wshf header
	// a partial task writes carries half of every DECIMAL value it holds
	// (ADR-0010), and the one output row nothing observed — the identity row
	// an ungrouped aggregate emits when its filter matched no rows — has no
	// input vector to read the pair from. Declaring (0,0) there made the
	// aggregate merging that file read every OTHER partial's unscaled integer
	// as unscaled-at-zero: SUM(a) WHERE id < 5 answered 3824.00 for 38.24
	// (#685). Zero for every non-DECIMAL aggregate.
	OutputPrecision int
	OutputScale     int
	// InputType is the plan-time type of the vector InputExpr evaluates
	// into, mirrored onto distributed.AggSpec at dispatch. Zero when
	// there is no derived input. The worker hardcoded Float64 here, which
	// is the projection-typing defect of #310/#333 living in a second
	// place: MAX(COALESCE(a, b)) over two string columns wrote strings
	// into a Float64 vector and the aggregate saw zeros.
	InputType parquet.TypeID
	// InputPrecision/InputScale carry a DECIMAL InputType's (p,s), for
	// Stage.GroupByDecimal's reason: the materialized input vector is built
	// from the declaration alone.
	InputPrecision int
	InputScale     int
	// InputCol2, Separator and Percentile carry the aggregate arguments
	// past the first one — the second column of CORR/COVAR_*/MIN_BY/MAX_BY,
	// STRING_AGG's delimiter, PERCENTILE_CONT/DISC's fraction. They are
	// mirrored onto distributed.AggSpec at dispatch. Before #353 nothing
	// carried them at all: the parser kept only Args[0], so MIN_BY had no
	// ordering column and answered NULL, STRING_AGG ignored the separator
	// the query asked for, and PERCENTILE_CONT read its fraction as 0.
	InputCol2 string
	// InputCol3 is the third column argument (ohlcv's volume, #965). It rides
	// beside InputCol2 everywhere the spec travels.
	InputCol3 string
	// OutputFields declares a ROW-valued aggregate's fields — today only
	// ohlcv's bar. A bare TypeID cannot hold them, and a ROW vector with no
	// FieldNames cannot be written at all, so the declaration travels with
	// the spec the way OutputPrecision/OutputScale do for a DECIMAL.
	OutputFields []parquet.Column
	Separator    string
	Percentile   float64
	// Distinct is SQL's `AGG(DISTINCT x)` for every aggregate but COUNT,
	// which travels as the Func string "count_distinct" instead. It is
	// mirrored onto distributed.AggSpec at dispatch and read back into
	// exec.AggColumn.Distinct by the worker; without it a distributed
	// SUM(DISTINCT x) was a plain SUM (#703). Every DISTINCT aggregate
	// already forces the one-level RawInputAggregate shape (hasDistinctAgg),
	// so the worker's final stage sees raw rows and the set is exact.
	Distinct bool
	// InputRefs is the planner's own DEFERRED decision about every reference
	// in this argument that names a derived table's SELECT-list alias, and it
	// never reaches the wire (agg_wire.go copies fields by name). It is
	// GroupKeyResolution.Alias/Def one consumer over: which spelling the
	// producing fragment publishes for such a value is decided by
	// attachScanSelectProjections and absorbWindowArmProjection, which run
	// after walkStages, so emission records the candidates and
	// bindConsumersToPublishedIdentity settles them against the finished
	// stream (ADR-0026 §2, #770).
	InputRefs []AggInputRef
}

// AggInputRef is one reference inside an aggregate's argument that names a
// derived table's SELECT-list alias, with the candidate spellings only the
// finished stage graph can settle.
//
// The three answers to "what does the producer call this value" are the same
// three a GROUP BY key has: it publishes the ALIAS (Written resolves), it
// publishes the SOURCE column a plain rename reads (Source), or it publishes
// nothing and the value has to be recomputed from the DEFINITION (Def).
type AggInputRef struct {
	// Written is the spelling the shipped argument text uses.
	Written string
	// Source is the source column a plain rename names, and "" when the
	// alias names an expression.
	Source string
	// Def is the alias's defining expression, re-spelled into the columns the
	// derived table's own input carries, and "" for a plain rename.
	Def string
}

// SortKeySpec defines a sort key in a stage.
type SortKeySpec struct {
	Column    string
	Desc      bool
	NullsLast bool
	// WrittenTerm is the ORDER BY term exactly as the QUERY wrote it, kept
	// because Column is rewritten by four passes before planning ends and a
	// consumer that has to ask "which OUTPUT column does this term name" can
	// no longer read the question off it. Planner-only; the wire carries
	// Column, SlotPos and the ordering flags (execute_stage_dag.go).
	WrittenTerm string
	// SlotPos is the 1-based position of the input column this key sorts on,
	// or 0 to resolve Column by name. A name is an address only while it is
	// unique, and two output columns may legally share one (#557).
	SlotPos int
	// NamesAggregateOutput records, at EMISSION, whether the SELECT item this
	// term names is an AGGREGATE call rather than a group-key reference —
	// the CLASS, recorded where the SELECT list is still in hand, exactly as
	// AggSpec.InputRefs and WrittenTerm are (ADR-0026 §6).
	//
	// It is the half a name cannot supply. An aggregate emits `[group keys…,
	// aggregate outputs…]` into ONE schema, and `SELECT x.a AS b, SUM(x.b) AS
	// a … GROUP BY x.a` puts two columns called `a` in it; `ORDER BY a` names
	// the SELECT list's second item, and without the class the sort binds the
	// first column of that name — the group key — and answers the right rows
	// in the wrong order on both DAG arms (#968).
	//
	// Planner-only: what reaches the wire is the SlotPos it settles.
	NamesAggregateOutput bool

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
	// SourcePrecision/SourceScale carry a materialized DECIMAL key's (p,s),
	// which SourceType alone cannot: the fragment builds the key's vector
	// from this declaration and a DECIMAL one with no scale reads every
	// value back at 10^0 (ADR-0024 item 2).
	SourcePrecision int
	SourceScale     int

	// AliasExpr is the DEFINING EXPRESSION of a key that names a DERIVED
	// TABLE's COMPUTED alias, with AliasExprType its declared type. It is
	// ADR-0026 §2's two names for a SORT key: `Column` is what the key is
	// CALLED — `w`, the alias the query wrote — and this is where the value
	// comes FROM, `g * 3`, spelled in the scope of the fragment that will
	// compute it.
	//
	// Two names are needed because neither one alone is right on every plan.
	// `derivedAliasSourceColumn` declines a computed alias by design (there
	// is no source column to point at), so `AliasSource` stays empty and the
	// stage keyed on a name nothing emits: `sort: key column "w" does not
	// exist in the input schema` on both DAG arms for a query the
	// single-process pipeline answers (#807). Answering with the DEFINITION
	// instead would be wrong wherever some fragment DOES materialize the
	// alias — which is the mistake ADR-0025 records for an aggregate's
	// argument below a join — so the alias is kept as the published name and
	// the definition is materialized under it.
	AliasExpr          string
	AliasExprType      parquet.TypeID
	AliasExprTypeKnown bool
	AliasExprPrecision int
	AliasExprScale     int

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
