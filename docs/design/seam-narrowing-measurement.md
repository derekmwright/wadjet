# Physical planner boundary measurement

Issue #1142. Baseline: `561fb5a4a3379456e6bfb054f8698f70f7826b52`.

## Method

The inventory resolves Go identifiers with `go/types`, including test variants of every package in the main module. It excludes generated test-main packages, nested modules, test entry-point declarations, type parameters and local variables. Imported package aliases are resolved by import path. A package-qualified reference is a selector whose operand resolves to the physical package; member references through values are recorded separately, and counted once per DECLARING object — a method reached through a type that embeds the context is the same method. See [Identifiers reached by any spelling](#identifiers-reached-by-any-spelling). Comments and strings are not references. Each declaration is identified by its source position, so equal field names on different types remain distinct.

Categories a, c and d may overlap. Category b is dagplan test references minus category a. Category e contains production declarations with no direct reference outside physical, including its external test package. A method reached through an interface need not have a direct reference; absence from a reference list alone does not justify renaming it.

## Baseline counts

| Measurement | Count |
|---|---:|
| Production export declarations (including fields and methods) | 424 |
| a: dagplan production package-qualified names | 117 |
| dagplan test package-qualified names, before subtracting a | 55 |
| b: dagplan tests only, package-qualified names | 29 |
| a union b | 146 |
| c: other AGPL package-qualified names, production and tests | 16 |
| d: other MIT package-qualified names, production and tests | 22 |
| All AGPL package-qualified names, production and tests | 152 |
| e: declarations without a direct outside reference | 165 |

The supplied 146 is the union of dagplan production and test names on this baseline, not the production-only count. The supplied 1,098 declarations and 308 unused declarations are not reproduced by this definition. Test entry points and interface implementations cannot be lowercased merely to reach those totals without changing behavior.

## a: Dagplan production

```text
AggDerivedGroupKey
AggInputColumnDecimal
AggInputIsWideInteger
AggOhlcvOutputFields
AggOutputFromInputDecl
AggScopePreservingWrapper
AggSpecOutputDecimal
AggSpecOutputType
AggregateGroupKeyName
AssignJoinKeySides
AstIsFieldPath
BlockBareName
BlockPublishedColumns
BuildJoinResidualFilter
BuildStreamAlias
CleanExpr
ColDecls
ColSet
CollectASTCols
CollectColRefs
CollectColRefsBelow
CollectOuterColumns
CollectTableAliases
DecimalCoercion
DeclTypeParts
DeclaredJoinSchema
DeclaredOutputSchema
DeclaredStringLengths
DeclaredWireUnconstrainedDecimal
DerivedAliasSourceColumn
DerivedGroupKeyDecl
DerivedScopeBareName
EmittedColDecls
EmittedColTypes
EmittedColumnNames
EmittedKeyNames
FindAggregateAncestor
FindOutputProjectionNode
FindOutputProjectionsForRename
GroupKeyByIdentity
GroupKeyNames
GroupKeyResolution
HasFilterOrPartition
HasLimit
InferProjectionDeclType
InferProjectionDeclTypeConf
InlinedInSetRowCap
InputColDecls
IsSimpleColRefForRename
JoinArmAlias
JoinSideSchemas
LateralEmptySpec
LateralMarkerDroppedAbove
LateralSideOf
LogicalAggOutNames
ManifestSnapshot
ManifestSnapshotCtxKey
MapJoinType
MatchesPartitionFilter
NameIsPlainColumn
NamedArmScope
NewComputedColumnsOpWithMeta
NodeDeclaredType
OwnedJoinArm
ParseJoinKeys
Planner
ProjSourceName
ProjectExprSpec
ProjectionForName
ProjectionOutputName
PublishedNamesOfProjection
QualifiedColumn
QueryCost
ReferencesSynthetic
ReferencesSyntheticAgg
RefuseJoinCond
RefuseUnexpandedStarAnywhere
RefuseUnrepresentableRealInList
RelationScopeSubtree
RepublishDeclaredNames
ResolveAggInputName
ResolveJoinKeyTypes
ResolveNullsLast
ResolveOutputRenameSource
ResolveRenameSource
ResolveWindowKeys
RespellDerivedAliasRefs
ReverseBloomInnerThreshold
RewriteColRefs
SemiAntiNE
SetOpArmIsUnknownLit
SetOpArmPlan
SetOpArmProjection
SetOpArmTypeConflict
SetOpBaseName
SetOpColType
SetOpName
SetOpOutputNames
SetOpTargetType
SetOpUnknownLiteralArms
SlotSubsumeFlag
SlotWindowKey
SortInputSetOpWidth
SortKeySlotPos
SortKeyWrittenSlotPos
SortMergeJoinsPlanned
SourceColDeclsThroughRenames
StrictIntArithCols
StrictIntArithColsThroughRenames
SubstituteNestedRenameRefs
SubtreeNaming
SubtreeNamingOf
ValidateColumnsUnderPolicy
WantBareName
WindowExecColumn
WindowKeySpecs
WindowSpecOutputType
```

## b: Dagplan tests only

```text
AggregateOutputNames
AlignSetOpRows
BuildTableFunctionSource
CsvTableFuncSource
DbScanSource
DecimalFromBytes
DeferredJoinBridge
EvalFilterTyped
ExtractFilterBuildColumns
FormatBytes
GroupKeysPublishedBelow
HiddenSortTrimOp
IsGlob
IsURL
JoinKeyCommonType
JoinSideColTypes
JsonTableFuncSource
MapExecJoinType
MapPredOp
NewManifestSnapshot
NewPlanner
NewPlannerForContext
ParseSemiAntiNE
PhysicalPlan
ScopePreservingWrapper
SetOpDecimalTarget
SetOpWiden
SmjSourceAdapter
WrapsAWindow
```

## c: Other AGPL packages

```text
BuildJoinResidualFilter
BuildSemiAntiFilter
GroupKeyResolution
NewManifestSnapshot
NewPlanner
NewPlannerForContext
ParseSemiAntiNE
Planner
ProjectExprSpec
QueryLimitSQLState
ReverseBloomInnerThreshold
SemiAntiBuildStoreCols
SetOpCarrierGapPairs
SlotGroupKey
SlotName
SortMergeJoinsPlanned
```

## d: Other MIT packages

```text
DeclaredTypeOfNode
IsPGSystemColumn
LateMatJoinsPlanned
MaxInlinedInSetRows
MetadataCountsPlanned
MetadataMinMaxPlanned
NewPlanner
Planner
PublishedOutputNames
QueryLimitSQLState
RefuseReservedSlotNames
ReverseBloomInnerThreshold
ReverseBloomThreshold
ReverseBloomsInstalled
RowGroupBuffersResident
RowGroupLoadStats
ScanFilterPushdowns
ShapeOnlyColumnsPlanned
SortMergeJoinsPlanned
TopNLateMatPlanned
ValidateColumnsUnderPolicy
WithIdentityQueryLimits
```

## Full declaration inventory

Each category column records direct references to that declaration. `b` here means tests only after excluding a; c and d include tests. `e` means no direct outside reference. Locations also distinguish fields with the same spelling.

| Declaration | Kind | Location | Categories |
|---|---|---|---|
| `*CsvTableFuncSource.Close` | Func | `internal/planner/physical/table_func.go:247` | e |
| `*CsvTableFuncSource.Init` | Func | `internal/planner/physical/table_func.go:209` | e |
| `*CsvTableFuncSource.Next` | Func | `internal/planner/physical/table_func.go:243` | e |
| `*DbScanSource.Close` | Func | `internal/planner/physical/table_func.go:473` | b |
| `*DbScanSource.Init` | Func | `internal/planner/physical/table_func.go:446` | e |
| `*DbScanSource.Next` | Func | `internal/planner/physical/table_func.go:469` | e |
| `*DeferredJoinBridge.Close` | Func | `internal/planner/physical/join_sources.go:76` | e |
| `*DeferredJoinBridge.Init` | Func | `internal/planner/physical/join_sources.go:34` | b |
| `*DeferredJoinBridge.Next` | Func | `internal/planner/physical/join_sources.go:72` | b |
| `*JsonTableFuncSource.Close` | Func | `internal/planner/physical/table_func.go:127` | e |
| `*JsonTableFuncSource.Init` | Func | `internal/planner/physical/table_func.go:107` | e |
| `*JsonTableFuncSource.Next` | Func | `internal/planner/physical/table_func.go:123` | e |
| `*ManifestSnapshot.AggregateColumnStats` | Func | `internal/planner/physical/manifest_snapshot.go:109` | b |
| `*ManifestSnapshot.Get` | Func | `internal/planner/physical/manifest_snapshot.go:72` | b |
| `*PhysicalPlan.PrettyPrint` | Func | `internal/planner/physical/plan_types.go:87` | b, c, d |
| `*Planner.AnnotateScanColumns` | Func | `internal/planner/physical/scan_annotation.go:17` | a, c, d |
| `*Planner.ApplyContextColumnPolicies` | Func | `internal/planner/physical/validate_policy.go:140` | a |
| `*Planner.ApplyContextColumnPoliciesToNewScans` | Func | `internal/planner/physical/validate_policy.go:201` | a |
| `*Planner.BuildTopN` | Func | `internal/planner/physical/sort_plan.go:261` | e |
| `*Planner.CheckPolicyPlanOrderFromContext` | Func | `internal/planner/physical/validate_policy.go:245` | a |
| `*Planner.CteKeyColTypes` | Func | `internal/planner/physical/join_key_types.go:272` | a |
| `*Planner.DeclaredOutputSchema` | Func | `internal/planner/physical/subquery_pipeline.go:171` | c, d |
| `*Planner.EnforceQueryLimits` | Func | `internal/planner/physical/query_limits.go:72` | a |
| `*Planner.EnsureMemoryTracker` | Func | `internal/planner/physical/subquery_env.go:20` | d |
| `*Planner.EstimatePlanScanBytes` | Func | `internal/planner/physical/scan_estimate.go:25` | b, c |
| `*Planner.EstimatePlanScanCost` | Func | `internal/planner/physical/scan_estimate.go:88` | b |
| `*Planner.EstimateSubtreeBytes` | Func | `internal/planner/physical/join_plan.go:30` | a |
| `*Planner.ExecuteSubquerySchema` | Func | `internal/planner/physical/subquery_pipeline.go:418` | a |
| `*Planner.GetAggregateColumnStats` | Func | `internal/planner/physical/manifest_snapshot.go:163` | a |
| `*Planner.GetManifest` | Func | `internal/planner/physical/manifest_snapshot.go:154` | a |
| `*Planner.Plan` | Func | `internal/planner/physical/planner_entry.go:87` | b, c, d |
| `*Planner.ShouldSortMergeJoin` | Func | `internal/planner/physical/sort_merge_join.go:27` | a |
| `*Planner.SubqueryEnv` | Func | `internal/planner/physical/subquery_env.go:24` | d |
| `*Planner.SubqueryInnerColumns` | Func | `internal/planner/physical/subquery_scope.go:119` | a |
| `*Planner.SubqueryOutputArity` | Func | `internal/planner/physical/subquery_pipeline.go:116` | a |
| `*Planner.SubqueryOutputColumn` | Func | `internal/planner/physical/subquery_pipeline.go:206` | a |
| `*Planner.ValidateColumns` | Func | `internal/planner/physical/validate.go:56` | b |
| `*SmjSourceAdapter.Close` | Func | `internal/planner/physical/sort_merge_join.go:180` | e |
| `*SmjSourceAdapter.Init` | Func | `internal/planner/physical/sort_merge_join.go:152` | e |
| `*SmjSourceAdapter.Next` | Func | `internal/planner/physical/sort_merge_join.go:156` | e |
| `*SmjSourceAdapter.RowsScanned` | Func | `internal/planner/physical/sort_merge_join.go:185` | e |
| `*SubtreeNaming.BuildColOrigins` | Func | `internal/planner/physical/subtree_naming.go:169` | a |
| `*SubtreeNaming.MaterializedBuildColOrigins` | Func | `internal/planner/physical/subtree_naming.go:192` | a |
| `*SubtreeNaming.OwnsKey` | Func | `internal/planner/physical/subtree_naming.go:131` | b |
| `*abandonedClaimError.Error` | Func | `internal/planner/physical/catalog_scan.go:205` | e |
| `*abandonedClaimError.Unwrap` | Func | `internal/planner/physical/catalog_scan.go:209` | e |
| `*aggPreProject.Clone` | Func | `internal/planner/physical/computed_columns.go:135` | e |
| `*aggPreProject.Close` | Func | `internal/planner/physical/computed_columns.go:375` | e |
| `*aggPreProject.EnableSharedOutputs` | Func | `internal/planner/physical/computed_columns.go:130` | e |
| `*aggPreProject.Execute` | Func | `internal/planner/physical/computed_columns.go:146` | e |
| `*aggPreProject.Init` | Func | `internal/planner/physical/computed_columns.go:119` | e |
| `*aggPreProject.ReusesOutputBuffers` | Func | `internal/planner/physical/computed_columns.go:124` | e |
| `*aggSourceAdapter.Close` | Func | `internal/planner/physical/operator_sources.go:90` | e |
| `*aggSourceAdapter.Init` | Func | `internal/planner/physical/operator_sources.go:62` | e |
| `*aggSourceAdapter.Next` | Func | `internal/planner/physical/operator_sources.go:66` | e |
| `*aggSourceAdapter.RowsScanned` | Func | `internal/planner/physical/operator_sources.go:83` | e |
| `*aggSourceAdapter.ServesHeldState` | Func | `internal/planner/physical/operator_sources.go:60` | e |
| `*catalogScanSource.Close` | Func | `internal/planner/physical/catalog_scan.go:407` | e |
| `*catalogScanSource.Init` | Func | `internal/planner/physical/catalog_scan.go:110` | e |
| `*catalogScanSource.Next` | Func | `internal/planner/physical/catalog_scan.go:256` | e |
| `*catalogScanSource.RefetchRows` | Func | `internal/planner/physical/catalog_scan.go:92` | e |
| `*catalogScanSource.RowsScanned` | Func | `internal/planner/physical/catalog_scan.go:422` | e |
| `*catalogScanSource.SetBloomFilter` | Func | `internal/planner/physical/catalog_scan.go:101` | e |
| `*catalogScanSource.SetDynamicFilter` | Func | `internal/planner/physical/catalog_scan.go:106` | e |
| `*cteMaterializingSink.Close` | Func | `internal/planner/physical/cte_materialization.go:222` | e |
| `*cteMaterializingSink.Consume` | Func | `internal/planner/physical/cte_materialization.go:213` | e |
| `*cteMaterializingSink.Finalize` | Func | `internal/planner/physical/cte_materialization.go:220` | e |
| `*cteMaterializingSink.Init` | Func | `internal/planner/physical/cte_materialization.go:211` | e |
| `*generateSeriesSource.Close` | Func | `internal/planner/physical/table_func.go:574` | e |
| `*generateSeriesSource.Init` | Func | `internal/planner/physical/table_func.go:524` | e |
| `*generateSeriesSource.Next` | Func | `internal/planner/physical/table_func.go:530` | e |
| `*joinFlushSource.Close` | Func | `internal/planner/physical/join_sources.go:291` | e |
| `*joinFlushSource.Init` | Func | `internal/planner/physical/join_sources.go:233` | e |
| `*joinFlushSource.Next` | Func | `internal/planner/physical/join_sources.go:238` | e |
| `*multiFileReadCloser.Close` | Func | `internal/planner/physical/table_func.go:331` | e |
| `*multiFileReadCloser.Read` | Func | `internal/planner/physical/table_func.go:289` | e |
| `*parquetTableFuncSource.Close` | Func | `internal/planner/physical/table_func.go:193` | e |
| `*parquetTableFuncSource.Init` | Func | `internal/planner/physical/table_func.go:144` | e |
| `*parquetTableFuncSource.Next` | Func | `internal/planner/physical/table_func.go:185` | e |
| `*pipelineSource.Close` | Func | `internal/planner/physical/pipeline_source.go:175` | e |
| `*pipelineSource.Init` | Func | `internal/planner/physical/pipeline_source.go:42` | e |
| `*pipelineSource.Next` | Func | `internal/planner/physical/pipeline_source.go:67` | e |
| `*reverseBloomBridge.Close` | Func | `internal/planner/physical/join_sources.go:214` | e |
| `*reverseBloomBridge.Init` | Func | `internal/planner/physical/join_sources.go:105` | e |
| `*reverseBloomBridge.Next` | Func | `internal/planner/physical/join_sources.go:210` | e |
| `*rgSlabs.RowGroupBytes` | Func | `internal/planner/physical/scan_rowgroup_load.go:153` | e |
| `*rightSemiFlushSource.Close` | Func | `internal/planner/physical/join_sources.go:391` | e |
| `*rightSemiFlushSource.Init` | Func | `internal/planner/physical/join_sources.go:313` | e |
| `*rightSemiFlushSource.Next` | Func | `internal/planner/physical/join_sources.go:318` | e |
| `*sampleOperator.Close` | Func | `internal/planner/physical/table_func.go:727` | e |
| `*sampleOperator.Execute` | Func | `internal/planner/physical/table_func.go:699` | e |
| `*sampleOperator.Init` | Func | `internal/planner/physical/table_func.go:697` | e |
| `*scanSourceInner.RefetchRows` | Func | `internal/planner/physical/util.go:1265` | e |
| `*scannerExecSource.Close` | Func | `internal/planner/physical/scanner_source.go:593` | e |
| `*scannerExecSource.Init` | Func | `internal/planner/physical/scanner_source.go:250` | e |
| `*scannerExecSource.Next` | Func | `internal/planner/physical/scanner_source.go:589` | e |
| `*scannerExecSource.RowsScanned` | Func | `internal/planner/physical/scanner_source.go:611` | e |
| `*setOpSourceAdapter.Close` | Func | `internal/planner/physical/set_op_plan.go:298` | e |
| `*setOpSourceAdapter.Init` | Func | `internal/planner/physical/set_op_plan.go:184` | e |
| `*setOpSourceAdapter.Next` | Func | `internal/planner/physical/set_op_plan.go:186` | e |
| `*setOpSourceAdapter.RowsScanned` | Func | `internal/planner/physical/set_op_plan.go:316` | e |
| `*sortSourceAdapter.Close` | Func | `internal/planner/physical/operator_sources.go:143` | e |
| `*sortSourceAdapter.Init` | Func | `internal/planner/physical/operator_sources.go:118` | e |
| `*sortSourceAdapter.Next` | Func | `internal/planner/physical/operator_sources.go:122` | e |
| `*sortSourceAdapter.RowsScanned` | Func | `internal/planner/physical/operator_sources.go:151` | e |
| `*sortSourceAdapter.ServesHeldState` | Func | `internal/planner/physical/operator_sources.go:116` | e |
| `*topNLateMatSource.Close` | Func | `internal/planner/physical/topn_late_mat.go:282` | e |
| `*topNLateMatSource.Init` | Func | `internal/planner/physical/topn_late_mat.go:219` | e |
| `*topNLateMatSource.Next` | Func | `internal/planner/physical/topn_late_mat.go:221` | e |
| `*topNLateMatSource.RowsScanned` | Func | `internal/planner/physical/topn_late_mat.go:287` | e |
| `*topNLateMatSource.ServesHeldState` | Func | `internal/planner/physical/topn_late_mat.go:217` | e |
| `*unnestSource.Close` | Func | `internal/planner/physical/table_func.go:657` | e |
| `*unnestSource.Init` | Func | `internal/planner/physical/table_func.go:607` | e |
| `*unnestSource.Next` | Func | `internal/planner/physical/table_func.go:612` | e |
| `*windowKeyError.Error` | Func | `internal/planner/physical/window_keys.go:621` | e |
| `*windowKeyError.Unwrap` | Func | `internal/planner/physical/window_keys.go:625` | e |
| `*windowSourceAdapter.Close` | Func | `internal/planner/physical/operator_sources.go:194` | e |
| `*windowSourceAdapter.Init` | Func | `internal/planner/physical/operator_sources.go:170` | e |
| `*windowSourceAdapter.Next` | Func | `internal/planner/physical/operator_sources.go:172` | e |
| `*windowSourceAdapter.RowsScanned` | Func | `internal/planner/physical/operator_sources.go:187` | e |
| `*windowSourceAdapter.ServesHeldState` | Func | `internal/planner/physical/operator_sources.go:160` | e |
| `AggDerivedGroupKey` | Func | `internal/planner/physical/group_key_binding.go:235` | a |
| `AggInputColumnDecimal` | Func | `internal/planner/physical/aggregate_declared_output.go:373` | a |
| `AggInputIsWideInteger` | Func | `internal/planner/physical/agg_integer_width.go:85` | a |
| `AggOhlcvOutputFields` | Func | `internal/planner/physical/aggregate_declared_output.go:42` | a |
| `AggOutputFromInputDecl` | Func | `internal/planner/physical/aggregate_declared_output.go:460` | a |
| `AggScopePreservingWrapper` | Func | `internal/planner/physical/group_key_identity.go:311` | a |
| `AggSpecOutputDecimal` | Func | `internal/planner/physical/aggregate_declared_output.go:255` | a |
| `AggSpecOutputType` | Func | `internal/planner/physical/aggregate_declared_output.go:87` | a |
| `AggregateGroupKeyName` | Func | `internal/planner/physical/output_rename_resolve.go:27` | a |
| `AggregateOutputNames` | Func | `internal/planner/physical/aggregate_declared_output.go:687` | b |
| `Alias` | Var | `internal/planner/physical/group_key_carrier.go:47` | a |
| `AliasCols` | Var | `internal/planner/physical/subtree_naming.go:27` | a |
| `AlignSetOpRows` | Func | `internal/planner/physical/set_op_plan.go:465` | b |
| `AssignJoinKeySides` | Func | `internal/planner/physical/subtree_naming.go:208` | a |
| `AstIsFieldPath` | Func | `internal/planner/physical/declared_output.go:1022` | a |
| `Barrier` | Var | `internal/planner/physical/join_sources.go:26` | b |
| `Barrier` | Var | `internal/planner/physical/join_sources.go:95` | e |
| `Barrier` | Var | `internal/planner/physical/sort_merge_join.go:147` | e |
| `BlockBareName` | Func | `internal/planner/physical/block_projection.go:28` | a |
| `BlockPublishedColumns` | Func | `internal/planner/physical/block_projection.go:66` | a |
| `BuildErr` | Var | `internal/planner/physical/join_sources.go:27` | b |
| `BuildErr` | Var | `internal/planner/physical/join_sources.go:96` | e |
| `BuildErr` | Var | `internal/planner/physical/sort_merge_join.go:148` | e |
| `BuildJoinResidualFilter` | Func | `internal/planner/physical/join_residual.go:26` | a, c |
| `BuildSemiAntiFilter` | Func | `internal/planner/physical/join_sources.go:493` | c |
| `BuildStreamAlias` | Func | `internal/planner/physical/join_keys.go:151` | a |
| `BuildTableFunctionSource` | Func | `internal/planner/physical/table_func.go:34` | b |
| `Catalog` | Var | `internal/planner/physical/planner_config.go:24` | a |
| `ChildOps` | Var | `internal/planner/physical/join_sources.go:25` | b |
| `ChildOps` | Var | `internal/planner/physical/join_sources.go:91` | e |
| `ChildOps` | Var | `internal/planner/physical/operator_sources.go:108` | e |
| `ChildOps` | Var | `internal/planner/physical/operator_sources.go:164` | e |
| `ChildOps` | Var | `internal/planner/physical/operator_sources.go:47` | e |
| `ChildOps` | Var | `internal/planner/physical/sort_merge_join.go:145` | e |
| `ChildSource` | Var | `internal/planner/physical/join_sources.go:24` | b |
| `ChildSource` | Var | `internal/planner/physical/join_sources.go:90` | e |
| `ChildSource` | Var | `internal/planner/physical/operator_sources.go:107` | e |
| `ChildSource` | Var | `internal/planner/physical/operator_sources.go:163` | e |
| `ChildSource` | Var | `internal/planner/physical/operator_sources.go:46` | e |
| `ChildSource` | Var | `internal/planner/physical/sort_merge_join.go:144` | e |
| `CleanExpr` | Func | `internal/planner/physical/plan.go:123` | a |
| `Cleanup` | Var | `internal/planner/physical/plan_types.go:18` | c, d |
| `Coerce` | Var | `internal/planner/physical/set_op_types.go:612` | a |
| `ColDecls` | TypeName | `internal/planner/physical/declared_output.go:839` | a |
| `ColSet` | Func | `internal/planner/physical/shared_subplan_dedup.go:5` | a |
| `CollectASTCols` | Func | `internal/planner/physical/scan_filter_pushdown.go:415` | a |
| `CollectColRefs` | Func | `internal/planner/physical/carrier_schema.go:27` | a |
| `CollectColRefsBelow` | Func | `internal/planner/physical/carrier_schema.go:44` | a |
| `CollectOuterColumns` | Func | `internal/planner/physical/subquery_scope.go:17` | a |
| `CollectTableAliases` | Func | `internal/planner/physical/filter_plan.go:624` | a |
| `Column` | Var | `internal/planner/physical/scanner_source.go:245` | e |
| `Computed` | Var | `internal/planner/physical/group_key_carrier.go:44` | a, c |
| `CsvTableFuncSource` | TypeName | `internal/planner/physical/table_func.go:202` | b |
| `Ctes` | Var | `internal/planner/physical/planner_config.go:27` | a |
| `DbScanSource` | TypeName | `internal/planner/physical/table_func.go:438` | b |
| `Dec` | Var | `internal/planner/physical/declared_output.go:847` | a |
| `Dec` | Var | `internal/planner/physical/set_op_types.go:632` | a |
| `DecKnown` | Var | `internal/planner/physical/set_op_types.go:633` | a |
| `DecimalCoercion` | TypeName | `internal/planner/physical/plan_types.go:74` | a |
| `DecimalFromBytes` | Func | `internal/planner/physical/util.go:315` | b |
| `Decl` | Var | `internal/planner/physical/block_projection.go:49` | a |
| `Decl` | Var | `internal/planner/physical/group_key_carrier.go:56` | a, c |
| `DeclKnown` | Var | `internal/planner/physical/block_projection.go:56` | a |
| `DeclTypeParts` | Func | `internal/planner/physical/declared_output.go:1108` | a |
| `DeclaredJoinSchema` | Func | `internal/planner/physical/join_declared_schema.go:19` | a |
| `DeclaredOutputSchema` | Func | `internal/planner/physical/output_declared_schema.go:22` | a |
| `DeclaredStringLengths` | Func | `internal/planner/physical/output_declared_schema.go:492` | a |
| `DeclaredTypeOfNode` | Func | `internal/planner/physical/declared_output.go:1147` | d |
| `DeclaredWireUnconstrainedDecimal` | Func | `internal/planner/physical/output_declared_schema.go:259` | a |
| `Def` | Var | `internal/planner/physical/group_key_carrier.go:50` | a |
| `DeferredJoinBridge` | TypeName | `internal/planner/physical/join_sources.go:23` | b |
| `Delimited` | Var | `internal/planner/physical/group_key_identity.go:61` | e |
| `Derived` | Var | `internal/planner/physical/group_key_identity.go:64` | e |
| `DerivedAliasSourceColumn` | Func | `internal/planner/physical/hidden_sort_key.go:30` | a |
| `DerivedGroupKeyDecl` | Func | `internal/planner/physical/group_key_binding.go:135` | a |
| `DerivedScopeBareName` | Func | `internal/planner/physical/derived_alias.go:17` | a |
| `EmittedColDecls` | Func | `internal/planner/physical/declared_output.go:1124` | a |
| `EmittedColTypes` | Func | `internal/planner/physical/output_declared_schema.go:1048` | a |
| `EmittedColumnNames` | Func | `internal/planner/physical/join_hidden_slots.go:191` | a |
| `EmittedKeyNames` | Func | `internal/planner/physical/group_key_carrier.go:201` | a |
| `EvalFilterTyped` | Func | `internal/planner/physical/join_sources.go:599` | b |
| `Expr` | Var | `internal/planner/physical/block_projection.go:48` | a |
| `Expr` | Var | `internal/planner/physical/group_key_carrier.go:38` | a, c |
| `Expr` | Var | `internal/planner/physical/plan_types.go:38` | a, c |
| `Expr` | Var | `internal/planner/physical/window_keys.go:37` | e |
| `ExtractFilterBuildColumns` | Func | `internal/planner/physical/join_keys.go:215` | b |
| `Field` | Var | `internal/planner/physical/window_keys.go:43` | e |
| `Fields` | Var | `internal/planner/physical/declared_output.go:841` | a |
| `Fields` | Var | `internal/planner/physical/plan_types.go:37` | a, c |
| `Fields` | Var | `internal/planner/physical/set_op_types.go:619` | a |
| `Fields` | Var | `internal/planner/physical/window_keys.go:30` | e |
| `FindAggregateAncestor` | Func | `internal/planner/physical/aggregate_declared_output.go:848` | a |
| `FindOutputProjectionNode` | Func | `internal/planner/physical/output_renames.go:143` | a |
| `FindOutputProjectionsForRename` | Func | `internal/planner/physical/output_renames.go:133` | a |
| `FormatBytes` | Func | `internal/planner/physical/query_limits.go:102` | b |
| `GroupKeyByIdentity` | Func | `internal/planner/physical/group_key_identity.go:276` | a |
| `GroupKeyNames` | Func | `internal/planner/physical/group_key_carrier.go:74` | a |
| `GroupKeyResolution` | TypeName | `internal/planner/physical/group_key_carrier.go:34` | a, c |
| `GroupKeyResolution.Deferred` | Func | `internal/planner/physical/group_key_carrier.go:61` | a |
| `GroupKeysPublishedBelow` | Func | `internal/planner/physical/group_key_identity.go:373` | b |
| `HasFilter` | Var | `internal/planner/physical/query_limits.go:23` | a |
| `HasFilterOrPartition` | Func | `internal/planner/physical/query_limits.go:27` | a |
| `HasLimit` | Var | `internal/planner/physical/query_limits.go:24` | a |
| `HasLimit` | Func | `internal/planner/physical/query_limits.go:45` | a |
| `HiddenSortTrimOp` | Func | `internal/planner/physical/output_renames.go:177` | b |
| `Identity` | Var | `internal/planner/physical/group_key_identity.go:54` | e |
| `IdentityQueryLimitsFromContext` | Func | `internal/planner/physical/identity_limits.go:34` | e |
| `InferProjectionDeclType` | Func | `internal/planner/physical/declared_output.go:54` | a |
| `InferProjectionDeclTypeConf` | Func | `internal/planner/physical/declared_output.go:71` | a |
| `InlinedInSetRowCap` | Func | `internal/planner/physical/in_subquery_set.go:51` | a |
| `InputColDecls` | Func | `internal/planner/physical/declared_output.go:655` | a |
| `IsGlob` | Func | `internal/planner/physical/table_func.go:366` | b |
| `IsPGSystemColumn` | Func | `internal/planner/physical/validate.go:1834` | d |
| `IsSimpleColRefForRename` | Func | `internal/planner/physical/output_renames.go:18` | a |
| `IsURL` | Func | `internal/planner/physical/table_func.go:395` | b |
| `JoinArmAlias` | Func | `internal/planner/physical/join_keys.go:130` | a |
| `JoinKeyCommonType` | Func | `internal/planner/physical/join_key_types.go:30` | b |
| `JoinSideColTypes` | Func | `internal/planner/physical/join_key_types.go:123` | b |
| `JoinSideSchemas` | Func | `internal/planner/physical/join_declared_schema.go:276` | a |
| `JsonTableFuncSource` | TypeName | `internal/planner/physical/table_func.go:101` | b |
| `Known` | Var | `internal/planner/physical/set_op_types.go:621` | a |
| `LateMatJoinsPlanned` | Var | `internal/planner/physical/late_mat.go:16` | d |
| `LateMaterialization` | Var | `internal/planner/physical/planner_config.go:119` | c, d |
| `LateralEmptySpec` | Func | `internal/planner/physical/join_hidden_slots.go:68` | a |
| `LateralMarkerDroppedAbove` | Func | `internal/planner/physical/join_hidden_slots.go:101` | a |
| `LateralSideOf` | Func | `internal/planner/physical/join_hidden_slots.go:126` | a |
| `Literal` | Var | `internal/planner/physical/group_key_identity.go:79` | e |
| `LogicalAggOutNames` | Func | `internal/planner/physical/group_key_carrier.go:217` | a |
| `ManifestSnapshot` | TypeName | `internal/planner/physical/manifest_snapshot.go:23` | a |
| `ManifestSnapshot` | Var | `internal/planner/physical/planner_config.go:41` | b |
| `ManifestSnapshotCtxKey` | TypeName | `internal/planner/physical/manifest_snapshot.go:185` | a |
| `ManifestSnapshotFromContext` | Func | `internal/planner/physical/manifest_snapshot.go:189` | e |
| `MapExecJoinType` | Func | `internal/planner/physical/join_sources.go:422` | b |
| `MapJoinType` | Func | `internal/planner/physical/join_sources.go:401` | a |
| `MapPredOp` | Func | `internal/planner/physical/util.go:295` | b |
| `MatchesPartitionFilter` | Func | `internal/planner/physical/scanner_source.go:576` | a |
| `MaterializedInputs` | Var | `internal/planner/physical/planner_config.go:125` | e |
| `MaxInlinedInSetRows` | Func | `internal/planner/physical/in_subquery_set.go:45` | d |
| `MemoryBudget` | Var | `internal/planner/physical/planner_config.go:33` | c, d |
| `MetadataCountsPlanned` | Var | `internal/planner/physical/metadata_count.go:36` | d |
| `MetadataMinMaxPlanned` | Var | `internal/planner/physical/metadata_minmax.go:35` | d |
| `Minted` | Var | `internal/planner/physical/group_key_identity.go:91` | e |
| `Name` | Var | `internal/planner/physical/block_projection.go:47` | a |
| `Name` | Var | `internal/planner/physical/group_key_identity.go:35` | e |
| `Name` | Var | `internal/planner/physical/plan_types.go:39` | a, c |
| `Name` | Var | `internal/planner/physical/plan_types.go:75` | a, c |
| `Name` | Var | `internal/planner/physical/window_keys.go:33` | e |
| `NameIsPlainColumn` | Func | `internal/planner/physical/agg_output_projection.go:24` | a |
| `NamedArgs` | Var | `internal/planner/physical/table_func.go:204` | b |
| `NamedArmScope` | Func | `internal/planner/physical/output_declared_schema.go:1234` | a |
| `NewComputedColumnsOpWithMeta` | Func | `internal/planner/physical/computed_columns.go:21` | a |
| `NewManifestSnapshot` | Func | `internal/planner/physical/manifest_snapshot.go:55` | b, c |
| `NewPlanner` | Func | `internal/planner/physical/planner_config.go:351` | b, c, d |
| `NewPlannerForContext` | Func | `internal/planner/physical/manifest_snapshot.go:200` | b, c |
| `NodeDeclaredType` | Func | `internal/planner/physical/declared_output.go:1166` | a |
| `Op` | Var | `internal/planner/physical/scanner_source.go:246` | e |
| `OutputSchema` | Var | `internal/planner/physical/plan_types.go:25` | e |
| `OwnedJoinArm` | Func | `internal/planner/physical/output_rename_resolve.go:156` | a |
| `ParseJoinKeys` | Func | `internal/planner/physical/join_keys.go:21` | a |
| `ParseSemiAntiNE` | Func | `internal/planner/physical/join_sources.go:446` | b, c |
| `PhysicalPlan` | TypeName | `internal/planner/physical/plan_types.go:16` | b |
| `Pipeline` | Var | `internal/planner/physical/plan_types.go:17` | b, c, d |
| `PlaceholderTypes` | Var | `internal/planner/physical/declared_output.go:891` | a |
| `PlanCtx` | Var | `internal/planner/physical/planner_config.go:26` | a |
| `Planner` | TypeName | `internal/planner/physical/planner_config.go:23` | a, c, d |
| `PoisonReleasedSlabs` | Func | `internal/planner/physical/scan_rowgroup_load.go:377` | e |
| `Precision` | Var | `internal/planner/physical/plan_types.go:55` | a, c |
| `Precision` | Var | `internal/planner/physical/plan_types.go:76` | a, c |
| `Precision` | Var | `internal/planner/physical/window_keys.go:49` | e |
| `ProjSourceName` | Func | `internal/planner/physical/derived_alias.go:88` | a |
| `ProjectExprSpec` | TypeName | `internal/planner/physical/plan_types.go:36` | a, c |
| `ProjectionForName` | Func | `internal/planner/physical/derived_alias.go:102` | a |
| `ProjectionOutputName` | Func | `internal/planner/physical/aggregate_declared_output.go:811` | a |
| `PublishedBelow` | Var | `internal/planner/physical/group_key_identity.go:76` | e |
| `PublishedNamesOfProjection` | Func | `internal/planner/physical/published_output_names.go:62` | a |
| `PublishedOutputNames` | Func | `internal/planner/physical/published_output_names.go:45` | d |
| `QualifiedColumn` | Func | `internal/planner/physical/group_key_binding.go:280` | a |
| `QueryCost` | TypeName | `internal/planner/physical/query_limits.go:19` | a |
| `QueryLimitSQLState` | Const | `internal/planner/physical/query_limits.go:65` | c, d |
| `QueryLimits` | Var | `internal/planner/physical/planner_config.go:51` | b, c, d |
| `RecordBatch` | TypeName | `internal/planner/physical/filter_plan.go:17` | e |
| `ReferencesSynthetic` | Func | `internal/planner/physical/output_renames.go:41` | a |
| `ReferencesSyntheticAgg` | Func | `internal/planner/physical/output_renames.go:125` | a |
| `RefuseJoinCond` | Func | `internal/planner/physical/join_keys.go:115` | a |
| `RefuseReservedSlotName` | Func | `internal/planner/physical/reserved_slots.go:56` | e |
| `RefuseReservedSlotNames` | Func | `internal/planner/physical/reserved_slots.go:61` | d |
| `RefuseUnexpandedStarAnywhere` | Func | `internal/planner/physical/star_refusals.go:53` | a |
| `RefuseUnrepresentableRealInList` | Func | `internal/planner/physical/real_list_refusal.go:27` | a |
| `RelationScopeSubtree` | Func | `internal/planner/physical/output_rename_resolve.go:218` | a |
| `RepublishDeclaredNames` | Func | `internal/planner/physical/published_output_names.go:102` | a |
| `ReservedSlotFamily` | Func | `internal/planner/physical/reserved_slots.go:53` | e |
| `ResetSlabPoolsForTest` | Func | `internal/planner/physical/scan_rowgroup_load.go:74` | e |
| `ResolveAggInputName` | Func | `internal/planner/physical/group_key_binding.go:23` | a |
| `ResolveJoinKeyTypes` | Func | `internal/planner/physical/join_key_types.go:76` | a |
| `ResolveNullsLast` | Func | `internal/planner/physical/plan.go:75` | a |
| `ResolveOutputRenameSource` | Func | `internal/planner/physical/output_rename_resolve.go:52` | a |
| `ResolveRenameSource` | Func | `internal/planner/physical/output_rename_resolve.go:56` | a |
| `ResolveWindowKeys` | Func | `internal/planner/physical/window_keys.go:61` | a |
| `RespellDerivedAliasRefs` | Func | `internal/planner/physical/window_alias_respell.go:18` | a |
| `ReverseBloomInnerThreshold` | Var | `internal/planner/physical/plan.go:27` | a, c, d |
| `ReverseBloomThreshold` | Var | `internal/planner/physical/plan.go:26` | d |
| `ReverseBloomsInstalled` | Var | `internal/planner/physical/plan.go:43` | d |
| `RewriteColRefs` | Func | `internal/planner/physical/colref_rewrite.go:16` | a |
| `RowGroupBuffersResident` | Func | `internal/planner/physical/scan_rowgroup_load.go:93` | d |
| `RowGroupLoadStats` | Func | `internal/planner/physical/scan_rowgroup_load.go:86` | d |
| `RowGroupSlabAllocs` | Func | `internal/planner/physical/scan_rowgroup_load.go:67` | e |
| `RowGroupSlabReleases` | Func | `internal/planner/physical/scan_rowgroup_load.go:63` | e |
| `RowGroupSlabReuses` | Func | `internal/planner/physical/scan_rowgroup_load.go:82` | e |
| `RowLocColumn` | Const | `internal/planner/physical/topn_late_mat.go:38` | e |
| `Scale` | Var | `internal/planner/physical/plan_types.go:56` | a, c |
| `Scale` | Var | `internal/planner/physical/plan_types.go:77` | a, c |
| `Scale` | Var | `internal/planner/physical/window_keys.go:50` | e |
| `ScanFileFilter` | Var | `internal/planner/physical/planner_config.go:136` | c |
| `ScanFilterPushdowns` | Var | `internal/planner/physical/scan_filter_pushdown.go:35` | d |
| `ScanRowGroupBuffers` | Var | `internal/planner/physical/scan_rowgroup_load.go:30` | e |
| `ScopePreservingWrapper` | Func | `internal/planner/physical/output_rename_resolve.go:263` | b |
| `SemiAntiBuildStoreCols` | Func | `internal/planner/physical/join_keys.go:191` | c |
| `SemiAntiNE` | Var | `internal/planner/physical/plan.go:60` | a |
| `SetOpArmIsUnknownLit` | Func | `internal/planner/physical/set_op_types.go:916` | a |
| `SetOpArmPlan` | TypeName | `internal/planner/physical/set_op_types.go:604` | a |
| `SetOpArmProjection` | Func | `internal/planner/physical/set_op_types.go:645` | a |
| `SetOpArmTypeConflict` | Func | `internal/planner/physical/set_op_types.go:399` | a |
| `SetOpBaseName` | Func | `internal/planner/physical/set_op_types.go:31` | a |
| `SetOpCarrierGapPairs` | Func | `internal/planner/physical/set_op_types.go:267` | c |
| `SetOpColType` | TypeName | `internal/planner/physical/set_op_types.go:618` | a |
| `SetOpDecimalTarget` | Func | `internal/planner/physical/set_op_decimal.go:28` | b |
| `SetOpName` | Func | `internal/planner/physical/set_op_types.go:21` | a |
| `SetOpOutputNames` | Func | `internal/planner/physical/set_op_types.go:568` | a |
| `SetOpTargetType` | Func | `internal/planner/physical/set_op_types.go:924` | a |
| `SetOpUnknownLiteralArms` | Func | `internal/planner/physical/set_op_types.go:367` | a |
| `SetOpWiden` | Func | `internal/planner/physical/set_op_types.go:1019` | b |
| `ShapeOnlyColumnsPlanned` | Var | `internal/planner/physical/scan_filter_pushdown.go:39` | d |
| `SharedSpillMgr` | Var | `internal/planner/physical/planner_config.go:50` | c |
| `SharedTracker` | Var | `internal/planner/physical/planner_config.go:49` | c |
| `Slot` | Var | `internal/planner/physical/group_key_identity.go:51` | e |
| `SlotAggInput` | Const | `internal/planner/physical/reserved_slots.go:31` | e |
| `SlotAvgCount` | Const | `internal/planner/physical/reserved_slots.go:38` | e |
| `SlotAvgSum` | Const | `internal/planner/physical/reserved_slots.go:37` | e |
| `SlotCovarState` | Const | `internal/planner/physical/reserved_slots.go:40` | e |
| `SlotDefaultPart` | Const | `internal/planner/physical/reserved_slots.go:46` | e |
| `SlotFamily` | TypeName | `internal/planner/physical/reserved_slots.go:24` | e |
| `SlotGroupKey` | Const | `internal/planner/physical/reserved_slots.go:30` | c |
| `SlotHaving` | Const | `internal/planner/physical/reserved_slots.go:34` | e |
| `SlotName` | Func | `internal/planner/physical/reserved_slots.go:50` | c |
| `SlotNestedAgg` | Const | `internal/planner/physical/reserved_slots.go:32` | e |
| `SlotPreComputedAgg` | Const | `internal/planner/physical/reserved_slots.go:42` | e |
| `SlotRowCountOnly` | Const | `internal/planner/physical/reserved_slots.go:45` | e |
| `SlotRowLocator` | Const | `internal/planner/physical/reserved_slots.go:44` | e |
| `SlotScalar` | Const | `internal/planner/physical/reserved_slots.go:33` | e |
| `SlotSetOpCount` | Const | `internal/planner/physical/reserved_slots.go:36` | e |
| `SlotSortKey` | Const | `internal/planner/physical/reserved_slots.go:29` | e |
| `SlotSubsumeFlag` | Const | `internal/planner/physical/reserved_slots.go:43` | a |
| `SlotTwoLevel` | Const | `internal/planner/physical/reserved_slots.go:35` | e |
| `SlotVarState` | Const | `internal/planner/physical/reserved_slots.go:39` | e |
| `SlotWindowKey` | Const | `internal/planner/physical/reserved_slots.go:28` | a |
| `SlotWindowOutput` | Const | `internal/planner/physical/reserved_slots.go:27` | e |
| `SmjSourceAdapter` | TypeName | `internal/planner/physical/sort_merge_join.go:143` | b |
| `SortInputSetOpWidth` | Func | `internal/planner/physical/sort_plan.go:71` | a |
| `SortKeySlotPos` | Func | `internal/planner/physical/sort_plan.go:24` | a |
| `SortKeyWrittenSlotPos` | Func | `internal/planner/physical/sort_plan.go:110` | a |
| `SortMergeJoin` | Var | `internal/planner/physical/sort_merge_join.go:134` | e |
| `SortMergeJoinBytes` | Var | `internal/planner/physical/planner_config.go:113` | b, c, d |
| `SortMergeJoinsPlanned` | Var | `internal/planner/physical/sort_merge_join.go:19` | a, c, d |
| `SourceColDeclsThroughRenames` | Func | `internal/planner/physical/declared_output.go:789` | a |
| `SourceSlot` | Var | `internal/planner/physical/plan_types.go:69` | a, c |
| `SourceSlotSet` | Var | `internal/planner/physical/plan_types.go:70` | a, c |
| `Specs` | Var | `internal/planner/physical/set_op_types.go:605` | a |
| `SpillDir` | Var | `internal/planner/physical/planner_config.go:34` | c, d |
| `StreamingSources` | Var | `internal/planner/physical/planner_config.go:131` | c |
| `StrictIntArithCols` | Func | `internal/planner/physical/declared_output.go:309` | a |
| `StrictIntArithColsThroughRenames` | Func | `internal/planner/physical/declared_output.go:819` | a |
| `SubstituteNestedRenameRefs` | Func | `internal/planner/physical/output_rename_resolve.go:282` | a |
| `SubtreeNaming` | TypeName | `internal/planner/physical/subtree_naming.go:22` | a |
| `SubtreeNamingOf` | Func | `internal/planner/physical/subtree_naming.go:46` | a |
| `Text` | Var | `internal/planner/physical/window_keys.go:38` | e |
| `TopNLateMatPlanned` | Var | `internal/planner/physical/topn_late_mat.go:53` | d |
| `TotalBytes` | Var | `internal/planner/physical/query_limits.go:20` | a |
| `TotalFiles` | Var | `internal/planner/physical/query_limits.go:22` | a |
| `TotalRows` | Var | `internal/planner/physical/query_limits.go:21` | a |
| `Typ` | Var | `internal/planner/physical/set_op_types.go:620` | a |
| `Type` | Var | `internal/planner/physical/plan_types.go:40` | a, c |
| `Type` | Var | `internal/planner/physical/window_keys.go:46` | e |
| `TypeKnown` | Var | `internal/planner/physical/plan_types.go:49` | a, c |
| `Types` | Var | `internal/planner/physical/declared_output.go:840` | a |
| `Types` | Var | `internal/planner/physical/set_op_types.go:606` | a |
| `ValidateColumnsUnderPolicy` | Func | `internal/planner/physical/validate_policy.go:117` | a, d |
| `Value` | Var | `internal/planner/physical/scanner_source.go:247` | e |
| `WantBareName` | Func | `internal/planner/physical/join_declared_schema.go:304` | a |
| `WindowExecColumn` | Func | `internal/planner/physical/window_declared_output.go:271` | a |
| `WindowKeySpecs` | Func | `internal/planner/physical/window_keys.go:512` | a |
| `WindowSpecOutputType` | Func | `internal/planner/physical/window_declared_output.go:104` | a |
| `WithIdentityQueryLimits` | Func | `internal/planner/physical/identity_limits.go:26` | d |
| `Workers` | Var | `internal/planner/physical/join_sources.go:28` | b |
| `Workers` | Var | `internal/planner/physical/join_sources.go:99` | e |
| `WrapsAWindow` | Func | `internal/planner/physical/aggregate_declared_output.go:796` | b |
| `policedColumnSource.AmbiguousTableNames` | Func | `internal/planner/physical/validate_policy.go:94` | e |
| `policedColumnSource.GetTable` | Func | `internal/planner/physical/validate_policy.go:57` | e |
| `policedColumnSource.ResolveTableName` | Func | `internal/planner/physical/validate_policy.go:90` | e |
| `readerAtStore.GetReaderAt` | Func | `internal/planner/physical/util.go:786` | e |
| `smjProbeSink.Finalize` | Func | `internal/planner/physical/sort_merge_join.go:137` | e |
| `tableColumnSource.GetTable` | Func | `internal/planner/physical/validate.go:24` | e |
| `tableNameResolver.AmbiguousTableNames` | Func | `internal/planner/physical/validate.go:31` | e |
| `tableNameResolver.ResolveTableName` | Func | `internal/planner/physical/validate.go:30` | e |

## e: No direct outside reference

This is a reference inventory, not an instruction to rename interface methods or fields used by reflection. The implementation must retain those contracts.

```text
*CsvTableFuncSource.Close (internal/planner/physical/table_func.go:247)
*CsvTableFuncSource.Init (internal/planner/physical/table_func.go:209)
*CsvTableFuncSource.Next (internal/planner/physical/table_func.go:243)
*DbScanSource.Init (internal/planner/physical/table_func.go:446)
*DbScanSource.Next (internal/planner/physical/table_func.go:469)
*DeferredJoinBridge.Close (internal/planner/physical/join_sources.go:76)
*JsonTableFuncSource.Close (internal/planner/physical/table_func.go:127)
*JsonTableFuncSource.Init (internal/planner/physical/table_func.go:107)
*JsonTableFuncSource.Next (internal/planner/physical/table_func.go:123)
*Planner.BuildTopN (internal/planner/physical/sort_plan.go:261)
*SmjSourceAdapter.Close (internal/planner/physical/sort_merge_join.go:180)
*SmjSourceAdapter.Init (internal/planner/physical/sort_merge_join.go:152)
*SmjSourceAdapter.Next (internal/planner/physical/sort_merge_join.go:156)
*SmjSourceAdapter.RowsScanned (internal/planner/physical/sort_merge_join.go:185)
*abandonedClaimError.Error (internal/planner/physical/catalog_scan.go:205)
*abandonedClaimError.Unwrap (internal/planner/physical/catalog_scan.go:209)
*aggPreProject.Clone (internal/planner/physical/computed_columns.go:135)
*aggPreProject.Close (internal/planner/physical/computed_columns.go:375)
*aggPreProject.EnableSharedOutputs (internal/planner/physical/computed_columns.go:130)
*aggPreProject.Execute (internal/planner/physical/computed_columns.go:146)
*aggPreProject.Init (internal/planner/physical/computed_columns.go:119)
*aggPreProject.ReusesOutputBuffers (internal/planner/physical/computed_columns.go:124)
*aggSourceAdapter.Close (internal/planner/physical/operator_sources.go:90)
*aggSourceAdapter.Init (internal/planner/physical/operator_sources.go:62)
*aggSourceAdapter.Next (internal/planner/physical/operator_sources.go:66)
*aggSourceAdapter.RowsScanned (internal/planner/physical/operator_sources.go:83)
*aggSourceAdapter.ServesHeldState (internal/planner/physical/operator_sources.go:60)
*catalogScanSource.Close (internal/planner/physical/catalog_scan.go:407)
*catalogScanSource.Init (internal/planner/physical/catalog_scan.go:110)
*catalogScanSource.Next (internal/planner/physical/catalog_scan.go:256)
*catalogScanSource.RefetchRows (internal/planner/physical/catalog_scan.go:92)
*catalogScanSource.RowsScanned (internal/planner/physical/catalog_scan.go:422)
*catalogScanSource.SetBloomFilter (internal/planner/physical/catalog_scan.go:101)
*catalogScanSource.SetDynamicFilter (internal/planner/physical/catalog_scan.go:106)
*cteMaterializingSink.Close (internal/planner/physical/cte_materialization.go:222)
*cteMaterializingSink.Consume (internal/planner/physical/cte_materialization.go:213)
*cteMaterializingSink.Finalize (internal/planner/physical/cte_materialization.go:220)
*cteMaterializingSink.Init (internal/planner/physical/cte_materialization.go:211)
*generateSeriesSource.Close (internal/planner/physical/table_func.go:574)
*generateSeriesSource.Init (internal/planner/physical/table_func.go:524)
*generateSeriesSource.Next (internal/planner/physical/table_func.go:530)
*joinFlushSource.Close (internal/planner/physical/join_sources.go:291)
*joinFlushSource.Init (internal/planner/physical/join_sources.go:233)
*joinFlushSource.Next (internal/planner/physical/join_sources.go:238)
*multiFileReadCloser.Close (internal/planner/physical/table_func.go:331)
*multiFileReadCloser.Read (internal/planner/physical/table_func.go:289)
*parquetTableFuncSource.Close (internal/planner/physical/table_func.go:193)
*parquetTableFuncSource.Init (internal/planner/physical/table_func.go:144)
*parquetTableFuncSource.Next (internal/planner/physical/table_func.go:185)
*pipelineSource.Close (internal/planner/physical/pipeline_source.go:175)
*pipelineSource.Init (internal/planner/physical/pipeline_source.go:42)
*pipelineSource.Next (internal/planner/physical/pipeline_source.go:67)
*reverseBloomBridge.Close (internal/planner/physical/join_sources.go:214)
*reverseBloomBridge.Init (internal/planner/physical/join_sources.go:105)
*reverseBloomBridge.Next (internal/planner/physical/join_sources.go:210)
*rgSlabs.RowGroupBytes (internal/planner/physical/scan_rowgroup_load.go:153)
*rightSemiFlushSource.Close (internal/planner/physical/join_sources.go:391)
*rightSemiFlushSource.Init (internal/planner/physical/join_sources.go:313)
*rightSemiFlushSource.Next (internal/planner/physical/join_sources.go:318)
*sampleOperator.Close (internal/planner/physical/table_func.go:727)
*sampleOperator.Execute (internal/planner/physical/table_func.go:699)
*sampleOperator.Init (internal/planner/physical/table_func.go:697)
*scanSourceInner.RefetchRows (internal/planner/physical/util.go:1265)
*scannerExecSource.Close (internal/planner/physical/scanner_source.go:593)
*scannerExecSource.Init (internal/planner/physical/scanner_source.go:250)
*scannerExecSource.Next (internal/planner/physical/scanner_source.go:589)
*scannerExecSource.RowsScanned (internal/planner/physical/scanner_source.go:611)
*setOpSourceAdapter.Close (internal/planner/physical/set_op_plan.go:298)
*setOpSourceAdapter.Init (internal/planner/physical/set_op_plan.go:184)
*setOpSourceAdapter.Next (internal/planner/physical/set_op_plan.go:186)
*setOpSourceAdapter.RowsScanned (internal/planner/physical/set_op_plan.go:316)
*sortSourceAdapter.Close (internal/planner/physical/operator_sources.go:143)
*sortSourceAdapter.Init (internal/planner/physical/operator_sources.go:118)
*sortSourceAdapter.Next (internal/planner/physical/operator_sources.go:122)
*sortSourceAdapter.RowsScanned (internal/planner/physical/operator_sources.go:151)
*sortSourceAdapter.ServesHeldState (internal/planner/physical/operator_sources.go:116)
*topNLateMatSource.Close (internal/planner/physical/topn_late_mat.go:282)
*topNLateMatSource.Init (internal/planner/physical/topn_late_mat.go:219)
*topNLateMatSource.Next (internal/planner/physical/topn_late_mat.go:221)
*topNLateMatSource.RowsScanned (internal/planner/physical/topn_late_mat.go:287)
*topNLateMatSource.ServesHeldState (internal/planner/physical/topn_late_mat.go:217)
*unnestSource.Close (internal/planner/physical/table_func.go:657)
*unnestSource.Init (internal/planner/physical/table_func.go:607)
*unnestSource.Next (internal/planner/physical/table_func.go:612)
*windowKeyError.Error (internal/planner/physical/window_keys.go:621)
*windowKeyError.Unwrap (internal/planner/physical/window_keys.go:625)
*windowSourceAdapter.Close (internal/planner/physical/operator_sources.go:194)
*windowSourceAdapter.Init (internal/planner/physical/operator_sources.go:170)
*windowSourceAdapter.Next (internal/planner/physical/operator_sources.go:172)
*windowSourceAdapter.RowsScanned (internal/planner/physical/operator_sources.go:187)
*windowSourceAdapter.ServesHeldState (internal/planner/physical/operator_sources.go:160)
Barrier (internal/planner/physical/join_sources.go:95)
Barrier (internal/planner/physical/sort_merge_join.go:147)
BuildErr (internal/planner/physical/join_sources.go:96)
BuildErr (internal/planner/physical/sort_merge_join.go:148)
ChildOps (internal/planner/physical/join_sources.go:91)
ChildOps (internal/planner/physical/operator_sources.go:47)
ChildOps (internal/planner/physical/operator_sources.go:108)
ChildOps (internal/planner/physical/operator_sources.go:164)
ChildOps (internal/planner/physical/sort_merge_join.go:145)
ChildSource (internal/planner/physical/join_sources.go:90)
ChildSource (internal/planner/physical/operator_sources.go:46)
ChildSource (internal/planner/physical/operator_sources.go:107)
ChildSource (internal/planner/physical/operator_sources.go:163)
ChildSource (internal/planner/physical/sort_merge_join.go:144)
Column (internal/planner/physical/scanner_source.go:245)
Delimited (internal/planner/physical/group_key_identity.go:61)
Derived (internal/planner/physical/group_key_identity.go:64)
Expr (internal/planner/physical/window_keys.go:37)
Field (internal/planner/physical/window_keys.go:43)
Fields (internal/planner/physical/window_keys.go:30)
Identity (internal/planner/physical/group_key_identity.go:54)
IdentityQueryLimitsFromContext (internal/planner/physical/identity_limits.go:34)
Literal (internal/planner/physical/group_key_identity.go:79)
ManifestSnapshotFromContext (internal/planner/physical/manifest_snapshot.go:189)
MaterializedInputs (internal/planner/physical/planner_config.go:125)
Minted (internal/planner/physical/group_key_identity.go:91)
Name (internal/planner/physical/group_key_identity.go:35)
Name (internal/planner/physical/window_keys.go:33)
Op (internal/planner/physical/scanner_source.go:246)
OutputSchema (internal/planner/physical/plan_types.go:25)
PoisonReleasedSlabs (internal/planner/physical/scan_rowgroup_load.go:377)
Precision (internal/planner/physical/window_keys.go:49)
PublishedBelow (internal/planner/physical/group_key_identity.go:76)
RecordBatch (internal/planner/physical/filter_plan.go:17)
RefuseReservedSlotName (internal/planner/physical/reserved_slots.go:56)
ReservedSlotFamily (internal/planner/physical/reserved_slots.go:53)
ResetSlabPoolsForTest (internal/planner/physical/scan_rowgroup_load.go:74)
RowGroupSlabAllocs (internal/planner/physical/scan_rowgroup_load.go:67)
RowGroupSlabReleases (internal/planner/physical/scan_rowgroup_load.go:63)
RowGroupSlabReuses (internal/planner/physical/scan_rowgroup_load.go:82)
RowLocColumn (internal/planner/physical/topn_late_mat.go:38)
Scale (internal/planner/physical/window_keys.go:50)
ScanRowGroupBuffers (internal/planner/physical/scan_rowgroup_load.go:30)
Slot (internal/planner/physical/group_key_identity.go:51)
SlotAggInput (internal/planner/physical/reserved_slots.go:31)
SlotAvgCount (internal/planner/physical/reserved_slots.go:38)
SlotAvgSum (internal/planner/physical/reserved_slots.go:37)
SlotCovarState (internal/planner/physical/reserved_slots.go:40)
SlotDefaultPart (internal/planner/physical/reserved_slots.go:46)
SlotFamily (internal/planner/physical/reserved_slots.go:24)
SlotHaving (internal/planner/physical/reserved_slots.go:34)
SlotNestedAgg (internal/planner/physical/reserved_slots.go:32)
SlotPreComputedAgg (internal/planner/physical/reserved_slots.go:42)
SlotRowCountOnly (internal/planner/physical/reserved_slots.go:45)
SlotRowLocator (internal/planner/physical/reserved_slots.go:44)
SlotScalar (internal/planner/physical/reserved_slots.go:33)
SlotSetOpCount (internal/planner/physical/reserved_slots.go:36)
SlotSortKey (internal/planner/physical/reserved_slots.go:29)
SlotTwoLevel (internal/planner/physical/reserved_slots.go:35)
SlotVarState (internal/planner/physical/reserved_slots.go:39)
SlotWindowOutput (internal/planner/physical/reserved_slots.go:27)
SortMergeJoin (internal/planner/physical/sort_merge_join.go:134)
Text (internal/planner/physical/window_keys.go:38)
Type (internal/planner/physical/window_keys.go:46)
Value (internal/planner/physical/scanner_source.go:247)
Workers (internal/planner/physical/join_sources.go:99)
policedColumnSource.AmbiguousTableNames (internal/planner/physical/validate_policy.go:94)
policedColumnSource.GetTable (internal/planner/physical/validate_policy.go:57)
policedColumnSource.ResolveTableName (internal/planner/physical/validate_policy.go:90)
readerAtStore.GetReaderAt (internal/planner/physical/util.go:786)
smjProbeSink.Finalize (internal/planner/physical/sort_merge_join.go:137)
tableColumnSource.GetTable (internal/planner/physical/validate.go:24)
tableNameResolver.AmbiguousTableNames (internal/planner/physical/validate.go:31)
tableNameResolver.ResolveTableName (internal/planner/physical/validate.go:30)
```

## Narrowed package-qualified boundary

The context callers reference 11 names in dagplan production code and 11 in
its tests; four test names are additional. The union is **146 → 15**. On the
production-only definition it is **117 → 11**. Other AGPL packages name eight
physical exports, for an all-AGPL union of **152 → 16**. The budget is 16,
because it covers every AGPL package and includes tests. MIT callers still
name the same 22 package-qualified exports.

`StagePlanner` obtains `Planner.PlanContext()` and shares that planner's
state. The context methods delegate to the same local operations. The zero
value serves argument-only walks and access to existing package settings;
its construction creates no planner or snapshot. No derived fact is cached
or recomputed at a different planning phase. The two published metadata maps
use concrete context methods around the existing generic republishing walk,
because Go methods cannot introduce type parameters.

Constructors remain ordinary named constructors: creating the first planner
or snapshot does not require a pre-existing context. Stored or passed values
retain their concrete type names; the shared error classification retains its
constant. These are the remaining separate names, not separate helper
functions for individual planning walks.

| Survivor | Why it remains a separate name |
|---|---|
| `ColDecls` | Column declarations appear in parameters and stored maps. |
| `DecimalCoercion` | Planned decimal conversions are stored in typed stage and execution specifications. |
| `GroupKeyResolution` | Group-key identities and resolution spellings travel as typed records. |
| `ManifestSnapshot` | Multiple planners share this snapshot value for one statement. |
| `NewManifestSnapshot` | Constructs the shared snapshot before a planner context exists. |
| `NewPlanner` | Constructs a local planner and its initial resources. |
| `NewPlannerForContext` | Constructs a local planner with the statement's existing snapshot and query limits. |
| `PhysicalPlan` | The local executable plan is a returned and passed value, including in mixed-planner tests. |
| `PlanContext` | The shared local planner state and planning operations. |
| `Planner` | The local planner is the borrowed input to the context and to `NewStagePlanner`. |
| `ProjectExprSpec` | Typed projected expressions are stored and passed to execution. |
| `QueryCost` | The cost walk returns a structured estimate. |
| `QueryLimitSQLState` | Callers classify the shared query-limit refusal by its canonical constant. |
| `SetOpArmPlan` | Arm plans cross the context as typed slices. |
| `SetOpColType` | Type reconciliation produces a stored target-column declaration. |
| `SubtreeNaming` | Join planning stores and passes the subtree's naming record. |

The dagplan production list is the table minus `NewManifestSnapshot`,
`NewPlanner`, `NewPlannerForContext`, `PhysicalPlan` and `QueryLimitSQLState`.
Its test-only additions are the three constructors and `PhysicalPlan`.
`QueryLimitSQLState` is referenced by other AGPL packages.

## Identifiers reached by any spelling

The counts above are package-qualified names — a `physical.X` written at the
call site. They do not count what a caller reaches THROUGH a value: a method on
`PlanContext`, a field of `ProjectExprSpec`. That is the second measurement,
and it is the one that says how big the seam is as a set of OPERATIONS rather
than as a list of spellings. Same resolution, same packages, counting each
exported object of physical that an AGPL package's syntax reaches:

| Scope | Before | After |
|---|---:|---:|
| All AGPL packages, production and tests | 236 | 209 |
| All AGPL packages, production only | 198 | 196 |
| dagplan production | 174 | 172 |
| dagplan production and tests | 221 | 194 |

| Kind | Before | After |
|---|---:|---:|
| Package-scope names | 152 | 16 |
| Methods | 29 | 139 |
| Fields | 55 | 54 |

**The operation set is unchanged in size.** dagplan's production code reached
174 exported physical identifiers before this work and reaches 172 after it.
What narrowed is the PACKAGE-SCOPE surface: 152 names an AGPL package could
write as `physical.X` became 16. The rest of the seam did not go away; it moved
onto one type. `PlanContext` declares 114 exported methods and every one of
them is reached (104 from dagplan production alone), so none was built
speculatively — but 112 of them take an unnamed receiver and forward to the
unexported function that used to be the exported one. The narrowing is of the
spelling, and of what a package outside physical can NAME, not of what the
AGPL side can do.

Keyed by the receiver expression's type instead of by the declaring object —
which spells a method reached through both `StagePlanner` and `PlanContext`
twice — the same measurement reads 242 → 215 for all AGPL packages, 202 → 200
for production only and 174 → 172 for dagplan production. Six members account
for the whole difference.

Four of the fields belong to an unexported type: dagplan reads `Expr`, `Name`,
`Decl` and `DeclKnown` from the `blockColumn` values
`PlanContext.BlockPublishedColumns` returns. It cannot name that type and does
not need to.

Two budgets hold the two halves, because a method added to the context moves
one and not the other. `TestAGPLPhysicalReferenceBudget` holds the
package-qualified count at `maxAGPLPhysicalNames` and
`TestAGPLPhysicalMemberBudget` holds the reached-identifier count at
`maxAGPLPhysicalMembers`, which lists every member by name — so a new
operation behind the context fails a gate that names it rather than passing
one that cannot see it. Both are in `tools/licensecheck`, and each raise is
recorded there beside the constant with the member and its reason: 16/209 at
this measurement, 16/210 (arc SR, `PlanContext.PublishedOutputProjectionNode`,
ADR-0026 §9a), 18/214 (arc JR, the outer-join residual refusal), 18/216 (arc
BJ, the planner's bushy option and its option set) and 19/217 (arc FR,
`ReaderSchemaReads` — the counter the door gates read to prove a refused
identity's file was never opened, ADR-0034). The change log lives with the
constants because that is where the next raise is written.

## Test placement and declaration renames

Step 2 lowercased the 29 package-scope declarations in category e, using only
physical files. Two names needed distinct local spellings because the direct
lowercase spelling was already declared: `RefuseReservedSlotName` became
`checkReservedSlotName`, and `PoisonReleasedSlabs` became
`setPoisonReleasedSlabs`.

43 tests of physical behavior moved into physical with their assertions
unchanged. The mixed-planner tests use the same context methods. A proposed
44th move, `TestParseSemiAntiNE`, was returned to dagplan: that test depends
on dagplan's initialization of a shared setting, which the planner test gate
identified. Its assertions remain unchanged and it reaches the helper through
the context. Test entry points and interface implementations stay exported;
a direct-reference count alone is insufficient to change their contracts.

## Final declaration counts

| Category | Before | After |
|---|---:|---:|
| Production export declarations, including members | 424 | 375 |
| Package-scope exports | 197 | 35 |
| Exported methods declared in physical | 126 | 238 |
| a: dagplan production package-qualified names | 117 | 11 |
| b: additional dagplan test names | 29 | 4 |
| c: other AGPL package-qualified names | 16 | 8 |
| d: other MIT package-qualified names | 22 | 22 |
| e: declarations without a direct outside reference | 165 | 145 |
| All dagplan package-qualified names | 146 | 15 |
| All AGPL package-qualified names | 152 | 16 |
| AGPL-reached identifiers, any spelling | 236 | 209 |
| of which members: methods and fields | 84 | 193 |

The declaration total falls by 49 while the method component rises by 112:
that is the same movement the reached-identifier table shows, seen from
inside physical. 162 package-scope names became private and the operations
they named became methods on the context.

Every remaining package-scope export has an outside reference. Category e is
now 101 interface methods and 44 fields. Interface method names retain their
contracts; fields retain the shape of returned values, embedded state and
records. They are not counted as unused helper functions.

After context callers were in place, another 134 package-scope declarations
became private. Three ordinary helper methods also became private:
`Planner.BuildTopN`, `Planner.CteKeyColTypes` and `SubtreeNaming.OwnsKey`.
The baseline did have a dagplan caller of `CteKeyColTypes`; the context now
supplies that callback internally. Its output-schema operation likewise uses
the same planner's subquery resolver. Two newly introduced context methods
that the combined metadata operations did not need were removed.

The final c list is `GroupKeyResolution`, `NewManifestSnapshot`, `NewPlanner`,
`NewPlannerForContext`, `PlanContext`, `Planner`, `ProjectExprSpec` and
`QueryLimitSQLState`. The d list is unchanged from the baseline section.
