// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func (PlanContext) AggDerivedGroupKey(key string, child *logical.Node) (string, bool) {
	return AggDerivedGroupKey(key, child)
}

func (PlanContext) AggInputColumnDecimal(node *logical.Node, col string) (logical.DecimalMeta, bool) {
	return AggInputColumnDecimal(node, col)
}

func (PlanContext) AggInputIsWideInteger(node plansql.Node, decls ColDecls) bool {
	return AggInputIsWideInteger(node, decls)
}

func (PlanContext) AggOhlcvOutputFields(node *logical.Node, agg logical.AggExpr) ([]parquet.Column, bool) {
	return AggOhlcvOutputFields(node, agg)
}

func (PlanContext) AggOutputFromInputDecl(fn string, distinct bool, in parquet.TypeID, precision, scale int, wideInt bool) (
	out parquet.TypeID, outPrecision, outScale int, ok bool,
) {
	return AggOutputFromInputDecl(fn, distinct, in, precision, scale, wideInt)
}

func (PlanContext) AggScopePreservingWrapper(t logical.NodeType) bool {
	return AggScopePreservingWrapper(t)
}

func (PlanContext) AggSpecOutputDecimal(node *logical.Node, agg logical.AggExpr) (logical.DecimalMeta, bool) {
	return AggSpecOutputDecimal(node, agg)
}

func (PlanContext) AggSpecOutputType(node *logical.Node, agg logical.AggExpr) (parquet.TypeID, bool) {
	return AggSpecOutputType(node, agg)
}

func (PlanContext) AggregateGroupKeyName(proj *logical.Projection, projectNode *logical.Node) (string, bool) {
	return AggregateGroupKeyName(proj, projectNode)
}

func (PlanContext) AssignJoinKeySides(leftKeys, rightKeys []string, probe, build *SubtreeNaming) {
	AssignJoinKeySides(leftKeys, rightKeys, probe, build)
}

func (PlanContext) AstIsFieldPath(node plansql.Node, decls ColDecls) bool {
	return AstIsFieldPath(node, decls)
}

func (PlanContext) BlockBareName(s string) string {
	return BlockBareName(s)
}

func (PlanContext) BlockPublishedColumns(p *logical.Node, published map[*logical.Node]bool,
	subqueryDecl func(string) (parquet.Column, bool)) ([]blockColumn, bool) {
	return BlockPublishedColumns(p, published, subqueryDecl)
}

func (PlanContext) BuildJoinResidualFilter(filter, buildAlias string) func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	return BuildJoinResidualFilter(filter, buildAlias)
}

func (PlanContext) BuildStreamAlias(node *logical.Node) string {
	return BuildStreamAlias(node)
}

func (PlanContext) CleanExpr(s string) string {
	return CleanExpr(s)
}

func (PlanContext) ColSet(cols []string) map[string]bool {
	return ColSet(cols)
}

func (PlanContext) CollectASTCols(n plansql.Node, out map[string]bool) {
	CollectASTCols(n, out)
}

func (PlanContext) CollectColRefs(n plansql.Node) []*plansql.ColRef {
	return CollectColRefs(n)
}

func (PlanContext) CollectColRefsBelow(n plansql.Node, stop func(plansql.Node) bool) []*plansql.ColRef {
	return CollectColRefsBelow(n, stop)
}

func (PlanContext) CollectOuterColumns(node *logical.Node) map[string]string {
	return CollectOuterColumns(node)
}

func (PlanContext) CollectTableAliases(node *logical.Node) map[string]bool {
	return CollectTableAliases(node)
}

func (PlanContext) DeclTypeParts(d expr.DeclType) parquet.Column {
	return DeclTypeParts(d)
}

func (PlanContext) DeclaredJoinSchema(n *logical.Node, want []string, published map[*logical.Node]bool,
	subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
	return DeclaredJoinSchema(n, want, published, subqueryDecl)
}

func (PlanContext) OutputSchema(root *logical.Node,
	subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
	return DeclaredOutputSchema(root, subqueryDecl)
}

func (PlanContext) DeclaredStringLengths(root *logical.Node) map[string]int {
	return DeclaredStringLengths(root)
}

func (PlanContext) DeclaredWireUnconstrainedDecimal(root *logical.Node) map[string]bool {
	return DeclaredWireUnconstrainedDecimal(root)
}

func (PlanContext) DerivedAliasSourceColumn(name string, child *logical.Node) string {
	return DerivedAliasSourceColumn(name, child)
}

func (PlanContext) DerivedGroupKeyDecl(key string, node plansql.Node, child *logical.Node) expr.DeclType {
	return DerivedGroupKeyDecl(key, node, child)
}

func (PlanContext) DerivedScopeBareName(name string, subtree *logical.Node) string {
	return DerivedScopeBareName(name, subtree)
}

func (PlanContext) EmittedColDecls(n *logical.Node) ColDecls {
	return EmittedColDecls(n)
}

func (PlanContext) EmittedColTypes(n *logical.Node) map[string]parquet.TypeID {
	return EmittedColTypes(n)
}

func (PlanContext) EmittedColumnNames(n *logical.Node) []string {
	return EmittedColumnNames(n)
}

func (PlanContext) EmittedKeyNames(published []string, resolve []GroupKeyResolution, aggOut []string) []string {
	return EmittedKeyNames(published, resolve, aggOut)
}

func (PlanContext) FindAggregateAncestor(node *logical.Node) *logical.Node {
	return FindAggregateAncestor(node)
}

func (PlanContext) FindOutputProjectionNode(n *logical.Node) *logical.Node {
	return FindOutputProjectionNode(n)
}

func (PlanContext) FindOutputProjectionsForRename(n *logical.Node) []logical.Projection {
	return FindOutputProjectionsForRename(n)
}

func (PlanContext) GroupKeyByIdentity(agg *logical.Node) map[string]string {
	return GroupKeyByIdentity(agg)
}

func (PlanContext) GroupKeyNames(agg, child *logical.Node) (published []string, resolve []GroupKeyResolution) {
	return GroupKeyNames(agg, child)
}

func (PlanContext) HasFilterOrPartition(n *logical.Node) bool {
	return HasFilterOrPartition(n)
}

func (PlanContext) HasLimit(n *logical.Node) bool {
	return HasLimit(n)
}

func (PlanContext) InferProjectionDeclType(node plansql.Node, fallback parquet.TypeID, strictInt map[string]bool, decls ColDecls) expr.DeclType {
	return InferProjectionDeclType(node, fallback, strictInt, decls)
}

func (PlanContext) InferProjectionDeclTypeConf(node plansql.Node, fallback parquet.TypeID,
	strictInt map[string]bool, decls ColDecls) (expr.DeclType, expr.Confidence) {
	return InferProjectionDeclTypeConf(node, fallback, strictInt, decls)
}

func (PlanContext) InlinedInSetRowCap() int {
	return InlinedInSetRowCap()
}

func (PlanContext) InputColDecls(n *logical.Node) ColDecls {
	return InputColDecls(n)
}

func (PlanContext) IsSimpleColRefForRename(n plansql.Node) bool {
	return IsSimpleColRefForRename(n)
}

func (PlanContext) JoinArmAlias(node *logical.Node) string {
	return JoinArmAlias(node)
}

func (PlanContext) JoinSideSchemas(node *logical.Node, leftKeys, rightKeys []string,
	published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool)) (probe, build []parquet.Column) {
	return JoinSideSchemas(node, leftKeys, rightKeys, published, subqueryDecl)
}

func (PlanContext) LateralEmptySpec(node *logical.Node) (marker string, cols []exec.LateralDefault, drop bool) {
	return LateralEmptySpec(node)
}

func (PlanContext) LateralMarkerDroppedAbove(node *logical.Node, name string) bool {
	return LateralMarkerDroppedAbove(node, name)
}

func (PlanContext) LateralSideOf(node *logical.Node) int {
	return LateralSideOf(node)
}

func (PlanContext) LogicalAggOutNames(agg *logical.Node) []string {
	return LogicalAggOutNames(agg)
}

func (PlanContext) MapJoinType(vt string) string {
	return MapJoinType(vt)
}

func (PlanContext) MatchesPartitionFilter(partValues, filter map[string]string) bool {
	return MatchesPartitionFilter(partValues, filter)
}

func (PlanContext) NameIsPlainColumn(s string) bool {
	return NameIsPlainColumn(s)
}

func (PlanContext) NamedArmScope(n *logical.Node) string {
	return NamedArmScope(n)
}

func (PlanContext) NewComputedColumnsOpWithMeta(cols []exec.ProjectColumn, meta []parquet.Column) exec.UnaryOperator {
	return NewComputedColumnsOpWithMeta(cols, meta)
}

func (PlanContext) NodeDeclaredType(node plansql.Node, decls ColDecls) (expr.DeclType, expr.Confidence) {
	return NodeDeclaredType(node, decls)
}

func (PlanContext) OwnedJoinArm(n *logical.Node, name string) *logical.Node {
	return OwnedJoinArm(n, name)
}

func (PlanContext) ParseJoinKeys(cond string) (leftKeys, rightKeys, residual []string) {
	return ParseJoinKeys(cond)
}

func (PlanContext) ProjSourceName(proj *logical.Projection) string {
	return ProjSourceName(proj)
}

func (PlanContext) ProjectionForName(projs []logical.Projection, name, bare string) *logical.Projection {
	return ProjectionForName(projs, name, bare)
}

func (PlanContext) ProjectionOutputName(proj logical.Projection) string {
	return ProjectionOutputName(proj)
}

func (PlanContext) PublishedNamesOfProjection(projNode *logical.Node) []string {
	return PublishedNamesOfProjection(projNode)
}

func (PlanContext) QualifiedColumn(ref *plansql.ColRef) string {
	return QualifiedColumn(ref)
}

func (PlanContext) ReferencesSynthetic(n plansql.Node, prefix string) bool {
	return ReferencesSynthetic(n, prefix)
}

func (PlanContext) ReferencesSyntheticAgg(n plansql.Node) bool {
	return ReferencesSyntheticAgg(n)
}

func (PlanContext) RefuseJoinCond(joinType, cond string, residual []string) error {
	return RefuseJoinCond(joinType, cond, residual)
}

func (PlanContext) RefuseUnexpandedStarAnywhere(node *logical.Node) error {
	return RefuseUnexpandedStarAnywhere(node)
}

func (PlanContext) RefuseUnrepresentableRealInList(root *logical.Node) error {
	return RefuseUnrepresentableRealInList(root)
}

func (PlanContext) RelationScopeSubtree(n *logical.Node, name string) *logical.Node {
	return RelationScopeSubtree(n, name)
}

func (PlanContext) ResolveAggInputName(name string, child *logical.Node) (resolved string, expr plansql.Node, exprInput *logical.Node, alias bool) {
	return ResolveAggInputName(name, child)
}

func (PlanContext) ResolveJoinKeyTypes(node *logical.Node, leftKeys, rightKeys []string, cte cteColTypes) []parquet.TypeID {
	return ResolveJoinKeyTypes(node, leftKeys, rightKeys, cte)
}

func (PlanContext) ResolveNullsLast(ob logical.OrderExpr) bool {
	return ResolveNullsLast(ob)
}

func (PlanContext) ResolveOutputRenameSource(name string, child *logical.Node) string {
	return ResolveOutputRenameSource(name, child)
}

func (PlanContext) ResolveRenameSource(name string, child *logical.Node, forGather bool) string {
	return ResolveRenameSource(name, child, forGather)
}

func (PlanContext) ResolveWindowKeys(node *logical.Node) map[string]windowKey {
	return ResolveWindowKeys(node)
}

func (PlanContext) RespellDerivedAliasRefs(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	return RespellDerivedAliasRefs(n, child)
}

func (PlanContext) RewriteColRefs(n plansql.Node, sub func(*plansql.ColRef) (plansql.Node, bool)) (
	out plansql.Node, changed, complete bool,
) {
	return RewriteColRefs(n, sub)
}

func (PlanContext) SetOpArmIsUnknownLit(unknown [][]bool, i, col int) bool {
	return SetOpArmIsUnknownLit(unknown, i, col)
}

func (PlanContext) SetOpArmProjection(arm *logical.Node, outNames []string) (SetOpArmPlan, error) {
	return SetOpArmProjection(arm, outNames)
}

func (PlanContext) SetOpArmTypeConflict(node *logical.Node) error {
	return SetOpArmTypeConflict(node)
}

func (PlanContext) SetOpBaseName(node *logical.Node) string {
	return SetOpBaseName(node)
}

func (PlanContext) SetOpName(node *logical.Node) string {
	return SetOpName(node)
}

func (PlanContext) SetOpOutputNames(arm *logical.Node) []string {
	return SetOpOutputNames(arm)
}

func (PlanContext) SetOpTargetType(plans []SetOpArmPlan, col int, name, op string, unknown [][]bool) (SetOpColType, bool, error) {
	return SetOpTargetType(plans, col, name, op, unknown)
}

func (PlanContext) SetOpUnknownLiteralArms(arm *logical.Node, cols int) []bool {
	return SetOpUnknownLiteralArms(arm, cols)
}

func (PlanContext) SortInputSetOpWidth(child *logical.Node) (int, bool) {
	return SortInputSetOpWidth(child)
}

func (PlanContext) SortKeySlotPos(ob logical.OrderExpr, sortNode *logical.Node) int {
	return SortKeySlotPos(ob, sortNode)
}

func (PlanContext) SortKeyWrittenSlotPos(ob logical.OrderExpr, sortNode *logical.Node) int {
	return SortKeyWrittenSlotPos(ob, sortNode)
}

func (PlanContext) SourceColDeclsThroughRenames(n *logical.Node) ColDecls {
	return SourceColDeclsThroughRenames(n)
}

func (PlanContext) StrictIntArithCols(n *logical.Node) map[string]bool {
	return StrictIntArithCols(n)
}

func (PlanContext) StrictIntArithColsThroughRenames(n *logical.Node) map[string]bool {
	return StrictIntArithColsThroughRenames(n)
}

func (PlanContext) SubstituteNestedRenameRefs(expr plansql.Node, child *logical.Node) (plansql.Node, bool) {
	return SubstituteNestedRenameRefs(expr, child)
}

func (PlanContext) SubtreeNamingOf(n *logical.Node) *SubtreeNaming {
	return SubtreeNamingOf(n)
}

func (PlanContext) ValidateColumnsUnderPolicy(ctx context.Context, cat *catalog.Catalog, info *plansql.SelectInfo,
	deniedFor func(table string) map[string]bool, denyTable func(table string) error) error {
	return ValidateColumnsUnderPolicy(ctx, cat, info, deniedFor, denyTable)
}

func (PlanContext) WantBareName(name string) string {
	return WantBareName(name)
}

func (PlanContext) WindowExecColumn(node *logical.Node, we logical.WindowExpr, keys map[string]windowKey) exec.WindowColumn {
	return WindowExecColumn(node, we, keys)
}

func (PlanContext) WindowKeySpecs(keys map[string]windowKey) []ProjectExprSpec {
	return WindowKeySpecs(keys)
}

func (PlanContext) WindowSpecOutputType(node *logical.Node, we logical.WindowExpr) expr.DeclType {
	return WindowSpecOutputType(node, we)
}

func (PlanContext) PublishedWireUnconstrainedDecimal(projection, node *logical.Node) map[string]bool {
	return RepublishDeclaredNames(projection, DeclaredWireUnconstrainedDecimal(node))
}
func (PlanContext) PublishedStringLengths(projection, node *logical.Node) map[string]int {
	return RepublishDeclaredNames(projection, DeclaredStringLengths(node))
}
func (PlanContext) WithManifestSnapshot(ctx context.Context, snapshot *ManifestSnapshot) context.Context {
	return context.WithValue(ctx, ManifestSnapshotCtxKey{}, snapshot)
}
func (PlanContext) SetReverseBloomInnerThreshold(n int64) { ReverseBloomInnerThreshold = n }
func (PlanContext) SemiAntiNE() *atomic.Bool              { return &SemiAntiNE }
func (PlanContext) SortMergeJoinsPlanned() *atomic.Int64  { return &SortMergeJoinsPlanned }
