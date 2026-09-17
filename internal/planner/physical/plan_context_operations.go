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
	return aggDerivedGroupKey(key, child)
}

func (PlanContext) AggInputColumnDecimal(node *logical.Node, col string) (logical.DecimalMeta, bool) {
	return aggInputColumnDecimal(node, col)
}

func (PlanContext) AggInputIsWideInteger(node plansql.Node, decls ColDecls) bool {
	return aggInputIsWideInteger(node, decls)
}

func (PlanContext) AggOhlcvOutputFields(node *logical.Node, agg logical.AggExpr) ([]parquet.Column, bool) {
	return aggOhlcvOutputFields(node, agg)
}

func (PlanContext) AggOutputFromInputDecl(fn string, distinct bool, in parquet.TypeID, precision, scale int, wideInt bool) (
	out parquet.TypeID, outPrecision, outScale int, ok bool,
) {
	return aggOutputFromInputDecl(fn, distinct, in, precision, scale, wideInt)
}

func (PlanContext) AggScopePreservingWrapper(t logical.NodeType) bool {
	return aggScopePreservingWrapper(t)
}

func (PlanContext) AggSpecOutputDecimal(node *logical.Node, agg logical.AggExpr) (logical.DecimalMeta, bool) {
	return aggSpecOutputDecimal(node, agg)
}

func (PlanContext) AggSpecOutputType(node *logical.Node, agg logical.AggExpr) (parquet.TypeID, bool) {
	return aggSpecOutputType(node, agg)
}

func (PlanContext) AggregateGroupKeyName(proj *logical.Projection, projectNode *logical.Node) (string, bool) {
	return aggregateGroupKeyName(proj, projectNode)
}

func (PlanContext) AssignJoinKeySides(leftKeys, rightKeys []string, probe, build *SubtreeNaming) {
	AssignJoinKeySides(leftKeys, rightKeys, probe, build)
}

func (PlanContext) AstIsFieldPath(node plansql.Node, decls ColDecls) bool {
	return astIsFieldPath(node, decls)
}

func (PlanContext) BlockBareName(s string) string {
	return blockBareName(s)
}

func (PlanContext) BlockPublishedColumns(p *logical.Node, published map[*logical.Node]bool,
	subqueryDecl func(string) (parquet.Column, bool)) ([]blockColumn, bool) {
	return blockPublishedColumns(p, published, subqueryDecl)
}

func (PlanContext) BuildJoinResidualFilter(filter, buildAlias string) func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	return BuildJoinResidualFilter(filter, buildAlias)
}

func (PlanContext) BuildStreamAlias(node *logical.Node) string {
	return BuildStreamAlias(node)
}

func (PlanContext) CleanExpr(s string) string {
	return cleanExpr(s)
}

func (PlanContext) ColSet(cols []string) map[string]bool {
	return colSet(cols)
}

func (PlanContext) CollectASTCols(n plansql.Node, out map[string]bool) {
	collectASTCols(n, out)
}

func (PlanContext) CollectColRefs(n plansql.Node) []*plansql.ColRef {
	return collectColRefs(n)
}

func (PlanContext) CollectColRefsBelow(n plansql.Node, stop func(plansql.Node) bool) []*plansql.ColRef {
	return collectColRefsBelow(n, stop)
}

func (PlanContext) CollectOuterColumns(node *logical.Node) map[string]string {
	return CollectOuterColumns(node)
}

func (PlanContext) CollectTableAliases(node *logical.Node) map[string]bool {
	return collectTableAliases(node)
}

func (PlanContext) DeclTypeParts(d expr.DeclType) parquet.Column {
	return declTypeParts(d)
}

func (PlanContext) DeclaredJoinSchema(n *logical.Node, want []string, published map[*logical.Node]bool,
	subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
	return declaredJoinSchema(n, want, published, subqueryDecl)
}

func (PlanContext) OutputSchema(root *logical.Node,
	subqueryDecl func(string) (parquet.Column, bool)) []parquet.Column {
	return declaredOutputSchema(root, subqueryDecl)
}

func (PlanContext) DerivedAliasSourceColumn(name string, child *logical.Node) string {
	return derivedAliasSourceColumn(name, child)
}

func (PlanContext) DerivedGroupKeyDecl(key string, node plansql.Node, child *logical.Node) expr.DeclType {
	return derivedGroupKeyDecl(key, node, child)
}

func (PlanContext) DerivedScopeBareName(name string, subtree *logical.Node) string {
	return derivedScopeBareName(name, subtree)
}

func (PlanContext) EmittedColDecls(n *logical.Node) ColDecls {
	return emittedColDecls(n)
}

func (PlanContext) EmittedColTypes(n *logical.Node) map[string]parquet.TypeID {
	return emittedColTypes(n)
}

func (PlanContext) EmittedColumnNames(n *logical.Node) []string {
	return emittedColumnNames(n)
}

func (PlanContext) EmittedKeyNames(published []string, resolve []GroupKeyResolution, aggOut []string) []string {
	return emittedKeyNames(published, resolve, aggOut)
}

func (PlanContext) FindAggregateAncestor(node *logical.Node) *logical.Node {
	return FindAggregateAncestor(node)
}

func (PlanContext) FindOutputProjectionNode(n *logical.Node) *logical.Node {
	return findOutputProjectionNode(n)
}

func (PlanContext) FindOutputProjectionsForRename(n *logical.Node) []logical.Projection {
	return findOutputProjectionsForRename(n)
}

func (PlanContext) GroupKeyByIdentity(agg *logical.Node) map[string]string {
	return groupKeyByIdentity(agg)
}

func (PlanContext) GroupKeyNames(agg, child *logical.Node) (published []string, resolve []GroupKeyResolution) {
	return groupKeyNames(agg, child)
}

func (PlanContext) HasFilterOrPartition(n *logical.Node) bool {
	return HasFilterOrPartition(n)
}

func (PlanContext) HasLimit(n *logical.Node) bool {
	return HasLimit(n)
}

func (PlanContext) InferProjectionDeclType(node plansql.Node, fallback parquet.TypeID, strictInt map[string]bool, decls ColDecls) expr.DeclType {
	return inferProjectionDeclType(node, fallback, strictInt, decls)
}

func (PlanContext) InferProjectionDeclTypeConf(node plansql.Node, fallback parquet.TypeID,
	strictInt map[string]bool, decls ColDecls) (expr.DeclType, expr.Confidence) {
	return inferProjectionDeclTypeConf(node, fallback, strictInt, decls)
}

func (PlanContext) InlinedInSetRowCap() int {
	return inlinedInSetRowCap()
}

func (PlanContext) InputColDecls(n *logical.Node) ColDecls {
	return inputColDecls(n)
}

func (PlanContext) IsSimpleColRefForRename(n plansql.Node) bool {
	return isSimpleColRefForRename(n)
}

func (PlanContext) JoinArmAlias(node *logical.Node) string {
	return joinArmAlias(node)
}

func (PlanContext) JoinSideSchemas(node *logical.Node, leftKeys, rightKeys []string,
	published map[*logical.Node]bool, subqueryDecl func(string) (parquet.Column, bool)) (probe, build []parquet.Column) {
	return joinSideSchemas(node, leftKeys, rightKeys, published, subqueryDecl)
}

func (PlanContext) LateralEmptySpec(node *logical.Node) (marker string, cols []exec.LateralDefault, drop bool) {
	return lateralEmptySpec(node)
}

func (PlanContext) LateralMarkerDroppedAbove(node *logical.Node, name string) bool {
	return lateralMarkerDroppedAbove(node, name)
}

func (PlanContext) LateralSideOf(node *logical.Node) int {
	return lateralSideOf(node)
}

func (PlanContext) LogicalAggOutNames(agg *logical.Node) []string {
	return logicalAggOutNames(agg)
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
	return namedArmScope(n)
}

func (PlanContext) NewComputedColumnsOpWithMeta(cols []exec.ProjectColumn, meta []parquet.Column) exec.UnaryOperator {
	return NewComputedColumnsOpWithMeta(cols, meta)
}

func (PlanContext) NodeDeclaredType(node plansql.Node, decls ColDecls) (expr.DeclType, expr.Confidence) {
	return nodeDeclaredType(node, decls)
}

func (PlanContext) OwnedJoinArm(n *logical.Node, name string) *logical.Node {
	return ownedJoinArm(n, name)
}

func (PlanContext) ParseJoinKeys(cond string) (leftKeys, rightKeys, residual []string) {
	return ParseJoinKeys(cond)
}

func (PlanContext) ProjSourceName(proj *logical.Projection) string {
	return projSourceName(proj)
}

func (PlanContext) ProjectionForName(projs []logical.Projection, name, bare string) *logical.Projection {
	return projectionForName(projs, name, bare)
}

func (PlanContext) ProjectionOutputName(proj logical.Projection) string {
	return projectionOutputName(proj)
}

func (PlanContext) PublishedNamesOfProjection(projNode *logical.Node) []string {
	return publishedNamesOfProjection(projNode)
}

func (PlanContext) QualifiedColumn(ref *plansql.ColRef) string {
	return qualifiedColumn(ref)
}

func (PlanContext) ReferencesSynthetic(n plansql.Node, prefix string) bool {
	return referencesSynthetic(n, prefix)
}

func (PlanContext) ReferencesSyntheticAgg(n plansql.Node) bool {
	return referencesSyntheticAgg(n)
}

func (PlanContext) RefuseJoinCond(joinType, cond string, residual []string) error {
	return refuseJoinCond(joinType, cond, residual)
}

func (PlanContext) RefuseUnexpandedStarAnywhere(node *logical.Node) error {
	return refuseUnexpandedStarAnywhere(node)
}

func (PlanContext) RefuseUnrepresentableRealInList(root *logical.Node) error {
	return refuseUnrepresentableRealInList(root)
}

func (PlanContext) RelationScopeSubtree(n *logical.Node, name string) *logical.Node {
	return relationScopeSubtree(n, name)
}

func (PlanContext) ResolveAggInputName(name string, child *logical.Node) (resolved string, expr plansql.Node, exprInput *logical.Node, alias bool) {
	return ResolveAggInputName(name, child)
}

func (PlanContext) ResolveJoinKeyTypes(node *logical.Node, leftKeys, rightKeys []string, cte cteColTypes) []parquet.TypeID {
	return resolveJoinKeyTypes(node, leftKeys, rightKeys, cte)
}

func (PlanContext) ResolveNullsLast(ob logical.OrderExpr) bool {
	return resolveNullsLast(ob)
}

func (PlanContext) ResolveOutputRenameSource(name string, child *logical.Node) string {
	return resolveOutputRenameSource(name, child)
}

func (PlanContext) ResolveRenameSource(name string, child *logical.Node, forGather bool) string {
	return resolveRenameSource(name, child, forGather)
}

func (PlanContext) ResolveWindowKeys(node *logical.Node) map[string]windowKey {
	return resolveWindowKeys(node)
}

func (PlanContext) RespellDerivedAliasRefs(n plansql.Node, child *logical.Node) (plansql.Node, bool) {
	return respellDerivedAliasRefs(n, child)
}

func (PlanContext) RewriteColRefs(n plansql.Node, sub func(*plansql.ColRef) (plansql.Node, bool)) (
	out plansql.Node, changed, complete bool,
) {
	return RewriteColRefs(n, sub)
}

func (PlanContext) SetOpArmIsUnknownLit(unknown [][]bool, i, col int) bool {
	return setOpArmIsUnknownLit(unknown, i, col)
}

func (PlanContext) SetOpArmProjection(arm *logical.Node, outNames []string) (SetOpArmPlan, error) {
	return setOpArmProjection(arm, outNames)
}

func (PlanContext) SetOpArmTypeConflict(node *logical.Node) error {
	return setOpArmTypeConflict(node)
}

func (PlanContext) SetOpBaseName(node *logical.Node) string {
	return setOpBaseName(node)
}

func (PlanContext) SetOpName(node *logical.Node) string {
	return setOpName(node)
}

func (PlanContext) SetOpOutputNames(arm *logical.Node) []string {
	return setOpOutputNames(arm)
}

func (PlanContext) SetOpTargetType(plans []SetOpArmPlan, col int, name, op string, unknown [][]bool) (SetOpColType, bool, error) {
	return setOpTargetType(plans, col, name, op, unknown)
}

func (PlanContext) SetOpUnknownLiteralArms(arm *logical.Node, cols int) []bool {
	return setOpUnknownLiteralArms(arm, cols)
}

func (PlanContext) SortInputSetOpWidth(child *logical.Node) (int, bool) {
	return sortInputSetOpWidth(child)
}

func (PlanContext) SortKeySlotPos(ob logical.OrderExpr, sortNode *logical.Node) int {
	return sortKeySlotPos(ob, sortNode)
}

func (PlanContext) SortKeyWrittenSlotPos(ob logical.OrderExpr, sortNode *logical.Node) int {
	return sortKeyWrittenSlotPos(ob, sortNode)
}

func (PlanContext) SourceColDeclsThroughRenames(n *logical.Node) ColDecls {
	return sourceColDeclsThroughRenames(n)
}

func (PlanContext) StrictIntArithCols(n *logical.Node) map[string]bool {
	return strictIntArithCols(n)
}

func (PlanContext) StrictIntArithColsThroughRenames(n *logical.Node) map[string]bool {
	return strictIntArithColsThroughRenames(n)
}

func (PlanContext) SubstituteNestedRenameRefs(expr plansql.Node, child *logical.Node) (plansql.Node, bool) {
	return substituteNestedRenameRefs(expr, child)
}

func (PlanContext) SubtreeNamingOf(n *logical.Node) *SubtreeNaming {
	return SubtreeNamingOf(n)
}

func (PlanContext) ValidateColumnsUnderPolicy(ctx context.Context, cat *catalog.Catalog, info *plansql.SelectInfo,
	deniedFor func(table string) map[string]bool, denyTable func(table string) error) error {
	return ValidateColumnsUnderPolicy(ctx, cat, info, deniedFor, denyTable)
}

func (PlanContext) WantBareName(name string) string {
	return wantBareName(name)
}

func (PlanContext) WindowExecColumn(node *logical.Node, we logical.WindowExpr, keys map[string]windowKey) exec.WindowColumn {
	return windowExecColumn(node, we, keys)
}

func (PlanContext) WindowKeySpecs(keys map[string]windowKey) []ProjectExprSpec {
	return windowKeySpecs(keys)
}

func (PlanContext) WindowSpecOutputType(node *logical.Node, we logical.WindowExpr) expr.DeclType {
	return windowSpecOutputType(node, we)
}

func (PlanContext) PublishedWireUnconstrainedDecimal(projection, node *logical.Node) map[string]bool {
	return republishDeclaredNames(projection, declaredWireUnconstrainedDecimal(node))
}
func (PlanContext) PublishedStringLengths(projection, node *logical.Node) map[string]int {
	return republishDeclaredNames(projection, declaredStringLengths(node))
}
func (PlanContext) WithManifestSnapshot(ctx context.Context, snapshot *ManifestSnapshot) context.Context {
	return context.WithValue(ctx, manifestSnapshotCtxKey{}, snapshot)
}
func (PlanContext) SetReverseBloomInnerThreshold(n int64) { ReverseBloomInnerThreshold = n }
func (PlanContext) SemiAntiNE() *atomic.Bool              { return &SemiAntiNE }
func (PlanContext) SortMergeJoinsPlanned() *atomic.Int64  { return &SortMergeJoinsPlanned }

func (PlanContext) BuildSemiAntiFilter(filter string) func(*batch.RecordBatch, int, *batch.RecordBatch, int) bool {
	return BuildSemiAntiFilter(filter)
}
func (PlanContext) SemiAntiBuildStoreCols(keys []string, filter string) []string {
	return semiAntiBuildStoreCols(keys, filter)
}
func (PlanContext) SetOpCarrierGapPairs() [][2]parquet.TypeID {
	return setOpCarrierGapPairs()
}
func (PlanContext) ReverseBloomInnerThreshold() *int64 { return &ReverseBloomInnerThreshold }
