// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// maxAGPLPhysicalMembers bounds the seam by OPERATION rather than by spelling.
//
// TestAGPLPhysicalReferenceBudget counts package-qualified names, and a method
// on a type that already crosses the seam adds none: a new exported method on
// physical.PlanContext, called through the context value dagplan already
// declares, leaves that count at 16. This budget counts what the AGPL side can
// reach — every exported object of physical its syntax names by any spelling,
// package-scope name, method or field — so an operation added behind the
// context fails here until it is listed below.
//
// Counted once per DECLARING object: an operation reached through an embedding
// receiver is the same operation. docs/design/seam-narrowing-measurement.md
// records the same set keyed by the receiver expression's type instead, which
// spells six of these twice and so reports 215.

// 209 → 210 (2026-09-18, arc SR, #1079): `PublishedOutputProjectionNode`, the
// question "whose names does the CLIENT read", which a set operation answers
// one node lower than `FindOutputProjectionNode` does. The alternative was to
// let dagplan descend the set operation itself, which puts a COPY of
// ADR-0026 §8b's rule on the AGPL side of the seam — and a copy of a naming
// rule in the distributed planner is how the two engines drift apart (§2b).
// One member is the smaller cost.
//
// 210 → 214 (2026-09-19, arc JR): `JoinResidualUnresolved`,
// `RefuseUnresolvedJoinResidual`, `PlanContext.ParseJoinCondExpr` and
// `PlanContext.RefuseJoinResidual` — the outer-join ON residual's refusal
// API, read by dagplan's stage emission and the worker's fragment executor
// (ADR-0006, 2026-09-19). Four members for one loud floor; the translation
// itself (`dagplan.residualWithStageSpellings`) lives on the AGPL side and
// adds nothing here.

// 214 → 216 (2026-09-19, arc BJ, #1223): `Planner.BushyJoinReorder` and
// `Planner.LogicalOptions`. The first is the instance's copy of the bushy
// option, set the way every AGPL caller already sets `LateMaterialization`.
// The second is the whole option set as the logical optimizer takes it, for
// the three AGPL sites that RE-OPTIMIZE a plan — the worker's pipeline
// re-plan, the HTTP server's two entries, and dagplan's subquery
// resolution. The alternative was to let each of them build a
// `logical.Options` from the field by hand, which is a COPY of the
// planner's configuration on the AGPL side: when Options grows a second
// field, every hand-built copy silently drops it and the second query of a
// statement is planned under settings the first one did not have. One
// member is the smaller cost, and it is the same member forever.
//
// 216 → 217 (2026-09-20, arc FR, #1230): `ReaderSchemaReads` — the same one
// member the reference budget above records, and for the same reason. It is
// a package-scope counter, so it appears in both counts.
const maxAGPLPhysicalMembers = 217

// measuredPhysicalMembers are the members reached through values. With the 16
// package-scope names of measuredPhysicalNames they are the whole surface, and
// the budget above is their count.
var measuredPhysicalMembers = strings.Fields(`
ColDecls.Dec
ColDecls.Fields
ColDecls.PlaceholderTypes
ColDecls.Types
DecimalCoercion.Name
DecimalCoercion.Precision
DecimalCoercion.Scale
GroupKeyResolution.Alias
GroupKeyResolution.Computed
GroupKeyResolution.Decl
GroupKeyResolution.Def
GroupKeyResolution.Deferred
GroupKeyResolution.Expr
JoinResidualUnresolved
ManifestSnapshot.AggregateColumnStats
ManifestSnapshot.Get
PhysicalPlan.Cleanup
PhysicalPlan.Pipeline
PhysicalPlan.PrettyPrint
PlanContext.AggDerivedGroupKey
PlanContext.AggInputColumnDecimal
PlanContext.AggInputIsWideInteger
PlanContext.AggOhlcvOutputFields
PlanContext.AggOutputFromInputDecl
PlanContext.AggScopePreservingWrapper
PlanContext.AggSpecOutputDecimal
PlanContext.AggSpecOutputType
PlanContext.AggregateGroupKeyName
PlanContext.AggregateOutputNames
PlanContext.AssignJoinKeySides
PlanContext.AstIsFieldPath
PlanContext.BlockBareName
PlanContext.BlockPublishedColumns
PlanContext.BuildJoinResidualFilter
PlanContext.BuildSemiAntiFilter
PlanContext.BuildStreamAlias
PlanContext.CleanExpr
PlanContext.ColSet
PlanContext.CollectASTCols
PlanContext.CollectColRefs
PlanContext.CollectColRefsBelow
PlanContext.CollectOuterColumns
PlanContext.CollectTableAliases
PlanContext.DeclTypeParts
PlanContext.DeclaredJoinSchema
PlanContext.DerivedAliasSourceColumn
PlanContext.DerivedGroupKeyDecl
PlanContext.DerivedScopeBareName
PlanContext.EmittedColDecls
PlanContext.EmittedColTypes
PlanContext.EmittedColumnNames
PlanContext.EmittedKeyNames
PlanContext.FindAggregateAncestor
PlanContext.FindOutputProjectionNode
PlanContext.FindOutputProjectionsForRename
PlanContext.GroupKeyByIdentity
PlanContext.GroupKeyNames
PlanContext.GroupKeysPublishedBelow
PlanContext.HasFilterOrPartition
PlanContext.HasLimit
PlanContext.InferProjectionDeclType
PlanContext.InferProjectionDeclTypeConf
PlanContext.InlinedInSetRowCap
PlanContext.InputColDecls
PlanContext.IsSimpleColRefForRename
PlanContext.IsSortMergeSource
PlanContext.JoinArmAlias
PlanContext.JoinSideSchemas
PlanContext.LateralEmptySpec
PlanContext.LateralMarkerDroppedAbove
PlanContext.LateralSideOf
PlanContext.LogicalAggOutNames
PlanContext.MapJoinType
PlanContext.MatchesPartitionFilter
PlanContext.NameIsPlainColumn
PlanContext.NamedArmScope
PlanContext.NewComputedColumnsOpWithMeta
PlanContext.NodeDeclaredType
PlanContext.OutputSchema
PlanContext.OwnedJoinArm
PlanContext.ParseJoinCondExpr
PlanContext.ParseJoinKeys
PlanContext.ParseSemiAntiNE
PlanContext.ProjSourceName
PlanContext.ProjectionForName
PlanContext.ProjectionOutputName
PlanContext.PublishedNamesOfProjection
PlanContext.PublishedOutputProjectionNode
PlanContext.PublishedStringLengths
PlanContext.PublishedWireUnconstrainedDecimal
PlanContext.QualifiedColumn
PlanContext.ReferencesSynthetic
PlanContext.ReferencesSyntheticAgg
PlanContext.RefuseJoinCond
PlanContext.RefuseJoinResidual
PlanContext.RefuseUnexpandedStarAnywhere
PlanContext.RefuseUnrepresentableRealInList
PlanContext.RelationScopeSubtree
PlanContext.ResolveAggInputName
PlanContext.ResolveJoinKeyTypes
PlanContext.ResolveNullsLast
PlanContext.ResolveOutputRenameSource
PlanContext.ResolveRenameSource
PlanContext.ResolveWindowKeys
PlanContext.RespellDerivedAliasRefs
PlanContext.ReverseBloomInnerThreshold
PlanContext.RewriteColRefs
PlanContext.ScopePreservingWrapper
PlanContext.SemiAntiBuildStoreCols
PlanContext.SemiAntiNE
PlanContext.SetOpArmIsUnknownLit
PlanContext.SetOpArmProjection
PlanContext.SetOpArmTypeConflict
PlanContext.SetOpBaseName
PlanContext.SetOpCarrierGapPairs
PlanContext.SetOpName
PlanContext.SetOpOutputNames
PlanContext.SetOpTargetType
PlanContext.SetOpUnknownLiteralArms
PlanContext.SetReverseBloomInnerThreshold
PlanContext.SortInputSetOpWidth
PlanContext.SortKeySlotPos
PlanContext.SortKeyWrittenSlotPos
PlanContext.SortMergeJoinsPlanned
PlanContext.SourceColDeclsThroughRenames
PlanContext.StrictIntArithCols
PlanContext.StrictIntArithColsThroughRenames
PlanContext.SubstituteNestedRenameRefs
PlanContext.SubtreeNamingOf
PlanContext.ValidateColumnsUnderPolicy
PlanContext.WantBareName
PlanContext.WindowExecColumn
PlanContext.WindowKeySpecs
PlanContext.WindowSpecOutputType
PlanContext.WithManifestSnapshot
PlanContext.WrapsAWindow
Planner.AnnotateScanColumns
Planner.ApplyContextColumnPolicies
Planner.ApplyContextColumnPoliciesToNewScans
Planner.BushyJoinReorder
Planner.Catalog
Planner.CheckPolicyPlanOrderFromContext
Planner.Ctes
Planner.DeclaredOutputSchema
Planner.EnforceQueryLimits
Planner.EstimatePlanScanBytes
Planner.EstimatePlanScanCost
Planner.EstimateSubtreeBytes
Planner.ExecuteSubquerySchema
Planner.GetAggregateColumnStats
Planner.GetManifest
Planner.LateMaterialization
Planner.LogicalOptions
Planner.ManifestSnapshot
Planner.MemoryBudget
Planner.Plan
Planner.PlanContext
Planner.PlanCtx
Planner.QueryLimits
Planner.ScanFileFilter
Planner.SharedSpillMgr
Planner.SharedTracker
Planner.ShouldSortMergeJoin
Planner.SortMergeJoinBytes
Planner.SpillDir
Planner.StreamingSources
Planner.SubqueryInnerColumns
Planner.SubqueryOutputArity
Planner.SubqueryOutputColumn
Planner.ValidateColumns
ProjectExprSpec.Expr
ProjectExprSpec.Fields
ProjectExprSpec.Name
ProjectExprSpec.Precision
ProjectExprSpec.Scale
ProjectExprSpec.SourceSlot
ProjectExprSpec.SourceSlotSet
ProjectExprSpec.Type
ProjectExprSpec.TypeKnown
QueryCost.HasFilter
QueryCost.HasLimit
QueryCost.TotalBytes
QueryCost.TotalFiles
QueryCost.TotalRows
ReaderSchemaReads
RefuseUnresolvedJoinResidual
SetOpArmPlan.Coerce
SetOpArmPlan.Specs
SetOpArmPlan.Types
SetOpColType.Dec
SetOpColType.DecKnown
SetOpColType.Fields
SetOpColType.Known
SetOpColType.Typ
SubtreeNaming.AliasCols
SubtreeNaming.BuildColOrigins
SubtreeNaming.MaterializedBuildColOrigins
blockColumn.Decl
blockColumn.DeclKnown
blockColumn.Expr
blockColumn.Name
`)

// physicalMemberPkg is the part of `go list -json` this gate reads.
type physicalMemberPkg struct {
	ImportPath string
	Name       string
	Dir        string
	Export     string
	ForTest    string
	GoFiles    []string
}

// loadPackagesWithExports lists every package of the module and its
// dependencies, test variants included, with the export data file the compiler
// wrote for each. Building that export data is what makes this gate cost more
// than the package-qualified one; it is also what lets it resolve a selector
// on a value, which no syntactic walk can do.
func loadPackagesWithExports(root string) ([]physicalMemberPkg, error) {
	cmd := exec.Command("go", "list",
		"-json=ImportPath,Name,Dir,Export,ForTest,GoFiles",
		"-export", "-deps", "-test", "./...")
	cmd.Dir = root
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w: %s", err, stderr.String())
	}
	var pkgs []physicalMemberPkg
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for {
		var p physicalMemberPkg
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decoding go list output: %w", err)
		}
		pkgs = append(pkgs, p)
	}
	if len(pkgs) < 50 {
		return nil, fmt.Errorf("go list reported %d packages; the scan is broken, not the code", len(pkgs))
	}
	return pkgs, nil
}

// agplPhysicalMembers records, for every AGPL package in the module, each
// exported object of internal/planner/physical that its type-checked syntax
// reaches, keyed by the type that declares it. Comments and strings resolve to
// nothing and are not reached. A type error is returned rather than tolerated:
// a package that does not resolve is a package whose references are invisible,
// which would read as a smaller seam.
func agplPhysicalMembers(root string) (map[string][]string, error) {
	pkgs, err := loadPackagesWithExports(root)
	if err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	exports := make(map[string]string)   // import path -> export data file
	augmented := make(map[string]string) // package under test -> its test-augmented variant
	for _, p := range pkgs {
		switch {
		case p.Export == "":
		case !strings.Contains(p.ImportPath, " ["):
			exports[p.ImportPath] = p.Export
		case p.ForTest != "" && !strings.HasSuffix(p.Name, "_test"):
			augmented[p.ForTest] = p.Export
		}
	}

	type target struct {
		dir      string
		files    []string
		override map[string]string
	}
	var targets []target
	for _, p := range pkgs {
		if p.Dir == "" || !strings.HasPrefix(p.Dir, abs+string(filepath.Separator)) {
			continue
		}
		rel := filepath.ToSlash(strings.TrimPrefix(p.Dir, abs+string(filepath.Separator)))
		if licenseOf(rel) != agpl {
			continue
		}
		if p.Name == "main" && strings.HasSuffix(p.ImportPath, ".test") {
			continue // the generated test main
		}
		variant := strings.Contains(p.ImportPath, " [")
		if !variant {
			if _, ok := augmented[p.ImportPath]; ok {
				continue // its test-augmented variant carries the same files and more
			}
			targets = append(targets, target{dir: p.Dir, files: p.GoFiles})
			continue
		}
		// An external test package compiles against the augmented variant of
		// the package it tests, not against the plain one.
		override := make(map[string]string)
		if strings.HasSuffix(p.Name, "_test") {
			if export, ok := augmented[p.ForTest]; ok {
				override[p.ForTest] = export
			}
		}
		targets = append(targets, target{dir: p.Dir, files: p.GoFiles, override: override})
	}

	members := make(map[string][]string)
	fset := token.NewFileSet()
	for _, tg := range targets {
		lookup := func(path string) (io.ReadCloser, error) {
			file, ok := tg.override[path]
			if !ok {
				if file, ok = exports[path]; !ok {
					return nil, fmt.Errorf("no export data for %s", path)
				}
			}
			return os.Open(file)
		}
		var files []*ast.File
		for _, name := range tg.files {
			file, err := parser.ParseFile(fset, filepath.Join(tg.dir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				return nil, err
			}
			files = append(files, file)
		}
		info := &types.Info{
			Uses:       make(map[*ast.Ident]types.Object),
			Selections: make(map[*ast.SelectorExpr]*types.Selection),
		}
		var firstErr error
		conf := types.Config{
			Importer:                 importer.ForCompiler(fset, "gc", lookup),
			DisableUnusedImportCheck: true,
			Error: func(err error) {
				if firstErr == nil {
					firstErr = err
				}
			},
		}
		if _, err := conf.Check(modulePath+"/"+filepath.Base(tg.dir), fset, files, info); firstErr != nil {
			return nil, fmt.Errorf("type-checking %s: %w", tg.dir, firstErr)
		} else if err != nil {
			return nil, fmt.Errorf("type-checking %s: %w", tg.dir, err)
		}
		for _, file := range files {
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				key := ""
				if selection, ok := info.Selections[selector]; ok {
					if obj := selection.Obj(); physicalObject(obj) {
						key = physicalMemberKey(selection, obj)
					}
				} else if obj := info.Uses[selector.Sel]; physicalObject(obj) {
					key = obj.Name()
				}
				if key == "" {
					return true
				}
				pos := fset.Position(selector.Pos())
				where, err := filepath.Rel(abs, pos.Filename)
				if err != nil {
					where = pos.Filename
				}
				members[key] = append(members[key], fmt.Sprintf("%s:%d", filepath.ToSlash(where), pos.Line))
				return true
			})
		}
	}
	return members, nil
}

func physicalObject(obj types.Object) bool {
	return obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == modulePath+"/internal/planner/physical" && obj.Exported()
}

// physicalMemberKey names a member by the type that DECLARES it, so that the
// same operation reached through an embedding receiver counts once.
func physicalMemberKey(selection *types.Selection, obj types.Object) string {
	if fn, ok := obj.(*types.Func); ok {
		if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
			if name := namedTypeName(sig.Recv().Type()); name != "" {
				return name + "." + obj.Name()
			}
		}
		return obj.Name()
	}
	// A field: walk the embedding path to the struct that declares it.
	owner := selection.Recv()
	index := selection.Index()
	for _, i := range index[:len(index)-1] {
		structure, ok := derefType(owner).Underlying().(*types.Struct)
		if !ok {
			return obj.Name()
		}
		owner = structure.Field(i).Type()
	}
	if name := namedTypeName(owner); name != "" {
		return name + "." + obj.Name()
	}
	return obj.Name()
}

func derefType(t types.Type) types.Type {
	if pointer, ok := t.(*types.Pointer); ok {
		return pointer.Elem()
	}
	return t
}

func namedTypeName(t types.Type) string {
	if named, ok := derefType(t).(*types.Named); ok {
		return named.Obj().Name()
	}
	return ""
}

func physicalMemberBudgetMessage(members map[string][]string, budget int, measured []string) string {
	if len(members) <= budget {
		return ""
	}
	known := make(map[string]bool, len(measured))
	for _, name := range measured {
		known[name] = true
	}
	keys := make([]string, 0, len(members))
	for key := range members {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var details []string
	for _, key := range keys {
		if !known[key] {
			details = append(details, fmt.Sprintf("physical.%s at %s", key, strings.Join(members[key], ", ")))
		}
	}
	return fmt.Sprintf("AGPL packages reach %d exported physical identifiers by some spelling; budget is %d; new members: %s", len(members), budget, strings.Join(details, "; "))
}

func measuredPhysicalSurface() []string {
	surface := make([]string, 0, len(measuredPhysicalNames)+len(measuredPhysicalMembers))
	surface = append(surface, measuredPhysicalNames...)
	return append(surface, measuredPhysicalMembers...)
}

// TestAGPLPhysicalMemberBudget holds the size of the operation set. The name
// budget above holds its spelling; this one holds what the AGPL side can do
// with the one type that crosses, which is where the surface moved.
func TestAGPLPhysicalMemberBudget(t *testing.T) {
	surface := measuredPhysicalSurface()
	if len(surface) != maxAGPLPhysicalMembers {
		t.Fatalf("the listed surface is %d members; the budget says %d; the budget is the list's size", len(surface), maxAGPLPhysicalMembers)
	}
	members, err := agplPhysicalMembers(moduleRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) == 0 {
		t.Fatal("no physical members reached from AGPL packages")
	}
	if message := physicalMemberBudgetMessage(members, maxAGPLPhysicalMembers, surface); message != "" {
		t.Fatal(message)
	}
}

// TestPhysicalMemberBudgetNamesANewMember: the failure has to say WHICH
// operation was added, or the next author reads only that a number moved.
func TestPhysicalMemberBudgetNamesANewMember(t *testing.T) {
	members := map[string][]string{
		"PlanContext":              {"internal/coordinator/dagplan/plan.go:10"},
		"PlanContext.OutputSchema": {"internal/coordinator/dagplan/plan.go:418"},
		"PlanContext.NewLocalFact": {"internal/coordinator/dagplan/plan_context.go:20"},
	}
	message := physicalMemberBudgetMessage(members, 2, []string{"PlanContext", "PlanContext.OutputSchema"})
	if !strings.Contains(message, "physical.PlanContext.NewLocalFact at internal/coordinator/dagplan/plan_context.go:20") {
		t.Fatalf("the new member is not named: %s", message)
	}
	if physicalMemberBudgetMessage(members, 3, nil) != "" {
		t.Fatal("a surface inside the budget is not a failure")
	}
}
