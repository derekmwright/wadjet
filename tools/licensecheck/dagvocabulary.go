// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The distributed planner's own words.
//
// The other two gates read package IMPORTS and file HEADERS; neither can see
// what a package CONTAINS. Before the stage planner moved, both were green
// with 540 distributed-planning declarations sitting inside the MIT directory
// internal/planner/physical — the split's whole subject, on the wrong side of
// its own boundary, invisible to its own gates.
//
// Since the move the property is mostly compiler-enforced: Stage,
// Distribution and the Stage* constants live in internal/coordinator/dagplan,
// so an MIT file that names one has to import it and the import-boundary gate
// fails. This gate is the residual — it stops NEW distributed planning being
// written on the MIT side, before anybody reaches for the import.
//
// Declared, not derived, for the same reason the regions are: a rule like
// "any identifier containing Shuffle" would relicense a directory the moment
// somebody renamed a local helper. Each entry says what the name IS, so the
// failure tells a reader whether to rename their symbol or move their code.
var dagVocabulary = map[string]string{
	"Stage":                         "the distributed plan node",
	"ExchangeStage":                 "an exchange in the stage DAG",
	"Distribution":                  "a stage's distribution property",
	"DistKind":                      "the distribution kinds",
	"RequiredKind":                  "the distribution a parent demands of a child",
	"RequiredDistribution":          "the distribution a parent demands of a child",
	"EnsureDistribution":            "the exchange-insertion pass",
	"OutputDistribution":            "a stage's produced distribution",
	"ValidateNativeDAGShape":        "the DAG shape check",
	"AssertExchangeConsistency":     "the exchange-consistency assertion",
	"PlanDistributed":               "the DAG planning entry",
	"PlanStages":                    "the DAG planning entry",
	"StagePlanner":                  "the distributed planner",
	"NewStagePlanner":               "the distributed planner's constructor",
	"walkStages":                    "stage emission",
	"generateStages":                "stage generation",
	"emitSetOpStages":               "set-op stage emission",
	"emitMergeAggregateTree":        "the merge-aggregate tree",
	"emitMergeSortTree":             "the merge-sort tree",
	"ExchangeSubsume":               "the exchange-subsumption switch",
	"HashPartitionCount":            "the shuffle fan-out",
	"CanProbeSplit":                 "probe-split routing",
	"PickShuffleCandidate":          "shuffle candidate selection",
	"ShuffleCandidate":              "a shuffle candidate",
	"LargeBuildScans":               "the large-build scan set",
	"PickAggregateShuffleCandidate": "aggregate-shuffle candidate selection",
	"BuildAggregateShuffleSQL":      "the aggregate-shuffle rewrite",
	"AggOverExchange":               "the aggregate-over-exchange switch",
	"ElidedCoPartitionedExchanges":  "the co-partitioned exchange elision counter",
	"GatherOutputSchema":            "the gather's declared output",
	"DimensionCascade":              "the dimension-cascade switch",
	"SharedSubplanDedup":            "the shared-subplan dedup switch",
	"StageFusion":                   "the stage-fusion switch",
	"PrettyPrintStages":             "the stage-list rendering",
	"EstimateCost":                  "the stage-list cost estimate",
}

// dagNamePrefixes catch the families it would be silly to enumerate: the 19
// Stage* stage-type constants, the Dist* distribution constants, the
// Required* set and the Err*Distributed refusals. Kept deliberately short —
// a prefix rule that fires on ordinary local code is worse than no rule,
// because it teaches people to work around the gate.
var dagNamePrefixes = []struct{ prefix, what string }{
	{"StageExchange", "a stage type in the DAG"},
	{"StageBroadcast", "a stage type in the DAG"},
	{"StageHashJoin", "a stage type in the DAG"},
	{"StageMerge", "a stage type in the DAG"},
	{"StageFinal", "a stage type in the DAG"},
	{"DistHash", "a distribution kind"},
	{"DistBroadcast", "a distribution kind"},
	{"DistSingleton", "a distribution kind"},
	{"DistRoundRobin", "a distribution kind"},
	{"RequiredHashPartitioned", "a required distribution"},
	{"RequiredClustered", "a required distribution"},
	{"RequiredBroadcast", "a required distribution"},
	{"RequiredSingleton", "a required distribution"},
}

// dagVocabularyExempt names declarations that match the rules above and are
// deliberately NOT distributed planning. Each needs its reason, the way a
// test crossing does.
var dagVocabularyExempt = map[string]string{}

// CheckNoDAGPlanningInMIT reports a declaration of the distributed planner's
// vocabulary inside an MIT directory.
func CheckNoDAGPlanningInMIT(root string) ([]Violation, int, error) {
	var out []Violation
	scanned := 0
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			// The same skip list the SPDX walk uses, and for the same
			// reason: a git worktree under .claude/ is a second copy of
			// every file in the tree.
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") ||
				name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		dir := filepath.ToSlash(filepath.Dir(rel))
		if licenseOf(dir) != mit {
			return nil
		}
		af, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return nil // the build gates syntax; this gate does not
		}
		scanned++
		for _, decl := range af.Decls {
			for _, n := range declaredNames(decl) {
				if _, ok := dagVocabularyExempt[n]; ok {
					continue
				}
				why, listed := dagVocabulary[n]
				if !listed {
					for _, p := range dagNamePrefixes {
						if strings.HasPrefix(n, p.prefix) {
							why, listed = p.what, true
							break
						}
					}
				}
				if !listed {
					continue
				}
				out = append(out, Violation{
					Where: filepath.ToSlash(rel),
					What: fmt.Sprintf("an MIT package declares %q — %s. Distributed planning belongs in "+
						"internal/coordinator/dagplan (LICENSING.md, ADR-0037). If this name is not "+
						"distributed planning, rename it; if it is, move it.", n, why),
				})
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Where != out[j].Where {
			return out[i].Where < out[j].Where
		}
		return out[i].What < out[j].What
	})
	return out, scanned, err
}

// declaredNames returns every top-level name a declaration introduces,
// methods included (a method named walkStages is stage emission wherever its
// receiver lives).
func declaredNames(d ast.Decl) []string {
	var out []string
	switch n := d.(type) {
	case *ast.FuncDecl:
		out = append(out, n.Name.Name)
	case *ast.GenDecl:
		for _, s := range n.Specs {
			switch sp := s.(type) {
			case *ast.TypeSpec:
				out = append(out, sp.Name.Name)
			case *ast.ValueSpec:
				for _, nm := range sp.Names {
					out = append(out, nm.Name)
				}
			}
		}
	}
	return out
}
