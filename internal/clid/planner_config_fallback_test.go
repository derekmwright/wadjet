// SPDX-License-Identifier: AGPL-3.0-only

package clid

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// fallbackDBFlags are the planner and engine options a database this server
// PLANS with must carry. The pgwire fallback DB is one: every statement the
// routing gate declines — a leading comment, `TABLE`, `VALUES`, and every
// statement when a provider is present but routing is disabled — is planned
// there rather than by the coordinator.
//
// It carried BushyJoinReorder after #1223 and nothing else, so the four below
// ran on their zero values while the coordinator and worker beside it ran on
// the resolved ones: `--memory-budget` and `--spill-dir` reached the worker
// and not this planner, `--sort-merge-join-bytes` was 0 here whatever was
// typed, and `--late-materialization`'s documented default of TRUE was false
// here because the field nobody sets is the zero value (#1226).
var fallbackDBFlags = []string{
	"SortMergeJoinBytes",
	"LateMaterialization",
	"BushyJoinReorder",
	"MemoryBudget",
	"SpillDir",
}

// TestEveryDatabaseThisServerPlansWithCarriesItsResolvedOptions reads this
// package's source and requires every wadjet.Config literal to set each of
// them. It is the clid half of internal/cli's
// TestEveryCLIDatabaseCarriesTheResolvedPlannerFlags: a source assertion,
// because standing a standalone server up to observe a plan choice would gate
// a wiring fact behind a whole cluster.
func TestEveryDatabaseThisServerPlansWithCarriesItsResolvedOptions(t *testing.T) {
	fset := token.NewFileSet()
	// One file, parsed — not a directory walk: a git worktree under
	// .claude/worktrees/ is a second copy of this source (CLAUDE.md).
	file, err := parser.ParseFile(fset, "serve.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing serve.go: %v", err)
	}

	found := 0
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Config" {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "wadjet" {
			return true
		}
		found++
		set := map[string]bool{}
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := kv.Key.(*ast.Ident); ok {
				set[key.Name] = true
			}
		}
		for _, field := range fallbackDBFlags {
			if !set[field] {
				t.Errorf("%s: this wadjet.Config sets no %s, so every statement this "+
					"database plans runs on the zero value while the coordinator and "+
					"worker beside it run on the operator's resolved one (#1226)",
					fset.Position(lit.Pos()), field)
			}
		}
		return true
	})
	// Non-vacuity: the pgwire fallback DB is the literal. A refactor that
	// leaves none is a gate that proves nothing.
	if found < 1 {
		t.Fatalf("found %d wadjet.Config literals in serve.go, want at least 1 — "+
			"the walk stopped finding the database it is meant to hold", found)
	}
}
