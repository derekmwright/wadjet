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
	"strconv"
	"strings"
	"testing"
)

// This budget includes production and test files in every declared AGPL region.
// The measured names and their reasons are in docs/design/seam-narrowing-measurement.md.
const maxAGPLPhysicalNames = 16

var measuredPhysicalNames = strings.Fields(`
ColDecls
DecimalCoercion
GroupKeyResolution
ManifestSnapshot
NewManifestSnapshot
NewPlanner
NewPlannerForContext
PhysicalPlan
PlanContext
Planner
ProjectExprSpec
QueryCost
QueryLimitSQLState
SetOpArmPlan
SetOpColType
SubtreeNaming
`)

func agplPhysicalReferences(root string) (map[string][]string, error) {
	refs := make(map[string][]string)
	fs := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor" || entry.Name() == "testdata") {
				return filepath.SkipDir
			}
			if path != root {
				if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if licenseOf(filepath.ToSlash(filepath.Dir(rel))) != agpl {
			return nil
		}
		file, err := parser.ParseFile(fs, path, nil, 0)
		if err != nil {
			return err
		}
		aliases := make(map[string]bool)
		for _, spec := range file.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if imported != modulePath+"/internal/planner/physical" {
				continue
			}
			alias := "physical"
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			if alias == "." {
				return fmt.Errorf("%s: physical needs a named import for the reference count", rel)
			}
			aliases[alias] = true
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			id, ok := selector.X.(*ast.Ident)
			if !ok || (id.Obj != nil && id.Obj.Kind != ast.Pkg) || !aliases[id.Name] {
				return true
			}
			pos := fs.Position(selector.Pos())
			refs[selector.Sel.Name] = append(refs[selector.Sel.Name], fmt.Sprintf("%s:%d", filepath.ToSlash(rel), pos.Line))
			return true
		})
		return nil
	})
	return refs, err
}

func physicalBudgetMessage(refs map[string][]string, budget int, measured []string) string {
	if len(refs) <= budget {
		return ""
	}
	known := make(map[string]bool)
	for _, name := range measured {
		known[name] = true
	}
	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}
	sort.Strings(names)
	var details []string
	for _, name := range names {
		if !known[name] {
			details = append(details, fmt.Sprintf("physical.%s at %s", name, strings.Join(refs[name], ", ")))
		}
	}
	return fmt.Sprintf("AGPL packages reference %d physical names; budget is %d; new names: %s", len(refs), budget, strings.Join(details, "; "))
}

func TestAGPLPhysicalReferenceBudget(t *testing.T) {
	refs, err := agplPhysicalReferences(moduleRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) == 0 {
		t.Fatal("no physical references found in AGPL packages")
	}
	if message := physicalBudgetMessage(refs, maxAGPLPhysicalNames, measuredPhysicalNames); message != "" {
		t.Fatal(message)
	}
}

func TestPhysicalReferenceBudgetNamesANewReference(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"internal/coordinator/example.go": `package coordinator
import local "github.com/derekmwright/wadjet/internal/planner/physical"
var _ = local.PlanContext{}
// local.CommentOnly is not a reference.
var text = "local.StringOnly"
func shadow() { local := struct { Field int }{}; _ = local.Field }
`,
		"internal/worker/example_test.go": `package worker
import "github.com/derekmwright/wadjet/internal/planner/physical"
var _ = physical.NewName
`,
		"internal/server/pgwire/example.go": `package pgwire
import "github.com/derekmwright/wadjet/internal/planner/physical"
var _ = physical.MITName
`,
	}
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := agplPhysicalReferences(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 {
		t.Fatalf("got references %v, want PlanContext and NewName", refs)
	}
	message := physicalBudgetMessage(refs, 1, []string{"PlanContext"})
	if !strings.Contains(message, "physical.NewName at internal/worker/example_test.go:3") {
		t.Fatalf("new reference not named: %s", message)
	}
}
