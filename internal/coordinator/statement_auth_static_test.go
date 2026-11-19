// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestTheStatementPathReadsTheLiveProviderOnce is ADR-0050 Acceptance 5's
// first half: in the non-test files of this package, the ExecuteSQL path
// reads the live provider (`c.authProvider`, or `EnforcesABAC`, which reads
// it) at exactly ONE site — the pin in statementAuth. Every decision after it
// is taken against the pinned copy.
//
// The path is everything ExecuteSQL can reach inside this package: a call
// graph over every function and method declared here, with a method matched
// by NAME on any receiver (conservative: a name shared by two types counts as
// both), and a method value or function value referenced without a call
// counted as reached.
//
// Excluded, by name, because they are separate statements or separate doors
// that the record leaves on the live provider (ADR-0050 "What is out"):
//   - the alert statements (handleCreateAlertSQL, handleDropAlertSQL,
//     handleAlterAlertSQL): ExecuteSQL dispatches them BEFORE it accepts a
//     SELECT and pins, and they return without planning one;
//   - SubmitSQL, the async door, and authorizeQueryAccess, the query-ownership
//     check: not reachable from ExecuteSQL at all; asserted below so a new
//     edge to them is seen;
//   - SetAuthProvider (the writer) and EnforcesABAC (the door's question).
func TestTheStatementPathReadsTheLiveProviderOnce(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	funcs := map[string][]*ast.FuncDecl{} // bare name -> declarations
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		if f.Name.Name != "coordinator" {
			continue
		}
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				funcs[fd.Name.Name] = append(funcs[fd.Name.Name], fd)
			}
		}
	}
	if len(funcs["ExecuteSQL"]) == 0 || len(funcs["statementAuth"]) != 1 {
		t.Fatalf("ExecuteSQL / statementAuth not found (%d, %d)",
			len(funcs["ExecuteSQL"]), len(funcs["statementAuth"]))
	}

	excluded := map[string]bool{
		"handleCreateAlertSQL": true, "handleDropAlertSQL": true, "handleAlterAlertSQL": true,
		"SetAuthProvider": true, "EnforcesABAC": true,
	}
	notReachable := []string{"SubmitSQL", "authorizeQueryAccess", "SetAuthProvider"}

	// liveReads lists the live-provider reads in a body.
	liveReads := func(fd *ast.FuncDecl) []token.Position {
		var out []token.Position
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if x.Sel.Name == "authProvider" || x.Sel.Name == "EnforcesABAC" {
					out = append(out, fset.Position(x.Pos()))
				}
			}
			return true
		})
		return out
	}
	// edges lists the package functions a body names.
	edges := func(fd *ast.FuncDecl) []string {
		var out []string
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if _, ok := funcs[x.Sel.Name]; ok {
					out = append(out, x.Sel.Name)
				}
			case *ast.Ident:
				if _, ok := funcs[x.Name]; ok {
					out = append(out, x.Name)
				}
			}
			return true
		})
		return out
	}

	reached := map[string]bool{"ExecuteSQL": true}
	queue := []string{"ExecuteSQL"}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if excluded[name] {
			continue
		}
		for _, fd := range funcs[name] {
			for _, e := range edges(fd) {
				if !reached[e] {
					reached[e] = true
					queue = append(queue, e)
				}
			}
		}
	}

	var sites []string
	for name := range reached {
		if excluded[name] {
			continue
		}
		for _, fd := range funcs[name] {
			for _, pos := range liveReads(fd) {
				sites = append(sites, name+" @ "+pos.String())
			}
		}
	}
	sort.Strings(sites)
	if len(sites) != 1 || !strings.HasPrefix(sites[0], "statementAuth @ ") {
		t.Errorf("the ExecuteSQL path reads the live provider at %d site(s), want exactly one, "+
			"the pin in statementAuth:\n  %s", len(sites), strings.Join(sites, "\n  "))
	}
	for _, name := range notReachable {
		if reached[name] && name != "SetAuthProvider" {
			t.Errorf("%s is now reachable from ExecuteSQL: it reads the live provider, so the "+
				"statement path would read it at a second site", name)
		}
	}
	// And ExecuteSQL takes the pin itself, once.
	calls := 0
	for _, fd := range funcs["ExecuteSQL"] {
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "statementAuth" {
					calls++
				}
			}
			return true
		})
	}
	if calls != 1 {
		t.Errorf("ExecuteSQL takes the statement's pin %d times, want 1", calls)
	}
}
