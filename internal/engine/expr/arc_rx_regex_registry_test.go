// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Arc RX (#1499), the seam's structural gates: the function registry and
// the module's source. Expected answers are PostgreSQL 17.11's, measured
// (tooling/arcs/rx_regexp_translation/rx_author/pg.tsv).

// rxOutcome is a registry call's answer rendered for a table: the value, or
// E:<SQLSTATE> for a raised refusal.
func rxOutcome(fn string, args ...any) (out string) {
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(fatalEval)
			if !ok {
				panic(r)
			}
			out = "E:" + sqlerr.StateOf(fe.err)
		}
	}()
	f := DefaultRegistry.Lookup(fn)
	if f == nil {
		return "E:unregistered"
	}
	v := f(args)
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}

// rxPatternFunctions is every registered function that takes a SQL pattern,
// with how to call it over (subject, pattern). A function whose name reads
// as a regular-expression function must be listed here
// (TestArcRXEveryRegexFunctionCompilesThroughTheSeam), and every listed one
// must answer the ARE reading.
var rxPatternFunctions = map[string]func(s, p string) []any{
	"textregexeq":        func(s, p string) []any { return []any{s, p} },
	"texticregexeq":      func(s, p string) []any { return []any{s, p} },
	"textregexne":        func(s, p string) []any { return []any{s, p} },
	"texticregexne":      func(s, p string) []any { return []any{s, p} },
	"regexp_like":        func(s, p string) []any { return []any{s, p} },
	"regexp_count":       func(s, p string) []any { return []any{s, p} },
	"regexp_replace":     func(s, p string) []any { return []any{s, p, "#"} },
	"regexp_extract":     func(s, p string) []any { return []any{s, p} },
	"regexp_extract_all": func(s, p string) []any { return []any{s, p} },
	"regexp_split":       func(s, p string) []any { return []any{s, p} },
	"payload_matches":    func(s, p string) []any { return []any{s, p} },
	"substring":          func(s, p string) []any { return []any{s, p} },
	// SIMILAR TO's pattern is its own language; its rewrite compiles
	// through the seam — the cell is an escaped ARE letter.
	"similar_to": func(s, p string) []any { return []any{s, "%" + strings.ReplaceAll(p, `\`, `#`) + "%", "#"} },
}

// TestArcRXEveryRegexFunctionCompilesThroughTheSeam walks the function
// registry: a function whose name reads as a regular-expression function
// cannot be registered without a row in rxPatternFunctions, and every row
// answers the ARE reading of two patterns the dialects read differently —
// `\b` over 'abc' (a backspace: no match; RE2's word boundary matches) and
// `b\z` (2201B: not an ARE escape; RE2's end of text).
func TestArcRXEveryRegexFunctionCompilesThroughTheSeam(t *testing.T) {
	nameRE := regexp.MustCompile(`regex|rlike|^similar|payload_match|glob`)
	for _, name := range DefaultRegistry.Names() {
		if nameRE.MatchString(name) {
			if _, ok := rxPatternFunctions[name]; !ok {
				t.Errorf("registered function %s reads as a regular-expression function but has no rxPatternFunctions row: "+
					"compile its pattern with translateAndCompile and add the row", name)
			}
		}
	}
	// What each function answers over 'abc' with `\b` under the ARE
	// reading (no match anywhere).
	noMatch := map[string]string{
		"textregexeq": "false", "texticregexeq": "false", "textregexne": "true", "texticregexne": "true",
		"regexp_like": "false", "regexp_count": "0", "regexp_replace": "abc", "regexp_extract": "NULL",
		"regexp_extract_all": "[]", "regexp_split": `["abc"]`, "payload_matches": "false",
		"substring": "NULL", "similar_to": "false",
	}
	for name, call := range rxPatternFunctions {
		if !DefaultRegistry.Has(name) {
			t.Errorf("%s is listed but not registered", name)
			continue
		}
		if got, want := rxOutcome(name, call("abc", `\b`)...), noMatch[name]; got != want {
			t.Errorf(`%s('abc', '\b') = %s, want %s: \b is a backspace in an ARE`, name, got, want)
		}
		if got := rxOutcome(name, call("ab", `b\z`)...); got != "E:2201B" {
			t.Errorf(`%s('ab', 'b\z') = %s, want E:2201B: \z is not an ARE escape`, name, got)
		}
	}
}

// TestArcRXNoOtherCompileOfASQLPattern is the seam's structural half: no
// non-test file in the module calls regexp.Compile / MustCompile /
// MatchString / … except compileSQLRegex — the one compile of a SQL pattern
// — and package-level initializers over a constant pattern. The ABAC
// policy evaluator's matchRegex compiles an operator-configured policy
// pattern, not a SQL one, and is named here.
func TestArcRXNoOtherCompileOfASQLPattern(t *testing.T) {
	compileFns := map[string]bool{
		"Compile": true, "MustCompile": true, "CompilePOSIX": true, "MustCompilePOSIX": true,
		"MatchString": true, "Match": true, "MatchReader": true,
	}
	allowedFuncs := map[string]bool{
		"internal/engine/expr/pg_regex.go:compileSQLRegex": true,
		"internal/auth/abac_eval.go:matchRegex":            true, // policy pattern (operator config)
	}
	root := filepath.Join("..", "..", "..")
	fset := token.NewFileSet()
	for _, dir := range []string{"internal", "wadjet", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return perr
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			alias := ""
			for _, imp := range f.Imports {
				if imp.Path.Value == `"regexp"` {
					alias = "regexp"
					if imp.Name != nil {
						alias = imp.Name.Name
					}
				}
			}
			if alias == "" {
				return nil
			}
			for _, decl := range f.Decls {
				switch dd := decl.(type) {
				case *ast.FuncDecl:
					if allowedFuncs[rel+":"+dd.Name.Name] {
						continue
					}
					ast.Inspect(dd, func(n ast.Node) bool {
						if call, ok := n.(*ast.CallExpr); ok && isRegexpCall(call, alias, compileFns) {
							t.Errorf("%s: %s calls regexp.%s: a SQL pattern compiles only through translateAndCompile",
								fset.Position(call.Pos()), dd.Name.Name, call.Fun.(*ast.SelectorExpr).Sel.Name)
						}
						return true
					})
				case *ast.GenDecl:
					// A package-level initializer may compile a CONSTANT
					// pattern (a string literal) and nothing else.
					ast.Inspect(dd, func(n ast.Node) bool {
						call, ok := n.(*ast.CallExpr)
						if !ok || !isRegexpCall(call, alias, compileFns) {
							return true
						}
						if len(call.Args) != 1 {
							t.Errorf("%s: package-level regexp call with %d arguments", fset.Position(call.Pos()), len(call.Args))
							return true
						}
						if lit, ok := call.Args[0].(*ast.BasicLit); !ok || lit.Kind != token.STRING {
							t.Errorf("%s: package-level regexp compile of a non-constant pattern", fset.Position(call.Pos()))
						}
						return true
					})
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func isRegexpCall(call *ast.CallExpr, alias string, fns map[string]bool) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == alias && fns[sel.Sel.Name]
}
