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

// rxPatternFunctions is how to call every function that takes a SQL
// pattern over (subject, pattern). Its keys are regexFunctionDialect's.
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

// TestArcRXEveryRegexFunctionDeclaresItsDialect walks the function
// registry: a function whose name reads as a regular-expression function
// cannot be registered without DECLARING its dialect (a regexFunctionDialect
// row — PostgreSQL's constructs the ARE, the engine's DuckDB-origin
// functions RE2), and every declared function answers in the dialect it
// declares on two patterns the dialects read differently: `\b` over 'a b'
// (ARE: a backspace, no match; RE2: a word boundary) and `b\z` over 'ab'
// (ARE: 2201B, not an ARE escape; RE2: the end of the text).
func TestArcRXEveryRegexFunctionDeclaresItsDialect(t *testing.T) {
	nameRE := regexp.MustCompile(`regex|rlike|^similar|payload_match|glob`)
	for _, name := range DefaultRegistry.Names() {
		if nameRE.MatchString(name) {
			if _, ok := regexFunctionDialect[name]; !ok {
				t.Errorf("registered function %s reads as a regular-expression function but declares no dialect: "+
					"add its regexFunctionDialect row (ARE for a PostgreSQL construct, RE2 for a DuckDB-origin function)", name)
			}
		}
	}
	for name := range regexFunctionDialect {
		if _, ok := rxPatternFunctions[name]; !ok {
			t.Errorf("%s declares a dialect but has no rxPatternFunctions row", name)
		}
	}
	// Each function's answer to (subject, pattern) in each dialect.
	type cell struct{ s, p string }
	backspace, endOfText := cell{"a b", `\b`}, cell{"ab", `b\z`}
	want := map[regexDialect]map[string]map[cell]string{
		dialectARE: {
			"textregexeq":    {backspace: "false", endOfText: "E:2201B"},
			"texticregexeq":  {backspace: "false", endOfText: "E:2201B"},
			"textregexne":    {backspace: "true", endOfText: "E:2201B"},
			"texticregexne":  {backspace: "true", endOfText: "E:2201B"},
			"regexp_like":    {backspace: "false", endOfText: "E:2201B"},
			"regexp_count":   {backspace: "0", endOfText: "E:2201B"},
			"regexp_replace": {backspace: "a b", endOfText: "E:2201B"},
			"substring":      {backspace: "NULL", endOfText: "E:2201B"},
			"similar_to":     {backspace: "false", endOfText: "E:2201B"},
		},
		dialectRE2: {
			"regexp_extract":     {backspace: "", endOfText: "b"},
			"regexp_extract_all": {backspace: `["","","",""]`, endOfText: `["b"]`},
			"regexp_split":       {backspace: `["a"," ","b"]`, endOfText: `["a",""]`},
			"payload_matches":    {backspace: "true", endOfText: "true"},
		},
	}
	for name, d := range regexFunctionDialect {
		call, ok := rxPatternFunctions[name]
		if !ok {
			continue
		}
		if !DefaultRegistry.Has(name) {
			t.Errorf("%s declares a dialect but is not registered", name)
			continue
		}
		cells, ok := want[d][name]
		if !ok {
			t.Errorf("%s declares dialect %d but the table has no answers for it in that dialect", name, d)
			continue
		}
		for c, w := range cells {
			if got := rxOutcome(name, call(c.s, c.p)...); got != w {
				t.Errorf("%s(%q, %q) = %s, want %s (declared dialect %d)", name, c.s, c.p, got, w, d)
			}
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
