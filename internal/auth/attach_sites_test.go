package auth

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

// The enumeration gate for ADR-0033 rule 2.
//
// #882 was fixed three times. Each time the position — a policy set that
// reaches a catalog is BOUND to it — was implemented by finding the entry
// points and wiring the bind into each: two of them, then five, then six. Each
// count was arrived at by reading, and each was wrong, and the cost of a
// missed one is a policy that loads clean, matches nothing and discloses.
//
// A list maintained by hand is not a property. This gate reads the source
// instead: every field that HOLDS an *auth.Provider is enumerated, and every
// function that assigns one into such a field must also call the binding
// function (`Provider.BindToCatalog`, or `auth.AttachProvider` for a
// constructor with no error return) in that same function. A new door, a new
// setter or a new constructor that stores a provider and forgets to bind fails
// here, naming itself.
//
// The exceptions are listed with their reason, and the list is asserted in
// BOTH directions: an exception that stops being reached fails too, so the
// list cannot rot into a blanket.

// attachAllowed maps "package.Function" to why it may store a provider without
// binding.
var attachAllowed = map[string]string{
	// The per-connection struct copies the SERVER's provider, which
	// pgwire.NewServer already bound to the DB's catalog. It is a
	// propagation, not an attach: there is no second catalog in sight.
	"pgwire.(*Server).handleConn": "propagates the server's already-bound provider to one connection",
	// A constructor that takes a provider but holds no catalog to bind against
	// is listed explicitly rather than skipped by a rule, so that one which
	// LATER gains a catalog shows up here instead of staying quiet.
	"server.NewAdminAPI": "the admin API holds no catalog; it edits the config whose reload binds",
}

// attachFieldsSeen is the census of provider-bearing fields. A new one is not
// a failure by itself — it is a prompt: decide whether that field is an
// ATTACH (bind) or a propagation (add it to attachAllowed with a reason).
var attachFieldsKnown = map[string]string{
	"coordinator.Coordinator.authProvider": "runtime — Coordinator.SetAuthProvider binds",
	"pgwire.Config.AuthProvider":           "config input — pgwire.NewServer binds what it carries",
	"pgwire.Server.authProvider":           "runtime — pgwire.NewServer binds",
	"pgwire.pgConn.authProvider":           "runtime — a copy of the server's bound provider",
	"server.Config.Provider":               "config input — server.New binds what it carries",
	"server.GRPCConfig.AuthProvider":       "config input — NewGRPCServer binds what it carries",
	"server.GRPCServer.authProvider":       "runtime — NewGRPCServer binds",
	"server.Server.provider":               "runtime — server.New binds",
	"server.AdminAPI.provider":             "runtime — holds no catalog (see attachAllowed)",
	"wadjet.Config.AuthProvider":           "config input — wadjet.Open binds what it carries",
	"wadjet.DB.authProvider":               "runtime — wadjet.Open and DB.SetAuthProvider bind",
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root %q has no go.mod: %v", root, err)
	}
	return root
}

// walkModuleFiles visits every non-test .go file of the module.
//
// Directories whose name starts with "." or "_" are skipped — what the Go
// toolchain itself skips, and load-bearing here: a git worktree lives at
// `.claude/worktrees/<name>/` INSIDE the module root and is a full second copy
// of the source, so a walk that descends into it sees every attach site twice
// and its verdict depends on whether anybody happens to have a worktree open
// (CLAUDE.md; this has been the defect twice).
func walkModuleFiles(t *testing.T, root string, visit func(path string, f *ast.File, fset *token.FileSet)) {
	t.Helper()
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") ||
				name == "testdata" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			return perr
		}
		visit(path, f, fset)
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
}

// providerFieldType reports whether e is the type *auth.Provider (outside the
// auth package) or *Provider (inside it).
func providerFieldType(e ast.Expr) bool {
	star, ok := e.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch x := star.X.(type) {
	case *ast.SelectorExpr:
		pkg, ok := x.X.(*ast.Ident)
		return ok && pkg.Name == "auth" && x.Sel.Name == "Provider"
	case *ast.Ident:
		return x.Name == "Provider"
	}
	return false
}

func enclosingFuncName(f *ast.File, fn *ast.FuncDecl) string {
	name := fn.Name.Name
	if fn.Recv != nil && len(fn.Recv.List) > 0 {
		switch rt := fn.Recv.List[0].Type.(type) {
		case *ast.StarExpr:
			if id, ok := rt.X.(*ast.Ident); ok {
				name = "(*" + id.Name + ")." + name
			}
		case *ast.Ident:
			name = rt.Name + "." + name
		}
	}
	return f.Name.Name + "." + name
}

// TestEveryProviderFieldIsAttachedThroughTheBindingFunction is the by-
// construction half of ADR-0033 rule 2.
func TestEveryProviderFieldIsAttachedThroughTheBindingFunction(t *testing.T) {
	root := moduleRoot(t)

	fields := map[string]bool{}     // "pkg.Struct.Field"
	fieldNames := map[string]bool{} // runtime field identifiers, for assignment matching
	walkModuleFiles(t, root, func(path string, f *ast.File, _ *token.FileSet) {
		if f.Name.Name == "auth" {
			return // the package that DEFINES the binding; its own state is not an attach
		}
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok || st.Fields == nil {
				return true
			}
			for _, fld := range st.Fields.List {
				if !providerFieldType(fld.Type) {
					continue
				}
				for _, nm := range fld.Names {
					fields[f.Name.Name+"."+ts.Name.Name+"."+nm.Name] = true
					// A field of a CONFIG struct is an input, not state: the
					// constructor that consumes the config is the attach, and
					// it is covered by the runtime field it assigns. Requiring
					// the bind at every caller that fills a Config would put it
					// in `main` and in every test rig instead of at the door.
					if !strings.Contains(ts.Name.Name, "Config") {
						fieldNames[nm.Name] = true
					}
				}
			}
			return true
		})
	})

	if len(fields) == 0 {
		t.Fatal("the census found NO *auth.Provider field: the walk is broken, and a gate that " +
			"finds nothing passes for the wrong reason")
	}

	// Direction 1: a new provider-bearing field must be classified.
	for name := range fields {
		if _, known := attachFieldsKnown[name]; !known {
			t.Errorf("new provider-bearing field %s: decide whether storing it is an ATTACH "+
				"(call BindToCatalog / auth.AttachProvider in the function that assigns it) or a "+
				"propagation of an already-bound provider, then record it in attachFieldsKnown", name)
		}
	}
	// Direction 2: a field that disappeared must leave the list.
	for name := range attachFieldsKnown {
		if !fields[name] {
			t.Errorf("attachFieldsKnown lists %s, which no longer exists: delete it, so the "+
				"census cannot rot into a list nobody reads", name)
		}
	}

	// Every function that ASSIGNS one of those fields must bind in the same
	// function.
	unbound := map[string]string{} // "pkg.Func" -> file
	reached := map[string]bool{}
	walkModuleFiles(t, root, func(path string, f *ast.File, _ *token.FileSet) {
		if f.Name.Name == "auth" {
			return
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			assigns, binds := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.KeyValueExpr:
					if id, ok := x.Key.(*ast.Ident); ok && fieldNames[id.Name] {
						// A composite literal that sets the field to nil is
						// not an attach.
						if lit, ok := x.Value.(*ast.Ident); !ok || lit.Name != "nil" {
							assigns = true
						}
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && fieldNames[sel.Sel.Name] {
							assigns = true
						}
					}
				case *ast.CallExpr:
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
						if sel.Sel.Name == "BindToCatalog" || sel.Sel.Name == "AttachProvider" ||
							sel.Sel.Name == "SetAuthProvider" {
							binds = true
						}
					}
				}
				return true
			})
			if !assigns {
				continue
			}
			name := enclosingFuncName(f, fn)
			if _, ok := attachAllowed[name]; ok {
				reached[name] = true
				continue
			}
			if !binds {
				unbound[name] = strings.TrimPrefix(path, root+string(filepath.Separator))
			}
		}
	})

	if len(unbound) > 0 {
		names := make([]string, 0, len(unbound))
		for n := range unbound {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			t.Errorf("%s (%s) stores an *auth.Provider without binding it to a catalog: call "+
				"BindToCatalog (or auth.AttachProvider from a constructor with no error return) "+
				"there, or add it to attachAllowed with the reason it is a propagation rather "+
				"than an attach. ADR-0033 rule 2, #882.", n, unbound[n])
		}
	}

	for name, why := range attachAllowed {
		if !reached[name] {
			t.Errorf("attachAllowed lists %s (%q), which no longer stores a provider: delete the "+
				"exception", name, why)
		}
	}
}
