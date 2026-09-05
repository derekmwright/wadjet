package auth

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
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
// constructor with no error return, or a `SetAuthProvider` that binds) on THAT
// provider, in that same function.
//
// WHAT IT SEES. A field holds a provider when its declared type MENTIONS
// `*auth.Provider` at any depth — the plain pointer, an EMBEDDED one, a slice,
// an array, a map key or value, a channel element, an anonymous struct's
// field, or a type argument of a generic instantiation — or when its type is
// an interface declared in this module whose method set is a subset of the
// provider's, which is the shape a "door holds something provider-like"
// refactor produces. Round 4's review defeated an earlier version with five of
// those; each is now a negative-control cell below.
//
// WHAT IT DOES NOT SEE, stated so the claim is not the fourth enumeration that
// was slightly wrong: it is syntactic. A provider reached through a named type
// declared elsewhere (a struct type whose own fields are censused where THEY
// are declared) is not chased into; an interface from another module is not
// resolved; and a value stored through reflection or an `any` is invisible.
// The claim the gate supports is "a new door that holds a provider in a field
// cannot be added and forget", and ADR-0033 says exactly that.
//
// The exceptions are listed with their reason, and both lists are asserted in
// BOTH directions: an exception that stops being reached fails too, so neither
// can rot into a blanket.

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

// attachFieldsKnown is the census of provider-bearing fields, with what each
// one is. A new entry is not a failure by itself — it is a prompt: decide
// whether storing it is an ATTACH (bind) or a propagation (attachAllowed).
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

// attachConfigTypes are the CONFIG structs, by exact "package.Type". A field
// of one is an INPUT rather than state: the constructor that consumes the
// config is the attach, and it is covered by the runtime field it assigns.
// Requiring the bind at every caller that fills a Config would put it in
// `main` and in every test rig instead of at the door.
//
// An exact set, not a name pattern: `strings.Contains(name, "Config")` would
// silently exempt a future `ConfigManager` that holds a provider (round-4
// review, P1(r4)).
var attachConfigTypes = map[string]bool{
	"pgwire.Config":     true,
	"server.Config":     true,
	"server.GRPCConfig": true,
	"wadjet.Config":     true,
}

// bindingCalls are the calls that count as binding a provider to a catalog.
var bindingCalls = map[string]bool{
	"BindToCatalog":   true,
	"AttachProvider":  true,
	"SetAuthProvider": true,
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

// moduleFiles parses every non-test .go file of the module.
//
// Directories whose name starts with "." or "_" are skipped — what the Go
// toolchain itself skips, and load-bearing here: a git worktree lives at
// `.claude/worktrees/<name>/` INSIDE the module root and is a full second copy
// of the source, so a walk that descends into it sees every attach site twice
// and its verdict depends on whether anybody happens to have a worktree open
// (CLAUDE.md; this has been the defect twice).
func moduleFiles(t *testing.T, root string) (*token.FileSet, []*ast.File, map[*ast.File]string) {
	t.Helper()
	fset := token.NewFileSet()
	var files []*ast.File
	paths := map[*ast.File]string{}
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
		f, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		files = append(files, f)
		paths[f] = strings.TrimPrefix(path, root+string(filepath.Separator))
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return fset, files, paths
}

// ---------------------------------------------------------------------------
// The census itself: a pure function over parsed files, so the negative
// controls below run the SAME code over ten shapes the reviewer wrote to
// defeat it.
// ---------------------------------------------------------------------------

type censusResult struct {
	fields    map[string]string // "pkg.Struct.Field" -> shape
	fieldName map[string]bool   // runtime field identifiers (config fields excluded)
	unbound   map[string]string // "pkg.Func" -> file
	reached   map[string]bool   // attachAllowed entries actually seen
}

// providerMethodNames collects the methods declared on *Provider in package
// auth, which is what an interface must be a subset of to hold a provider.
func providerMethodNames(files []*ast.File) map[string]bool {
	out := map[string]bool{}
	for _, f := range files {
		if f.Name.Name != "auth" {
			continue
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
				continue
			}
			star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			if id, ok := star.X.(*ast.Ident); ok && id.Name == "Provider" {
				out[fn.Name.Name] = true
			}
		}
	}
	return out
}

// providerLikeInterfaces indexes "pkg.Name" for every interface declared in
// these files whose method set is a non-empty subset of the provider's. A
// field of such a type can hold a provider without ever naming one.
func providerLikeInterfaces(files []*ast.File, methods map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			it, ok := ts.Type.(*ast.InterfaceType)
			if !ok || it.Methods == nil || len(it.Methods.List) == 0 {
				return true
			}
			named := 0
			for _, m := range it.Methods.List {
				for _, nm := range m.Names {
					named++
					if !methods[nm.Name] {
						return true // a method the provider does not have
					}
				}
			}
			if named > 0 {
				out[f.Name.Name+"."+ts.Name.Name] = true
			}
			return true
		})
	}
	return out
}

// isProviderNamed reports whether e names the auth.Provider TYPE (qualified
// from outside the package, bare inside it).
func isProviderNamed(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.SelectorExpr:
		pkg, ok := x.X.(*ast.Ident)
		return ok && pkg.Name == "auth" && x.Sel.Name == "Provider"
	case *ast.Ident:
		return x.Name == "Provider"
	}
	return false
}

// mentionsProvider reports whether a type expression mentions *auth.Provider
// at any depth, or is an interface a provider satisfies.
func mentionsProvider(e ast.Expr, pkg string, ifaces map[string]bool) bool {
	switch t := e.(type) {
	case *ast.StarExpr:
		if isProviderNamed(t.X) {
			return true
		}
		return mentionsProvider(t.X, pkg, ifaces)
	case *ast.ParenExpr:
		return mentionsProvider(t.X, pkg, ifaces)
	case *ast.ArrayType:
		return mentionsProvider(t.Elt, pkg, ifaces)
	case *ast.MapType:
		return mentionsProvider(t.Key, pkg, ifaces) || mentionsProvider(t.Value, pkg, ifaces)
	case *ast.ChanType:
		return mentionsProvider(t.Value, pkg, ifaces)
	case *ast.Ellipsis:
		return mentionsProvider(t.Elt, pkg, ifaces)
	case *ast.IndexExpr: // Holder[*auth.Provider]
		return mentionsProvider(t.X, pkg, ifaces) || mentionsProvider(t.Index, pkg, ifaces)
	case *ast.IndexListExpr: // Holder[K, *auth.Provider]
		if mentionsProvider(t.X, pkg, ifaces) {
			return true
		}
		for _, idx := range t.Indices {
			if mentionsProvider(idx, pkg, ifaces) {
				return true
			}
		}
	case *ast.StructType: // an anonymous struct in a field position
		if t.Fields == nil {
			return false
		}
		for _, fld := range t.Fields.List {
			if mentionsProvider(fld.Type, pkg, ifaces) {
				return true
			}
		}
	case *ast.InterfaceType: // an inline interface — judged by its methods
		return false
	case *ast.Ident:
		return ifaces[pkg+"."+t.Name]
	case *ast.SelectorExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return ifaces[id.Name+"."+t.Sel.Name]
		}
	}
	return false
}

// fieldIdent is the name a field is referenced by: its own for a named field,
// the type's base identifier for an EMBEDDED one (`*auth.Provider` embeds as
// `Provider`).
func fieldIdent(fld *ast.Field) []string {
	if len(fld.Names) > 0 {
		out := make([]string, 0, len(fld.Names))
		for _, nm := range fld.Names {
			out = append(out, nm.Name)
		}
		return out
	}
	t := fld.Type
	for {
		switch x := t.(type) {
		case *ast.StarExpr:
			t = x.X
			continue
		case *ast.SelectorExpr:
			return []string{x.Sel.Name}
		case *ast.Ident:
			return []string{x.Name}
		case *ast.IndexExpr:
			t = x.X
			continue
		}
		return nil
	}
}

func exprText(fset *token.FileSet, e ast.Expr) string {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, fset, e); err != nil {
		return ""
	}
	return strings.Join(strings.Fields(buf.String()), "")
}

// funcBody is one function-like body: a declaration or a literal (a package
// level `var f = func(...)` is the shape that slipped past the first version).
type funcBody struct {
	name string
	body *ast.BlockStmt
	file *ast.File
}

func funcBodies(f *ast.File) []funcBody {
	var out []funcBody
	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
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
			out = append(out, funcBody{name: f.Name.Name + "." + name, body: fn.Body, file: f})
		}
	}
	// Every function LITERAL too, wherever it is — including in a package
	// level var, which has no FuncDecl at all.
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.FuncLit)
		if !ok || lit.Body == nil {
			return true
		}
		out = append(out, funcBody{
			name: f.Name.Name + ".func literal",
			body: lit.Body,
			file: f,
		})
		return true
	})
	return out
}

// deadRanges are the bodies of `if false { … }`, so a bind parked in one does
// not count. (Syntactic and deliberately narrow: the threat model is an honest
// mistake, not an adversary obfuscating a constant.)
func deadRanges(body *ast.BlockStmt) [][2]token.Pos {
	var out [][2]token.Pos
	ast.Inspect(body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		if id, ok := ifs.Cond.(*ast.Ident); ok && id.Name == "false" {
			out = append(out, [2]token.Pos{ifs.Body.Pos(), ifs.Body.End()})
		}
		return true
	})
	return out
}

func inRanges(p token.Pos, ranges [][2]token.Pos) bool {
	for _, r := range ranges {
		if p >= r[0] && p <= r[1] {
			return true
		}
	}
	return false
}

// runCensus is the whole analysis over one set of parsed files.
func runCensus(fset *token.FileSet, files []*ast.File, paths map[*ast.File]string) censusResult {
	methods := providerMethodNames(files)
	if len(methods) == 0 {
		// The negative controls do not include package auth; give them the
		// method set that matters for the interface shape.
		methods = map[string]bool{"Enabled": true, "BindError": true, "Audit": true,
			"Authenticator": true, "Authorizer": true, "Policies": true, "Evaluator": true,
			"BindToCatalog": true, "Update": true, "UpdateWithEvaluator": true,
			"UpdateFromConfig": true}
	}
	ifaces := providerLikeInterfaces(files, methods)

	res := censusResult{
		fields:    map[string]string{},
		fieldName: map[string]bool{},
		unbound:   map[string]string{},
		reached:   map[string]bool{},
	}

	for _, f := range files {
		if f.Name.Name == "auth" {
			continue // the package that DEFINES the binding; its state is not an attach
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
				if !mentionsProvider(fld.Type, f.Name.Name, ifaces) {
					continue
				}
				for _, nm := range fieldIdent(fld) {
					res.fields[f.Name.Name+"."+ts.Name.Name+"."+nm] = exprText(fset, fld.Type)
					if !attachConfigTypes[f.Name.Name+"."+ts.Name.Name] {
						res.fieldName[nm] = true
					}
				}
			}
			return true
		})
	}

	for _, f := range files {
		if f.Name.Name == "auth" {
			continue
		}
		for _, fb := range funcBodies(f) {
			dead := deadRanges(fb.body)
			var assigned []string // the provider VALUES and the holder.field paths
			var bound []string    // what the binding calls were given
			ast.Inspect(fb.body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.KeyValueExpr:
					id, ok := x.Key.(*ast.Ident)
					if !ok || !res.fieldName[id.Name] {
						return true
					}
					if lit, ok := x.Value.(*ast.Ident); ok && lit.Name == "nil" {
						return true // storing nil is not an attach
					}
					assigned = append(assigned, exprText(fset, x.Value))
				case *ast.AssignStmt:
					for i, lhs := range x.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || !res.fieldName[sel.Sel.Name] {
							continue
						}
						assigned = append(assigned, exprText(fset, sel))
						if i < len(x.Rhs) {
							assigned = append(assigned, exprText(fset, x.Rhs[i]))
						}
					}
				case *ast.CallExpr:
					sel, ok := x.Fun.(*ast.SelectorExpr)
					if !ok || !bindingCalls[sel.Sel.Name] || inRanges(x.Pos(), dead) {
						return true
					}
					// The RECEIVER (p.BindToCatalog) and every ARGUMENT
					// (auth.AttachProvider(ctx, p, cat, log)), so the bind has
					// to be on the value that was stored — not merely a call
					// with the right name somewhere in the function.
					bound = append(bound, exprText(fset, sel.X))
					for _, a := range x.Args {
						bound = append(bound, exprText(fset, a))
					}
				}
				return true
			})
			if len(assigned) == 0 {
				continue
			}
			// A composite literal assigned to a variable is reachable as
			// `<var>.<field>` too: `db := &DB{authProvider: …}` binds through
			// `db.authProvider.BindToCatalog(…)`.
			assigned = append(assigned, holderPaths(fset, fb.body, res.fieldName)...)
			if _, ok := attachAllowed[fb.name]; ok {
				res.reached[fb.name] = true
				continue
			}
			if !intersects(assigned, bound) {
				res.unbound[fb.name] = paths[fb.file]
			}
		}
	}
	return res
}

// holderPaths returns `<var>.<field>` for every `var := &T{field: …}` in the
// body, so a bind through the holder counts as binding what was stored.
func holderPaths(fset *token.FileSet, body *ast.BlockStmt, fieldNames map[string]bool) []string {
	var out []string
	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range as.Rhs {
			if i >= len(as.Lhs) {
				break
			}
			lhsID, ok := as.Lhs[i].(*ast.Ident)
			if !ok {
				continue
			}
			e := rhs
			if u, ok := e.(*ast.UnaryExpr); ok && u.Op == token.AND {
				e = u.X
			}
			cl, ok := e.(*ast.CompositeLit)
			if !ok {
				continue
			}
			for _, elt := range cl.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if id, ok := kv.Key.(*ast.Ident); ok && fieldNames[id.Name] {
					out = append(out, lhsID.Name+"."+id.Name)
				}
			}
		}
		return true
	})
	return out
}

func intersects(a, b []string) bool {
	set := make(map[string]bool, len(a))
	for _, s := range a {
		if s != "" {
			set[s] = true
		}
	}
	for _, s := range b {
		if set[s] {
			return true
		}
	}
	return false
}

// TestEveryProviderFieldIsAttachedThroughTheBindingFunction is the by-
// construction half of ADR-0033 rule 2.
func TestEveryProviderFieldIsAttachedThroughTheBindingFunction(t *testing.T) {
	root := moduleRoot(t)
	fset, files, paths := moduleFiles(t, root)
	res := runCensus(fset, files, paths)

	if len(res.fields) == 0 {
		t.Fatal("the census found NO provider-bearing field: the walk is broken, and a gate " +
			"that finds nothing passes for the wrong reason")
	}

	for name, shape := range res.fields {
		if _, known := attachFieldsKnown[name]; !known {
			t.Errorf("new provider-bearing field %s (type %s): decide whether storing it is an "+
				"ATTACH (call BindToCatalog / auth.AttachProvider on it in the function that "+
				"assigns it) or a propagation of an already-bound provider, then record it in "+
				"attachFieldsKnown", name, shape)
		}
	}
	for name := range attachFieldsKnown {
		if _, ok := res.fields[name]; !ok {
			t.Errorf("attachFieldsKnown lists %s, which no longer exists: delete it, so the "+
				"census cannot rot into a list nobody reads", name)
		}
	}
	for name := range attachConfigTypes {
		found := false
		for f := range res.fields {
			if strings.HasPrefix(f, name+".") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("attachConfigTypes exempts %s, which carries no provider field any more: "+
				"delete the exemption", name)
		}
	}

	if len(res.unbound) > 0 {
		names := make([]string, 0, len(res.unbound))
		for n := range res.unbound {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			t.Errorf("%s (%s) stores an *auth.Provider without binding THAT provider to a "+
				"catalog: call BindToCatalog (or auth.AttachProvider from a constructor with no "+
				"error return) on it there, or add it to attachAllowed with the reason it is a "+
				"propagation rather than an attach. ADR-0033 rule 2, #882.", n, res.unbound[n])
		}
	}
	for name, why := range attachAllowed {
		if !res.reached[name] {
			t.Errorf("attachAllowed lists %s (%q), which no longer stores a provider: delete "+
				"the exception", name, why)
		}
	}
}

// TestTheCensusCatchesTheShapesThatDefeatedIt runs the census over the ten
// shapes the round-4 review wrote to defeat it. Five were missed by the field
// half (embedded, slice, map, interface, generic) and three by the bind half
// (a func literal in a package-level var, a bind on an unrelated receiver, a
// bind in a dead branch). Each must now be caught, and each is legal Go the
// codebase could write tomorrow.
func TestTheCensusCatchesTheShapesThatDefeatedIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		src   string
		field string // the field the census must enumerate ("" = only the bind half)
		fn    string // the function it must flag ("" = only the field half)
	}{
		{
			name: "plain named field, constructor",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type Door struct{ provider *auth.Provider }
func NewDoor(p *auth.Provider) *Door { return &Door{provider: p} }`,
			field: "probe.Door.provider", fn: "probe.NewDoor",
		},
		{
			name: "plain named field, setter",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type Door struct{ provider *auth.Provider }
func (d *Door) SetProvider(p *auth.Provider) { d.provider = p }`,
			field: "probe.Door.provider", fn: "probe.(*Door).SetProvider",
		},
		{
			name: "assigned from a package-level func literal",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type Door struct{ provider *auth.Provider }
var install = func(d *Door, p *auth.Provider) { d.provider = p }`,
			field: "probe.Door.provider", fn: "probe.func literal",
		},
		{
			name: "binds on an unrelated receiver",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type Door struct{ provider *auth.Provider }
type Other struct{}
func (o *Other) SetAuthProvider(p *auth.Provider) {}
func NewDoor(p *auth.Provider, other *Other, unrelated *auth.Provider) *Door {
	other.SetAuthProvider(unrelated)
	return &Door{provider: p}
}`,
			field: "probe.Door.provider", fn: "probe.NewDoor",
		},
		{
			name: "binds inside a dead branch",
			src: `package probe
import (
	"context"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)
type Door struct{ provider *auth.Provider }
func NewDoor(ctx context.Context, p *auth.Provider, cat *catalog.Catalog) *Door {
	if false {
		_ = p.BindToCatalog(ctx, cat)
	}
	return &Door{provider: p}
}`,
			field: "probe.Door.provider", fn: "probe.NewDoor",
		},
		{
			name: "embedded provider",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type Door struct{ *auth.Provider }
func NewDoor(p *auth.Provider) *Door { return &Door{Provider: p} }`,
			field: "probe.Door.Provider", fn: "probe.NewDoor",
		},
		{
			name: "slice of providers",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type Door struct{ providers []*auth.Provider }
func NewDoor(ps []*auth.Provider) *Door { return &Door{providers: ps} }`,
			field: "probe.Door.providers", fn: "probe.NewDoor",
		},
		{
			name: "map of providers",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type Door struct{ byTenant map[string]*auth.Provider }
func NewDoor(m map[string]*auth.Provider) *Door { return &Door{byTenant: m} }`,
			field: "probe.Door.byTenant", fn: "probe.NewDoor",
		},
		{
			name: "interface a provider satisfies",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type providerish interface{ Enabled() bool }
type Door struct{ provider providerish }
func NewDoor(p *auth.Provider) *Door { return &Door{provider: p} }`,
			field: "probe.Door.provider", fn: "probe.NewDoor",
		},
		{
			name: "generic holder instantiated with a provider",
			src: `package probe
import "github.com/derekmwright/wadjet/internal/auth"
type Holder[T any] struct{ v T }
type Door struct{ held Holder[*auth.Provider] }
func NewDoor(h Holder[*auth.Provider]) *Door { return &Door{held: h} }`,
			field: "probe.Door.held", fn: "probe.NewDoor",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "probe.go", tc.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("the shape does not parse: %v", err)
			}
			res := runCensus(fset, []*ast.File{f}, map[*ast.File]string{f: "probe.go"})
			if tc.field != "" {
				if _, ok := res.fields[tc.field]; !ok {
					got := make([]string, 0, len(res.fields))
					for k := range res.fields {
						got = append(got, k)
					}
					sort.Strings(got)
					t.Errorf("the field census MISSED %s — it saw %v", tc.field, got)
				}
			}
			if tc.fn != "" {
				if _, ok := res.unbound[tc.fn]; !ok {
					t.Errorf("the bind census MISSED %s: it stores a provider and never binds it",
						tc.fn)
				}
			}
		})
	}

	// The control: a shape that DOES bind must not be flagged, or the cells
	// above would pass by flagging everything.
	t.Run("a constructor that binds is not flagged", func(t *testing.T) {
		src := `package probe
import (
	"context"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)
type Door struct{ provider *auth.Provider }
func NewDoor(ctx context.Context, p *auth.Provider, cat *catalog.Catalog) *Door {
	_ = p.BindToCatalog(ctx, cat)
	return &Door{provider: p}
}`
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "probe.go", src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		res := runCensus(fset, []*ast.File{f}, map[*ast.File]string{f: "probe.go"})
		if _, ok := res.unbound["probe.NewDoor"]; ok {
			t.Error("a constructor that binds the provider it stores was flagged as unbound")
		}
		if _, ok := res.fields["probe.Door.provider"]; !ok {
			t.Error("the control's field was not censused, so it proves nothing")
		}
	})
}
