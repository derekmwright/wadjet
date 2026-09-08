package logical

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// A star reads what its source PUBLISHES.
//
// The security projection a column policy puts over a scan (#859, ADR-0033
// decision 1) IS the relation's published column list for that identity: it
// drops the DENIED columns and replaces the MASKED ones. A star is a consumer
// above that projection like any other, so it expands from the projection's
// list and never from the scan's catalog-annotated `ScanColumns` underneath it.
//
// Two gates, because the rule has two halves that fail differently. The first
// asserts the ANSWER for every star spelling. The second asserts there is only
// one PLACE the answer can come from — a new expansion site that reads the
// catalog list directly would pass the first gate for every shape nobody
// happened to write a cell for, which is how `SELECT a.*` shipped leaking in
// v0.18.61 while the bare-star cell beside it stayed green.
// ---------------------------------------------------------------------------

// starGateScan is a policed base table: six declared columns, of which the
// identity may see five (salary DENIED, ssn MASKED).
func starGateScan() *Node {
	return &Node{
		Type:        NodeScan,
		TableName:   "emp",
		TableAlias:  "a",
		ScanColumns: []string{"id", "dept", "ssn", "acct", "salary", "amt"},
	}
}

// starGateBarrier is the security projection InjectColumnPolicies builds over
// that scan: salary gone, ssn replaced by its mask, everything else itself.
func starGateBarrier(scan *Node) *Node {
	return &Node{
		Type:            NodeProject,
		Children:        []*Node{scan},
		SecurityBarrier: true,
		Projections: []Projection{
			{Column: "id", Alias: "id"},
			{Column: "dept", Alias: "dept"},
			{Alias: "ssn", Expr: "'***'"},
			{Column: "acct", Alias: "acct"},
			{Column: "amt", Alias: "amt"},
		},
	}
}

func starProj(expr string) Projection {
	return Projection{Column: expr, Expr: expr}
}

func projAliases(n *Node) []string {
	out := make([]string, 0, len(n.Projections))
	for _, p := range n.Projections {
		name := p.Alias
		if name == "" {
			name = p.Column
		}
		out = append(out, name)
	}
	return out
}

func TestEveryStarSpellingExpandsFromThePolicedList(t *testing.T) {
	policed := "id,dept,ssn,acct,amt"
	catalog := "id,dept,ssn,acct,salary,amt"

	cases := []struct {
		name string
		// plan builds the Project whose star is under test; the second return
		// is the node whose projections are read back (the same node, except
		// where a block above it is the one that expands).
		plan func() (root, read *Node)
		want string
	}{
		{
			name: "a qualified star alone",
			plan: func() (*Node, *Node) {
				n := &Node{Type: NodeProject, Projections: []Projection{starProj("a.*")}}
				n.Children = []*Node{starGateBarrier(starGateScan())}
				return n, n
			},
			want: policed,
		},
		{
			name: "a qualified star beside an item",
			plan: func() (*Node, *Node) {
				n := &Node{Type: NodeProject, Projections: []Projection{
					starProj("a.*"), {Column: "a.id", Alias: "z", Expr: "a.id"},
				}}
				n.Children = []*Node{starGateBarrier(starGateScan())}
				return n, n
			},
			want: policed + ",z",
		},
		{
			name: "a bare star beside an item",
			plan: func() (*Node, *Node) {
				n := &Node{Type: NodeProject, Projections: []Projection{
					starProj("*"), {Column: "id", Alias: "z", Expr: "id"},
				}}
				n.Children = []*Node{starGateBarrier(starGateScan())}
				return n, n
			},
			want: policed + ",z",
		},
		{
			name: "a star over a derived table whose own list is a star",
			plan: func() (*Node, *Node) {
				inner := &Node{
					Type:         NodeProject,
					DerivedAlias: "d",
					Projections:  []Projection{starProj("a.*")},
					Children:     []*Node{starGateBarrier(starGateScan())},
				}
				outer := &Node{Type: NodeProject,
					Projections: []Projection{starProj("d.*")},
					Children:    []*Node{inner}}
				return outer, outer
			},
			want: policed,
		},
		{
			name: "a star over a CTE whose own list is a star",
			plan: func() (*Node, *Node) {
				inner := &Node{
					Type:        NodeProject,
					CTEName:     "c",
					Projections: []Projection{starProj("a.*")},
					Children:    []*Node{starGateBarrier(starGateScan())},
				}
				outer := &Node{Type: NodeProject,
					Projections: []Projection{starProj("c.*")},
					Children:    []*Node{inner}}
				return outer, outer
			},
			want: policed,
		},
		{
			name: "a star under a filter over the barrier",
			plan: func() (*Node, *Node) {
				f := &Node{Type: NodeFilter, Children: []*Node{starGateBarrier(starGateScan())}}
				n := &Node{Type: NodeProject,
					Projections: []Projection{starProj("a.*")}, Children: []*Node{f}}
				return n, n
			},
			want: policed,
		},
		{
			name: "a star over a join names ONE relation",
			plan: func() (*Node, *Node) {
				other := &Node{Type: NodeScan, TableName: "other", TableAlias: "b",
					ScanColumns: []string{"id", "note"}}
				j := &Node{Type: NodeJoin,
					Children: []*Node{starGateBarrier(starGateScan()), other}}
				n := &Node{Type: NodeProject,
					Projections: []Projection{starProj("a.*")}, Children: []*Node{j}}
				return n, n
			},
			want: policed,
		},
		{
			// The control from the other side: with no policy there is no
			// barrier, and the star reads the catalog list exactly as it
			// always has. A gate that only ever saw the policed arm could not
			// tell this fix from one that narrowed every star.
			name: "no policy, no barrier: the catalog list",
			plan: func() (*Node, *Node) {
				n := &Node{Type: NodeProject, Projections: []Projection{starProj("a.*")}}
				n.Children = []*Node{starGateScan()}
				return n, n
			},
			want: catalog,
		},
		{
			// A barrier whose own list cannot be read answers nil, so the
			// star stays unexpanded and the planner refuses it one pass later.
			// A security control never degrades to a grant.
			name: "an unreadable barrier refuses rather than falling back",
			plan: func() (*Node, *Node) {
				b := starGateBarrier(starGateScan())
				b.Projections = []Projection{{Expr: "1 + 1"}}
				n := &Node{Type: NodeProject,
					Projections: []Projection{starProj("a.*")}, Children: []*Node{b}}
				return n, n
			},
			want: "a.*",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, read := tc.plan()
			ExpandStarProjections(root)
			got := strings.Join(projAliases(read), ",")
			if got != tc.want {
				t.Fatalf("expanded to %q, want %q", got, tc.want)
			}
			if tc.want != catalog && strings.Contains(got, "salary") {
				t.Fatalf("the DENIED column reached the output list: %q", got)
			}
		})
	}
}

// TestOnlyOnePathReadsAScanColumnListForAStar is the structural half.
//
// `logical.StarSourceColumns` is the one function a star's column list comes
// from, and `publishedScanColumns` inside it is the one place a scan's own
// `ScanColumns` may be read for that purpose — because that is where the
// security projection over the scan is consulted first. Any OTHER function in
// the planner that both reads a `ScanColumns` field and talks about stars is a
// second answer to the same question, and a second answer is how the policed
// list gets bypassed by a spelling nobody wrote a cell for.
//
// The allow-list is per FUNCTION, not per file, and each entry states why that
// function is not an expansion site. Adding one is a decision, which is the
// point of the gate.
//
// WHAT IT CANNOT CATCH, so the next reader does not over-trust it (round-1
// review, N1/N2). Arm (c) needs a `.ScanColumns` read AND a star token in the
// SAME function, and arm (a) is scoped to `star_expansion.go`: a helper in
// another file that reads `.ScanColumns` and names no star token passes both —
// measured, by moving `subtreeOutputNames`' scan arm into a bare
// `func(n *Node) []string { return n.ScanColumns }`. Arm (b) pins call sites
// per FILE, so a SECOND call inside an already-pinned file does not fire it.
// `TestEveryStarSpellingExpandsFromThePolicedList` and the door matrix are the
// backstop for both; this gate narrows where a second answer can hide, it does
// not close the set.
func TestOnlyOnePathReadsAScanColumnListForAStar(t *testing.T) {
	allowed := map[string]string{
		// Not an expansion. It DECLARES the output schema of a bare `SELECT *`
		// that produced no Project at all, and its descent stops at the first
		// Project — which a security projection is — so a policed scan is never
		// reached through it; the declaration for a policed table comes from the
		// barrier through the ordinary projection walk.
		// TestABareStarOverAPolicedScanDeclinesToDeclareFromTheCatalog
		// (physical) asserts that, so this is a claim and not an exemption.
		"physical.starOnlyDeclaredOutputSchema": "declares a schema; declines at any Project",
		// Three "what does this subtree publish" helpers, not expansion sites.
		// Each answers from a Project BEFORE it ever reaches a Scan, and a
		// security projection IS a Project — so their Scan arm is reached only
		// where no projection stands over the scan, which is where no policy
		// applies. TestTheSubtreeOutputHelpersReadTheBarrier (here) and
		// TestTheCTEOutputHelperReadsTheBarrier (physical) assert exactly that,
		// so these are claims and not exemptions.
		"logical.projectOutputNamesBelow": "publishes a Project's names first; Scan arm is unpoliced",
		"logical.subtreeOutputNames":      "publishes a Project's names first; Scan arm is unpoliced",
		"physical.cteOutputNames":         "publishes a Project's names first; Scan arm is unpoliced",
	}
	// Tokens that mean "this function is about star expansion".
	starTokens := map[string]bool{
		"isStarProjection": true, "starQualifier": true, "HasStarProjection": true,
		"StarSourceColumns": true, "ExpandStarProjections": true,
		"relationOutputColumns": true, "Star": true, "starOnlySourceScan": true,
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	logicalDir := filepath.Dir(thisFile)
	dirs := map[string]string{
		"logical":  logicalDir,
		"physical": filepath.Join(logicalDir, "..", "physical"),
		"sql":      filepath.Join(logicalDir, "..", "sql"),
	}

	// (a) Inside star_expansion.go — the file that OWNS star expansion — the
	// only function allowed to read a scan's own column list is
	// publishedScanColumns, which consults the security projection first. This
	// is the assertion v0.18.61 would have failed: the list came straight off
	// the scan, inside the expansion.
	const sanctioned = "publishedScanColumns"
	expansionFile := filepath.Join(logicalDir, "star_expansion.go")
	readers := starGateFuncsReadingScanColumns(t, expansionFile)
	if strings.Join(readers, ",") != sanctioned {
		t.Errorf("star_expansion.go reads a scan's ScanColumns in %v; a star's list comes from "+
			"%s alone, which asks the security projection first", readers, sanctioned)
	}

	// (b) ExpandStarProjections is the ONE expansion entry, and its call sites
	// are pinned. A new one is a new place this rule has to hold, so it is a
	// decision and not a diff nobody read.
	wantCallers := []string{
		"logical/optimizer.go",      // the statement's own plan, before column pruning
		"logical/star_expansion.go", // its own recursion
		"physical/plan.go",          // PlanDistributed, and the unoptimized-plan path
	}
	gotCallers := starGateCallersOf(t, dirs, "ExpandStarProjections")
	if strings.Join(gotCallers, " ") != strings.Join(wantCallers, " ") {
		t.Errorf("ExpandStarProjections is called from %v, pinned %v — a new expansion site reads "+
			"the policed list through StarSourceColumns, then is added here",
			gotCallers, wantCallers)
	}

	var offenders []string
	seen := map[string]bool{}
	for pkg, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading %s: %v", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") ||
				strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, e.Name())
			fset := token.NewFileSet()
			// No ParseComments: a comment that merely NAMES a star must not
			// make its function look like an expansion site.
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parsing %s: %v", path, err)
			}
			for _, decl := range f.Decls {
				fn, isFn := decl.(*ast.FuncDecl)
				if !isFn || fn.Body == nil {
					continue
				}
				readsScanColumns, mentionsStar := false, false
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch t := n.(type) {
					case *ast.SelectorExpr:
						if t.Sel.Name == "ScanColumns" {
							readsScanColumns = true
						}
						if starTokens[t.Sel.Name] {
							mentionsStar = true
						}
					case *ast.Ident:
						if starTokens[t.Name] {
							mentionsStar = true
						}
					case *ast.BasicLit:
						if t.Kind == token.STRING &&
							(t.Value == `"*"` || strings.HasSuffix(t.Value, `.*"`)) {
							mentionsStar = true
						}
					}
					return true
				})
				if !readsScanColumns || !mentionsStar {
					continue
				}
				key := pkg + "." + fn.Name.Name
				seen[key] = true
				if _, okAllowed := allowed[key]; !okAllowed {
					offenders = append(offenders,
						key+" ("+filepath.Base(path)+")")
				}
			}
		}
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Fatalf("these functions read a scan's catalog column list for a STAR without going "+
			"through logical.StarSourceColumns / publishedScanColumns, so they can publish a "+
			"column an identity's policy DENIES:\n  %s\n"+
			"Route the list through StarSourceColumns, or add the function to this gate's "+
			"allow-list with the reason it is not an expansion site.",
			strings.Join(offenders, "\n  "))
	}
	// The allow-list must not outlive what it allows: an entry for a function
	// that no longer reads a column list for a star is a stale exemption, and
	// a stale exemption is how the next one gets waved through.
	for key := range allowed {
		if !seen[key] {
			t.Fatalf("allow-list entry %q no longer matches any function that reads a scan's "+
				"column list for a star — delete it", key)
		}
	}
}

// TestTheSubtreeOutputHelpersReadTheBarrier backs two of this gate's
// allow-list entries. Both helpers answer "what does this subtree publish",
// and the answer for a policed scan must be the SECURITY PROJECTION's list —
// which it is, structurally, because each returns at the first Project and a
// security projection is one. Asserted rather than asserted-in-a-comment: the
// `SELECT * FROM t ORDER BY 1` position count is exactly this question, and
// counting the catalog's columns there would number a denied column.
func TestTheSubtreeOutputHelpersReadTheBarrier(t *testing.T) {
	policed := "id,dept,ssn,acct,amt"
	barrier := starGateBarrier(starGateScan())

	if got := strings.Join(projectOutputNamesBelow(barrier), ","); got != policed {
		t.Errorf("projectOutputNamesBelow = %q, want the policed list %q", got, policed)
	}
	if got := strings.Join(subtreeOutputNames(barrier), ","); got != policed {
		t.Errorf("subtreeOutputNames = %q, want the policed list %q", got, policed)
	}
	// Through a Filter — the shape a policy row filter makes — and through a
	// Sort, which is what a positional ORDER BY puts above it.
	f := &Node{Type: NodeFilter, Children: []*Node{starGateBarrier(starGateScan())}}
	if got := strings.Join(projectOutputNamesBelow(f), ","); got != policed {
		t.Errorf("projectOutputNamesBelow through a Filter = %q, want %q", got, policed)
	}
	if got := strings.Join(subtreeOutputNames(f), ","); got != policed {
		t.Errorf("subtreeOutputNames through a Filter = %q, want %q", got, policed)
	}
	// The control: no barrier, the scan's own list, unchanged.
	catalog := "id,dept,ssn,acct,salary,amt"
	if got := strings.Join(projectOutputNamesBelow(starGateScan()), ","); got != catalog {
		t.Errorf("projectOutputNamesBelow over a bare scan = %q, want %q", got, catalog)
	}
}

// starGateFuncsReadingScanColumns names every function in one file whose body
// reads a `.ScanColumns` field, sorted.
func starGateFuncsReadingScanColumns(t *testing.T, path string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	var out []string
	for _, decl := range f.Decls {
		fn, isFn := decl.(*ast.FuncDecl)
		if !isFn || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if sel, isSel := n.(*ast.SelectorExpr); isSel && sel.Sel.Name == "ScanColumns" {
				out = append(out, fn.Name.Name)
				return false
			}
			return true
		})
	}
	sort.Strings(out)
	return out
}

// starGateCallersOf names the `<pkg>/<file>` of every non-test call site of a
// function, sorted and deduplicated.
func starGateCallersOf(t *testing.T, dirs map[string]string, name string) []string {
	t.Helper()
	files := map[string]bool{}
	for pkg, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading %s: %v", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") ||
				strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, e.Name())
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parsing %s: %v", path, err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if fun.Name == name {
						files[pkg+"/"+e.Name()] = true
					}
				case *ast.SelectorExpr:
					if fun.Sel.Name == name {
						files[pkg+"/"+e.Name()] = true
					}
				}
				return true
			})
		}
	}
	out := make([]string, 0, len(files))
	for f := range files {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}
