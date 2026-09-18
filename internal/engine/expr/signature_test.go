// SPDX-License-Identifier: MIT

package expr

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// #1053: the registry records an ARITY for every function it holds, and the
// table is closed in BOTH directions.
//
// A one-directional check is the shape that lets a table rot: a new function
// registered with no row here would simply never be arity-checked, and a row
// for a function that no longer exists would look like coverage it is not. The
// same rule holds textDomains against funcSignatures — that one is enforced in
// signature.go's init, because a domain for an unregistered function is a
// programming mistake rather than a test failure.
func TestEveryRegisteredFunctionDeclaresItsArity(t *testing.T) {
	registered := map[string]bool{}
	for _, n := range DefaultRegistry.Names() {
		// A UDF SHADOWS into the same registry (udf.go's Register), and its
		// arity is its own CREATE FUNCTION parameter list, checked there. It
		// is excluded here for the same reason SignatureOf excludes it — and
		// because a UDF another test in this package created is still in the
		// registry when this one runs.
		if DefaultRegistry.IsUDF(n) {
			continue
		}
		registered[n] = true
	}
	if len(registered) < 300 {
		t.Fatalf("the registry holds %d functions; this gate is vacuous below ~300", len(registered))
	}
	var missing, orphan []string
	for n := range registered {
		if _, ok := funcSignatures[n]; !ok {
			missing = append(missing, n)
		}
	}
	// The embedding family is registered by internal/embedding when a server
	// wires an embedding provider, not by this package's own tables, so it is
	// absent from the registry HERE and present in the signature table — which
	// is what gives it an arity wherever it IS registered. Named explicitly
	// rather than silently tolerated.
	registeredElsewhere := map[string]bool{"embed": true, "embed_model": true, "embed_dim": true}
	for _, n := range SignatureNames() {
		if !registered[n] && !registeredElsewhere[n] {
			orphan = append(orphan, n)
		}
	}
	sort.Strings(missing)
	sort.Strings(orphan)
	if len(missing) > 0 {
		t.Errorf("%d registered functions declare no arity, so a call with the wrong "+
			"number of arguments answers NULL instead of 42883 (#1053): %s",
			len(missing), strings.Join(missing, " "))
	}
	if len(orphan) > 0 {
		t.Errorf("%d signatures name nothing registered, which is coverage this table "+
			"does not have: %s", len(orphan), strings.Join(orphan, " "))
	}
	// A signature that cannot be satisfied would refuse every call.
	for _, n := range SignatureNames() {
		s, _ := SignatureOf(n)
		if s.Min < 0 || (s.Max != Variadic && s.Max < s.Min) {
			t.Errorf("%s declares %s, which no call can satisfy", n, s)
		}
	}
}

// TestTheDocumentedSignatureIsTheDeclaredArity holds docs/sql-reference.md and
// funcSignatures in step. The documented spelling IS the arity — `UPPER(s)`,
// `LPAD(s, n [, pad])`, `CONCAT(a, b, ...)` — so a change to one that is not a
// change to the other is drift, and drift in this direction means the reference
// tells a user to write a call the engine now refuses.
//
// It reads the two files by NAME rather than walking the tree: a git worktree
// lives inside the module root and a walk sees every file a second time.
func TestTheDocumentedSignatureIsTheDeclaredArity(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(root, "docs", "sql-reference.md"))
	if err != nil {
		t.Fatalf("read sql-reference: %v", err)
	}
	// Only the first cell of a table row, and only the backticked spellings in
	// it: a description may name a function it is not documenting.
	row := regexp.MustCompile("(?m)^\\| ([^|]*) \\|")
	spell := regexp.MustCompile("`([A-Za-z_][A-Za-z0-9_]*)\\s*\\(([^`]*)\\)`")
	seen := map[string]bool{}
	checked := 0
	for _, m := range row.FindAllStringSubmatch(string(body), -1) {
		for _, c := range spell.FindAllStringSubmatch(m[1], -1) {
			name := strings.ToLower(c[1])
			sig, ok := SignatureOf(name)
			if !ok {
				continue // an aggregate, a window function, a statement form
			}
			lo, hi, ok := documentedArity(c[2])
			if !ok {
				continue
			}
			if strings.TrimSpace(c[2]) == "\u2026" {
				// `EXTRACT(\u2026)` in the operator-precedence table is a
				// PLACEHOLDER for an argument list, not one: the ellipsis says
				// "whatever this form takes".
				continue
			}
			if isSQLSpecialForm(c[2]) {
				// `EXTRACT(part FROM ts)` and `POSITION(sub IN s)` are SQL
				// special forms, not comma-separated argument lists: the
				// parser rewrites each into a two-argument call, and the
				// reference documents the SPELLING a user writes. There is no
				// arity to compare.
				continue
			}
			// A name may be documented in SEVERAL rows (ARRAY_LENGTH has a
			// row per form), so the union of its rows is what must match.
			if seen[name] {
				if lo >= sig.Min && (sig.Max == Variadic || hi <= sig.Max) {
					continue
				}
			}
			seen[name] = true
			checked++
			if lo < sig.Min || (sig.Max != Variadic && hi > sig.Max) {
				t.Errorf("%s: documented as taking %d..%d arguments, declared %s — "+
					"docs/sql-reference.md and expr.funcSignatures disagree, so the "+
					"reference describes a call the engine refuses (or the other way round)",
					name, lo, hi, sig)
			}
		}
	}
	if checked < 150 {
		t.Errorf("only %d documented signatures were compared; the parser has stopped "+
			"matching the reference's table rows and this gate is vacuous", checked)
	}
}

// documentedArity reads the argument list of a documented signature. The
// reference has exactly three conventions and they are already consistent
// across every table: a bare list is exact, `[, x]` is one optional argument,
// and `...` or `args...` is a variadic tail.
func documentedArity(args string) (lo, hi int, ok bool) {
	args = strings.TrimSpace(args)
	if args == "" {
		return 0, 0, true
	}
	depth := 0
	var parts []string
	cur := strings.Builder{}
	for _, r := range args {
		switch r {
		case '[', '(':
			depth++
		case ']', ')':
			depth--
		}
		if r == ',' && depth == 0 {
			parts = append(parts, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteRune(r)
	}
	parts = append(parts, cur.String())
	variadic := false
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.HasSuffix(p, "...") {
			variadic = true
			continue
		}
		if strings.HasPrefix(p, "[") {
			inner := strings.TrimSpace(strings.Trim(p, "[]"))
			if strings.HasSuffix(inner, "...") {
				variadic = true
				continue
			}
			hi++
			continue
		}
		lo++
		hi++
	}
	if variadic {
		return lo, 1 << 30, true
	}
	return lo, hi, true
}

// TestEveryFunctionRefusesNMinusOneAndNPlusOne is #1053's own ask: every
// registered function called with one argument too few and one too many is
// 42883, and a VARIADIC declaration is the explicit exception list rather than
// a silence.
//
// It calls the BINDER's refusal rather than evaluating anything, because that
// is where the decision is: the bodies still read args[i] defensively and this
// gate's whole point is that they are never reached.
func TestEveryFunctionRefusesNMinusOneAndNPlusOne(t *testing.T) {
	var variadic []string
	checked := 0
	for _, name := range SignatureNames() {
		sig, _ := SignatureOf(name)
		if sig.Max == Variadic {
			variadic = append(variadic, name)
		}
		for _, n := range []int{sig.Min - 1, maxArgsPlusOne(sig)} {
			if n < 0 {
				continue // a zero-arity function has no "one too few"
			}
			if sig.accepts(n) {
				continue
			}
			checked++
			err := RefuseUnresolvableCall(callWithArgs(name, n), nil)
			if err == nil {
				t.Errorf("%s(%d arguments) was not refused; its signature is %s and "+
					"PostgreSQL answers 42883 for a count no overload takes", name, n, sig)
				continue
			}
			if !IsWrongSignature(err) {
				t.Errorf("%s(%d arguments): %v, want a WrongSignatureError", name, n, err)
			}
			if got := sqlerrState(err); got != "42883" {
				t.Errorf("%s(%d arguments): SQLSTATE %q, want 42883", name, n, got)
			}
		}
	}
	if checked < 600 {
		t.Errorf("only %d wrong-arity calls were exercised over %d functions; this gate "+
			"is not reaching the table", checked, len(SignatureNames()))
	}
	sort.Strings(variadic)
	// THE EXPLICIT EXCEPTION LIST. A function that takes any number of
	// arguments cannot be refused for taking too many, so each one is named
	// here and a new one has to be added deliberately.
	want := []string{"coalesce", "concat", "concat_ws", "format", "greatest", "least",
		"tcp_flag_mask", "tcp_flags_has_all", "tcp_flags_has_any", "tcp_flags_has_none"}
	if strings.Join(variadic, " ") != strings.Join(want, " ") {
		t.Errorf("the variadic exception list is [%s], declared [%s] — a variadic "+
			"signature is an exemption from the n+1 half of this gate and must be "+
			"named on purpose", strings.Join(variadic, " "), strings.Join(want, " "))
	}
}

// isSQLSpecialForm reports whether a documented argument list is one of SQL's
// keyword-separated forms rather than a comma-separated one.
func isSQLSpecialForm(args string) bool {
	u := " " + strings.ToUpper(args) + " "
	return strings.Contains(u, " FROM ") || strings.Contains(u, " IN ") ||
		strings.Contains(u, " PLACING ") || strings.Contains(u, " FOR ")
}

func maxArgsPlusOne(s Signature) int {
	if s.Max == Variadic {
		return -1
	}
	return s.Max + 1
}

func callWithArgs(name string, n int) *plansql.FuncCallNode {
	args := make([]plansql.Node, 0, n)
	for i := 0; i < n; i++ {
		args = append(args, &plansql.Lit{Value: "x"})
	}
	return &plansql.FuncCallNode{Name: name, Args: args}
}

func sqlerrState(err error) string { return sqlerr.StateOf(err) }
