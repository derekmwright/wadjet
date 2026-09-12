package expr

import (
	"strconv"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// THE PUBLISHED RANGE TABLE IS THE ORACLE (node-semver README, "Advanced Range
// Syntax": Hyphen Ranges, X-Ranges, Tilde Ranges, Caret Ranges).
//
// Each row is the grammar's own documented EXPANSION, transcribed. The gate
// asserts it twice, because the two assertions fail for different reasons:
//
//   - the desugaring RENDERS to the published text, which catches a bound
//     that is off by one or has lost its `-0`; and
//   - the range and its published expansion admit the SAME versions over a
//     corpus, which catches a desugaring that renders right and tests wrong,
//     and which is the property a user actually depends on.
//
// The `-0` on every exclusive upper bound is the row most easily lost:
// `<2.0.0-0` and `<2.0.0` are different sets, because `2.0.0-beta` is below
// `2.0.0` and above `2.0.0-0`.
func TestTheRangeGrammarMatchesTheNodeSemverTable(t *testing.T) {
	rows := []struct {
		rng, expansion string
		// sameAs is the expansion to compare MEMBERSHIP against when it is
		// not the rendered text — only `*`, whose published expansion
		// `>=0.0.0` is the ANY comparator here for the same reason it is one
		// in node: the trivial lower bound is deleted from every set
		// (`replaceGTE0`), so both spellings parse to the same thing.
		sameAs string
		// renders is what the desugaring RENDERS when node's own parser
		// renders something other than the README's published expansion —
		// which happens for exactly one reason, and it is the same deletion:
		// `new Range('^0.x').range` is `<1.0.0-0` on node-semver 7.7.3
		// (measured), not the README's `>=0.0.0 <1.0.0-0`. The published text
		// stays the MEMBERSHIP oracle and parses to the same set here,
		// because the rule applies to it too.
		renders string
	}{
		{rng: "1.2.3 - 2.3.4", expansion: ">=1.2.3 <=2.3.4"},
		{rng: "1.2 - 2.3.4", expansion: ">=1.2.0 <=2.3.4"},
		{rng: "1.2.3 - 2.3", expansion: ">=1.2.3 <2.4.0-0"},
		{rng: "1.2.3 - 2", expansion: ">=1.2.3 <3.0.0-0"},
		{rng: "*", expansion: "*", sameAs: ">=0.0.0"},
		{rng: "x", expansion: "*", sameAs: ">=0.0.0"},
		{rng: "1.x", expansion: ">=1.0.0 <2.0.0-0"},
		{rng: "1.X", expansion: ">=1.0.0 <2.0.0-0"},
		{rng: "1.*", expansion: ">=1.0.0 <2.0.0-0"},
		{rng: "1", expansion: ">=1.0.0 <2.0.0-0"},
		{rng: "1.2.x", expansion: ">=1.2.0 <1.3.0-0"},
		{rng: "1.2.*", expansion: ">=1.2.0 <1.3.0-0"},
		{rng: "1.2", expansion: ">=1.2.0 <1.3.0-0"},
		{rng: "~1.2.3", expansion: ">=1.2.3 <1.3.0-0"},
		{rng: "~1.2", expansion: ">=1.2.0 <1.3.0-0"},
		{rng: "~1", expansion: ">=1.0.0 <2.0.0-0"},
		{rng: "~0.2.3", expansion: ">=0.2.3 <0.3.0-0"},
		{rng: "~0.2", expansion: ">=0.2.0 <0.3.0-0"},
		{rng: "~0", expansion: ">=0.0.0 <1.0.0-0", renders: "<1.0.0-0"},
		{rng: "~1.2.3-beta.2", expansion: ">=1.2.3-beta.2 <1.3.0-0"},
		{rng: "^1.2.3", expansion: ">=1.2.3 <2.0.0-0"},
		{rng: "^0.2.3", expansion: ">=0.2.3 <0.3.0-0"},
		{rng: "^0.0.3", expansion: ">=0.0.3 <0.0.4-0"},
		{rng: "^1.2.3-beta.2", expansion: ">=1.2.3-beta.2 <2.0.0-0"},
		{rng: "^0.0.3-beta", expansion: ">=0.0.3-beta <0.0.4-0"},
		{rng: "^1.2.x", expansion: ">=1.2.0 <2.0.0-0"},
		{rng: "^0.0.x", expansion: ">=0.0.0 <0.1.0-0", renders: "<0.1.0-0"},
		{rng: "^0.0", expansion: ">=0.0.0 <0.1.0-0", renders: "<0.1.0-0"},
		{rng: "^1.x", expansion: ">=1.0.0 <2.0.0-0"},
		{rng: "^0.x", expansion: ">=0.0.0 <1.0.0-0", renders: "<1.0.0-0"},
		// The exact and the explicit comparators, which the same code path
		// has to leave alone.
		{rng: "1.2.3", expansion: "1.2.3"},
		{rng: ">=1.2.3", expansion: ">=1.2.3"},
		{rng: ">1.2.3", expansion: ">1.2.3"},
		{rng: "<=1.2.3", expansion: "<=1.2.3"},
		{rng: "<1.2.3", expansion: "<1.2.3"},
		{rng: "=1.2.3", expansion: "1.2.3"},
		// Intersection and union.
		{rng: ">=1.2.0 <2.0.0", expansion: ">=1.2.0 <2.0.0"},
		{rng: "^1.0.0 || ^2.0.0", expansion: ">=1.0.0 <2.0.0-0 || >=2.0.0 <3.0.0-0"},
		// Spellings node accepts that must reach the same expansion.
		{rng: ">= 1.2.3", expansion: ">=1.2.3"},
		{rng: "~>1.2.3", expansion: ">=1.2.3 <1.3.0-0"},
		{rng: "v1.2.3", expansion: "1.2.3"},
		{rng: "^v1.2.3", expansion: ">=1.2.3 <2.0.0-0"},
	}
	corpus := semverRangeCorpus()
	for _, row := range rows {
		r, err := ParseSemverRange("semver_satisfies", row.rng)
		if err != nil {
			t.Errorf("%q: %v", row.rng, err)
			continue
		}
		wantRender := row.renders
		if wantRender == "" {
			wantRender = row.expansion
		}
		if got := r.String(); got != wantRender {
			t.Errorf("%q desugars to %q, want %q (the published expansion is %q)",
				row.rng, got, wantRender, row.expansion)
		}
		want := row.sameAs
		if want == "" {
			want = row.expansion
		}
		ref, err := ParseSemverRange("semver_satisfies", want)
		if err != nil {
			t.Fatalf("the published expansion %q does not parse: %v", want, err)
		}
		for _, s := range corpus {
			v, ok := parseSemver(s)
			if !ok {
				t.Fatalf("the corpus holds %q, which is not a version", s)
			}
			if a, b := semverSatisfies(v, r), semverSatisfies(v, ref); a != b {
				t.Errorf("%s: %q says %v and its published expansion %q says %v",
					s, row.rng, a, want, b)
			}
		}
	}
}

// semverRangeCorpus is the membership corpus: the boundary of every published
// expansion, from both sides, with the pre-releases that make the `-0` bounds
// and the pre-release rule visible.
func semverRangeCorpus() []string {
	return []string{
		"0.0.0", "0.0.1", "0.0.2", "0.0.3", "0.0.4", "0.1.0", "0.1.1",
		"0.2.0", "0.2.2", "0.2.3", "0.2.4", "0.3.0", "0.9.9",
		"1.0.0", "1.0.1", "1.1.0", "1.2.0", "1.2.2", "1.2.3", "1.2.4",
		"1.2.10", "1.3.0", "1.9.9", "1.10.0",
		"2.0.0", "2.3.3", "2.3.4", "2.3.5", "2.4.0", "2.9.9",
		"3.0.0", "3.4.5", "10.0.0",
		"0.0.3-beta", "0.0.3-alpha", "0.0.4-0", "0.0.4-beta",
		"1.2.3-0", "1.2.3-alpha", "1.2.3-alpha.3", "1.2.3-alpha.7",
		"1.2.3-beta", "1.2.3-beta.2", "1.2.3-beta.11", "1.2.3-rc.1",
		"1.2.4-beta", "1.3.0-0", "1.3.0-beta",
		"2.0.0-0", "2.0.0-alpha", "2.0.0-rc.1", "2.4.0-0",
		"3.0.0-0", "3.4.5-alpha.9", "1.0.0-beta",
	}
}

// THE PRE-RELEASE RULE, in the README's own words and with its own examples:
// "if a version has a prerelease tag then it will only be allowed to satisfy
// comparator sets if at least one comparator with the same [major, minor,
// patch] tuple also has a prerelease tag."
//
// The rows are the ones the README spells out, plus the two that the rule is
// most often got wrong on: `*` admits no pre-release at all, and a
// pre-release of the NEXT major is kept out of a caret range by the `-0`.
func TestTheNodeSemverPrereleaseRule(t *testing.T) {
	for _, tc := range []struct {
		version, rng string
		want         bool
	}{
		// The README's own three.
		{"1.2.3-alpha.7", ">1.2.3-alpha.3", true},
		{"3.4.5-alpha.9", ">1.2.3-alpha.3", false},
		{"3.4.5", ">1.2.3-alpha.3", true},
		// A pre-release never slips into a range that named none.
		{"1.2.3-beta", "^1.2.3", false},
		{"1.2.4-beta", "^1.2.3", false},
		{"2.0.0-rc.1", "^1.2.3", false},
		{"1.0.0-beta", "*", false},
		{"1.0.0-beta", ">=0.0.0", false},
		{"1.3.0-beta", "~1.2.3", false},
		// A range that DOES name one admits the pre-releases of that tuple.
		{"1.2.3-beta.2", "^1.2.3-beta.2", true},
		{"1.2.3-beta.11", "^1.2.3-beta.2", true},
		{"1.2.3-alpha", "^1.2.3-beta.2", false},
		{"1.2.3", "^1.2.3-beta.2", true},
		{"0.0.3-beta", "^0.0.3-beta", true},
		{"0.0.4-0", "^0.0.3-beta", false},
		// And a release is unaffected by the rule.
		{"1.2.3", "^1.2.3", true},
		{"1.9.9", "^1.2.3", true},
		{"2.0.0", "^1.2.3", false},
	} {
		r, err := ParseSemverRange("semver_satisfies", tc.rng)
		if err != nil {
			t.Fatalf("%q: %v", tc.rng, err)
		}
		v, ok := parseSemver(tc.version)
		if !ok {
			t.Fatalf("%q is not a version", tc.version)
		}
		if got := semverSatisfies(v, r); got != tc.want {
			t.Errorf("semver_satisfies(%q, %q) = %v, want %v", tc.version, tc.rng, got, tc.want)
		}
	}
}

// A RANGE THIS GRAMMAR DOES NOT KNOW IS 22023 NAMING IT, NEVER A SILENT FALSE.
//
// A range is the query author's own text, so a spelling nobody implements is a
// property of the QUERY. Answering FALSE for it drops every row the author
// meant to select and looks exactly like an empty table.
func TestARangeOffTheGrammarIsRefused(t *testing.T) {
	for _, rng := range []string{
		"", "   ", "\t",
		">=", "<", "=", "^", "~",
		"^^1.0.0", "~~1.0.0", ">>1.0.0",
		"1.2.3 -", "- 1.2.3", "1.2.3 - 2.3.4 - 3.0.0",
		"1.2.x-beta", "1.x+build", "^1.2.x-beta",
		"01.2.3", "^01.2.3", "1.2.3.4", ">=1.2.3.4",
		"99999999999999999999.0.0", "^99999999999999999999.0.0",
		"latest", "stable", "1.2.3 || ", " || 1.2.3", "||",
		"1.0.0 2.0.0 garbage", "1,2,3",
	} {
		r, err := ParseSemverRange("semver_satisfies", rng)
		if err == nil {
			t.Errorf("%q is not a range this grammar knows and parsed as %s", rng, r)
			continue
		}
		if code := sqlerr.StateOf(err); code != "22023" {
			t.Errorf("%q: SQLSTATE %q, want 22023 (%v)", rng, code, err)
		}
		if !strings.Contains(err.Error(), "semver_satisfies") {
			t.Errorf("%q: the refusal does not name the function: %v", rng, err)
		}
	}
	// And the refusal comes through the FUNCTION too, as a fatalEval, with
	// the range quoted — including when the version argument is NULL, which
	// is the ordering A2's flag family settled: the operand the QUERY wrote
	// is read first.
	for _, args := range [][]any{
		{"1.2.3", "^^1.0.0"},
		{nil, "^^1.0.0"},
		{"not-a-version", "^^1.0.0"},
	} {
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					if fe, ok := r.(fatalEval); ok {
						err = fe.err
					} else {
						t.Fatalf("semver_satisfies panicked with %T", r)
					}
				}
			}()
			fnSemverSatisfies(args)
			return nil
		}()
		if err == nil {
			t.Errorf("semver_satisfies%v did not refuse the range", args)
			continue
		}
		if code := sqlerr.StateOf(err); code != "22023" {
			t.Errorf("semver_satisfies%v: SQLSTATE %q, want 22023", args, code)
		}
		if !strings.Contains(err.Error(), "^^1.0.0") {
			t.Errorf("semver_satisfies%v: the refusal does not quote the range: %v", args, err)
		}
	}
}

// THE ACCEPTED SPELLINGS, so the refusal list above cannot quietly grow to
// swallow a range people really write.
func TestTheRangeSpellingsThisGrammarAccepts(t *testing.T) {
	for _, tc := range []struct {
		rng, version string
		want         bool
	}{
		{"*", "1.2.3", true},
		{">=1.2.0 <2.0.0", "1.9.9", true},
		{">=1.2.0 <2.0.0", "2.0.0", false},
		{">= 1.2.0 < 2.0.0", "1.9.9", true},
		{"^1.0.0 || ^2.0.0", "2.5.0", true},
		{"^1.0.0 || ^2.0.0", "3.0.0", false},
		{"1.2.3 - 2.3.4", "2.3.4", true},
		{"1.2.3 - 2.3.4", "2.3.5", false},
		{"~>1.2.3", "1.2.9", true},
		{"~>1.2.3", "1.3.0", false},
		{"v1.2.3", "1.2.3", true},
		{"^v1.2.3", "1.5.0", true},
		{">=1.2.3+build.1", "1.2.3", true},
		{"<0.0.0-0", "0.0.0", false},
		{">*", "1.2.3", false},
		{"<*", "1.2.3", false},
		{"1.2.3||>=2.0.0", "2.0.1", true},
	} {
		got := fnSemverSatisfies([]any{tc.version, tc.rng})
		if got != tc.want {
			t.Errorf("semver_satisfies(%q, %q) = %v, want %v", tc.version, tc.rng, got, tc.want)
		}
	}
}

// THE RANGE MEMO REMEMBERS THE REFUSAL, NOT ONLY THE PARSE.
//
// A memo that cached successes alone would make a malformed range raise on the
// first row and answer from a miss on every row after — or, worse, raise only
// until some other query warmed a neighbouring key. The cached value carries
// the error, so the second call raises the same sentence as the first.
func TestTheRangeMemoIsBoundedAndRemembersRefusals(t *testing.T) {
	semverRangeCache.reset()
	t.Cleanup(semverRangeCache.reset)

	const bad = "^^9.9.9"
	first, err1 := parseSemverRangeCached("semver_satisfies", bad)
	if err1 == nil {
		t.Fatalf("%q parsed as %s", bad, first)
	}
	for i := 0; i < 5; i++ {
		_, err := parseSemverRangeCached("semver_satisfies", bad)
		if err == nil || err.Error() != err1.Error() {
			t.Fatalf("call %d answered %v, want the first call's %v", i, err, err1)
		}
	}
	good, err := parseSemverRangeCached("semver_satisfies", "^1.2.3")
	if err != nil {
		t.Fatalf("^1.2.3: %v", err)
	}
	for i := 0; i < 5; i++ {
		again, err := parseSemverRangeCached("semver_satisfies", "^1.2.3")
		if err != nil || again.String() != good.String() {
			t.Fatalf("call %d answered (%v, %v)", i, again, err)
		}
	}

	// Bounded: a range that comes from a COLUMN rather than a literal can
	// hold as many distinct values as the table has rows, and the memo must
	// not grow with them.
	semverRangeCache.reset()
	for i := 0; i < semverRangeMemoCap*3; i++ {
		_, _ = parseSemverRangeCached("semver_satisfies", ">="+strconv.Itoa(i)+".0.0")
	}
	if n := semverRangeCache.entries(); n > semverRangeMemoCap {
		t.Errorf("the memo holds %d entries, past its %d cap", n, semverRangeMemoCap)
	}
}

// THE COMPILE-TIME BACKSTOP, which is the layer that answers for the doors the
// binder does not walk — ADR-0031's DML predicate above all, which is not
// planned at all and reaches the engine as a compiled expression.
//
// It is deliberately the SAME function the binder calls, so the two layers
// cannot come to different conclusions about which ranges exist.
func TestCompilingACallWithAConstantRangeRefusesBeforeAnyRow(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		want      string
	}{
		{"a range that names no range", `semver_satisfies(v, '^^1.0')`, "22023"},
		{"an empty range", `semver_satisfies(v, '')`, "22023"},
		{"a pre-release on a partial", `semver_satisfies(v, '1.2.x-beta')`, "22023"},
		{"a parenthesised bad range", `semver_satisfies(v, ('~~1.0'))`, "22023"},
		{"a valid range compiles", `semver_satisfies(v, '^1.2.3')`, ""},
		{"a column range compiles", `semver_satisfies(v, r)`, ""},
		{"a NULL range compiles", `semver_satisfies(v, NULL)`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, err := plansql.ParseExpression(tc.sql)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.sql, err)
			}
			_, err = Compile(node)
			if got := sqlerr.StateOf(err); got != tc.want {
				t.Fatalf("compiling %q answered %v (%q), want SQLSTATE %q",
					tc.sql, err, got, tc.want)
			}
			if tc.want == "" && err != nil {
				t.Fatalf("compiling %q failed: %v", tc.sql, err)
			}
		})
	}
}
