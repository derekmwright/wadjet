package expr

import (
	"os"
	"strings"
	"testing"
)

// node-semver 7.7.3 ITSELF IS THE ORACLE FOR THE TRIVIAL LOWER BOUND (#967,
// round-2 review B1).
//
// The published README is the oracle for the range GRAMMAR, and
// `TestTheRangeGrammarMatchesTheNodeSemverTable` transcribes it. It cannot
// answer this question, because what the README publishes as an expansion
// (`^0.x` → `>=0.0.0 <1.0.0-0`) is not what the library's own parser builds:
// `replaceGTE0` (`classes/range.js:139`) deletes a comparator whose text is
// exactly `>=0.0.0`, and the deletion changes an ANSWER — a pre-release of
// 0.0.0 sorts below 0.0.0, so the comparator the README shows is the one that
// was dropping it.
//
// So this gate compares against the LIBRARY, captured. `testdata/
// node_semver_gte0.tsv` holds one line per range: the range text, the answer
// for each of 30 versions as T/F, and the library's own rendering of the
// parsed range. It was produced by `testdata/node_semver_gte0.js`, which
// builds the same 99 ranges and 30 versions from the same lists as this file,
// against the copy of node-semver that ships with npm on this host:
//
//	node internal/engine/expr/testdata/node_semver_gte0.js \
//	  > internal/engine/expr/testdata/node_semver_gte0.tsv
//
// The corpus is aimed: every spelling that desugars to a trivial lower bound,
// paired with an upper bound that names a pre-release of the same core, plus
// the shapes that must NOT change (`0.0.0` the equality, `>=0.0.0-0`, and
// cores other than 0.0.0). 2,970 cells; 40 of them answered the other boolean
// before the strip.
func TestTheTrivialLowerBoundAnswersWhatNodeSemverAnswers(t *testing.T) {
	ranges, versions := semverGTE0Corpus()
	lines := readNodeSemverGTE0Golden(t)
	if len(lines) != len(ranges) {
		t.Fatalf("the golden holds %d ranges and this corpus builds %d; they are meant to be "+
			"the same list — regenerate with testdata/node_semver_gte0.js", len(lines), len(ranges))
	}
	cells, diverged := 0, 0
	for i, rng := range ranges {
		fields := strings.Split(lines[i], "\t")
		if len(fields) < 2 {
			t.Fatalf("golden line %d is malformed: %q", i, lines[i])
		}
		if fields[0] != rng {
			t.Fatalf("golden line %d is for %q and this corpus has %q at that position",
				i, fields[0], rng)
		}
		if len(fields[1]) != len(versions) {
			t.Fatalf("golden line %d carries %d answers for %d versions",
				i, len(fields[1]), len(versions))
		}
		r, err := ParseSemverRange("semver_satisfies", rng)
		if err != nil {
			t.Errorf("%q was refused; node reads it as %s", rng, fields[2])
			continue
		}
		for j, vs := range versions {
			v, ok := parseSemver(vs)
			if !ok {
				t.Fatalf("%q is not a version", vs)
			}
			want := fields[1][j] == 'T'
			cells++
			if got := semverSatisfies(v, r); got != want {
				diverged++
				t.Errorf("semver_satisfies(%q, %q) = %v; node-semver 7.7.3 answers %v "+
					"(it reads the range as %s; this engine desugars it to %q)",
					vs, rng, got, want, fields[2], r.String())
			}
		}
	}
	if cells != 2970 {
		t.Errorf("compared %d cells, want 2970 — the corpus changed shape", cells)
	}
	if diverged == 0 {
		t.Logf("%d cells, 0 divergences from node-semver 7.7.3", cells)
	}
}

// AND THE STRIP IS THE MECHANISM, not a coincidence of this corpus: no
// alternative of any range built from a trivial lower bound still carries one.
//
// The corpus above would also pass if the answers happened to agree for some
// other reason; this reads the parsed set and says the comparator is gone,
// which is what the ADR entry claims.
func TestNoAlternativeKeepsATrivialLowerBound(t *testing.T) {
	ranges, _ := semverGTE0Corpus()
	ranges = append(ranges, ">=0", ">=0.x", ">=0.0", "0.0", "0.0.x", "~0", "~0.0",
		"^0.0.0", "^0.0", "0 - 1.2.3", "0.0.0 - 1.2.3")
	seen := 0
	for _, rng := range ranges {
		r, err := ParseSemverRange("semver_satisfies", rng)
		if err != nil {
			continue
		}
		seen++
		for _, set := range r {
			if len(set) == 0 {
				t.Errorf("%q left an EMPTY alternative, which reads as no constraint at all", rng)
			}
			for _, c := range set {
				if c.op == semverOpGTE && c.ver.major == 0 && c.ver.minor == 0 &&
					c.ver.patch == 0 && !c.ver.hasPre {
					t.Errorf("%q desugars to %q, which still carries the trivial lower bound",
						rng, r.String())
				}
			}
		}
	}
	if seen < 100 {
		t.Fatalf("only %d ranges were read; this sweep is meant to cover the corpus", seen)
	}
	// The shapes that must NOT be stripped, read the same way.
	for _, tc := range []struct{ rng, want string }{
		{"0.0.0", "0.0.0"},               // an EQUALITY, whose text is not >=0.0.0
		{">=0.0.0-0", ">=0.0.0-0"},       // a different comparator text (node's GTE0PRE)
		{">=0.0.1", ">=0.0.1"},           // a lower bound that excludes something
		{">=0.0.0 <1.0.0-0", "<1.0.0-0"}, // stripped, and the rest survives
	} {
		r, err := ParseSemverRange("semver_satisfies", tc.rng)
		if err != nil {
			t.Fatalf("%q: %v", tc.rng, err)
		}
		if got := r.String(); got != tc.want {
			t.Errorf("%q desugars to %q, want %q", tc.rng, got, tc.want)
		}
	}
}

// semverGTE0Corpus is the round-2 review's corpus, kept in the shape the
// generator script builds it so the two lists can be compared position by
// position.
func semverGTE0Corpus() (ranges, versions []string) {
	for _, lo := range []string{"0", "0.x", "0.0", "0.0.x", "0.0.0", "*", "x", "1", "1.0.0", "0.0.1"} {
		for _, hi := range []string{"0.0.0-alpha", "0.0.0-0", "0.0.0", "0.0.1-alpha", "1.0.0-alpha", "0.1.0-beta", "2.0.0-x", "0.0.0-alpha.1"} {
			ranges = append(ranges, lo+" - "+hi)
		}
	}
	ranges = append(ranges,
		">=0.0.0 <=0.0.0-alpha", ">=0.0.0 <0.0.0-alpha", ">=0.0.0", ">=0.0.0-0",
		">=0.0.0 <=1.0.0-alpha", ">=0.0.0 || <=0.0.0-alpha", "* <=0.0.0-alpha",
		">=0.0.0 >=0.0.0-alpha", "<=0.0.0-alpha", "<=0.0.0-alpha >=0.0.0",
		"0.0.0 - 0.0.0", "^0.0.0-alpha", "~0.0.0-alpha", ">=0.0.0-alpha <=0.0.0-alpha",
		"0 - 0", "0.x", "^0.x", "*", ">=0.0.0 <1.0.0-0")
	for _, core := range []string{"0.0.0", "0.0.1", "0.1.0", "1.0.0", "2.0.0"} {
		for _, suf := range []string{"", "-alpha", "-0", "-alpha.1", "-beta", "+b"} {
			versions = append(versions, core+suf)
		}
	}
	return ranges, versions
}

// readNodeSemverGTE0Golden reads the captured library answers, and asserts the
// VERSION order it was captured in is the one this file builds — a golden read
// against a different column order would compare the wrong cells silently.
func readNodeSemverGTE0Golden(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("testdata/node_semver_gte0.tsv")
	if err != nil {
		t.Fatalf("reading the node-semver golden: %v", err)
	}
	_, versions := semverGTE0Corpus()
	var out []string
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "#versions\t"):
			if got := strings.TrimPrefix(line, "#versions\t"); got != strings.Join(versions, ",") {
				t.Fatalf("the golden was captured for versions\n  %s\nand this corpus builds\n  %s",
					got, strings.Join(versions, ","))
			}
		case strings.HasPrefix(line, "#"), line == "":
		default:
			out = append(out, line)
		}
	}
	return out
}

// THE DELETION APPLIES TO THE PARSED COMPARATOR, `v` INCLUDED.
//
// node's rule is a REGEX over the desugared TEXT, and that has two visible
// consequences. It has had to be corrected once — until 2022 its dots were
// unescaped, so `>=09090` matched the `>=0.0.0` pattern and was deleted
// (node-semver 11494f14, #432) — and it still sees a spelling rather than a
// version, so `>=v0.0.0` survives there while `>=0.0.0` does not.
//
// This engine decides on the comparator the parser built, which puts both of
// those on the other side of the line: `>=09090` never reaches the question,
// because a leading zero is not a numeric identifier (§9), and `>=v0.0.0` IS
// the same comparator as `>=0.0.0`, because the `v` concession says the prefix
// carries no meaning. The second is a DIVERGENCE from node and is recorded in
// ADR-0012 beside the concession that causes it, not hidden here.
//
// The rows below are the boundary from both sides: what is deleted, what is
// kept, and what never reaches the question at all.
func TestTheStripAppliesToTheParsedComparator(t *testing.T) {
	for _, tc := range []struct {
		rng, renders, why string
	}{
		{">=0.0.0", "*", "the comparator itself: the whole set becomes ANY"},
		{">= 0.0.0", "*", "a space between the operator and the version is the same comparator"},
		{">=0.0.0+b", "*", "build metadata has no precedence (§10), and node deletes this too"},
		// The one row that DIVERGES from node-semver 7.7.3, deliberately and
		// for a reason this arc already settled: node's regex sees the `v`
		// and keeps `>=v0.0.0` (it renders the comparator as `>=0.0.0` a
		// moment later, measured), while here the prefix carries no meaning
		// at all, so it is the same comparator and is deleted. ADR-0012
		// records it beside the concession.
		{">=v0.0.0", "*", "the v concession reaches the strip; node keeps this spelling"},
		{">=0.0.0-0", ">=0.0.0-0", "a pre-release bound is a DIFFERENT comparator and is kept"},
		{">=0.0.1", ">=0.0.1", "a lower bound that excludes something is kept"},
		{">=1.0.0", ">=1.0.0", "and so is any other"},
		{"0.0.0", "0.0.0", "an EQUALITY is not a lower bound"},
		{">0.0.0", ">0.0.0", "and neither is a strict one"},
		{"<=0.0.0", "<=0.0.0", "nor an upper bound at the same version"},
	} {
		r, err := ParseSemverRange("semver_satisfies", tc.rng)
		if err != nil {
			t.Errorf("%q: %v", tc.rng, err)
			continue
		}
		if got := r.String(); got != tc.renders {
			t.Errorf("%q desugars to %q, want %q (%s)", tc.rng, got, tc.renders, tc.why)
		}
	}
	// The spellings that never reach the deletion because they are not
	// versions at all. `>=09090` is the one node's own regex used to match.
	for _, rng := range []string{">=09090", ">=00.0.0", ">=0.00.0", ">=0.0.00", ">=0.0.0.0"} {
		if r, err := ParseSemverRange("semver_satisfies", rng); err == nil {
			t.Errorf("%q was accepted and desugars to %q; a leading zero is not a numeric "+
				"identifier, and a pattern that matched it is the defect node-semver "+
				"11494f14 fixed", rng, r.String())
		}
	}
}

// A RANGE WHOSE AUTHOR OPTED IN TO PRE-RELEASES OF 0.0.0 NOW ANSWERS IT — and
// one that did not, still does not.
//
// The reachable shape is Go's module pseudo-versions, which are literally
// `v0.0.0-<timestamp>-<hash>` and fill a `go.sum` or an SBOM. "Every
// pseudo-version built before 2022" is a range a person writes, and the
// numeric reading of `>=0.0.0` made it name nothing at all: no version is both
// at least 0.0.0 and below a pre-release of 0.0.0.
func TestARangeOverGoModulePseudoVersions(t *testing.T) {
	const rng = ">=0.0.0 <0.0.0-20220101000000-000000000000"
	for _, tc := range []struct {
		ver  string
		want bool
	}{
		{"0.0.0-20210101000000-abcdef123456", true},
		{"v0.0.0-20210101000000-abcdef123456", true},
		{"0.0.0-20230101000000-abcdef123456", false},
		{"0.0.0-20220101000000-000000000000", false},
		{"0.0.0", false},
		{"0.1.0", false},
	} {
		r, err := ParseSemverRange("semver_satisfies", rng)
		if err != nil {
			t.Fatalf("%q: %v", rng, err)
		}
		v, ok := parseSemver(tc.ver)
		if !ok {
			t.Fatalf("%q is not a version", tc.ver)
		}
		if got := semverSatisfies(v, r); got != tc.want {
			t.Errorf("semver_satisfies(%q, %q) = %v, want %v (node-semver 7.7.3 answers %v)",
				tc.ver, rng, got, tc.want, tc.want)
		}
	}
	// The opt-in is what changed, and nothing else: a range that names no
	// pre-release admits no pre-release, which is the published rule and is
	// node's answer too.
	for _, rng := range []string{"*", "x", ">=0.0.0", ">=0.0.0 <1.0.0-0", "^0.x", "0.x"} {
		r, err := ParseSemverRange("semver_satisfies", rng)
		if err != nil {
			t.Fatalf("%q: %v", rng, err)
		}
		for _, vs := range []string{"0.0.0-alpha", "0.0.0-0", "0.0.1-alpha", "1.0.0-alpha"} {
			v, _ := parseSemver(vs)
			if semverSatisfies(v, r) {
				t.Errorf("%q admits %q; a range that names no pre-release admits none", rng, vs)
			}
		}
	}
}
