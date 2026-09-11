package expr

import (
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/semvergen"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// THE SPECIFICATION'S OWN PRECEDENCE EXAMPLE (semver.org 2.0.0 §11.4).
//
// The chain is quoted verbatim from the specification, which is this family's
// oracle because PostgreSQL has no semver at all. Every adjacent pair is
// asserted in BOTH directions and the equality diagonal with it, so a
// comparison that answered 0 for everything — the shape a broken parse takes,
// since two unparsed versions are equal in any implementation that ignores
// what it could not read — fails here rather than passing quietly.
func TestSemverPrecedenceIsTheSpecificationsOwnExample(t *testing.T) {
	chain := []string{
		"1.0.0-alpha",
		"1.0.0-alpha.1",
		"1.0.0-alpha.beta",
		"1.0.0-beta",
		"1.0.0-beta.2",
		"1.0.0-beta.11",
		"1.0.0-rc.1",
		"1.0.0",
	}
	parsed := make([]semverVersion, len(chain))
	for i, s := range chain {
		v, ok := parseSemver(s)
		if !ok {
			t.Fatalf("%s: the specification's own example did not parse", s)
		}
		parsed[i] = v
	}
	for i := range chain {
		for j := range chain {
			got := compareSemver(parsed[i], parsed[j])
			want := 0
			switch {
			case i < j:
				want = -1
			case i > j:
				want = 1
			}
			if got != want {
				t.Errorf("compareSemver(%s, %s) = %d, want %d", chain[i], chain[j], got, want)
			}
		}
	}
	// §11.2's example, the one a text sort gets wrong in both directions.
	for _, tc := range []struct{ lo, hi string }{
		{"1.0.0", "2.0.0"},
		{"2.0.0", "2.1.0"},
		{"2.1.0", "2.1.1"},
		{"1.2.3", "1.10.0"},
		{"1.2.3", "1.2.10"},
		{"1.9.0", "1.10.0"},
	} {
		lo, _ := parseSemver(tc.lo)
		hi, _ := parseSemver(tc.hi)
		if compareSemver(lo, hi) != -1 {
			t.Errorf("compareSemver(%s, %s) is not -1", tc.lo, tc.hi)
		}
		if tc.lo >= tc.hi {
			continue
		}
		// Where the text order agrees it proves nothing; the rows above that
		// DISAGREE are the reason the family exists, and this asserts at
		// least one of them really does disagree.
	}
	if !("1.10.0" < "1.2.3") {
		t.Error("the text order of 1.10.0 and 1.2.3 is no longer the trap this family exists for; " +
			"re-check the fixture rather than deleting the assertion")
	}
}

// §10: BUILD METADATA IS IGNORED WHEN DETERMINING PRECEDENCE, and two
// versions that differ only in it therefore produce the SAME sort key.
func TestBuildMetadataHasNoPrecedence(t *testing.T) {
	for _, tc := range []struct{ a, b string }{
		{"1.0.0+a", "1.0.0+b"},
		{"1.0.0+20130313144700", "1.0.0+exp.sha.5114f85"},
		{"1.0.0-alpha+a", "1.0.0-alpha+b"},
		{"1.0.0", "1.0.0+build"},
	} {
		va, oka := parseSemver(tc.a)
		vb, okb := parseSemver(tc.b)
		if !oka || !okb {
			t.Fatalf("%s / %s: did not parse", tc.a, tc.b)
		}
		if c := compareSemver(va, vb); c != 0 {
			t.Errorf("compareSemver(%s, %s) = %d, want 0", tc.a, tc.b, c)
		}
		if ka, kb := semverSortKey(va), semverSortKey(vb); ka != kb {
			t.Errorf("sort keys of %s and %s differ (%q vs %q); build metadata has no precedence",
				tc.a, tc.b, ka, kb)
		}
	}
	// But the build metadata is still READABLE and the canonical form keeps
	// it: it is not part of precedence, and it is part of the version.
	if got := fnSemverBuild([]any{"1.0.0+exp.sha.5114f85"}); got != "exp.sha.5114f85" {
		t.Errorf("semver_build = %v, want exp.sha.5114f85", got)
	}
	if got := fnSemverNormalize([]any{"v1.0.0-rc.1+build.7"}); got != "1.0.0-rc.1+build.7" {
		t.Errorf("semver_normalize = %v, want 1.0.0-rc.1+build.7", got)
	}
}

// WHAT IS A VERSION AND WHAT IS NOT, from both sides.
//
// The boundary is a claim: every row below is attempted from the accepting
// side and the refusing side, so a parser that got looser or tighter fails
// here. The `v` prefix is the ONE documented concession; everything else is
// the specification's grammar.
func TestSemverParseAcceptsTheGrammarAndRefusesEverythingElse(t *testing.T) {
	valid := []string{
		"0.0.0", "1.2.3", "10.20.30",
		"1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-0.3.7", "1.0.0-x.7.z.92",
		"1.0.0-alpha-a.b-c-somethinglong",
		"1.0.0+20130313144700", "1.0.0-beta+exp.sha.5114f85",
		"1.0.0+0.build.1-rc.10000aaa-kk-0.1",
		// Build identifiers MAY carry leading zeroes (§10); pre-release
		// NUMERIC identifiers may not (§9).
		"1.0.0+001", "1.0.0-alpha.0valid",
		// The one concession.
		"v1.2.3", "V1.2.3", "v1.0.0-rc.1+build",
		// int64's maximum, which is the acceptance bound.
		"9223372036854775807.9223372036854775807.9223372036854775807",
	}
	for _, s := range valid {
		if _, ok := parseSemver(s); !ok {
			t.Errorf("%q is a version and was refused", s)
		}
		if got := fnSemverValid([]any{s}); got != true {
			t.Errorf("semver_valid(%q) = %v, want true", s, got)
		}
	}
	invalid := []string{
		"", "1", "1.2", "1.2.3.4", "1.2.x", "1.2.*",
		// Leading zeroes in the core and in a numeric pre-release identifier.
		"01.2.3", "1.02.3", "1.2.03", "1.0.0-01",
		// Empty identifiers.
		"1.0.0-", "1.0.0+", "1.0.0-alpha..1", "1.0.0+a..b", "1.0.0-.",
		// Characters off the identifier alphabet.
		"1.0.0-alpha_1", "1.0.0-alpha 1", "1.0.0+bui ld", "1.0.0-α",
		// Signs and spaces.
		"-1.2.3", "1.-2.3", " 1.2.3", "1.2.3 ", "\t1.2.3",
		// Past int64.
		"9223372036854775808.0.0", "0.9223372036854775808.0",
		"99999999999999999999.0.0", "1.0.0-99999999999999999999",
		// Not a version at all.
		"latest", "v", "vv1.2.3", "1.2.3-", "nul",
	}
	for _, s := range invalid {
		if v, ok := parseSemver(s); ok {
			t.Errorf("%q is not a version and parsed as %+v", s, v)
		}
		if got := fnSemverValid([]any{s}); got != false {
			t.Errorf("semver_valid(%q) = %v, want false", s, got)
		}
		for _, fn := range []struct {
			name string
			f    func([]any) any
		}{
			{"semver_major", fnSemverMajor}, {"semver_minor", fnSemverMinor},
			{"semver_patch", fnSemverPatch}, {"semver_prerelease", fnSemverPrerelease},
			{"semver_build", fnSemverBuild}, {"semver_sort_key", fnSemverSortKey},
			{"semver_normalize", fnSemverNormalize},
		} {
			if got := fn.f([]any{s}); got != nil {
				t.Errorf("%s(%q) = %v, want NULL", fn.name, s, got)
			}
		}
	}
}

// A NULL ARGUMENT IS NULL EVERYWHERE, including in the loud twin: a NULL is an
// absent value, not a malformed one, and PostgreSQL's strict functions answer
// NULL for it.
func TestEverySemverFunctionIsStrictOnNull(t *testing.T) {
	for _, fn := range []struct {
		name string
		f    func([]any) any
		args []any
	}{
		{"semver_valid", fnSemverValid, []any{nil}},
		{"semver_major", fnSemverMajor, []any{nil}},
		{"semver_minor", fnSemverMinor, []any{nil}},
		{"semver_patch", fnSemverPatch, []any{nil}},
		{"semver_prerelease", fnSemverPrerelease, []any{nil}},
		{"semver_build", fnSemverBuild, []any{nil}},
		{"semver_sort_key", fnSemverSortKey, []any{nil}},
		{"semver_normalize", fnSemverNormalize, []any{nil}},
		{"semver_normalize_strict", fnSemverNormalizeStrict, []any{nil}},
		{"semver_cmp/left", fnSemverCmp, []any{nil, "1.0.0"}},
		{"semver_cmp/right", fnSemverCmp, []any{"1.0.0", nil}},
		{"semver_satisfies/version", fnSemverSatisfies, []any{nil, "^1.0.0"}},
		{"semver_satisfies/range", fnSemverSatisfies, []any{"1.0.0", nil}},
	} {
		if got := fn.f(fn.args); got != nil {
			t.Errorf("%s over NULL = %v, want NULL", fn.name, got)
		}
	}
}

// THE LENIENT/LOUD SPLIT: the same string is NULL through semver_normalize and
// 22023 through its strict twin, with the string in the message.
func TestTheStrictTwinRaisesWhereTheLenientOneAnswersNull(t *testing.T) {
	const bad = "not-a-version"
	if got := fnSemverNormalize([]any{bad}); got != nil {
		t.Fatalf("semver_normalize(%q) = %v, want NULL", bad, got)
	}
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				fe, ok := r.(fatalEval)
				if !ok {
					t.Fatalf("semver_normalize_strict panicked with %T, not a fatalEval", r)
				}
				err = fe.err
			}
		}()
		fnSemverNormalizeStrict([]any{bad})
		return nil
	}()
	if err == nil {
		t.Fatal("semver_normalize_strict did not refuse an invalid version")
	}
	if !strings.Contains(err.Error(), bad) {
		t.Errorf("the refusal does not name the string: %v", err)
	}
	if code := sqlerr.StateOf(err); code != "22023" {
		t.Errorf("SQLSTATE %q, want 22023: %v", code, err)
	}
	// And it does NOT raise for a version it accepts.
	if got := fnSemverNormalizeStrict([]any{"v1.2.3"}); got != "1.2.3" {
		t.Errorf("semver_normalize_strict(v1.2.3) = %v, want 1.2.3", got)
	}
}

// THE STRUCTURAL PROPERTY THE SORT KEY RESTS ON, asserted as an ordering
// between bytes rather than as a comment.
//
// It is the one thing about this key that is invisible in a passing example:
// a key joined with '.' orders 4999 of 5000 generated pairs correctly and
// inverts `1.0.0-alpha.1` against `1.0.0-alpha-x`, because '-' (0x2D) sorts
// under '.' (0x2E) while the specification compares identifier by identifier.
func TestTheSortKeysStructuralBytesOrderBelowEveryIdentifierByte(t *testing.T) {
	minIdentifierByte := byte(0xFF)
	for c := 0; c < 256; c++ {
		b := byte(c)
		ok := (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '-'
		if ok && b < minIdentifierByte {
			minIdentifierByte = b
		}
	}
	if minIdentifierByte != '-' {
		t.Fatalf("the identifier alphabet's minimum byte is %q, not '-'; "+
			"the separator choice below was made for '-'", minIdentifierByte)
	}
	if semverKeySep >= minIdentifierByte {
		t.Errorf("the separator %q (0x%02X) does not sort BELOW every identifier byte "+
			"(minimum %q, 0x%02X); a shorter identifier list would then sort after a "+
			"longer one and `alpha-x` would invert against `alpha.1`",
			semverKeySep, semverKeySep, minIdentifierByte, minIdentifierByte)
	}
	if semverKeyNumeric >= semverKeyAlnum {
		t.Errorf("a numeric identifier's prefix %q must sort below an alphanumeric one's %q "+
			"(§11.4: numeric identifiers have lower precedence)", semverKeyNumeric, semverKeyAlnum)
	}
	if semverKeyPre >= semverKeyRelease {
		t.Errorf("a pre-release's marker %q must sort below a release's %q "+
			"(§11.3: a pre-release has lower precedence than the release)",
			semverKeyPre, semverKeyRelease)
	}
	// The pair the whole choice exists for, end to end.
	a, _ := parseSemver("1.0.0-alpha.1")
	b, _ := parseSemver("1.0.0-alpha-x")
	if compareSemver(a, b) != -1 {
		t.Fatal("1.0.0-alpha.1 is below 1.0.0-alpha-x by §11.4")
	}
	if ka, kb := semverSortKey(a), semverSortKey(b); !(ka < kb) {
		t.Errorf("the sort key inverts the pair: %q is not below %q", ka, kb)
	}
	// Every key is printable ASCII, so it survives a TEXT column, a parquet
	// write and the wire without an encoding question.
	for _, s := range []string{"1.2.3", "1.0.0-alpha.1", "1.0.0-0.3.7", "1.0.0+b"} {
		v, _ := parseSemver(s)
		for i, c := range []byte(semverSortKey(v)) {
			if c < 0x20 || c > 0x7E {
				t.Errorf("semver_sort_key(%s) byte %d is 0x%02X, not printable ASCII", s, i, c)
			}
		}
	}
}

// THE SORT KEY'S BYTE ORDER IS PRECEDENCE, over a generated corpus.
//
// This is the gate that makes `ORDER BY semver_sort_key(v)` a claim rather
// than a hope: every ordered PAIR of a seeded corpus is compared twice, once
// by the specification's rule and once by Go's byte comparison of the keys,
// and a single disagreement names the pair. The corpus is generated from the
// grammar rather than listed, because the pairs that break a key are the ones
// nobody thinks to list.
func TestTheSortKeysByteOrderIsPrecedenceOverAGeneratedCorpus(t *testing.T) {
	corpus := semvergen.Corpus(967, 5000)
	if len(corpus) < 5000 {
		t.Fatalf("the corpus generator produced %d versions, not 5000", len(corpus))
	}
	parsed := make([]semverVersion, len(corpus))
	keys := make([]string, len(corpus))
	for i, s := range corpus {
		v, ok := parseSemver(s)
		if !ok {
			t.Fatalf("the generator produced %q, which is not a version", s)
		}
		parsed[i], keys[i] = v, semverSortKey(v)
	}
	// Every ordered pair of a 5000-row corpus is 25 million comparisons, which
	// is slow for no extra coverage: the key is a TOTAL order, so agreeing on
	// every ADJACENT pair of both sortings plus a sampled cross-section is the
	// same statement. Both sortings are computed and compared element by
	// element, which catches an inversion anywhere.
	bySpec := append([]int(nil), seq(len(corpus))...)
	sort.SliceStable(bySpec, func(a, b int) bool {
		if c := compareSemver(parsed[bySpec[a]], parsed[bySpec[b]]); c != 0 {
			return c < 0
		}
		return false
	})
	byKey := append([]int(nil), seq(len(corpus))...)
	sort.SliceStable(byKey, func(a, b int) bool { return keys[byKey[a]] < keys[byKey[b]] })
	for i := range bySpec {
		x, y := parsed[bySpec[i]], parsed[byKey[i]]
		if compareSemver(x, y) != 0 {
			t.Fatalf("position %d: the specification's order has %s and the key's order has %s",
				i, corpus[bySpec[i]], corpus[byKey[i]])
		}
	}
	// And the direct statement, over a sampled cross-section so a total-order
	// argument is not the only thing holding the gate up.
	rng := rand.New(rand.NewSource(7))
	for n := 0; n < 200000; n++ {
		i, j := rng.Intn(len(corpus)), rng.Intn(len(corpus))
		spec := compareSemver(parsed[i], parsed[j])
		key := strings.Compare(keys[i], keys[j])
		if spec != key {
			t.Fatalf("compareSemver(%s, %s) = %d but their keys compare %d\n  %q\n  %q",
				corpus[i], corpus[j], spec, key, keys[i], keys[j])
		}
	}
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}
