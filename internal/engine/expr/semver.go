package expr

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// THE SEMVER FAMILY (#967) — Semantic Versioning 2.0.0 over STRING columns.
//
// A version is TEXT in every table that holds one — a package inventory, an
// agent or firmware roster, a CVE feed, a container image tag. Sorting or
// filtering it as text is wrong in a way that looks right: '1.10.0' < '1.2.3'
// and '1.2.10' < '1.2.3' are both true as bytes and both false as versions.
// This family gives the string the ordering the spec gives it, WITHOUT adding
// a type (a 23rd type costs the whole 22-wide gate matrix before any kernel).
//
//	semver_valid('1.2.3')            -- true
//	semver_major('v1.2.3')           -- 1
//	semver_cmp('1.2.3','1.10.0')     -- -1
//	semver_satisfies('1.2.3','^1.0') -- true
//	semver_sort_key(v)               -- a TEXT whose BYTE ORDER is precedence
//
// THE ORACLE. PostgreSQL has no semver at all — `pg_proc` names nothing
// matching `%semver%` on 17.11 — so the SPECIFICATION decides precedence
// (semver.org 2.0.0 §11), and node-semver's published range grammar decides
// `semver_satisfies` (semver_range.go). Where a pure-SQL spelling exists it is
// the value oracle and is written into ADR-0012: the version CORE is
// `string_to_array(v,'.')::int[]`, which PostgreSQL compares element-wise, and
// the two rows above are exactly the rows that spelling gets right.
//
// DATA IS LENIENT, THE QUERY'S OWN TEXT IS LOUD. Every function here answers
// NULL for a string that is not a version: a filter over a column of mixed
// junk must not abort the query, and a version column collected from the wild
// always holds junk. What is NOT lenient is a RANGE — the second argument of
// `semver_satisfies` is almost always a literal the query's author wrote, so a
// range this grammar does not know is 22023 naming it, never a silent FALSE
// (and, when it is written as a literal, refused before any row exists —
// semver_range_literal_check.go). `semver_normalize_strict` is the loud twin
// on the data side, for the caller who wants an ingest-time assertion rather
// than a NULL.
//
// TWO DELIBERATE DIVERGENCES FROM THE SPEC, both recorded in ADR-0012:
//
//   - A LEADING `v` OR `V` IS ACCEPTED. The spec's grammar does not allow it
//     ("Note that the 'v' prefix is not part of the semantic version"), and
//     every real dataset has it — git tags, GitHub releases, Go module
//     versions. Accepting it cannot produce a wrong value: `v1.2.3` and
//     `1.2.3` are the same version, and `semver_normalize` renders the
//     spec's spelling. Refusing it would make the family useless on the data
//     it exists for. It is the ONLY concession: surrounding whitespace, a
//     missing component (`1.2`), a leading zero (`01.2.3`) and anything else
//     off the grammar are invalid.
//
//   - A NUMERIC IDENTIFIER PAST int64 IS INVALID. The spec bounds no numeric
//     identifier ("Numeric identifiers MUST NOT include leading zeroes" is the
//     only constraint), so `99999999999999999999.0.0` is a valid version there
//     and is NULL here (22023 in the strict form). The alternative is to carry
//     it as text and compare it as text, which is the defect this family
//     exists to fix. int64 is also the width `semver_major/minor/patch`
//     declare, so the acceptance bound and the declaration are ONE number.
//
// ONE PARSE, THREE READERS. `parseSemver` is the only thing in this package
// that decides what a version is; the component accessors, the comparison and
// the sort key all read its result, so a string can never be a version for one
// function and not for another. `semver_satisfies` reads it too, for both its
// arguments and for every comparator it desugars a range into.

// semverMaxComponentDigits is the width every numeric field is zero-padded to
// in a sort key: int64's maximum, 9223372036854775807, is 19 digits, and equal
// width is what makes ASCII digit order equal numeric order.
const semverMaxComponentDigits = 19

// The sort key's structural bytes. The ONE property that makes the key work is
// an ordering between them and the identifier alphabet, and it is asserted by
// TestTheSortKeysStructuralBytesOrderBelowEveryIdentifierByte:
//
//	semverKeySep (0x2C) < '-' (0x2D) == min(identifier alphabet)
//	semverKeyNumeric (0x2D) < semverKeyAlnum (0x2E)
//	semverKeyPre (0x2D) < semverKeyRelease (0x7E)
//
// The first is the one that is easy to get wrong and impossible to see: a key
// that joins pre-release identifiers with '.' (0x2E) puts `1.0.0-alpha-x`
// BELOW `1.0.0-alpha.1`, because '-' sorts under '.' as bytes, while the spec
// compares identifier by identifier and puts `alpha` under `alpha-x`. The
// separator must sort below every byte an identifier can contain, and the
// identifier alphabet is [0-9A-Za-z-] whose minimum is '-' (0x2D).
const (
	semverKeySep     = ',' // 0x2C, between pre-release identifiers
	semverKeyNumeric = '-' // 0x2D, prefix of a NUMERIC identifier
	semverKeyAlnum   = '.' // 0x2E, prefix of an ALPHANUMERIC identifier
	semverKeyPre     = '-' // 0x2D, this version HAS a pre-release
	semverKeyRelease = '~' // 0x7E, this version is a release
)

// semverVersion is a parsed version. `pre` holds the pre-release identifiers
// in order, exactly as written; `hasPre` is carried apart from len(pre)
// because `1.0.0-` is not a version at all and an EMPTY pre-release list is
// never a legal parse — the two are kept separable so a reader of this struct
// cannot mistake "no pre-release" (a release, the HIGHEST precedence at its
// core) for "an empty one".
type semverVersion struct {
	major, minor, patch int64
	pre                 []string
	hasPre              bool
	build               string
}

// parseSemver reads a version string, and is the ONLY place in this package
// that decides what a version is.
//
// ok=false for anything off the spec's grammar, with the single documented
// concession of a leading `v`/`V`. It allocates nothing for a release with no
// pre-release and no build, which is the overwhelming majority of real rows.
func parseSemver(s string) (semverVersion, bool) {
	var v semverVersion
	if s == "" {
		return v, false
	}
	if s[0] == 'v' || s[0] == 'V' {
		s = s[1:]
	}
	// The core holds neither '-' nor '+', so splitting on the FIRST of each,
	// build before pre-release, cannot take a byte from the wrong field.
	if i := strings.IndexByte(s, '+'); i >= 0 {
		v.build = s[i+1:]
		s = s[:i]
		if !semverIdentifierListOK(v.build, true) {
			return semverVersion{}, false
		}
	}
	if i := strings.IndexByte(s, '-'); i >= 0 {
		pre := s[i+1:]
		s = s[:i]
		if !semverIdentifierListOK(pre, false) {
			return semverVersion{}, false
		}
		v.pre = strings.Split(pre, ".")
		v.hasPre = true
	}
	first := strings.IndexByte(s, '.')
	if first < 0 {
		return semverVersion{}, false
	}
	second := strings.IndexByte(s[first+1:], '.')
	if second < 0 {
		return semverVersion{}, false
	}
	second += first + 1
	var ok bool
	if v.major, ok = semverNumericIdentifier(s[:first]); !ok {
		return semverVersion{}, false
	}
	if v.minor, ok = semverNumericIdentifier(s[first+1 : second]); !ok {
		return semverVersion{}, false
	}
	if v.patch, ok = semverNumericIdentifier(s[second+1:]); !ok {
		return semverVersion{}, false
	}
	return v, true
}

// semverNumericIdentifier reads `0 | [1-9][0-9]*` and refuses a leading zero,
// an empty field, a sign, and a value past int64.
//
// The leading-zero rule is the spec's own (§9: "Numeric identifiers MUST NOT
// include leading zeroes"), and it is NOT pedantry here: `01.2.3` and `1.2.3`
// would otherwise be two spellings of one version, which makes GROUP BY and
// DISTINCT over the column answer two different numbers depending on which
// spelling a row happened to carry.
func semverNumericIdentifier(s string) (int64, bool) {
	// The length test is not an optimization: it is what keeps a megabyte of
	// digits from reaching ParseInt on every row. Nineteen digits is int64's
	// widest spelling, and a NINETEEN-digit value can still overflow
	// (9999999999999999999 does), which is why ParseInt still decides.
	if s == "" || len(s) > semverMaxComponentDigits {
		return 0, false
	}
	if s[0] == '0' && len(s) > 1 {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// semverIdentifierListOK validates a dot-separated pre-release or build list.
//
// build=true relaxes exactly one rule, as the spec does: a build identifier
// may carry leading zeroes (§10) because build metadata has no precedence at
// all, so two spellings of it cannot order differently.
func semverIdentifierListOK(list string, build bool) bool {
	if list == "" {
		return false
	}
	start := 0
	for i := 0; i <= len(list); i++ {
		if i < len(list) && list[i] != '.' {
			continue
		}
		field := list[start:i]
		start = i + 1
		if field == "" {
			return false
		}
		digits := true
		for j := 0; j < len(field); j++ {
			c := field[j]
			switch {
			case c >= '0' && c <= '9':
			case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c == '-':
				digits = false
			default:
				return false
			}
		}
		if digits && !build {
			if _, ok := semverNumericIdentifier(field); !ok {
				return false
			}
		}
	}
	return true
}

// semverIsNumericIdentifier reports whether a pre-release identifier is the
// NUMERIC kind. Every identifier reaching it has already passed
// semverIdentifierListOK, so "all digits" is the whole test.
func semverIsNumericIdentifier(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// compareSemver is Semantic Versioning 2.0.0 §11, and it is the definition
// `semver_cmp`, `semver_satisfies` and `semver_sort_key` all answer to.
//
//	§11.2 major, minor and patch are compared NUMERICALLY, in that order.
//	§11.3 a version WITH a pre-release has LOWER precedence than the same
//	      core without one.
//	§11.4 pre-release identifiers are compared left to right until a
//	      difference: two numeric identifiers compare numerically, two
//	      alphanumeric ones compare lexically in ASCII order, a numeric
//	      identifier always has LOWER precedence than an alphanumeric one,
//	      and when every preceding identifier is equal the LARGER set wins.
//	§10   build metadata is IGNORED, so 1.0.0+a and 1.0.0+b are equal.
func compareSemver(a, b semverVersion) int {
	if c := semverCompareInt(a.major, b.major); c != 0 {
		return c
	}
	if c := semverCompareInt(a.minor, b.minor); c != 0 {
		return c
	}
	if c := semverCompareInt(a.patch, b.patch); c != 0 {
		return c
	}
	switch {
	case a.hasPre && !b.hasPre:
		return -1
	case !a.hasPre && b.hasPre:
		return 1
	case !a.hasPre && !b.hasPre:
		return 0
	}
	for i := 0; i < len(a.pre) && i < len(b.pre); i++ {
		x, y := a.pre[i], b.pre[i]
		xn, yn := semverIsNumericIdentifier(x), semverIsNumericIdentifier(y)
		switch {
		case xn && yn:
			// Both already fit int64: semverIdentifierListOK said so.
			xi, _ := strconv.ParseInt(x, 10, 64)
			yi, _ := strconv.ParseInt(y, 10, 64)
			if c := semverCompareInt(xi, yi); c != 0 {
				return c
			}
		case xn:
			return -1
		case yn:
			return 1
		default:
			if c := strings.Compare(x, y); c != 0 {
				return c
			}
		}
	}
	return semverCompareInt(int64(len(a.pre)), int64(len(b.pre)))
}

func semverCompareInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// semverSortKey renders a version as a string whose BYTE ORDER is precedence.
//
// This is the whole point of the family for a distributed engine: an
// ORDER BY, a MIN/MAX, a merge across DAG stages and an external sort run all
// already know how to order bytes, so a version column ordered by this key is
// right on every arm with NO new comparator, no operator change and nothing
// for the spill or the shuffle to learn. It is a pure function of the string.
//
// The layout, with the ordering property each piece carries:
//
//	<19 digits major><19 digits minor><19 digits patch>   equal width, so
//	                                                      ASCII digit order
//	                                                      IS numeric order
//	'~' for a release / '-' for a pre-release             '-' < '~', so a
//	                                                      release sorts ABOVE
//	                                                      every pre-release of
//	                                                      the same core (§11.3)
//	then, for a pre-release, the identifiers joined by ',' each carrying its
//	kind as a prefix:
//	  '-' + 19 digits   a numeric identifier               '-' < '.', so every
//	  '.' + the bytes   an alphanumeric identifier         numeric identifier
//	                                                       sorts below every
//	                                                       alphanumeric one
//	                                                       (§11.4)
//
// Two properties fall out of the separator being BELOW the identifier
// alphabet's minimum byte: end-of-string sorts below ',' so a SHORTER
// identifier list sorts first (§11.4's "a larger set of fields wins"), and a
// ',' meets an identifier byte only where the spec's own field-by-field
// comparison has already decided the pair.
//
// BUILD METADATA IS NOT IN THE KEY, deliberately: §10 gives it no precedence,
// so `1.0.0+a` and `1.0.0+b` produce the SAME key and compare equal, exactly
// as compareSemver says they do. The key is therefore not injective on the
// original string and is not a canonical form — `semver_normalize` is.
func semverSortKey(v semverVersion) string {
	var b strings.Builder
	b.Grow(3*semverMaxComponentDigits + 1 + len(v.pre)*8)
	semverPadInt(&b, v.major)
	semverPadInt(&b, v.minor)
	semverPadInt(&b, v.patch)
	if !v.hasPre {
		b.WriteByte(semverKeyRelease)
		return b.String()
	}
	b.WriteByte(semverKeyPre)
	for i, id := range v.pre {
		if i > 0 {
			b.WriteByte(semverKeySep)
		}
		if semverIsNumericIdentifier(id) {
			b.WriteByte(semverKeyNumeric)
			n, _ := strconv.ParseInt(id, 10, 64)
			semverPadInt(&b, n)
			continue
		}
		b.WriteByte(semverKeyAlnum)
		b.WriteString(id)
	}
	return b.String()
}

// semverPadInt writes a non-negative int64 zero-padded to 19 digits. Every
// value reaching it came from semverNumericIdentifier, which refuses a sign
// and anything wider, so the padding can never truncate.
func semverPadInt(b *strings.Builder, n int64) {
	var buf [semverMaxComponentDigits]byte
	for i := semverMaxComponentDigits - 1; i >= 0; i-- {
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	b.Write(buf[:])
}

// semverRender is the canonical spelling of a parsed version: the spec's
// grammar, with the `v` prefix gone and every field exactly as written. It is
// what `semver_normalize` answers, and the DEDUPE key a GROUP BY over a
// version column wants — `v1.2.3` and `1.2.3` collapse to one group, which is
// the shape a package inventory arrives in.
func semverRender(v semverVersion) string {
	var b strings.Builder
	b.WriteString(strconv.FormatInt(v.major, 10))
	b.WriteByte('.')
	b.WriteString(strconv.FormatInt(v.minor, 10))
	b.WriteByte('.')
	b.WriteString(strconv.FormatInt(v.patch, 10))
	if v.hasPre {
		b.WriteByte('-')
		b.WriteString(strings.Join(v.pre, "."))
	}
	if v.build != "" {
		b.WriteByte('+')
		b.WriteString(v.build)
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// The registered functions.

// semverArg reads one argument as the version string it is meant to be.
// ok=false means SQL NULL — a NULL argument, or a string that is not a
// version. Both answer NULL, and that is the family's lenient half: a version
// column collected from the wild holds junk, and a WHERE over it must filter
// rather than abort.
func semverArg(v any) (semverVersion, bool) {
	if v == nil {
		return semverVersion{}, false
	}
	s, ok := v.(string)
	if !ok {
		// Anything else is rendered the way every other text-reading function
		// in this package renders it, and then parsed. A number or a date is
		// not a version and lands on NULL, which is the same answer reading
		// its raw box would have given — but through ONE rule.
		s = toString(v)
	}
	return parseSemver(s)
}

func fnSemverValid(args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	_, ok := semverArg(args[0])
	return ok
}

func fnSemverMajor(args []any) any { return semverComponent(args, 0) }
func fnSemverMinor(args []any) any { return semverComponent(args, 1) }
func fnSemverPatch(args []any) any { return semverComponent(args, 2) }

func semverComponent(args []any, which int) any {
	if len(args) != 1 {
		return nil
	}
	v, ok := semverArg(args[0])
	if !ok {
		return nil
	}
	switch which {
	case 0:
		return v.major
	case 1:
		return v.minor
	default:
		return v.patch
	}
}

// fnSemverPrerelease answers the pre-release as written, and the EMPTY STRING
// for a valid version that has none.
//
// The empty string rather than NULL is the load-bearing choice: a release IS a
// version with no pre-release, and answering NULL for it would make
// `semver_prerelease(v) IS NULL` true for both `1.0.0` and `not-a-version` —
// two facts a query needs to tell apart. NULL means "not a version", here as
// everywhere else in the family.
func fnSemverPrerelease(args []any) any {
	if len(args) != 1 {
		return nil
	}
	v, ok := semverArg(args[0])
	if !ok {
		return nil
	}
	if !v.hasPre {
		return ""
	}
	return strings.Join(v.pre, ".")
}

// fnSemverBuild answers the build metadata as written, and the empty string
// for a valid version that carries none — the same rule, for the same reason,
// as fnSemverPrerelease.
func fnSemverBuild(args []any) any {
	if len(args) != 1 {
		return nil
	}
	v, ok := semverArg(args[0])
	if !ok {
		return nil
	}
	return v.build
}

// fnSemverCmp is the three-way comparison: -1, 0 or 1, and NULL when either
// argument is NULL or is not a version.
//
// int32 rather than int64 because the domain is exactly {-1,0,1} and
// PostgreSQL types such a function `integer` — see pgIntegerResultWidths,
// which is what decides the accumulator and the OID of `SUM(semver_cmp(...))`.
func fnSemverCmp(args []any) any {
	if len(args) != 2 {
		return nil
	}
	a, aok := semverArg(args[0])
	if !aok {
		return nil
	}
	b, bok := semverArg(args[1])
	if !bok {
		return nil
	}
	return int32(compareSemver(a, b))
}

// fnSemverSortKey answers the byte-ordered key, or NULL for a string that is
// not a version. A NULL key sorts where SQL puts NULLs, which is the same
// place the version itself would sort.
func fnSemverSortKey(args []any) any {
	if len(args) != 1 {
		return nil
	}
	v, ok := semverArg(args[0])
	if !ok {
		return nil
	}
	return semverSortKey(v)
}

// fnSemverNormalize is the canonical spelling, or NULL.
func fnSemverNormalize(args []any) any {
	if len(args) != 1 {
		return nil
	}
	v, ok := semverArg(args[0])
	if !ok {
		return nil
	}
	return semverRender(v)
}

// fnSemverNormalizeStrict is the LOUD twin of the whole lenient half: the same
// canonical spelling, and 22023 naming the string when it is not a version.
//
// It exists because "NULL for junk" is right for a query that FILTERS and
// wrong for a job that ASSERTS. An ingest check or a CHECK-style guard wants
// the row that is not a version to stop the statement, not to become a NULL
// that a later COUNT quietly excludes. A NULL argument is still NULL — a NULL
// is an absent value, not a malformed one, and PostgreSQL's strict functions
// answer NULL for it everywhere.
func fnSemverNormalizeStrict(args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	s, ok := args[0].(string)
	if !ok {
		s = toString(args[0])
	}
	v, ok := parseSemver(s)
	if !ok {
		panic(fatalEval{errNotASemver("semver_normalize_strict", s)})
	}
	return semverRender(v)
}

// errNotASemver is the strict form's refusal as a VALUE. 22023
// (invalid_parameter_value) is the class PostgreSQL's own `date_trunc` uses
// for a unit it does not know, and the one A2's flag family uses for a name it
// does not know; the string is quoted in the message because a version that
// does not parse is unfindable without it.
func errNotASemver(fn, s string) error {
	return sqlerr.New("22023", "%s: %s is not a semantic version", fn, sqlerr.Quote(s))
}
