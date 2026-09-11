package expr

import (
	"math"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// THE RANGE GRAMMAR `semver_satisfies` IMPLEMENTS (#967).
//
// The Semantic Versioning specification defines PRECEDENCE and nothing else —
// it has no range syntax at all. The syntax people actually write is
// node-semver's, and it is what a `package.json`, a Dependabot alert, a
// Renovate rule and every CVE advisory's "affected versions" field are spelled
// in. That published grammar is this function's oracle, and the expansions
// below are its own documented ones (node-semver README, "Advanced Range
// Syntax"), which is what `TestTheRangeGrammarMatchesTheNodeSemverTable`
// checks each spelling against:
//
//	*                   any release
//	1.2.3               exactly 1.2.3
//	>=1.2.3  >1.2.3  <=1.2.3  <1.2.3  =1.2.3
//	1.2.3 - 2.3.4       >=1.2.3 <=2.3.4          (hyphen range)
//	1.2 - 2.3.4         >=1.2.0 <=2.3.4
//	1.2.3 - 2.3         >=1.2.3 <2.4.0-0
//	1.2.3 - 2           >=1.2.3 <3.0.0-0
//	1.x   1.X   1.*  1  >=1.0.0 <2.0.0-0         (X-range)
//	1.2.x 1.2.X 1.2.* 1.2  >=1.2.0 <1.3.0-0
//	~1.2.3              >=1.2.3 <1.3.0-0         (tilde: patch-level changes)
//	~1.2                >=1.2.0 <1.3.0-0
//	~1                  >=1.0.0 <2.0.0-0
//	~0.2.3              >=0.2.3 <0.3.0-0
//	~1.2.3-beta.2       >=1.2.3-beta.2 <1.3.0-0
//	^1.2.3              >=1.2.3 <2.0.0-0         (caret: compatible changes)
//	^0.2.3              >=0.2.3 <0.3.0-0
//	^0.0.3              >=0.0.3 <0.0.4-0
//	^1.2.x              >=1.2.0 <2.0.0-0
//	^0.0.x  ^0.0        >=0.0.0 <0.1.0-0
//	^1.x                >=1.0.0 <2.0.0-0
//	^0.x                >=0.0.0 <1.0.0-0
//	A B                 both (intersection; any whitespace separates)
//	A || B              either (union)
//
// THE `-0` ON EVERY UPPER BOUND IS NOT DECORATION. `<2.0.0-0` and `<2.0.0` are
// different sets: `2.0.0-beta` is below `2.0.0` by §11.3 and ABOVE `2.0.0-0`
// by §11.4 (a numeric identifier sorts under an alphanumeric one), so the
// `-0` is what keeps a pre-release of the NEXT major out of `^1.2.3`. Dropping
// it is a silently larger result set, which is why the expansions are
// transcribed from the published table rather than re-derived.
//
// THE PRE-RELEASE RULE, also node-semver's and also published: "if a version
// has a prerelease tag then it will only be allowed to satisfy comparator sets
// if at least one comparator with the same [major,minor,patch] tuple also has
// a prerelease tag." So `1.2.3-beta` does NOT satisfy `^1.2.3`, and
// `3.4.5-alpha.9` does NOT satisfy `>1.2.3-alpha.3` even though it is greater,
// while `3.4.5` does. An opt-in `includePrerelease` is NOT implemented; a
// query that wants pre-releases says so with a comparator that has one.
//
// WHAT IS REFUSED, AND WHY IT IS LOUD (22023, never a silent FALSE). A range
// is the second argument of a predicate and is almost always a LITERAL the
// query's author typed. A spelling this grammar does not know is therefore a
// property of the QUERY, not of the rows, and answering FALSE for it silently
// drops every row the author meant to select. Three things are refused that
// node-semver accepts, each recorded in ADR-0012:
//
//   - THE EMPTY RANGE. node treats `''` as `*`. Here it is 22023, for the
//     reason A2's empty flag list is: a range with nothing in it is a query
//     that meant something and did not say it, and "every row" is the
//     plausible answer rather than the right one. `*` is the explicit
//     spelling and is accepted.
//   - A PRE-RELEASE OR BUILD ON A PARTIAL VERSION (`1.2.x-beta`). node's
//     regex captures it and then ignores it. Ignoring part of what the author
//     wrote is exactly the silent-larger-set failure above.
//   - EVERYTHING ELSE OFF THE GRAMMAR — `>=`, `^^1.0.0`, `1.2.3 -`, a
//     comparator whose version has a leading zero, a numeric identifier past
//     int64.
//
// A range written as a STRING LITERAL is refused BEFORE ANY ROW, by the binder
// and again at compile time (semver_range_literal_check.go), so whether a typo
// is an error never depends on the data or on which plan ran.

type semverOp uint8

const (
	semverOpAny semverOp = iota // matches every version; carries no bound
	semverOpLT
	semverOpLTE
	semverOpGT
	semverOpGTE
	semverOpEQ
)

// semverComp is one comparator: an operator and the version it bounds.
type semverComp struct {
	op  semverOp
	ver semverVersion
}

// semverRange is a union of intersections — `A B || C` is
// [[A, B], [C]] — which is the shape node-semver's `Range.set` has and the
// shape the pre-release rule is defined over (it asks a whole intersection,
// not one comparator).
type semverRange [][]semverComp

// String renders a range back into the grammar it was written in, so a gate
// can compare a desugaring against the PUBLISHED expansion text rather than
// against another run of the same code.
func (r semverRange) String() string {
	sets := make([]string, 0, len(r))
	for _, set := range r {
		parts := make([]string, 0, len(set))
		for _, c := range set {
			parts = append(parts, c.String())
		}
		sets = append(sets, strings.Join(parts, " "))
	}
	return strings.Join(sets, " || ")
}

func (c semverComp) String() string {
	switch c.op {
	case semverOpAny:
		return "*"
	case semverOpLT:
		return "<" + semverRender(c.ver)
	case semverOpLTE:
		return "<=" + semverRender(c.ver)
	case semverOpGT:
		return ">" + semverRender(c.ver)
	case semverOpGTE:
		return ">=" + semverRender(c.ver)
	default:
		return semverRender(c.ver)
	}
}

// semverPartial is a version with any of its three components left unsaid —
// `1`, `1.2`, `1.2.x`, `*`. Every range spelling except an exact version is
// defined by DESUGARING one of these into two comparators, so it is the one
// shape the desugaring rules read.
type semverPartial struct {
	major, minor, patch int64
	xMajor, xMinor      bool
	xPatch              bool
	pre                 string
	hasPre              bool
	build               string
}

// exact reads a fully-specified partial back as a version.
func (p semverPartial) exact() semverVersion {
	return semverVersion{
		major: p.major, minor: p.minor, patch: p.patch,
		pre: p.pre, hasPre: p.hasPre, build: p.build,
	}
}

// semverZeroPre is the `-0` every exclusive upper bound carries: the LOWEST
// pre-release of a core, so a pre-release of that core is excluded along with
// the release itself.
func semverBound(major, minor, patch int64, zeroPre bool) semverVersion {
	v := semverVersion{major: major, minor: minor, patch: patch}
	if zeroPre {
		v.pre = "0"
		v.hasPre = true
	}
	return v
}

// THE RAISED BOUND, AND WHAT IT MEANS AT THE TOP OF THE DOMAIN (#967).
//
// Every desugaring except an exact version closes its band by raising ONE
// component by one: `^1.2.3` is `>=1.2.3 <2.0.0-0`, `~1.2` is
// `>=1.2.0 <1.3.0-0`, `>1.2.x` is `>=1.3.0`. A component is accepted up to
// int64's MAXIMUM — that is this family's recorded acceptance bound, and
// `semver_major('9223372036854775807.0.0')` answers it — so the raise can land
// one past a value the grammar can spell, and `+1` on an int64 at its maximum
// is a NEGATIVE number. A negative upper bound is below every version, so the
// `<` half drops every row the author meant to select; a negative lower bound
// is below every version, so the `>=` half admits every row. Both are the
// silent wrong boolean this file's header exists to prevent, and both are
// reachable: the shipped corpus draws a component from that maximum.
//
// The bound is therefore SATURATED rather than wrapped, and the rewrite is
// EXACT rather than approximate. Every component is bounded by int64, so no
// version exists between `X.Y.max` and the unspellable `X.(Y+1).0`, and
// therefore over the versions that exist
//
//	<X.(Y+1).0-0   is exactly   <=X.Y.max
//	>=X.(Y+1).0    is exactly   >X.Y.max
//
// with `max` int64's maximum in every component below the raised one. The
// `-0` the exclusive form carries is not lost with the rewrite: a `-0` bound
// can never be the comparator that ADMITS a pre-release under the pre-release
// rule (nothing sorts below the lowest pre-release of its own core), and the
// saturated form carries no pre-release at all, so the rule answers the same.
//
// DROPPING the bound instead would be wrong for a minor or patch raise:
// `1.9223372036854775807.x` bounded by nothing would admit `2.0.0`, which is
// outside the band the author wrote. Only the MAJOR raise saturates to a
// comparator true of every version, and it gets there by the same identity as
// the other two rather than by a special case.
//
// REFUSING the range was the alternative, and is what node-semver does — it
// refuses any component past 2^53-1, in a version and in a range alike. It is
// not what this family does, because this family ACCEPTS such a version:
// refusing `^M.0.0` while `semver_major('M.0.0')` answers M would make the
// acceptance bound depend on which function was asked, and the range's meaning
// is not in doubt — it is the one the saturated bound spells. Recorded in
// ADR-0012 with the acceptance band.

// semverUpperMajor is the exclusive upper bound `<(major+1).0.0-0`, saturated
// to `<=major.max.max` when the raise would pass int64's maximum.
func semverUpperMajor(major int64) semverComp {
	if major == math.MaxInt64 {
		return semverComp{op: semverOpLTE, ver: semverBound(major, math.MaxInt64, math.MaxInt64, false)}
	}
	return semverComp{op: semverOpLT, ver: semverBound(major+1, 0, 0, true)}
}

// semverUpperMinor is the exclusive upper bound `<major.(minor+1).0-0`,
// saturated to `<=major.minor.max` when the raise would pass int64's maximum.
func semverUpperMinor(major, minor int64) semverComp {
	if minor == math.MaxInt64 {
		return semverComp{op: semverOpLTE, ver: semverBound(major, minor, math.MaxInt64, false)}
	}
	return semverComp{op: semverOpLT, ver: semverBound(major, minor+1, 0, true)}
}

// semverUpperPatch is the exclusive upper bound `<major.minor.(patch+1)-0`,
// saturated to `<=major.minor.patch` when the raise would pass int64's
// maximum — the band is then the one version at its floor.
func semverUpperPatch(major, minor, patch int64) semverComp {
	if patch == math.MaxInt64 {
		return semverComp{op: semverOpLTE, ver: semverBound(major, minor, patch, false)}
	}
	return semverComp{op: semverOpLT, ver: semverBound(major, minor, patch+1, true)}
}

// semverLowerMajor is the inclusive lower bound `>=(major+1).0.0`, saturated
// to `>major.max.max` when the raise would pass int64's maximum — which no
// version can satisfy, and nothing can: `>9223372036854775807.x` names the
// versions above the top of the domain, and there are none.
func semverLowerMajor(major int64) semverComp {
	if major == math.MaxInt64 {
		return semverComp{op: semverOpGT, ver: semverBound(major, math.MaxInt64, math.MaxInt64, false)}
	}
	return semverComp{op: semverOpGTE, ver: semverBound(major+1, 0, 0, false)}
}

// semverLowerMinor is the inclusive lower bound `>=major.(minor+1).0`,
// saturated to `>major.minor.max` when the raise would pass int64's maximum.
func semverLowerMinor(major, minor int64) semverComp {
	if minor == math.MaxInt64 {
		return semverComp{op: semverOpGT, ver: semverBound(major, minor, math.MaxInt64, false)}
	}
	return semverComp{op: semverOpGTE, ver: semverBound(major, minor+1, 0, false)}
}

// ParseSemverRange parses a node-semver range, and is the ONE reader of the
// grammar: the evaluator, the binder's literal refusal and the compile-time
// backstop all call it, so no two layers can disagree about which ranges
// exist. `fn` names the caller in the refusal message.
func ParseSemverRange(fn, text string) (semverRange, error) {
	if strings.TrimSpace(text) == "" {
		return nil, sqlerr.New("22023",
			"%s: the version range is empty; write %s for every version", fn, sqlerr.Quote("*"))
	}
	var out semverRange
	for _, alt := range strings.Split(text, "||") {
		set, err := parseSemverRangeSet(fn, text, alt)
		if err != nil {
			return nil, err
		}
		out = append(out, set)
	}
	return out, nil
}

// parseSemverRangeSet parses one `||`-separated alternative: an intersection
// of comparators, or a hyphen range.
func parseSemverRangeSet(fn, whole, text string) ([]semverComp, error) {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		// `a || ` — an alternative with nothing in it, refused for the same
		// reason the whole empty range is.
		return nil, errBadSemverRange(fn, whole)
	}
	if i := semverHyphenAt(fields); i >= 0 {
		return semverHyphenRange(fn, whole, fields, i)
	}
	// `>= 1.2.3` is one comparator written with a space, which node's
	// comparatorTrim also joins. A bare operator with nothing after it is not
	// a comparator at all and falls to the refusal below.
	var out []semverComp
	for i := 0; i < len(fields); i++ {
		tok := fields[i]
		if semverIsBareOperator(tok) {
			if i+1 >= len(fields) {
				return nil, errBadSemverRange(fn, whole)
			}
			tok += fields[i+1]
			i++
		}
		comps, err := desugarSemverComparator(fn, whole, tok)
		if err != nil {
			return nil, err
		}
		out = append(out, comps...)
	}
	if len(out) == 0 {
		return nil, errBadSemverRange(fn, whole)
	}
	return out, nil
}

func semverIsBareOperator(tok string) bool {
	switch tok {
	case ">", ">=", "<", "<=", "=", "^", "~", "~>":
		return true
	}
	return false
}

// semverHyphenAt finds the `-` that separates a hyphen range's two ends. The
// separator is a field of its own — `1.2.3 - 2.3.4`, whitespace on both sides
// — which is what keeps it apart from the `-` that introduces a pre-release.
func semverHyphenAt(fields []string) int {
	for i, f := range fields {
		if f == "-" {
			return i
		}
	}
	return -1
}

func semverHyphenRange(fn, whole string, fields []string, at int) ([]semverComp, error) {
	if at != 1 || len(fields) != 3 {
		return nil, errBadSemverRange(fn, whole)
	}
	lo, err := parseSemverPartial(fn, whole, fields[0])
	if err != nil {
		return nil, err
	}
	hi, err := parseSemverPartial(fn, whole, fields[2])
	if err != nil {
		return nil, err
	}
	var out []semverComp
	// The LOW end: an unsaid component means zero, and an unsaid MAJOR means
	// no lower bound at all.
	switch {
	case lo.xMajor:
	case lo.xMinor:
		out = append(out, semverComp{op: semverOpGTE, ver: semverBound(lo.major, 0, 0, false)})
	case lo.xPatch:
		out = append(out, semverComp{op: semverOpGTE, ver: semverBound(lo.major, lo.minor, 0, false)})
	default:
		out = append(out, semverComp{op: semverOpGTE, ver: lo.exact()})
	}
	// The HIGH end: an unsaid component widens the bound to the next value up
	// and makes it EXCLUSIVE, so `1.2.3 - 2.3` ends below 2.4.0.
	switch {
	case hi.xMajor:
	case hi.xMinor:
		out = append(out, semverUpperMajor(hi.major))
	case hi.xPatch:
		out = append(out, semverUpperMinor(hi.major, hi.minor))
	default:
		out = append(out, semverComp{op: semverOpLTE, ver: hi.exact()})
	}
	if len(out) == 0 {
		out = append(out, semverComp{op: semverOpAny})
	}
	return out, nil
}

// desugarSemverComparator turns one token into the comparators it denotes.
func desugarSemverComparator(fn, whole, tok string) ([]semverComp, error) {
	op, rest := semverSplitOperator(tok)
	switch op {
	case "^":
		return semverCaret(fn, whole, rest)
	case "~", "~>":
		return semverTilde(fn, whole, rest)
	}
	p, err := parseSemverPartial(fn, whole, rest)
	if err != nil {
		return nil, err
	}
	anyX := p.xMajor || p.xMinor || p.xPatch
	if !anyX {
		v := p.exact()
		switch op {
		case "<":
			return []semverComp{{op: semverOpLT, ver: v}}, nil
		case "<=":
			return []semverComp{{op: semverOpLTE, ver: v}}, nil
		case ">":
			return []semverComp{{op: semverOpGT, ver: v}}, nil
		case ">=":
			return []semverComp{{op: semverOpGTE, ver: v}}, nil
		default:
			return []semverComp{{op: semverOpEQ, ver: v}}, nil
		}
	}
	// An X-range under an INEQUALITY collapses to a single bound, and the
	// rules are node's own: `>1.2.x` is `>=1.3.0`, `<=1.2.x` is `<1.3.0-0`,
	// and a bare `>`/`<` over `*` matches NOTHING rather than everything.
	if p.xMajor {
		switch op {
		case ">", "<":
			// `<0.0.0-0` — the empty set, which is what node produces so that
			// `>*` and `<*` are unsatisfiable rather than universal.
			return []semverComp{{op: semverOpLT, ver: semverBound(0, 0, 0, true)}}, nil
		default:
			return []semverComp{{op: semverOpAny}}, nil
		}
	}
	minor := p.minor
	if p.xMinor {
		minor = 0
	}
	switch op {
	case ">":
		if p.xMinor {
			return []semverComp{semverLowerMajor(p.major)}, nil
		}
		return []semverComp{semverLowerMinor(p.major, minor)}, nil
	case ">=":
		return []semverComp{{op: semverOpGTE, ver: semverBound(p.major, minor, 0, false)}}, nil
	case "<":
		return []semverComp{{op: semverOpLT, ver: semverBound(p.major, minor, 0, true)}}, nil
	case "<=":
		if p.xMinor {
			return []semverComp{semverUpperMajor(p.major)}, nil
		}
		return []semverComp{semverUpperMinor(p.major, minor)}, nil
	}
	// `=1.2.x`, or no operator at all: the whole band the unsaid components
	// leave open.
	if p.xMinor {
		return []semverComp{
			{op: semverOpGTE, ver: semverBound(p.major, 0, 0, false)},
			semverUpperMajor(p.major),
		}, nil
	}
	return []semverComp{
		{op: semverOpGTE, ver: semverBound(p.major, p.minor, 0, false)},
		semverUpperMinor(p.major, p.minor),
	}, nil
}

// semverCaret is `^` — "compatible with", which means "up to the next change
// that would alter the LEFTMOST NON-ZERO component". That is why `^0.2.3` is
// bounded by 0.3.0 and `^0.0.3` by 0.0.4: below 1.0.0 the spec gives no
// compatibility promise, so each position down is treated as the major.
func semverCaret(fn, whole, rest string) ([]semverComp, error) {
	p, err := parseSemverPartial(fn, whole, rest)
	if err != nil {
		return nil, err
	}
	if p.xMajor {
		return []semverComp{{op: semverOpAny}}, nil
	}
	if p.xMinor {
		return []semverComp{
			{op: semverOpGTE, ver: semverBound(p.major, 0, 0, false)},
			semverUpperMajor(p.major),
		}, nil
	}
	if p.xPatch {
		lo := semverBound(p.major, p.minor, 0, false)
		if p.major == 0 {
			return []semverComp{
				{op: semverOpGTE, ver: lo},
				semverUpperMinor(0, p.minor),
			}, nil
		}
		return []semverComp{
			{op: semverOpGTE, ver: lo},
			semverUpperMajor(p.major),
		}, nil
	}
	lo := p.exact()
	switch {
	case p.major == 0 && p.minor == 0:
		return []semverComp{
			{op: semverOpGTE, ver: lo},
			semverUpperPatch(0, 0, p.patch),
		}, nil
	case p.major == 0:
		return []semverComp{
			{op: semverOpGTE, ver: lo},
			semverUpperMinor(0, p.minor),
		}, nil
	}
	return []semverComp{
		{op: semverOpGTE, ver: lo},
		semverUpperMajor(p.major),
	}, nil
}

// semverTilde is `~` — "reasonably close to", which allows patch-level
// changes when a minor is written and minor-level changes when one is not.
func semverTilde(fn, whole, rest string) ([]semverComp, error) {
	p, err := parseSemverPartial(fn, whole, rest)
	if err != nil {
		return nil, err
	}
	if p.xMajor {
		return []semverComp{{op: semverOpAny}}, nil
	}
	if p.xMinor {
		return []semverComp{
			{op: semverOpGTE, ver: semverBound(p.major, 0, 0, false)},
			semverUpperMajor(p.major),
		}, nil
	}
	lo := semverBound(p.major, p.minor, 0, false)
	if !p.xPatch {
		lo = p.exact()
	}
	return []semverComp{
		{op: semverOpGTE, ver: lo},
		semverUpperMinor(p.major, p.minor),
	}, nil
}

// semverSplitOperator takes the comparator's operator off the front. `~>` is
// accepted for `~` because Rubygems spelled it that way and node accepts it.
func semverSplitOperator(tok string) (string, string) {
	for _, op := range []string{">=", "<=", "~>", ">", "<", "=", "^", "~"} {
		if strings.HasPrefix(tok, op) {
			return op, strings.TrimSpace(tok[len(op):])
		}
	}
	return "", tok
}

// parseSemverPartial reads a version with any of its components left unsaid.
//
// It shares NOTHING with parseSemver's leniency: a component that is present
// is read by semverNumericIdentifier, so `01.2.3` is refused in a range
// exactly as it is refused as a version, and a numeric identifier past int64
// is refused in both. The leading `v` concession is the same one.
func parseSemverPartial(fn, whole, s string) (semverPartial, error) {
	var p semverPartial
	s = strings.TrimSpace(s)
	if s == "" {
		return p, errBadSemverRange(fn, whole)
	}
	if s[0] == 'v' || s[0] == 'V' {
		s = s[1:]
	}
	if s == "*" || s == "x" || s == "X" || s == "" {
		p.xMajor, p.xMinor, p.xPatch = true, true, true
		return p, nil
	}
	if i := strings.IndexByte(s, '+'); i >= 0 {
		p.build = s[i+1:]
		s = s[:i]
		if !semverIdentifierListOK(p.build, true) {
			return semverPartial{}, errBadSemverRange(fn, whole)
		}
	}
	if i := strings.IndexByte(s, '-'); i >= 0 {
		pre := s[i+1:]
		s = s[:i]
		if !semverIdentifierListOK(pre, false) {
			return semverPartial{}, errBadSemverRange(fn, whole)
		}
		p.pre = pre
		p.hasPre = true
	}
	parts := strings.Split(s, ".")
	if len(parts) > 3 {
		return semverPartial{}, errBadSemverRange(fn, whole)
	}
	fields := [3]*int64{&p.major, &p.minor, &p.patch}
	xs := [3]*bool{&p.xMajor, &p.xMinor, &p.xPatch}
	for i := 0; i < 3; i++ {
		if i >= len(parts) {
			*xs[i] = true
			continue
		}
		part := parts[i]
		if part == "x" || part == "X" || part == "*" {
			*xs[i] = true
			continue
		}
		n, ok := semverNumericIdentifier(part)
		if !ok {
			return semverPartial{}, errBadSemverRange(fn, whole)
		}
		*fields[i] = n
	}
	// An X below a named component makes every component below it unsaid too:
	// `1.x.3` is `1.x.x`, which is the only reading that keeps the desugaring
	// rules from having to invent a meaning for a hole in the middle.
	if p.xMajor {
		p.xMinor, p.xPatch = true, true
	}
	if p.xMinor {
		p.xPatch = true
	}
	// A pre-release or build on a partial is refused rather than ignored:
	// `1.2.x-beta` says something the grammar cannot honour, and node-semver
	// drops it silently. See this file's header.
	if (p.hasPre || p.build != "") && (p.xMajor || p.xMinor || p.xPatch) {
		return semverPartial{}, errBadSemverRange(fn, whole)
	}
	return p, nil
}

func errBadSemverRange(fn, text string) error {
	return sqlerr.New("22023", "%s: %s is not a version range", fn, sqlerr.Quote(text))
}

// semverSatisfies answers node-semver's `satisfies`: the version is in the
// range when it is in ANY of the range's intersections, and a PRE-RELEASE
// version is in an intersection only when some comparator of that same
// intersection pins the same [major, minor, patch] AND itself names a
// pre-release.
func semverSatisfies(v semverVersion, r semverRange) bool {
	for _, set := range r {
		if semverSatisfiesSet(v, set) {
			return true
		}
	}
	return false
}

func semverSatisfiesSet(v semverVersion, set []semverComp) bool {
	for _, c := range set {
		if !semverCompTest(v, c) {
			return false
		}
	}
	if !v.hasPre {
		return true
	}
	// The pre-release rule. A bare `*` carries no bound and therefore names
	// no tuple, so it can never admit a pre-release — which is why
	// `semver_satisfies('1.0.0-beta','*')` is FALSE, node-semver's own answer.
	for _, c := range set {
		if c.op == semverOpAny || !c.ver.hasPre {
			continue
		}
		if c.ver.major == v.major && c.ver.minor == v.minor && c.ver.patch == v.patch {
			return true
		}
	}
	return false
}

func semverCompTest(v semverVersion, c semverComp) bool {
	if c.op == semverOpAny {
		return true
	}
	cmp := compareSemver(v, c.ver)
	switch c.op {
	case semverOpLT:
		return cmp < 0
	case semverOpLTE:
		return cmp <= 0
	case semverOpGT:
		return cmp > 0
	case semverOpGTE:
		return cmp >= 0
	default:
		return cmp == 0
	}
}

// ---------------------------------------------------------------------------
// The compiled-range memo.

// semverRangeMemoCap bounds the memo the way temporalMemo is bounded, and for
// the same reason: the range is usually one literal per query, but nothing
// stops it being a COLUMN, and an unbounded process-wide map keyed by user
// data is a memory leak with a query for a key.
const semverRangeMemoCap = 1024

type semverRangeMemo struct {
	m sync.Map
	n atomic.Int64
}

// semverRangeParse is one range text's outcome — the compiled range or the
// refusal it earned. It is allocated ONCE per distinct text and held by
// POINTER, which is what lets the hot path below publish the last one it used
// without allocating anything.
type semverRangeParse struct {
	text string
	r    semverRange
	err  error
}

var semverRangeCache semverRangeMemo

// semverRangeLast is the last range text this process compiled.
//
// A range is ONE LITERAL per query in every shape but a range-valued column,
// so after the first row every row reads this and never reaches the memo. What
// that saves is MEASURED rather than assumed, because the round-1 review's
// reading of the benchmark — that a sync.Map lookup boxes its string key and
// therefore allocates once per row — does not reproduce: the lookup allocates
// NOTHING (`BenchmarkSemverRangeMemoLookup`, 0 allocs/op either way; the
// predicate benchmark's one allocation per row is the registry's `[]any`
// argument seam, which is every scalar function's and not this one's). What it
// costs is the hash and the map probe, and an atomic pointer load with a
// string compare is about five times cheaper: 30.6 µs against 6.1 µs per 2048
// lookups, which is 476.8 µs against 444.4 µs on BenchmarkSemverSatisfies.
// A miss falls through to the bounded memo, which is unchanged and is still
// what keeps a range-valued COLUMN from parsing the same text twice.
var semverRangeLast atomic.Pointer[semverRangeParse]

func (c *semverRangeMemo) load(s string) (*semverRangeParse, bool) {
	v, ok := c.m.Load(s)
	if !ok {
		return nil, false
	}
	return v.(*semverRangeParse), true
}

func (c *semverRangeMemo) store(s string, r *semverRangeParse) {
	if c.n.Load() >= semverRangeMemoCap {
		// Drop the generation, as temporalMemo does. Concurrent stores may
		// overshoot by however many are in flight, which is bounded by the
		// worker count.
		c.m.Clear()
		c.n.Store(0)
	}
	if _, loaded := c.m.LoadOrStore(s, r); !loaded {
		c.n.Add(1)
	}
}

func (c *semverRangeMemo) entries() int {
	n := 0
	c.m.Range(func(_, _ any) bool { n++; return true })
	return n
}

func (c *semverRangeMemo) reset() {
	c.m.Clear()
	c.n.Store(0)
	semverRangeLast.Store(nil)
}

// parseSemverRangeCached is the per-row entry point. The memo key is the range
// TEXT and the memoized value carries the REFUSAL as well as the parse, so a
// bad range raises identically on the first row and on every row after it —
// a cache that remembered only successes would make the error depend on
// whether some earlier row had warmed it.
//
// The one-entry fast path in front of the memo is the allocation, not the
// parse: see semverRangeLast.
func parseSemverRangeCached(fn, text string) (semverRange, error) {
	if p := semverRangeLast.Load(); p != nil && p.text == text {
		return p.r, p.err
	}
	p, ok := semverRangeCache.load(text)
	if !ok {
		r, err := ParseSemverRange(fn, text)
		p = &semverRangeParse{text: text, r: r, err: err}
		semverRangeCache.store(text, p)
	}
	semverRangeLast.Store(p)
	return p.r, p.err
}

// fnSemverSatisfies is `semver_satisfies(version, range)`.
//
// THE ORDER OF THE TWO ARGUMENTS' DISPOSITIONS IS THE CONTRACT, and it is A2's
// rule under a different name: the RANGE is read FIRST, so a range this
// grammar does not know is 22023 whatever the version argument holds —
// including NULL, and including a query that reaches no rows at all (the
// literal check refuses that one before execution). A NULL range is not a
// misspelling but an absent operand, and answers NULL, exactly as a NULL flag
// NAME does. Only then does a NULL or unparseable VERSION answer NULL.
func fnSemverSatisfies(args []any) any {
	if len(args) != 2 {
		return nil
	}
	if args[1] == nil {
		return nil
	}
	text, ok := args[1].(string)
	if !ok {
		text = toString(args[1])
	}
	r, err := parseSemverRangeCached("semver_satisfies", text)
	if err != nil {
		panic(fatalEval{err})
	}
	v, ok := semverArg(args[0])
	if !ok {
		return nil
	}
	return semverSatisfies(v, r)
}
