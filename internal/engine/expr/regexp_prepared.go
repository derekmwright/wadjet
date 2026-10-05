// SPDX-License-Identifier: MIT

package expr

import (
	"regexp"
	"regexp/syntax"
	"strings"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Prepared regexp_replace: when the pattern and replacement are literals
// (the memoizable shape — see tryEvalMemoized), everything except the
// match itself can be done ONCE per compiled expression instead of once
// per evaluated value:
//
//   - the pattern compiles once (previously a cache lookup per call);
//   - the SQL replacement template ("\1" backreferences) parses once into
//     literal/group segments (previously sqlBackrefsToGo re-built the Go
//     template string per call, and ReplaceAllString re-parsed that
//     template on every match via Expand);
//   - evaluation is a direct string→string call with no []any boxing.
//
// Profile motivation (ClickBench Q29, REGEXP_REPLACE over Referer): the
// per-call template machinery and boxing sat next to the actual RE2 match
// as measurable overhead even after per-batch memoization.
var preparedRegexpToggle = optswitch.Register("prepared-regexp", "WADJET_PREPARED_REGEXP",
	"compile-once regexp_replace with pre-parsed replacement template")

// replSeg is one piece of a parsed replacement template: a literal chunk,
// or a capture-group backreference.
type replSeg struct {
	lit   string
	group int // 0 for the whole match (\\&), 1-9 for a backreference, -1 for a literal segment
}

// preparedRegexp is the compile-once state for a regexp_replace call with
// literal pattern and replacement. ok=false means the shape didn't
// qualify (bad pattern) and callers must use the generic path.
type preparedRegexp struct {
	re       *regexp.Regexp
	segs     []replSeg
	anchored bool // every match must start at offset 0 → at most one match
	ok       bool
	// global: every match is replaced (the 'g' flag); otherwise the first.
	global bool
	// emptyAt, when set, makes replaceAll take PostgreSQL's matches rather
	// than Go's: an empty match right after a non-empty one is a match
	// there (regexp_replace('q"b', 'q*', '-', 'g') is `--"-b-`), where Go's
	// FindAll drops it (`-"-b-`). It reports whether the pattern matches the
	// empty string at offset e of src.
	emptyAt func(src string, e int) bool
}

// withAbuttingEmpty is matches (Go's, leftmost-first) with the empty match
// PostgreSQL also takes at the end e of every non-empty match: Go's search
// from e found a match there and dropped it only because it was empty and
// abutted the previous one — so when Go's next match does not start at e,
// the preferred match at e was empty exactly when the pattern can match the
// empty string at e. Its groups are empty or unset, which expand alike.
func withAbuttingEmpty(emptyAt func(string, int) bool, src string, matches [][]int) [][]int {
	var out [][]int // nil until the first insertion
	for i, m := range matches {
		e := m[1]
		insert := m[0] != e && (i+1 == len(matches) || matches[i+1][0] != e) && emptyAt(src, e)
		if insert && out == nil {
			out = append(make([][]int, 0, len(matches)+1), matches[:i]...)
		}
		if out != nil {
			out = append(out, m)
		}
		if insert {
			em := make([]int, len(m))
			for j := range em {
				em[j] = -1
			}
			em[0], em[1] = e, e
			out = append(out, em)
		}
	}
	if out == nil {
		return matches
	}
	return out
}

// emptyMatcherOf is emptyAt for a parsed, simplified Go pattern: whether it
// matches the empty string at an offset, given the zero-width assertions
// true there.
func emptyMatcherOf(re *syntax.Regexp) func(string, int) bool {
	return func(src string, e int) bool {
		return canMatchEmpty(re, src, e)
	}
}

func canMatchEmpty(re *syntax.Regexp, src string, e int) bool {
	switch re.Op {
	case syntax.OpEmptyMatch, syntax.OpStar, syntax.OpQuest:
		return true
	case syntax.OpLiteral:
		return len(re.Rune) == 0
	case syntax.OpCapture, syntax.OpPlus:
		return canMatchEmpty(re.Sub[0], src, e)
	case syntax.OpRepeat:
		return re.Min == 0 || canMatchEmpty(re.Sub[0], src, e)
	case syntax.OpConcat:
		for _, sub := range re.Sub {
			if !canMatchEmpty(sub, src, e) {
				return false
			}
		}
		return true
	case syntax.OpAlternate:
		for _, sub := range re.Sub {
			if canMatchEmpty(sub, src, e) {
				return true
			}
		}
		return false
	case syntax.OpBeginText:
		return e == 0
	case syntax.OpEndText:
		return e == len(src)
	case syntax.OpBeginLine:
		return e == 0 || src[e-1] == '\n'
	case syntax.OpEndLine:
		return e == len(src) || src[e] == '\n'
	case syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		isWord := func(i int) bool {
			if i < 0 || i >= len(src) {
				return false
			}
			c := src[i]
			return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		}
		boundary := isWord(e-1) != isWord(e)
		return boundary == (re.Op == syntax.OpWordBoundary)
	}
	return false
}

// withTemplate is p with replacement template repl, sharing p's compiled
// pattern.
func (p *preparedRegexp) withTemplate(repl string) *preparedRegexp {
	q := *p
	q.segs = parseSQLReplacement(repl)
	return &q
}

// replace is regexp_replace's answer for src: the first match replaced, or
// every match under the 'g' flag.
func (p *preparedRegexp) replace(src string) string {
	if p.global {
		return p.replaceAll(src)
	}
	m := p.re.FindStringSubmatchIndex(src)
	if m == nil {
		return src
	}
	return p.spliceOne(src, m)
}

// parseSQLReplacement splits a SQL-convention replacement string into
// segments. Semantics mirror sqlBackrefsToGo + Go's Expand exactly:
// \1..\9 are backreferences, \\ is a literal backslash, any other
// backslash pair stays literal, and $ has no special meaning.
func parseSQLReplacement(repl string) []replSeg {
	var segs []replSeg
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			segs = append(segs, replSeg{lit: lit.String(), group: -1})
			lit.Reset()
		}
	}
	for i := 0; i < len(repl); i++ {
		c := repl[i]
		if c == '\\' && i+1 < len(repl) {
			next := repl[i+1]
			if next >= '1' && next <= '9' {
				flush()
				segs = append(segs, replSeg{group: int(next - '0')})
				i++
				continue
			}
			if next == '\\' {
				lit.WriteByte('\\')
				i++
				continue
			}
			if next == '&' {
				// \& is the whole match (PostgreSQL's replacement syntax).
				flush()
				segs = append(segs, replSeg{group: 0})
				i++
				continue
			}
		}
		lit.WriteByte(c)
	}
	flush()
	return segs
}

// preparedFrom is the prepared state over a pattern translateAndCompile
// compiled, with no replacement template yet (withTemplate adds it).
func preparedFrom(rx *sqlRegex, global bool) *preparedRegexp {
	return &preparedRegexp{
		re:       rx.re,
		anchored: rx.anchored,
		emptyAt:  rx.emptyAt,
		global:   global,
		ok:       true,
	}
}

// anchoredAtTextStart reports whether every match of pattern must begin at
// offset 0 of the subject — which makes at most one match possible, so
// replaceAll can call FindStringSubmatchIndex once instead of asking
// FindAllStringSubmatchIndex to build a slice-of-slices.
//
// Detection is structural, via the same parse regexp.Compile uses (never a
// "starts with ^" string test, which would misread `[^/]+`, `\^`, or an
// alternation anchored on only its first branch). The claim rests on
// OpBeginText — Go's `^` outside (?m), plus `\A` — matching only at
// absolute position 0 even when the search resumes from a later offset;
// OpBeginLine (`^` under (?m)) does not qualify and parses to a different
// op, so the multiline case falls out automatically. Empty first matches
// are safe for the same reason: FindAll's advance-past-empty-match retry
// starts at offset 1, where OpBeginText cannot match.
func anchoredAtTextStart(pattern string) bool {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return false
	}
	return beginsWithTextAnchor(re)
}

// beginsWithTextAnchor walks only the operators through which "the match
// starts here" propagates unconditionally. Everything else — including
// OpStar and OpQuest, whose sub-expression may be skipped entirely, so
// `(^a)*` can match empty anywhere — answers false and keeps the generic
// path.
func beginsWithTextAnchor(re *syntax.Regexp) bool {
	switch re.Op {
	case syntax.OpBeginText:
		return true
	case syntax.OpCapture, syntax.OpPlus:
		// x+ must match x at least once, at the same start offset.
		return len(re.Sub) == 1 && beginsWithTextAnchor(re.Sub[0])
	case syntax.OpConcat:
		return len(re.Sub) > 0 && beginsWithTextAnchor(re.Sub[0])
	case syntax.OpAlternate:
		if len(re.Sub) == 0 {
			return false
		}
		for _, sub := range re.Sub {
			if !beginsWithTextAnchor(sub) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// replaceAll is ReplaceAllString with the template pre-parsed. Splicing
// submatch indices reproduces ReplaceAllString's output exactly: both use
// the same non-overlapping match iteration (including empty-match
// advancement), and an out-of-range or unmatched group expands to nothing,
// as with Expand. An anchored pattern skips the iteration entirely, since
// it cannot match twice.
//
// The no-match case returns src unchanged — callers passing zero-copy
// views into batch memory get the view back, which is safe for the
// per-batch memo (values never outlive the batch) and for SetValue
// (which copies into the output vector).
func (p *preparedRegexp) replaceAll(src string) string {
	if p.anchored {
		// At most one match (see anchoredAtTextStart): one []int instead
		// of FindAll's slice-of-slices plus its scan of the remainder.
		m := p.re.FindStringSubmatchIndex(src)
		if m == nil {
			return src
		}
		return p.spliceOne(src, m)
	}
	matches := p.re.FindAllStringSubmatchIndex(src, -1)
	if p.emptyAt != nil {
		matches = withAbuttingEmpty(p.emptyAt, src, matches)
	}
	if len(matches) == 0 {
		return src
	}
	if len(matches) == 1 {
		return p.spliceOne(src, matches[0])
	}
	var b strings.Builder
	b.Grow(len(src))
	last := 0
	for _, m := range matches {
		b.WriteString(src[last:m[0]])
		p.expand(&b, src, m)
		last = m[1]
	}
	b.WriteString(src[last:])
	return b.String()
}

// spliceOne builds the result for a single match. Two things the general
// loop can't do once the match count is known to be one: size the output
// buffer exactly (Q29 turns a ~70-byte URL into a ~15-byte host, where
// Grow(len(src)) over-allocates by 4-5x), and recognise the pure-extract
// shape — replacement is one backreference and the match spans the whole
// subject — whose result IS a substring of src, returnable with no
// allocation at all. That zero-copy return has the same contract as the
// no-match `return src` above: valid for the batch, and the memo/SetValue
// callers copy on the way out.
func (p *preparedRegexp) spliceOne(src string, m []int) string {
	if len(p.segs) == 1 && p.segs[0].group > 0 && m[0] == 0 && m[1] == len(src) {
		hi := 2*p.segs[0].group + 1
		if hi < len(m) && m[hi-1] >= 0 {
			return src[m[hi-1]:m[hi]]
		}
		return "" // unmatched or out-of-range group expands to nothing
	}
	size := m[0] + (len(src) - m[1])
	for _, seg := range p.segs {
		if seg.group < 0 {
			size += len(seg.lit)
			continue
		}
		hi := 2*seg.group + 1
		if hi < len(m) && m[hi-1] >= 0 {
			size += m[hi] - m[hi-1]
		}
	}
	var b strings.Builder
	b.Grow(size)
	b.WriteString(src[:m[0]])
	p.expand(&b, src, m)
	b.WriteString(src[m[1]:])
	return b.String()
}

// expand writes the replacement template for one match, with the same
// semantics as Go's Expand: an unmatched or out-of-range group
// contributes nothing.
func (p *preparedRegexp) expand(b *strings.Builder, src string, m []int) {
	for _, seg := range p.segs {
		if seg.group < 0 {
			b.WriteString(seg.lit)
			continue
		}
		hi := 2*seg.group + 1
		if hi < len(m) && m[hi-1] >= 0 {
			b.WriteString(src[m[hi-1]:m[hi]])
		}
	}
}

// preparedReplace returns the compile-once state for this call, building
// it on first use. Returns nil when the call isn't a qualifying
// regexp_replace (wrong name, non-literal pattern/replacement, or the
// kill switch is off). Safe for the concurrent goroutines that share a
// *FuncCall.
func (e *FuncCall) preparedReplace() *preparedRegexp {
	if !preparedRegexpToggle.On() || !strings.EqualFold(e.Name, "regexp_replace") || len(e.Args) < 3 {
		return nil
	}
	e.prepOnce.Do(func() {
		pat, okP := e.Args[1].(*Lit)
		rep, okR := e.Args[2].(*Lit)
		if !okP || !okR || pat.Val == nil || rep.Val == nil {
			return
		}
		flags := ""
		if len(e.Args) >= 4 {
			fl, okF := e.Args[3].(*Lit)
			if !okF || len(e.Args) > 4 {
				return
			}
			f, isText := fl.Val.(string)
			if !isText {
				return
			}
			flags = f
		}
		// A pattern or flags the generic path refuses leaves e.prepared nil,
		// and the per-row path raises the refusal.
		p, err := cachedRegexpReplace(toString(pat.Val), flags)
		if err != nil {
			return
		}
		e.prepared = p.withTemplate(toString(rep.Val))
	})
	if e.prepared == nil || !e.prepared.ok {
		return nil
	}
	return e.prepared
}
