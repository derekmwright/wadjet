package expr

import (
	"regexp"
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
	group int // 1-9 for a backreference, -1 for a literal segment
}

// preparedRegexp is the compile-once state for a regexp_replace call with
// literal pattern and replacement. ok=false means the shape didn't
// qualify (bad pattern) and callers must use the generic path.
type preparedRegexp struct {
	re   *regexp.Regexp
	segs []replSeg
	ok   bool
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
		}
		lit.WriteByte(c)
	}
	flush()
	return segs
}

// prepareRegexpReplace builds the prepared state for literal pattern and
// replacement strings.
func prepareRegexpReplace(pattern, repl string) *preparedRegexp {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return &preparedRegexp{}
	}
	return &preparedRegexp{re: re, segs: parseSQLReplacement(repl), ok: true}
}

// replaceAll is ReplaceAllString with the template pre-parsed. Splicing
// FindAllStringSubmatchIndex results reproduces ReplaceAllString's output
// exactly: both use the same non-overlapping match iteration (including
// empty-match advancement), and an out-of-range or unmatched group
// expands to nothing, as with Expand.
//
// The no-match case returns src unchanged — callers passing zero-copy
// views into batch memory get the view back, which is safe for the
// per-batch memo (values never outlive the batch) and for SetValue
// (which copies into the output vector).
func (p *preparedRegexp) replaceAll(src string) string {
	matches := p.re.FindAllStringSubmatchIndex(src, -1)
	if len(matches) == 0 {
		return src
	}
	var b strings.Builder
	b.Grow(len(src))
	last := 0
	for _, m := range matches {
		b.WriteString(src[last:m[0]])
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
		last = m[1]
	}
	b.WriteString(src[last:])
	return b.String()
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
		e.prepared = prepareRegexpReplace(toString(pat.Val), toString(rep.Val))
	})
	if e.prepared == nil || !e.prepared.ok {
		return nil
	}
	return e.prepared
}
