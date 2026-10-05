// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// PostgreSQL's POSIX regular-expression match operators: `~` (textregexeq),
// `~*` (texticregexeq), `!~` (textregexne) and `!~*` (texticregexne).
//
// PostgreSQL matches with its own engine, whose language is the ARE
// ("advanced regular expression", §9.7.3). This engine matches with Go's
// RE2, which reads a different language under mostly the same spelling. The
// match is therefore not handed the client's pattern: aregexToRE2 TRANSLATES
// it, form by form, into the RE2 pattern that matches the same strings, and a
// form RE2 has no equivalent for is REFUSED rather than read as something
// else. The forms that read differently under the same spelling are the
// point — `\b` is a backspace in an ARE and a word boundary in RE2, `.`
// matches a newline in an ARE and not in RE2, `\Z` is end-of-string there and
// unknown here — and a pattern handed over verbatim would answer a different
// question for each of them.
//
// Only whether a match EXISTS is asked, so the two engines' different match
// preferences (an ARE's leftmost-longest against RE2's leftmost-first) cannot
// change an answer: some match exists under one exactly when one exists under
// the other.
func init() {
	for name, spec := range map[string]struct {
		icase, negate bool
	}{
		"textregexeq":   {false, false},
		"texticregexeq": {true, false},
		"textregexne":   {false, true},
		"texticregexne": {true, true},
	} {
		icase, negate := spec.icase, spec.negate
		DefaultRegistry.Register(name, func(args []any) any {
			return pgRegexMatch(args, icase, negate)
		}, RetBool)
		stringInputFuncs[name] = true
	}
}

func pgRegexMatch(args []any, icase, negate bool) any {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := mustCompileSQLRegex(toString(args[1]), reFlags{icase: icase})
	matched := re.re.MatchString(toString(args[0]))
	return matched != negate
}

// reFlags is a regular-expression function's flags argument as PostgreSQL's
// parse_re_flags reads it: the options the pattern compiles under (icase,
// literal) and the one that chooses how many matches a function takes
// (global).
type reFlags struct {
	icase, literal, global bool
	// The newline-sensitivity and syntax options (PostgreSQL §9.7.3.5,
	// Table 9.25): nlStop — `.` and a negated bracket expression do not
	// match a newline (n, m, p); nlAnchor — `^` and `$` match at a line's
	// start and end (n, m, w); expanded — white space and #-comments in the
	// pattern are ignored (x).
	nlStop, nlAnchor, expanded bool
	// dialect is the language the pattern is written in.
	dialect regexDialect
}

// regexDialect names the language a SQL construct's pattern is read in. The
// dialect is chosen by the function's ORIGIN: PostgreSQL's own constructs
// read PostgreSQL's ARE; the engine's DuckDB-origin functions — which
// PostgreSQL does not have, and which every engine that has them reads in
// RE2 syntax (`\b` a word boundary) — read RE2, with DuckDB as their oracle.
// Every function that takes a pattern declares its dialect in
// regexFunctionDialect.
type regexDialect uint8

const (
	// dialectARE is PostgreSQL's advanced regular expression, translated
	// into RE2 by aregexToRE2 (or refused where RE2 cannot express it).
	dialectARE regexDialect = iota
	// dialectRE2 is the pattern as written, compiled by RE2 with its own
	// leftmost-first preference and no translation.
	dialectRE2
)

// regexFunctionDialect is the declaration: every registered function that
// compiles a SQL pattern, and the dialect its origin gives it. The registry
// walk (TestArcRXEveryRegexFunctionDeclaresItsDialect) fails a function that
// takes a pattern without a row here, and a row whose function answers in
// the other dialect.
var regexFunctionDialect = map[string]regexDialect{
	// PostgreSQL's constructs.
	"textregexeq":    dialectARE, // ~
	"texticregexeq":  dialectARE, // ~*
	"textregexne":    dialectARE, // !~
	"texticregexne":  dialectARE, // !~*
	"similar_to":     dialectARE, // SIMILAR TO (its rewrite is an ARE)
	"substring":      dialectARE, // substring(s FROM pattern)
	"regexp_replace": dialectARE,
	"regexp_like":    dialectARE,
	"regexp_count":   dialectARE,
	// The engine's own, DuckDB-origin functions.
	"regexp_extract":     dialectRE2,
	"regexp_extract_all": dialectRE2,
	"regexp_split":       dialectRE2,
	"payload_matches":    dialectRE2,
}

// ownFunctionFlags are the flags the engine's own (DuckDB-origin) regular-
// expression functions compile their pattern under: RE2, as written.
var ownFunctionFlags = reFlags{dialect: dialectRE2}

// ownFunctionRegex is the compiled pattern of one of the engine's own
// regular-expression functions, or nil when RE2 rejects the pattern — the
// function then answers NULL, as these functions always have.
func ownFunctionRegex(pattern string) *regexp.Regexp {
	re, err := translateAndCompile(pattern, ownFunctionFlags)
	if err != nil {
		return nil
	}
	return re.re
}

// cacheOpts packs every option the compile reads into one byte, so the
// cache key stays a string and a byte.
func (f reFlags) cacheOpts() uint8 {
	o := uint8(f.dialect) << 5
	if f.icase {
		o |= 1
	}
	if f.literal {
		o |= 2
	}
	if f.nlStop {
		o |= 4
	}
	if f.nlAnchor {
		o |= 8
	}
	if f.expanded {
		o |= 16
	}
	return o
}

// reMode is the part of reFlags the translation reads.
func (f reFlags) mode() reMode {
	return reMode{icase: f.icase, nlStop: f.nlStop, nlAnchor: f.nlAnchor, expanded: f.expanded, dialect: f.dialect}
}

// parseREFlags reads the flags string of the regular-expression function
// fn. The letters, measured on 17.11:
//
//	g      every match — only regexp_replace takes it; any other function
//	       raises 22023 "<fn>() does not support the "global" option"
//	i, c   case-insensitive / case-sensitive, the last one written wins
//	q      the pattern is a literal string
//	s      non-newline-sensitive (the default)
//	n, m   newline-sensitive: `.` and [^…] stop at a newline, ^ $ match at lines
//	p      partial: `.` and [^…] stop at a newline, ^ $ at the string's ends
//	w      inverse partial: ^ $ match at lines, `.` and [^…] cross a newline
//	x, t   expanded syntax (white space and #-comments ignored) / tight
//
// The basic and extended dialects (b, e) are refused 0A000, as the
// translator refuses their embedded-option spellings; any other letter is
// PostgreSQL's own 22023.
func parseREFlags(fn, flags string) (reFlags, error) {
	var f reFlags
	var unsupported rune
	for _, r := range flags {
		switch r {
		case 'g':
			if fn != "regexp_replace" {
				return f, sqlerr.New("22023", "%s() does not support the \"global\" option", fn)
			}
			f.global = true
		case 'i':
			f.icase = true
		case 'c':
			f.icase = false
		case 'q':
			f.literal = true
		case 's', 'n', 'm', 'p', 'w':
			f.nlStop, f.nlAnchor = newlineOption(r)
		case 'x':
			f.expanded = true
		case 't':
			f.expanded = false
		case 'b', 'e':
			if unsupported == 0 {
				unsupported = r
			}
		default:
			return f, sqlerr.New("22023", "invalid regular expression option: %s", sqlerr.Quote(string(r)))
		}
	}
	if unsupported != 0 {
		return f, sqlerr.New("0A000", "%s flag %s is not supported", fn, sqlerr.Quote(string(unsupported)))
	}
	return f, nil
}

// newlineOption is the (nlStop, nlAnchor) pair a newline option letter
// sets — the letter written last wins, as on the server.
func newlineOption(r rune) (nlStop, nlAnchor bool) {
	switch r {
	case 'n', 'm':
		return true, true
	case 'p':
		return true, false
	case 'w':
		return false, true
	}
	return false, false // s
}

// sqlRegex is a pattern a SQL construct supplied, compiled in its
// dialect: an ARE translated into RE2 (aregexToRE2) under its flags, or an
// RE2 pattern as written (only re is set).
type sqlRegex struct {
	re *regexp.Regexp
	// emptyAt reports whether the pattern matches the empty string at
	// offset e of src; findAll uses it to take the empty match PostgreSQL
	// takes right after a non-empty one (see withAbuttingEmpty).
	emptyAt func(src string, e int) bool
	// anchored: every match starts at offset 0 (anchoredAtTextStart).
	anchored bool
	// readsLeft: the pattern holds an empty-width assertion that reads the
	// character before a position (\A, ^, \y, \Y), so it cannot be matched
	// over a suffix of the subject as if the suffix were the whole text.
	readsLeft bool
}

// translateAndCompile is THE compile of a SQL-supplied pattern: every
// construct that takes one — the `~` operators, SIMILAR TO, substring(s
// FROM p), regexp_like, regexp_count, regexp_replace, regexp_extract,
// regexp_extract_all, regexp_split, payload_matches — compiles it here, in
// the dialect its origin declares (regexFunctionDialect). Under the RE2
// dialect the pattern compiles as written. Under the ARE dialect (#1499) it
// is translated by aregexToRE2 (or quoted, under the q flag); a form RE2
// cannot express is refused 0A000 and a malformed one is 2201B, never a
// NULL or a match read in another dialect. An ARE whose quantifiers are all
// greedy prefers the LONGEST match at the leftmost position, as an ARE does
// (§9.7.3.5: `substring('abc' FROM 'a|ab')` is ab where RE2's leftmost-first
// answers a); one holding a non-greedy quantifier keeps RE2's leftmost-first
// preference (catalog r24).
//
// The result — a compiled regex or the refusal — is cached per (pattern,
// options) in a bounded cache (regexCacheBound entries): a literal pattern
// compiles once per process, and a column of distinct patterns cannot grow
// the cache past the bound.
func translateAndCompile(pattern string, f reFlags) (*sqlRegex, error) {
	key := regexCacheKey{pattern: pattern, opts: f.cacheOpts()}
	if v, ok := regexCache[key.opts].Load(pattern); ok {
		e := v.(*regexCacheEntry)
		return e.re, e.err
	}
	re, err := compileSQLRegex(pattern, f)
	regexCacheStore(key, &regexCacheEntry{re: re, err: err})
	return re, err
}

// mustCompileSQLRegex is translateAndCompile for an evaluator: a refusal is
// raised.
func mustCompileSQLRegex(pattern string, f reFlags) *sqlRegex {
	re, err := translateAndCompile(pattern, f)
	if err != nil {
		panic(fatalEval{err})
	}
	return re
}

func compileSQLRegex(pattern string, f reFlags) (*sqlRegex, error) {
	regexCompiles.Add(1)
	if f.dialect == dialectRE2 {
		// The pattern as written, RE2's leftmost-first preference, no
		// empty match beside a non-empty one: the reading these functions
		// have always had.
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, sqlerr.New("2201B", "invalid regular expression: %s", reErrorText(err))
		}
		return &sqlRegex{re: re}, nil
	}
	var translated string
	if f.literal {
		translated = "(?s)" + literalARE(pattern, f.icase)
	} else {
		var err error
		if translated, err = aregexToRE2(pattern, f.mode()); err != nil {
			return nil, err
		}
	}
	re, err := regexp.Compile(translated)
	if err != nil {
		return nil, sqlerr.New("2201B", "invalid regular expression: %s", reErrorText(err))
	}
	// One parse answers every structural question about the pattern.
	parsed, perr := syntax.Parse(translated, syntax.Perl)
	if perr != nil {
		return nil, sqlerr.New("2201B", "invalid regular expression: %s", reErrorText(perr))
	}
	if !hasLazyIn(parsed) {
		re.Longest()
	}
	return &sqlRegex{
		re:        re,
		emptyAt:   emptyMatcherOf(parsed.Simplify()),
		anchored:  beginsWithTextAnchor(parsed),
		readsLeft: readsLeftContext(parsed),
	}, nil
}

// readsLeftContext reports whether a parsed pattern holds an empty-width
// assertion whose answer depends on the text before the position.
func readsLeftContext(re *syntax.Regexp) bool {
	switch re.Op {
	case syntax.OpBeginText, syntax.OpBeginLine, syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		return true
	}
	for _, sub := range re.Sub {
		if readsLeftContext(sub) {
			return true
		}
	}
	return false
}

// findAll is every match PostgreSQL takes in src, as submatch index slices:
// RE2's non-overlapping matches plus the empty match an ARE also takes at
// the end of a non-empty one (`regexp_count('baaac', 'a*')` is 4 there and
// 3 under Go's FindAll).
func (r *sqlRegex) findAll(src string) [][]int {
	matches := r.re.FindAllStringSubmatchIndex(src, -1)
	if r.emptyAt != nil {
		matches = withAbuttingEmpty(r.emptyAt, src, matches)
	}
	return matches
}

// The bounded compile cache: one sync.Map per option byte, keyed by the
// pattern text (lock-free reads of a string key — the pattern is almost
// always a constant looked up once per row), holding at most
// regexCacheBound patterns. A ring of the stored keys, under a mutex taken
// only on a miss, names the victim: a store past the bound evicts ONE entry,
// the oldest (first in, first out), never the whole cache — so a working set
// under the bound keeps hitting after its first pass.
const regexCacheBound = 4096

type regexCacheKey struct {
	pattern string
	opts    uint8 // reFlags.cacheOpts: every option the compile reads
}

type regexCacheEntry struct {
	re  *sqlRegex
	err error
}

var (
	regexCache [64]sync.Map // [reFlags.cacheOpts] pattern → *regexCacheEntry
	regexRing  struct {
		mu   sync.Mutex
		keys []regexCacheKey
		next int
	}
	// regexCompiles counts compileSQLRegex calls (the benchmarks and the
	// cache gate read it).
	regexCompiles atomic.Int64
)

func regexCacheStore(k regexCacheKey, e *regexCacheEntry) {
	if _, loaded := regexCache[k.opts].LoadOrStore(k.pattern, e); loaded {
		return
	}
	regexRing.mu.Lock()
	if len(regexRing.keys) < regexCacheBound {
		regexRing.keys = append(regexRing.keys, k)
		regexRing.mu.Unlock()
		return
	}
	victim := regexRing.keys[regexRing.next]
	regexRing.keys[regexRing.next] = k
	regexRing.next = (regexRing.next + 1) % regexCacheBound
	regexRing.mu.Unlock()
	regexCache[victim.opts].Delete(victim.pattern)
}

// regexCacheLen is the number of patterns the cache holds.
func regexCacheLen() int {
	n := 0
	for i := range regexCache {
		regexCache[i].Range(func(_, _ any) bool { n++; return true })
	}
	return n
}

func reErrorText(err error) string {
	msg := err.Error()
	if i := strings.Index(msg, ": "); i >= 0 && strings.HasPrefix(msg, "error parsing regexp") {
		return msg[i+2:]
	}
	return msg
}

// refuseARE is the refusal for an ARE form RE2 cannot express.
func refuseARE(form string) error {
	return sqlerr.New("0A000",
		"regular expression %s is not supported: this server matches with RE2, "+
			"which has no equivalent for it", form)
}

// invalidARE is PostgreSQL's own refusal of a malformed pattern.
func invalidARE(why string) error {
	return sqlerr.New("2201B", "invalid regular expression: %s", why)
}

// aregexToRE2 translates PostgreSQL ARE forms into equivalent RE2 forms.
// It preserves newline matching (and the newline-sensitive options n m p w),
// anchors, literal modes, expanded syntax (x), character escapes and bounds
// up to 255. Case-insensitive matching takes, for every letter, its lower-
// and upper-case forms (caseVariants), as PostgreSQL does.
// Back references, lookaround, word-edge forms, collating elements and the
// b / e dialects refuse rather than change the match.
// Malformed forms raise 2201B; unrepresentable forms raise 0A000.
// Traps: \b is backspace and \B backslash (\y/\Y are word boundaries);
// \mnn is a back reference only with that many groups, else octal, else 2201B;
// a { that starts no bound is literal; . matches newline (RE2 needs (?s)).
// See ADR-0044 and TestPatternMatchOperatorsAnswerAsPostgreSQL.
// reMode is what the translation reads from the flags: case-insensitivity
// and the newline / syntax options (see reFlags).
type reMode struct {
	icase, nlStop, nlAnchor, expanded bool
	dialect                           regexDialect
}

func aregexToRE2(p string, m reMode) (string, error) {
	// Metasyntax: ***= and ***: director prefixes.
	switch {
	case strings.HasPrefix(p, "***="):
		return "(?s)" + literalARE(p[4:], m.icase), nil
	case strings.HasPrefix(p, "***:"):
		p = p[4:]
	}
	// Embedded options, only at the very start.
	if strings.HasPrefix(p, "(?") && len(p) > 2 && isOptionLetter(p[2]) {
		end := strings.IndexByte(p, ')')
		if end < 0 {
			return "", invalidARE("parentheses () not balanced")
		}
		literal := false
		for _, c := range p[2:end] {
			switch c {
			case 'i':
				m.icase = true
			case 'c':
				m.icase = false
			case 's', 'n', 'm', 'p', 'w':
				m.nlStop, m.nlAnchor = newlineOption(c)
			case 'x':
				m.expanded = true
			case 't':
				m.expanded = false
			case 'q':
				literal = true
			case 'b', 'e':
				return "", refuseARE(fmt.Sprintf("option (?%c)", c))
			default:
				return "", invalidARE("invalid embedded option")
			}
		}
		p = p[end+1:]
		if literal {
			return "(?s)" + literalARE(p, m.icase), nil
		}
	}
	icase := m.icase
	var b strings.Builder
	switch {
	case !m.nlStop && m.nlAnchor:
		b.WriteString("(?ms)")
	case !m.nlStop:
		b.WriteString("(?s)")
	case m.nlAnchor:
		b.WriteString("(?m)")
	}

	runes := []rune(p)
	if m.expanded {
		runes = stripExpanded(runes)
	}
	groups := countGroups(runes)
	inBracket := false
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if inBracket {
			switch {
			case r == ']':
				inBracket = false
				b.WriteRune(r)
			case r == '[' && i+1 < len(runes) && (runes[i+1] == '.' || runes[i+1] == '=' || runes[i+1] == ':'):
				kind := runes[i+1]
				end := -1
				for j := i + 2; j+1 < len(runes); j++ {
					if runes[j] == kind && runes[j+1] == ']' {
						end = j
						break
					}
				}
				if end < 0 {
					return "", invalidARE("brackets [] not balanced")
				}
				name := string(runes[i+2 : end])
				if kind != ':' || name == "<" || name == ">" {
					return "", refuseARE("[" + string(runes[i:end+2]) + "]")
				}
				if !posixClasses[name] {
					return "", invalidARE("invalid character class")
				}
				// Under case-insensitivity PostgreSQL reads [:lower:] and
				// [:upper:] as [:alpha:] (measured: 'A' ~* '[[:lower:]]').
				if icase && (name == "lower" || name == "upper") {
					name = "alpha"
				}
				b.WriteString("[:" + name + ":]")
				i = end + 1
			case r == '\\':
				s, n, err := translateEscape(runes, i, true, groups, icase)
				if err != nil {
					return "", err
				}
				b.WriteString(s)
				i += n - 1
			case i+2 < len(runes) && runes[i+1] == '-' && runes[i+2] != ']':
				// A range. Under case-insensitivity PostgreSQL folds it
				// letter by letter: the other cases of every letter INSIDE it
				// join the set, so `[A-_]` takes a-z, `[Z-a]` takes z and A,
				// and `[à-æ]` takes À-Æ (measured on 17.11).
				lo, hi := r, runes[i+2]
				b.WriteString(bracketLiteral(lo) + "-" + bracketLiteral(hi))
				if icase {
					b.WriteString(rangeCaseCounterparts(lo, hi))
				}
				i += 2
			default:
				b.WriteString(bracketLiteral(r))
				if icase {
					for _, v := range caseVariants(r)[1:] {
						b.WriteString(bracketLiteral(v))
					}
				}
			}
			continue
		}
		// A quantifier directly after an anchor quantifies nothing:
		// PostgreSQL's "quantifier operand invalid" (`^*`, `$+`, `^{2}`).
		if (r == '*' || r == '+' || r == '?' || (r == '{' && i+1 < len(runes) && isDigit(runes[i+1]))) &&
			i > 0 && (runes[i-1] == '^' || runes[i-1] == '$') && !escapedAt(runes, i-1) {
			return "", invalidARE("quantifier operand invalid")
		}
		switch r {
		case '\\':
			s, n, err := translateEscape(runes, i, false, groups, icase)
			if err != nil {
				return "", err
			}
			b.WriteString(s)
			i += n - 1
		case '[':
			// A bracket expression: a leading ^ and a ] in first position
			// belong to it.
			b.WriteRune('[')
			inBracket = true
			if i+1 < len(runes) && runes[i+1] == '^' {
				b.WriteRune('^')
				if m.nlStop {
					// Newline-sensitive: a negated bracket expression does
					// not match a newline.
					b.WriteString(`\n`)
				}
				i++
			}
			if i+1 < len(runes) && runes[i+1] == ']' {
				b.WriteString(`\]`)
				i++
			}
			// [[:<:]] and [[:>:]] (word start / end) are whole bracket
			// expressions RE2 has no form for.
			if rest := string(runes[i+1:]); strings.HasPrefix(rest, "[:<:]]") ||
				strings.HasPrefix(rest, "[:>:]]") {
				return "", refuseARE("[" + rest[:6])
			}
		case '(':
			if i+2 < len(runes) && runes[i+1] == '?' {
				switch {
				case runes[i+2] == ':':
					b.WriteString("(?:")
					i += 2
					continue
				case runes[i+2] == '=' || runes[i+2] == '!':
					return "", refuseARE("lookahead (?" + string(runes[i+2]) + "…)")
				case runes[i+2] == '<':
					return "", refuseARE("lookbehind (?<…)")
				}
				return "", invalidARE("quantifier operand invalid")
			}
			b.WriteRune(r)
		case '{':
			// A bound — digits, optionally a comma and more digits — or,
			// when what follows is not one, the literal brace.
			end := i + 1
			for end < len(runes) && runes[end] != '}' {
				end++
			}
			if end >= len(runes) || !isBound(string(runes[i+1:end])) {
				// A `{` that begins with a digit is a bound, and an
				// unfinished or malformed one is PostgreSQL's error
				// (`a{1`, `a{1,2`, `a{1x}`); any other `{` is a literal.
				if i+1 < len(runes) && isDigit(runes[i+1]) {
					return "", invalidARE("invalid repetition count(s)")
				}
				b.WriteString(`\{`)
				continue
			}
			parts := strings.Split(string(runes[i+1:end]), ",")
			lo, _ := strconv.Atoi(parts[0])
			hi := lo
			if len(parts) == 2 && parts[1] != "" {
				hi, _ = strconv.Atoi(parts[1])
			}
			if lo > 255 || hi > 255 || hi < lo {
				return "", invalidARE("invalid repetition count(s)")
			}
			b.WriteString(string(runes[i : end+1]))
			i = end
		default:
			if v := caseVariants(r); icase && len(v) > 1 {
				b.WriteString(bracketOf(v))
				continue
			}
			b.WriteRune(r)
		}
	}
	if inBracket {
		return "", invalidARE("brackets [] not balanced")
	}
	return b.String(), nil
}

// literalARE is a pattern read literally (***= and (?q)), every letter
// matching its other cases when asked.
func literalARE(s string, icase bool) string {
	if !icase {
		return regexp.QuoteMeta(s)
	}
	var b strings.Builder
	for _, r := range s {
		if v := caseVariants(r); len(v) > 1 {
			b.WriteString(bracketOf(v))
			continue
		}
		b.WriteString(regexp.QuoteMeta(string(r)))
	}
	return b.String()
}

// isBound reports whether s is a bound's body: m, m, or m,n in digits.
func isBound(s string) bool {
	parts := strings.Split(s, ",")
	if len(parts) > 2 || parts[0] == "" {
		return false
	}
	for _, p := range parts {
		for _, c := range p {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

// countGroups is the number of capturing groups in a pattern — every `(`
// outside a bracket expression that is not escaped and does not open a
// `(?` construct — which decides whether \mnn is a back reference.
func countGroups(runes []rune) int {
	n := 0
	inBracket := false
	for i := 0; i < len(runes); i++ {
		switch r := runes[i]; {
		case r == '\\':
			i++
		case inBracket:
			if r == ']' {
				inBracket = false
			}
		case r == '[':
			inBracket = true
			if i+1 < len(runes) && runes[i+1] == ']' {
				i++
			}
		case r == '(' && (i+1 >= len(runes) || runes[i+1] != '?'):
			n++
		}
	}
	return n
}

// bracketLiteral is one character inside an RE2 bracket expression.
func bracketLiteral(r rune) string {
	switch r {
	case '\\', ']', '[', '^', '-':
		return `\` + string(r)
	}
	return string(r)
}

var posixClasses = map[string]bool{
	"alnum": true, "alpha": true, "blank": true, "cntrl": true, "digit": true,
	"graph": true, "lower": true, "print": true, "punct": true, "space": true,
	"upper": true, "xdigit": true, "word": true,
}

func isOptionLetter(c byte) bool {
	return strings.IndexByte("bceimnpqstwx", c) >= 0
}

// translateEscape translates the escape starting at runes[i] (a backslash)
// and reports how many runes it spans. A character-entry escape that names a
// letter matches its other cases like any other letter under
// case-insensitivity.
func translateEscape(runes []rune, i int, inBracket bool, groups int, icase bool) (string, int, error) {
	out, n, err := translateEscapeRaw(runes, i, inBracket, groups)
	if err != nil || !icase || !strings.HasPrefix(out, `\x{`) {
		return out, n, err
	}
	v, perr := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(out, `\x{`), "}"), 16, 32)
	if perr != nil {
		return out, n, err
	}
	vs := caseVariants(rune(v))
	if len(vs) == 1 {
		return out, n, err
	}
	var set strings.Builder
	for _, c := range vs {
		fmt.Fprintf(&set, `\x{%x}`, c)
	}
	if inBracket {
		return set.String(), n, nil
	}
	return "[" + set.String() + "]", n, nil
}

func translateEscapeRaw(runes []rune, i int, inBracket bool, groups int) (string, int, error) {
	if i+1 >= len(runes) {
		return "", 0, invalidARE("invalid escape \\ sequence")
	}
	c := runes[i+1]
	lit := func(r rune) (string, int, error) { return fmt.Sprintf(`\x{%x}`, r), 2, nil }
	switch c {
	case 'a':
		return lit(0x07)
	case 'b':
		return lit(0x08)
	case 'B':
		return `\\`, 2, nil
	case 'e':
		return lit(0x1B)
	case 'f':
		return lit(0x0C)
	case 'n':
		return lit(0x0A)
	case 'r':
		return lit(0x0D)
	case 't':
		return lit(0x09)
	case 'v':
		return lit(0x0B)
	case 's', 'S':
		// PostgreSQL's \s is [[:space:]], which includes the vertical tab;
		// RE2's \s does not. The POSIX class is the same in both.
		class := "[:space:]"
		if c == 'S' {
			class = "[:^space:]"
		}
		if inBracket {
			return class, 2, nil
		}
		return "[" + class + "]", 2, nil
	case 'd', 'w':
		return `\` + string(c), 2, nil
	case 'D', 'W':
		return `\` + string(c), 2, nil
	case 'c':
		if i+2 >= len(runes) {
			return "", 0, invalidARE("invalid escape \\ sequence")
		}
		return fmt.Sprintf(`\x{%x}`, runes[i+2]&0x1F), 3, nil
	case 'u', 'U':
		want := 4
		if c == 'U' {
			want = 8
		}
		if i+2+want > len(runes) {
			return "", 0, invalidARE("invalid escape \\ sequence")
		}
		hex := string(runes[i+2 : i+2+want])
		v, err := strconv.ParseUint(hex, 16, 32)
		if err != nil || v > unicode.MaxRune {
			return "", 0, invalidARE("invalid escape \\ sequence")
		}
		return fmt.Sprintf(`\x{%x}`, v), 2 + want, nil
	case 'x':
		j := i + 2
		for j < len(runes) && isHexDigit(runes[j]) {
			j++
		}
		if j == i+2 {
			return "", 0, invalidARE("invalid escape \\ sequence")
		}
		v, err := strconv.ParseUint(string(runes[i+2:j]), 16, 32)
		if err != nil || v > unicode.MaxRune {
			return "", 0, invalidARE("invalid escape \\ sequence")
		}
		return fmt.Sprintf(`\x{%x}`, v), j - i, nil
	}
	if inBracket {
		switch c {
		case 'A', 'Z', 'm', 'M', 'y', 'Y':
			return "", 0, invalidARE("invalid escape \\ sequence")
		}
	} else {
		switch c {
		case 'A':
			return `\A`, 2, nil
		case 'Z':
			return `\z`, 2, nil
		case 'y':
			return `\b`, 2, nil
		case 'Y':
			return `\B`, 2, nil
		case 'm', 'M':
			return "", 0, refuseARE(`\` + string(c) + " (word start/end)")
		}
	}
	if c >= '0' && c <= '9' {
		// \0 and \0nn are octal. A digit 1–9 outside a bracket is a back
		// reference when the pattern has that many groups (up to three
		// digits), else three octal digits are that character; inside a
		// bracket there are no back references.
		j := i + 1
		for j < len(runes) && j < i+4 && runes[j] >= '0' && runes[j] <= '9' {
			j++
		}
		digits := string(runes[i+1 : j])
		if c != '0' && !inBracket {
			if n, _ := strconv.Atoi(digits); n <= groups {
				return "", 0, refuseARE("back reference \\" + digits)
			}
			if len(digits) < 3 || strings.ContainsAny(digits, "89") {
				return "", 0, invalidARE("invalid backreference number")
			}
		}
		k := i + 1
		for k < len(runes) && k < i+4 && runes[k] >= '0' && runes[k] <= '7' {
			k++
		}
		v, _ := strconv.ParseUint(string(runes[i+1:k]), 8, 32)
		return fmt.Sprintf(`\x{%x}`, v), k - i, nil
	}
	if unicode.IsLetter(c) || unicode.IsDigit(c) {
		return "", 0, invalidARE("invalid escape \\ sequence")
	}
	if inBracket {
		return bracketLiteral(c), 2, nil
	}
	return regexp.QuoteMeta(string(c)), 2, nil
}

func isHexDigit(r rune) bool {
	return r >= '0' && r <= '9' || r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F'
}

func isDigit(r rune) bool { return r >= '0' && r <= '9' }

// escapedAt reports whether runes[i] is preceded by an odd run of
// backslashes, i.e. is a literal rather than an operator.
func escapedAt(runes []rune, i int) bool {
	n := 0
	for j := i - 1; j >= 0 && runes[j] == '\\'; j-- {
		n++
	}
	return n%2 == 1
}

// caseForms is r's lower- and upper-case forms as PostgreSQL's oracle reads
// them: Go's unicode.ToLower / ToUpper, except where the oracle's C library
// differs (libcCaseForms, measured).
func caseForms(r rune) (lower, upper rune) {
	if f, ok := libcCaseForms[r]; ok {
		return f[0], f[1]
	}
	return unicode.ToLower(r), unicode.ToUpper(r)
}

// caseVariants is r followed by its lower- and upper-case forms, distinct:
// the characters a case-insensitive ARE matches for r. PostgreSQL takes
// exactly these (towlower / towupper), not the whole case-folding orbit:
// 'ς' ~* 'σ' is false there (σ's forms are σ and Σ), and 'İ' ~* 'i' false
// while 'i' ~* 'İ' is true.
func caseVariants(r rune) []rune {
	out := []rune{r}
	lo, up := caseForms(r)
	if lo != r {
		out = append(out, lo)
	}
	if up != r && up != lo {
		out = append(out, up)
	}
	return out
}

// bracketOf is a bracket expression matching exactly the runes given.
func bracketOf(rs []rune) string {
	var b strings.Builder
	b.WriteByte('[')
	for _, r := range rs {
		b.WriteString(bracketLiteral(r))
	}
	b.WriteByte(']')
	return b.String()
}

// rangeCaseCounterparts is the other-case forms of every character in
// [lo, hi] that lie outside it, as bracket-expression members (runs written
// as ranges). A range wider than 65536 characters takes no counterparts.
func rangeCaseCounterparts(lo, hi rune) string {
	if hi < lo || hi-lo > 65536 {
		return ""
	}
	set := map[rune]bool{}
	for r := lo; r <= hi; r++ {
		for _, v := range caseVariants(r)[1:] {
			if v < lo || v > hi {
				set[v] = true
			}
		}
	}
	rs := make([]rune, 0, len(set))
	for r := range set {
		rs = append(rs, r)
	}
	slices.Sort(rs)
	var b strings.Builder
	for i := 0; i < len(rs); {
		j := i
		for j+1 < len(rs) && rs[j+1] == rs[j]+1 {
			j++
		}
		b.WriteString(bracketLiteral(rs[i]))
		if j > i {
			b.WriteString("-" + bracketLiteral(rs[j]))
		}
		i = j + 1
	}
	return b.String()
}

// stripExpanded is an expanded-syntax (x) pattern with its white space and
// #-comments removed: outside a bracket expression an unescaped space, tab,
// newline or `#…` to the end of the line is not part of the RE; an escaped
// one, and anything inside brackets, is.
func stripExpanded(runes []rune) []rune {
	out := make([]rune, 0, len(runes))
	inBracket := false
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case r == '\\' && i+1 < len(runes):
			out = append(out, r, runes[i+1])
			i++
		case inBracket:
			out = append(out, r)
			if r == '[' && i+1 < len(runes) && (runes[i+1] == ':' || runes[i+1] == '.' || runes[i+1] == '=') {
				// A class, collating element or equivalence class: copy
				// through its closing `x]`.
				kind := runes[i+1]
				for j := i + 1; j < len(runes); j++ {
					out = append(out, runes[j])
					if runes[j] == ']' && runes[j-1] == kind && j > i+2 {
						i = j
						break
					}
					i = j
				}
				continue
			}
			if r == ']' {
				inBracket = false
			}
		case r == '[':
			out = append(out, r)
			inBracket = true
			if i+1 < len(runes) && runes[i+1] == '^' {
				out = append(out, '^')
				i++
			}
			if i+1 < len(runes) && runes[i+1] == ']' {
				out = append(out, ']')
				i++
			}
		case r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\f' || r == '\v':
		case r == '#':
			for i+1 < len(runes) && runes[i+1] != '\n' {
				i++
			}
		default:
			out = append(out, r)
		}
	}
	return out
}
