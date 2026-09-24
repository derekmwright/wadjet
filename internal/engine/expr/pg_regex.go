// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	re := compileARE(toString(args[1]), icase)
	matched := re.MatchString(toString(args[0]))
	return matched != negate
}

type areKey struct {
	pattern string
	icase   bool
}

var areCache sync.Map // areKey → *regexp.Regexp or error

// compileARE compiles an ARE pattern through its RE2 translation, raising
// the refusal a pattern that cannot be translated earns. The result — either
// way — is cached per pattern, because the pattern is almost always a
// constant evaluated once per row.
func compileARE(pattern string, icase bool) *regexp.Regexp {
	key := areKey{pattern, icase}
	if v, ok := areCache.Load(key); ok {
		if err, bad := v.(error); bad {
			panic(fatalEval{err})
		}
		return v.(*regexp.Regexp)
	}
	re, err := translateAndCompile(pattern, icase)
	if err != nil {
		areCache.Store(key, err)
		panic(fatalEval{err})
	}
	areCache.Store(key, re)
	return re
}

func translateAndCompile(pattern string, icase bool) (*regexp.Regexp, error) {
	translated, err := aregexToRE2(pattern, icase)
	if err != nil {
		return nil, err
	}
	re, cerr := regexp.Compile(translated)
	if cerr != nil {
		return nil, sqlerr.New("2201B", "invalid regular expression: %s", reErrorText(cerr))
	}
	return re, nil
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
// It preserves newline matching, anchors, literal modes, character escapes
// and bounds up to 255. Case-insensitive matching folds ASCII only.
// Back references, lookaround, word-edge forms, collating elements and
// unsupported embedded options refuse rather than change the match.
// Malformed forms raise 2201B; unrepresentable forms raise 0A000.
// See ADR-0044 and TestPatternMatchOperatorsAnswerAsPostgreSQL.
func aregexToRE2(p string, icase bool) (string, error) {
	// Metasyntax: ***= and ***: director prefixes.
	switch {
	case strings.HasPrefix(p, "***="):
		return "(?s)" + literalARE(p[4:], icase), nil
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
				icase = true
			case 'c':
				icase = false
			case 's', 't':
			case 'q':
				literal = true
			case 'b', 'e', 'm', 'n', 'p', 'w', 'x':
				return "", refuseARE(fmt.Sprintf("option (?%c)", c))
			default:
				return "", invalidARE("invalid embedded option")
			}
		}
		p = p[end+1:]
		if literal {
			return "(?s)" + literalARE(p, icase), nil
		}
	}
	var b strings.Builder
	b.WriteString("(?s)")

	runes := []rune(p)
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
				// A range. Under case-insensitivity an ASCII letter range
				// takes its other case's range too.
				lo, hi := r, runes[i+2]
				b.WriteString(bracketLiteral(lo) + "-" + bracketLiteral(hi))
				if icase {
					// PostgreSQL folds a range letter by letter: the other
					// case of every ASCII letter INSIDE it joins the set, so
					// `[A-_]` takes a-z and `[Z-a]` takes z and A, whatever
					// the endpoints are (measured on 17.11).
					b.WriteString(foldedRangeCounterparts(lo, hi))
				}
				i += 2
			default:
				b.WriteString(bracketLiteral(r))
				if icase && isASCIILetter(r) {
					b.WriteString(bracketLiteral(swapCase(r)))
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
			if icase && isASCIILetter(r) {
				b.WriteString("[" + string(r) + string(swapCase(r)) + "]")
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

// literalARE is a pattern read literally (***= and (?q)), case-folded over
// ASCII letters when asked.
func literalARE(s string, icase bool) string {
	if !icase {
		return regexp.QuoteMeta(s)
	}
	var b strings.Builder
	for _, r := range s {
		if isASCIILetter(r) {
			b.WriteString("[" + string(r) + string(swapCase(r)) + "]")
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

func isASCIILetter(r rune) bool { return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' }

func swapCase(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - 'a' + 'A'
	}
	return r - 'A' + 'a'
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
// and reports how many runes it spans. A character-entry escape that names an
// ASCII letter is case-folded like any other letter under case-insensitivity.
func translateEscape(runes []rune, i int, inBracket bool, groups int, icase bool) (string, int, error) {
	out, n, err := translateEscapeRaw(runes, i, inBracket, groups)
	if err != nil || !icase || !strings.HasPrefix(out, `\x{`) {
		return out, n, err
	}
	v, perr := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(out, `\x{`), "}"), 16, 32)
	if perr != nil || !isASCIILetter(rune(v)) {
		return out, n, err
	}
	pair := fmt.Sprintf(`\x{%x}\x{%x}`, v, swapCase(rune(v)))
	if inBracket {
		return pair, n, nil
	}
	return "[" + pair + "]", n, nil
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

// foldedRangeCounterparts is the other-case range of every ASCII letter in
// [lo, hi], as bracket-expression members.
func foldedRangeCounterparts(lo, hi rune) string {
	var b strings.Builder
	add := func(from, to, base, other rune) {
		a, z := max(lo, from), min(hi, to)
		if a > z {
			return
		}
		b.WriteString(bracketLiteral(a-base+other) + "-" + bracketLiteral(z-base+other))
	}
	add('A', 'Z', 'A', 'a')
	add('a', 'z', 'a', 'A')
	return b.String()
}
