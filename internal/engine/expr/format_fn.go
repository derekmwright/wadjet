// SPDX-License-Identifier: MIT

package expr

import (
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// format(formatstr, VARIADIC args) is PostgreSQL's text_format: a format
// specifier is %[position$][-][width]type with type s (the value's text), I
// (quote_ident of it) or L (quote_literal of it, NULL unquoted), width a
// number, `*` (the next argument) or `*n$`, and `%%` a literal percent. Any
// other type is 22023, as PostgreSQL raises it.
//
// It used to be Go's fmt.Sprintf over the argument boxes, which printed Go's
// own notation for every box that was not a string — `%!s(float64=6.375)`,
// `%!s(<nil>)` (#1467) — and read `%d`, `%5.2s` and `%1$s` by Go's grammar.
//
// Every value argument is the text its DECLARED type prints
// (FuncCall.renderFormatArgs: batch.FormatPGText, the one renderer pgwire's
// text format and CAST(… AS TEXT) use), so a double is 6.375, a boolean t,
// an array {1,2}; a NULL is the empty string for %s.

// renderFormatArgs replaces every value argument of a format() call (every
// position after the format string) with its text under its declaration.
func (e *FuncCall) renderFormatArgs(b *batch.RecordBatch, row int, args []any) {
	for i := 1; i < len(args); i++ {
		if args[i] == nil {
			continue
		}
		if _, ok := args[i].(string); ok {
			continue
		}
		args[i] = e.argText(b, row, i, args[i])
	}
}

// argText is argument i's box as its declared type's text.
func (e *FuncCall) argText(b *batch.RecordBatch, row, i int, v any) string {
	if i < len(e.argDecls) && i < len(e.Args) {
		col := e.argDecls[i].shape(b, row, e.Args[i])
		switch v.(type) {
		case []any, map[string]any:
			v = declaredBox(v, col)
		}
		return batch.FormatPGText(v, col)
	}
	return batch.FormatPGText(v, nil)
}

func raiseFormat(code, msg string) {
	panic(fatalEval{sqlerr.New(code, "%s", msg)})
}

func fnFormat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	f := toString(args[0])
	text := func(v any) (string, bool) {
		switch x := v.(type) {
		case nil:
			return "", false
		case string:
			return x, true
		}
		return batch.FormatPGText(v, nil), true
	}
	var out strings.Builder
	arg := 1 // the next sequential argument (args[0] is the format string)
	for i := 0; i < len(f); i++ {
		c := f[i]
		if c != '%' {
			out.WriteByte(c)
			continue
		}
		i++
		if i >= len(f) {
			raiseFormat("22023", "unterminated format() type specifier")
		}
		if f[i] == '%' {
			out.WriteByte('%')
			continue
		}
		argpos, widthpos, minus, width, next := parseFormatSpec(f, i)
		i = next
		conv := f[i]
		if conv != 's' && conv != 'I' && conv != 'L' {
			r, _ := utf8.DecodeRuneInString(f[i:])
			raiseFormat("22023", `unrecognized format() type specifier "`+string(r)+`"`)
		}
		if widthpos >= 0 {
			if widthpos > 0 {
				arg = widthpos
			}
			if arg >= len(args) {
				raiseFormat("22023", "too few arguments for format()")
			}
			width = 0
			if s, ok := text(args[arg]); ok {
				n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32)
				if err != nil {
					raiseFormat("22P02", `invalid input syntax for type integer: "`+s+`"`)
				}
				if n < 0 {
					if n == math.MinInt32 {
						raiseFormat("22003", "number is out of range")
					}
					minus, n = true, -n
				}
				width = int(n)
			}
			arg++
		}
		if argpos > 0 {
			arg = argpos
		}
		if arg >= len(args) {
			raiseFormat("22023", "too few arguments for format()")
		}
		s, ok := text(args[arg])
		arg++
		switch conv {
		case 's':
		case 'I':
			if !ok {
				raiseFormat("22004", "null values cannot be formatted as an SQL identifier")
			}
			s = quoteIdent(s)
		case 'L':
			if !ok {
				s = "NULL"
			} else {
				s = quoteLiteral(s)
			}
		}
		pad := width - utf8.RuneCountInString(s)
		if pad > 0 && !minus {
			out.WriteString(strings.Repeat(" ", pad))
		}
		out.WriteString(s)
		if pad > 0 && minus {
			out.WriteString(strings.Repeat(" ", pad))
		}
	}
	return out.String()
}

// parseFormatSpec reads the part of a specifier between `%` and its type,
// starting at f[i]: PostgreSQL's text_format_parse_format. argpos and
// widthpos are -1 when absent, widthpos 0 for a bare `*`. next is the index
// of the type character.
func parseFormatSpec(f string, i int) (argpos, widthpos int, minus bool, width, next int) {
	argpos, widthpos = -1, -1
	advance := func() {
		i++
		if i >= len(f) {
			raiseFormat("22023", "unterminated format() type specifier")
		}
	}
	digits := func() (int, bool) {
		if f[i] < '0' || f[i] > '9' {
			return 0, false
		}
		n := 0
		for f[i] >= '0' && f[i] <= '9' {
			n = n*10 + int(f[i]-'0')
			if n > math.MaxInt32/10 && i+1 < len(f) && f[i+1] >= '0' && f[i+1] <= '9' {
				raiseFormat("22003", "number is out of range")
			}
			advance()
		}
		return n, true
	}
	if n, ok := digits(); ok {
		if f[i] != '$' {
			return argpos, widthpos, false, n, i
		}
		if n == 0 {
			raiseFormat("22023", "format specifies argument 0, but arguments are numbered from 1")
		}
		argpos = n
		advance()
	}
	for f[i] == '-' {
		minus = true
		advance()
	}
	if f[i] == '*' {
		advance()
		if n, ok := digits(); ok {
			if f[i] != '$' {
				raiseFormat("22023", `width argument position must be ended by "$"`)
			}
			if n == 0 {
				raiseFormat("22023", "format specifies argument 0, but arguments are numbered from 1")
			}
			widthpos = n
			advance()
		} else {
			widthpos = 0
		}
	} else if n, ok := digits(); ok {
		width = n
	}
	return argpos, widthpos, minus, width, i
}
