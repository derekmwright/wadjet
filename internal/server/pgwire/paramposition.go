// SPDX-License-Identifier: MIT

package pgwire

// Where a placeholder stands, as the splice needs to know it.
//
// Bind splices each parameter into the statement as a literal of its type
// (bindparams.go). Most positions take any expression, and the typed literal —
// a CAST, for most types — is read there as the value of that type. A few
// positions read a CONSTANT only: a cast there is a syntax error or a refusal,
// so the parameter is spelled by what that position reads. The positions are
// found lexically, the way the parser's own grammar for them is: by the
// keyword or call the placeholder stands in.

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// paramPosition is the grammar a placeholder stands in. Most positions take
// any expression, and a parameter there is its typed literal. Four read a
// constant only, and are spelled by what they read (bindparams.go): a count
// (LIMIT, OFFSET, FETCH, a TABLESAMPLE percentage), a window function's
// integer argument, LAG / LEAD's default, and the whole value of a MERGE
// action (`SET c = $1`, an INSERT VALUES item), which over a subquery source
// takes only a bare constant (#1398).
type paramPosition uint8

const (
	posValue paramPosition = iota
	posCount
	posWindowInt
	posWindowDefault
	posMergeValue
)

// paramPositions classifies each of refs (scanParamRefs' order).
func paramPositions(sql string, refs []paramRef) []paramPosition {
	calls := callArgPositions(sql)
	actions := -1
	if strings.EqualFold(plansql.LeadingKeyword(sql), "MERGE") {
		actions = mergeActionsStart(sql)
	}
	out := make([]paramPosition, len(refs))
	for i, r := range refs {
		c := calls[r.start]
		switch {
		case countPosition(sql, r):
			out[i] = posCount
		case (c.fn == "lag" || c.fn == "lead" || c.fn == "nth_value") && c.arg == 1, c.fn == "ntile" && c.arg == 0:
			out[i] = posWindowInt
		case (c.fn == "lag" || c.fn == "lead") && c.arg == 2:
			out[i] = posWindowDefault
		case actions >= 0 && r.start > actions && (c.fn == "values" || wholeSetValue(sql, r)):
			out[i] = posMergeValue
		}
	}
	return out
}

// mergeActionsStart is the offset of a MERGE statement's first top-level
// THEN — where its actions begin — or -1.
func mergeActionsStart(sql string) int {
	depth := 0
	for i := 0; i < len(sql); i++ {
		switch c := sql[i]; {
		case c == '\'' || c == '"':
			for i++; i < len(sql) && sql[i] != c; i++ {
			}
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && (c == 'T' || c == 't') && (i == 0 || !isWordByte(sql[i-1])) &&
			i+4 <= len(sql) && strings.EqualFold(sql[i:i+4], "THEN") && (i+4 == len(sql) || !isWordByte(sql[i+4])):
			return i
		}
	}
	return -1
}

// wholeSetValue reports `col = $n` where the placeholder is the whole value:
// an `=` before it and the item's end (a comma, the statement's end or the
// next WHEN) after it.
func wholeSetValue(sql string, r paramRef) bool {
	i := skipSpacesBack(sql, r.start)
	if i == 0 || sql[i-1] != '=' {
		return false
	}
	j := skipSpaces(sql, r.end)
	if j >= len(sql) || sql[j] == ',' || sql[j] == ';' {
		return true
	}
	return len(sql) >= j+4 && strings.EqualFold(sql[j:j+4], "WHEN") && (j+4 == len(sql) || !isWordByte(sql[j+4]))
}

// countPosition reports whether the placeholder r is a LIMIT, OFFSET or FETCH
// count (the token before it is one of those keywords; FETCH's count follows
// FIRST or NEXT) or a TABLESAMPLE percentage.
func countPosition(sql string, r paramRef) bool {
	switch strings.ToUpper(wordBefore(sql, r.start)) {
	case "LIMIT", "OFFSET", "FIRST", "NEXT":
		return true
	}
	return tablesamplePosition(sql, r)
}

// tablesamplePosition reports `TABLESAMPLE BERNOULLI ($n)` / `SYSTEM ($n)`.
func tablesamplePosition(sql string, r paramRef) bool {
	i := skipSpacesBack(sql, r.start)
	if i == 0 || sql[i-1] != '(' {
		return false
	}
	switch strings.ToUpper(wordBefore(sql, i-1)) {
	case "BERNOULLI", "SYSTEM":
		return true
	}
	return false
}

// wordBefore is the word ending at the last non-space byte before i.
func wordBefore(sql string, i int) string {
	i = skipSpacesBack(sql, i)
	j := i
	for j > 0 && isWordByte(sql[j-1]) {
		j--
	}
	return sql[j:i]
}

// callArg is the call a placeholder is an argument of: the function name
// before the opening parenthesis (lower-cased) and the argument's index.
type callArg struct {
	fn  string
	arg int
}

// callArgPositions maps each placeholder's start offset to the call it is an
// immediate argument of — a placeholder that is the WHOLE argument, alone
// between the call's parenthesis or commas. Quoted text is skipped as the
// placeholder scanner skips it.
func callArgPositions(sql string) map[int]callArg {
	type frame struct {
		fn   string
		arg  int
		bare bool // nothing but spaces seen in the current argument so far
	}
	out := map[int]callArg{}
	var stack []frame
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case c == '\'' || c == '"':
			for i++; i < len(sql); i++ {
				if sql[i] == c {
					if i+1 < len(sql) && sql[i+1] == c {
						i++
						continue
					}
					break
				}
			}
			if len(stack) > 0 {
				stack[len(stack)-1].bare = false
			}
		case c == '(':
			j := skipSpacesBack(sql, i)
			k := j
			for k > 0 && isWordByte(sql[k-1]) {
				k--
			}
			if len(stack) > 0 {
				stack[len(stack)-1].bare = false
			}
			stack = append(stack, frame{fn: strings.ToLower(sql[k:j]), bare: true})
		case c == ')':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case c == ',':
			if len(stack) > 0 {
				stack[len(stack)-1].arg++
				stack[len(stack)-1].bare = true
			}
		case c == '$' && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9':
			j := i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			if len(stack) > 0 {
				top := stack[len(stack)-1]
				rest := skipSpaces(sql, j)
				if top.bare && rest < len(sql) && (sql[rest] == ',' || sql[rest] == ')') {
					out[i] = callArg{fn: top.fn, arg: top.arg}
				}
				stack[len(stack)-1].bare = false
			}
			i = j - 1
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
		default:
			if len(stack) > 0 {
				stack[len(stack)-1].bare = false
			}
		}
	}
	return out
}

func isWordByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}
