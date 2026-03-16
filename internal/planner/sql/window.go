package sql

import (
	"fmt"
	"strings"
)

// WindowSpec describes a window function extracted from SQL before vitess parsing.
type WindowSpec struct {
	FuncName    string
	Args        string // raw arg string (e.g., "amount", "*", "")
	PartitionBy []string
	OrderBy     []WindowOrderItem
	Alias       string // output column name
}

// WindowOrderItem describes a column + direction in a window ORDER BY.
type WindowOrderItem struct {
	Column string
	Desc   bool
}

type windowReplacement struct {
	start int
	end   int
	alias string
	spec  WindowSpec
}

// rewriteWindowFunctions scans SQL for FUNC(...) OVER (...) patterns and
// replaces them with placeholder literals, returning the modified SQL and
// extracted window specs. This pre-processing step handles window syntax
// before the main parser sees the query.
func rewriteWindowFunctions(sqlStr string) (string, []WindowSpec, error) {
	reps := findWindowFunctions(sqlStr)
	if len(reps) == 0 {
		return sqlStr, nil, nil
	}

	var specs []WindowSpec
	var result strings.Builder
	prev := 0
	for _, r := range reps {
		result.WriteString(sqlStr[prev:r.start])
		result.WriteString("0 as ")
		result.WriteString(r.alias)
		prev = r.end
		specs = append(specs, r.spec)
	}
	result.WriteString(sqlStr[prev:])

	return result.String(), specs, nil
}

func findWindowFunctions(sql string) []windowReplacement {
	var reps []windowReplacement
	n := len(sql)
	winIdx := 0
	i := 0

	for i < n {
		// Skip string literals
		if sql[i] == '\'' {
			i = skipStringLit(sql, i)
			continue
		}

		// Look for identifier (potential function name)
		if !isIdentStartByte(sql[i]) {
			i++
			continue
		}

		identStart := i
		for i < n && isIdentByte(sql[i]) {
			i++
		}
		ident := sql[identStart:i]

		// Skip whitespace between identifier and '('
		j := skipWS(sql, i)
		if j >= n || sql[j] != '(' {
			i = j
			continue
		}

		// It's a function call. Find the matching ')'.
		argsStart := j + 1
		k := j + 1
		depth := 1
		for k < n && depth > 0 {
			if sql[k] == '\'' {
				k = skipStringLit(sql, k)
				continue
			}
			if sql[k] == '(' {
				depth++
			}
			if sql[k] == ')' {
				depth--
			}
			if depth > 0 {
				k++
			}
		}
		if depth != 0 {
			i = k
			continue
		}
		argsEnd := k
		k++ // skip ')'

		// Check if OVER follows
		m := skipWS(sql, k)
		if m+4 > n || !strings.EqualFold(sql[m:m+4], "OVER") {
			i = k
			continue
		}
		// Ensure OVER isn't part of a longer identifier (e.g., OVERLAY)
		if m+4 < n && isIdentByte(sql[m+4]) {
			i = k
			continue
		}

		// Found OVER — now find the OVER(...) spec
		m += 4
		m = skipWS(sql, m)
		if m >= n || sql[m] != '(' {
			i = k
			continue
		}

		overStart := m + 1
		m++
		depth = 1
		for m < n && depth > 0 {
			if sql[m] == '\'' {
				m = skipStringLit(sql, m)
				continue
			}
			if sql[m] == '(' {
				depth++
			}
			if sql[m] == ')' {
				depth--
			}
			if depth > 0 {
				m++
			}
		}
		if depth != 0 {
			i = k
			continue
		}
		overEnd := m
		m++ // skip ')'

		// Extract function args and window spec
		args := strings.TrimSpace(sql[argsStart:argsEnd])
		overSpecStr := strings.TrimSpace(sql[overStart:overEnd])

		// Check for AS alias after OVER(...)
		p := skipWS(sql, m)
		alias := fmt.Sprintf("__w%d", winIdx)
		endPos := m

		if p+2 <= n && strings.EqualFold(sql[p:p+2], "AS") {
			afterAs := p + 2
			// Ensure AS isn't part of a longer identifier
			if afterAs >= n || !isIdentByte(sql[afterAs]) {
				afterAs = skipWS(sql, afterAs)
				if afterAs < n && isIdentStartByte(sql[afterAs]) {
					aliasStart := afterAs
					for afterAs < n && isIdentByte(sql[afterAs]) {
						afterAs++
					}
					alias = sql[aliasStart:afterAs]
					endPos = afterAs
				}
			}
		}

		spec := WindowSpec{
			FuncName: strings.ToLower(ident),
			Args:     args,
			Alias:    alias,
		}
		parseOverSpec(overSpecStr, &spec)

		reps = append(reps, windowReplacement{
			start: identStart,
			end:   endPos,
			alias: alias,
			spec:  spec,
		})

		winIdx++
		i = endPos
	}

	return reps
}

// parseOverSpec extracts PARTITION BY and ORDER BY from a window spec string.
func parseOverSpec(spec string, ws *WindowSpec) {
	upper := strings.ToUpper(spec)

	partIdx := strings.Index(upper, "PARTITION BY")
	orderIdx := strings.Index(upper, "ORDER BY")

	if partIdx >= 0 {
		partStart := partIdx + len("PARTITION BY")
		partEnd := len(spec)
		if orderIdx > partIdx {
			partEnd = orderIdx
		}
		partStr := strings.TrimSpace(spec[partStart:partEnd])
		for _, col := range strings.Split(partStr, ",") {
			col = strings.TrimSpace(col)
			if col != "" {
				ws.PartitionBy = append(ws.PartitionBy, col)
			}
		}
	}

	if orderIdx >= 0 {
		orderStart := orderIdx + len("ORDER BY")
		orderStr := strings.TrimSpace(spec[orderStart:])
		for _, item := range strings.Split(orderStr, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			parts := strings.Fields(item)
			col := parts[0]
			desc := false
			if len(parts) > 1 && strings.EqualFold(parts[1], "DESC") {
				desc = true
			}
			ws.OrderBy = append(ws.OrderBy, WindowOrderItem{
				Column: col,
				Desc:   desc,
			})
		}
	}
}

func skipStringLit(s string, i int) int {
	if i >= len(s) || s[i] != '\'' {
		return i
	}
	i++ // skip opening quote
	for i < len(s) {
		if s[i] == '\'' {
			i++
			if i < len(s) && s[i] == '\'' {
				i++ // escaped quote
				continue
			}
			return i
		}
		i++
	}
	return i
}

func skipWS(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func isIdentStartByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentByte(c byte) bool {
	return isIdentStartByte(c) || (c >= '0' && c <= '9')
}
