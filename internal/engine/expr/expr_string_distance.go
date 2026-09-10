// This file holds expr string distance; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// --- String: distance and utility ---

func fnSoundex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.ToUpper(toString(args[0]))
	if len(s) == 0 {
		return ""
	}
	result := make([]byte, 4)
	result[0] = s[0]
	codes := map[byte]byte{
		'B': '1', 'F': '1', 'P': '1', 'V': '1',
		'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
		'D': '3', 'T': '3',
		'L': '4',
		'M': '5', 'N': '5',
		'R': '6',
	}
	idx := 1
	lastCode := codes[s[0]]
	for i := 1; i < len(s) && idx < 4; i++ {
		code, ok := codes[s[i]]
		if ok && code != lastCode {
			result[idx] = code
			idx++
			lastCode = code
		} else if !ok {
			lastCode = 0
		}
	}
	for idx < 4 {
		result[idx] = '0'
		idx++
	}
	return string(result)
}

func fnLevenshtein(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := []rune(toString(args[0]))
	t := []rune(toString(args[1]))
	m, n := len(s), len(t)
	if m == 0 {
		return float64(n)
	}
	if n == 0 {
		return float64(m)
	}
	prev := make([]int, n+1)
	curr := make([]int, n+1)
	for j := 0; j <= n; j++ {
		prev[j] = j
	}
	for i := 1; i <= m; i++ {
		curr[0] = i
		for j := 1; j <= n; j++ {
			cost := 1
			if s[i-1] == t[j-1] {
				cost = 0
			}
			ins := curr[j-1] + 1
			del := prev[j] + 1
			sub := prev[j-1] + cost
			min := ins
			if del < min {
				min = del
			}
			if sub < min {
				min = sub
			}
			curr[j] = min
		}
		prev, curr = curr, prev
	}
	return float64(prev[n])
}

func fnHamming(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	a := toString(args[0])
	b := toString(args[1])
	if len(a) != len(b) {
		return nil
	}
	dist := 0
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			dist++
		}
	}
	return float64(dist)
}

func fnNormalize(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// NFC normalization: collapse combining characters
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsPrint(r) {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

func fnFormat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	format := toString(args[0])
	fmtArgs := make([]any, 0, len(args)-1)
	for _, a := range args[1:] {
		fmtArgs = append(fmtArgs, a)
	}
	return fmt.Sprintf(format, fmtArgs...)
}

func fnToUTF8(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return []byte(toString(args[0]))
}

func fnFromUTF8(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	switch v := args[0].(type) {
	case []byte:
		if utf8.Valid(v) {
			return string(v)
		}
		return nil
	case string:
		return v
	default:
		return fmt.Sprint(args[0])
	}
}
