// SPDX-License-Identifier: MIT

// This file holds expr string distance; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"

	"github.com/derekmwright/wadjet/internal/sqlerr"
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

// fnNormalize is NORMALIZE(text [, NFC | NFD | NFKC | NFKD]) — the Unicode
// normalization the SQL standard spells with a bare form keyword and the
// grammar rewrites into this call's second argument (#1169).
//
// It used to STRIP every non-printing character and call that NFC, which is
// neither normalization nor a form the standard has: the combining sequence
// U+0065 U+0301 came back unchanged (length 2) where PostgreSQL 17.11 answers
// the composed character (length 1), and a legitimate control character was
// deleted from the value. golang.org/x/text/unicode/norm is the normalizer,
// and the form defaults to NFC exactly as the one-argument spelling does on
// the server.
func fnNormalize(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	form := "NFC"
	if len(args) >= 2 {
		if args[1] == nil {
			return nil
		}
		form = strings.ToUpper(strings.TrimSpace(toString(args[1])))
	}
	var f norm.Form
	switch form {
	case "NFC":
		f = norm.NFC
	case "NFD":
		f = norm.NFD
	case "NFKC":
		f = norm.NFKC
	case "NFKD":
		f = norm.NFKD
	default:
		// The grammar admits only the four forms, so this is reachable only
		// through the function spelling with a computed argument. The server
		// refuses the unknown form at parse time; this refuses it loudly with
		// the class it uses for a bad function argument.
		panic(fatalEval{sqlerr.New("22023",
			"invalid normalization form: %s", sqlerr.Quote(form))})
	}
	return f.String(toString(args[0]))
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
