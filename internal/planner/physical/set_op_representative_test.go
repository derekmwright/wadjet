// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The local set operations choose each member's representative by the
// representative rule (#1713; ADR-0024's 2026-11-24 amendment): the smallest
// eligible member under the representative order, whole-row, whatever order
// the rows arrive in. INTERSECT's and EXCEPT's eligible members are the left
// arm's; an ALL form repeats that one representative.
func TestSetOpRepresentativeRule(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
		{Name: "b", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
	}
	k := newSetOpKeyer(schema)
	row := func(a, b string) map[string]any { return map[string]any{"a": a, "b": b} }
	render := func(rows []map[string]any) string {
		var out []string
		for _, r := range rows {
			out = append(out, fmt.Sprintf("%v/%v", r["a"], r["b"]))
		}
		sort.Strings(out)
		return strings.Join(out, " ")
	}
	reverse := func(rows []map[string]any) []map[string]any {
		out := make([]map[string]any, len(rows))
		for i, r := range rows {
			out[len(rows)-1-i] = r
		}
		return out
	}
	// The issue's EXCEPT ALL: left = {1, 1.5, 7, 1.00, 1.50, 7.00, 1, ...}
	// minus right = {1.25, 7}: PostgreSQL 17.11 answers 1, 1, 1, 1.5, 1.5, 7
	// (its representative of each group, repeated).
	left := []map[string]any{
		row("1", "0"), row("1.5", "0"), row("7", "0"), row("1", "0"),
		row("1.00", "0"), row("1.50", "0"), row("7.00", "0"),
	}
	right := []map[string]any{row("1.25", "0"), row("7", "0")}
	for _, tc := range []struct {
		name string
		got  func(l, r []map[string]any) []map[string]any
		want string
	}{
		{"except all", func(l, r []map[string]any) []map[string]any { return exceptRows(k, l, r, true) },
			"1.5/0 1.5/0 1/0 1/0 1/0 7/0"},
		{"except", func(l, r []map[string]any) []map[string]any { return exceptRows(k, l, r, false) },
			"1.5/0 1/0"},
		{"intersect", func(l, r []map[string]any) []map[string]any { return intersectRows(k, l, r, false) },
			"7/0"},
		{"intersect all", func(l, r []map[string]any) []map[string]any { return intersectRows(k, l, r, true) },
			"7/0"},
		// A right member is never the representative: the left arm's 7.00
		// survives a right 7.
		{"intersect, left only", func(l, r []map[string]any) []map[string]any {
			return intersectRows(k, []map[string]any{row("7.00", "0")}, []map[string]any{row("7", "0")}, false)
		}, "7.00/0"},
		{"union", func(l, r []map[string]any) []map[string]any {
			return deduplicateRows(k, append(append([]map[string]any(nil), l...), r...))
		},
			"1.25/0 1.5/0 1/0 7/0"},
		// Whole row: the smaller spellings are in different members; the
		// survivor is one member (the smaller first column), never 7/8.
		{"union whole row", func(l, r []map[string]any) []map[string]any {
			return deduplicateRows(k, []map[string]any{row("7.00", "8"), row("7", "8.00")})
		}, "7/8.00"},
	} {
		a := render(tc.got(left, right))
		b := render(tc.got(reverse(left), reverse(right)))
		if a != tc.want || b != tc.want {
			t.Errorf("%s: got %q (reversed %q), want %q", tc.name, a, b, tc.want)
		}
	}
}
