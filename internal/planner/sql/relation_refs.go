// SPDX-License-Identifier: MIT

package sql

import "strings"

// This file answers "does this block read relation X" over the PARSED tree —
// the question a recursive CTE's form is decided by (ADR-0021 §1o-a).

// SelectNamesRelation reports whether info's FROM — at any nesting, through a
// derived table and through both arms of a set operation — names `want`.
func SelectNamesRelation(info *SelectInfo, want string) bool {
	if info == nil {
		return false
	}
	if info.Union != nil {
		if SelectNamesRelation(info.Union.Left, want) || SelectNamesRelation(info.Union.Right, want) {
			return true
		}
	}
	refNames := func(t TableRef) bool {
		if strings.HasPrefix(t.Name, "(") {
			sub, err := t.SubSelect()
			if err != nil {
				return false
			}
			return SelectNamesRelation(sub, want)
		}
		return strings.EqualFold(strings.TrimSpace(t.Name), want)
	}
	for i := range info.Tables {
		if refNames(info.Tables[i]) {
			return true
		}
	}
	for i := range info.Joins {
		ref := TableRef{Name: info.Joins[i].RightTable}
		if info.Joins[i].RightTableRef != nil {
			ref = *info.Joins[i].RightTableRef
		}
		if refNames(ref) {
			return true
		}
	}
	// A nested block's OWN `WITH` may shadow the name; this walk deliberately
	// does not, because a shadowing item is answered from the enclosing scope
	// here anyway (docs/internals/nested-with-scope-precedence.md) and a
	// false positive costs a refusal where a wrong answer would otherwise
	// stand.
	for i := range info.CTEs {
		if b, err := info.CTEs[i].BodySelect(); err == nil && SelectNamesRelation(b, want) {
			return true
		}
	}
	return false
}
