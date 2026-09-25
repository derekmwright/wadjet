// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// THE TEXT PATH'S NET IS THE PREDICATE'S SHAPE (arc JP round 4, B3). The raw
// fallback evaluates `<column> <op> <constants>` and nothing else, so every
// other shape — a column in a value position, NOT BETWEEN / NOT IN (read as
// a column named `c NOT`), `<>`, a boolean combination, CASE, IS DISTINCT
// FROM, a function or an expression on the subject side — is refused rather
// than compared as text or dropped. Round 3's net inspected only a
// comparison's right side, and `q.qv BETWEEN o.total AND o.total + 20`
// answered zero rows through it.
func TestTheTextPathReadsOnlyAColumnAgainstConstants(t *testing.T) {
	reads := []string{
		"c = 5", "c >= 5", "c != 'x'", "c < -1.5", "c BETWEEN 1 AND 3",
		"c IN (1, 2, 3)", "c LIKE 'a%'", "c NOT LIKE 'a%'", "c IS NULL",
		"c IS NOT NULL", "(c = 5)", "t.c = 5",
	}
	refused := []string{
		// a column in a value position
		"c = o.k", "c = o.k - 0", "c BETWEEN o.total AND o.total + 20",
		"c BETWEEN 1 AND o.total", "c IN (o.total - 5, 3)", "c LIKE o.pat",
		"c LIKE CASE WHEN o.k = 1 THEN 'a%' ELSE 'x%' END",
		// the text path misreads these even over constants
		"c NOT BETWEEN 1 AND 3", "c NOT IN (1, 2)", "c <> 5",
		// shapes it cannot read at all
		"c = 1 AND d = 2", "c = 1 OR d = 2", "NOT (c = 1)",
		"CASE WHEN o.k = 1 THEN c > 20 ELSE c < 40 END",
		"c IS DISTINCT FROM 5", "c IS NOT DISTINCT FROM o.total",
		"abs(c - o.total) < 10", "lower(c) = 'x'", "(c > o.total) = true",
		"c + o.total > 50", "c = (SELECT 1)", "c IS TRUE",
	}
	for _, raw := range reads {
		if !rawTextPathReads(raw) {
			t.Errorf("%q: the text path reads this shape, but the net refuses it", raw)
		}
		op, err := (&Planner{}).buildFilterOp(logical.Predicate{Raw: raw}, nil, nil)
		if err != nil || op == nil {
			t.Errorf("%q: want a text-path filter, got %v / %v", raw, op, err)
		}
	}
	for _, raw := range refused {
		if rawTextPathReads(raw) {
			t.Errorf("%q: the text path would read this as a column against a literal", raw)
		}
		if _, err := (&Planner{}).buildFilterOp(logical.Predicate{Raw: raw}, nil, nil); err == nil {
			t.Errorf("%q: planned through the text path; want a refusal", raw)
		}
	}
}
