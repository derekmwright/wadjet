package expr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// THE TCP FLAG FAMILY'S REFUSAL IS STATED ONCE, AND IT IS THE PLAN-TIME ONE
// (#1018 round 6, B2).
//
// `docs/network-analytics.md` carried both rules at once: a new paragraph
// saying a constant name is folded before execution, and — four lines below it,
// in the same section — the sentence it replaced, "the refusal is raised per
// ROW … so a predicate that no row reaches answers zero rows rather than an
// error". Two paragraphs of the same document told a user opposite things about
// the same query, and the surviving one was false against the engine.
//
// A prose contradiction is not something a value gate can see, so this one
// reads the files. It asserts BOTH directions — the retired sentence is absent
// AND the rule that replaced it is present — because a gate that only forbids
// a phrase passes on a document that says nothing at all.
//
// It reads three NAMED files rather than walking the tree: a walking gate sees
// a git worktree's second copy of everything (CLAUDE.md's standing rule).
func TestTheTCPFlagDocsStateOneRefusalRule(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		file    string
		absent  []string
		present []string
	}{
		{
			file: "docs/network-analytics.md",
			absent: []string{
				"The refusal is raised per ROW",
				"no row reaches answers zero rows rather than an error",
			},
			present: []string{
				"Whatever the rows are includes NO rows.",
				"every position and on every plan",
				"Recursive CTE bodies include both the seed",
				"shadowing CTE body",
				// The three positions round 7 measured the walk missing, now
				// named where a reader looks for them (#1018 round 7, B1).
				"a `GROUP BY` key",
				"those positions, and for one nested inside another subquery",
			},
		},
		{
			file: "docs/sql-reference.md",
			absent: []string{
				"The refusal is raised per ROW",
				"no row reaches answers zero rows rather than an error",
			},
			present: []string{
				"constant is refused before any row",
				"every expression position and on both plans",
				"Recursive CTE bodies include both the seed",
				"shadowing CTE body",
				"a `GROUP BY` key",
				"including one nested inside another subquery",
			},
		},
		{
			file: "docs/adr/0012-sql-semantics-authority.md",
			absent: []string{
				// The claim round 5 made and round 6 measured false: one site
				// at compilation is NOT one rule everywhere, because a DAG
				// stage compiles its fragment only when a task runs.
				"at COMPILATION, which is the seam every door goes through whether",
			},
			present: []string{
				"AN INVALID LITERAL NAME IS REFUSED FROM THE DECLARATION, ROWS OR NO",
				"THE DECIDING LAYER IS THE BINDER, NOT COMPILATION",
				"RECURSIVE CTE BODIES ARE VALIDATED BEFORE ROWS",
				"#1037",
				"cte_shadowed_body",
				"set_order_by",
				`"EVERY EXPRESSION POSITION" IS A CLAIM ABOUT THE WALK, AND THE WALK HAD`,
			},
		},
	} {
		t.Run(tc.file, func(t *testing.T) {
			b, err := os.ReadFile(filepath.Join(root, tc.file))
			if err != nil {
				t.Fatalf("reading %s: %v", tc.file, err)
			}
			text := string(b)
			for _, phrase := range tc.absent {
				if strings.Contains(text, phrase) {
					t.Errorf("%s still says %q — the per-row rule was retired when the "+
						"fold moved to plan time; a document that states both states neither",
						tc.file, phrase)
				}
			}
			for _, phrase := range tc.present {
				if !strings.Contains(text, phrase) {
					t.Errorf("%s no longer says %q — the rule this gate protects has to be "+
						"WRITTEN somewhere, or forbidding its opposite proves nothing",
						tc.file, phrase)
				}
			}
		})
	}
}
