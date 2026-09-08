package expr

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
)

// THE DOCUMENTED SCALAR-FUNCTION COUNT IS THE REGISTRY'S OWN (#965 round 3).
//
// `README.md` and `docs/sql-reference.md` each state a number of built-in
// scalar functions. Nothing checked it, so it drifted the way every
// hand-maintained count drifts: this arc registered `time_bucket` and bumped
// the AGGREGATE count beside it, and both scalar counts stayed at 359 through
// two review rounds. The 2026-09-02 drift audit found 130 claims in this
// class; a number a test can derive should not be one of them.
//
// The registry is the authority. A function added or removed changes this
// number, the test fails, and the two sentences move with the code — which is
// what "user-facing docs move with the code" means for a claim that is a
// single integer.
//
// It reads two named files rather than walking the tree, deliberately: a gate
// that WALKS sees a git worktree's second copy of everything (CLAUDE.md's
// standing rule, the defect twice already).
func TestTheDocumentedScalarFunctionCountIsTheRegistrys(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	want := len(DefaultRegistry.Names())
	if want == 0 {
		t.Fatal("the registry is empty; this gate would pass vacuously")
	}
	// Both sentences, as they are written. The pattern is anchored on the
	// words around the number so a rewording that drops the claim fails here
	// rather than silently stopping the check.
	re := regexp.MustCompile(`(\d+) built-in scalar functions`)
	for _, rel := range []string{"README.md", filepath.Join("docs", "sql-reference.md")} {
		path := filepath.Join(root, rel)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		m := re.FindAllStringSubmatch(string(body), -1)
		if len(m) == 0 {
			t.Errorf("%s no longer states a built-in scalar function count.\n"+
				"If the claim was deliberately removed, remove it from this gate's "+
				"list too; if it was reworded, the pattern `N built-in scalar "+
				"functions` is what the gate reads.", rel)
			continue
		}
		for _, hit := range m {
			got, err := strconv.Atoi(hit[1])
			if err != nil {
				t.Fatalf("%s: %v", rel, err)
			}
			if got != want {
				t.Errorf("%s says %d built-in scalar functions; the registry has %d.\n"+
					"Update the sentence — `expr.DefaultRegistry` is the authority, and "+
					"this number is derived from it rather than remembered.", rel, got, want)
			}
		}
	}
	// A guard against the gate becoming vacuous the other way: the number has
	// to be one a reader would recognise as this engine's, not a stray match.
	if want < 100 {
		t.Errorf("the registry reports only %d scalar functions — either the "+
			"registration moved or this gate is reading the wrong thing", want)
	}
	t.Log(fmt.Sprintf("registry scalar functions: %d", want))
}
