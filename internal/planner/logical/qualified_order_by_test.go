package logical

import (
	"strings"
	"testing"
)

// The rule these tests pin, in PostgreSQL's words: an ORDER BY term that is a
// BARE identifier is matched against the SELECT list's OUTPUT names first, and
// a QUALIFIED one never is — `x.col` is resolved in the FROM scope like any
// other expression, so it names the INPUT column even when a select alias
// shadows it.
//
// Every expectation below was read off postgres:17-alpine over
// `CREATE TABLE s (a INT, b INT, nm TEXT)`:
//
//	SELECT s.b AS a, s.nm FROM s s ORDER BY s.a DESC  → ordered by the real a
//	SELECT s.b AS a, s.nm FROM s s ORDER BY a   DESC  → ordered by the ALIAS
//
// Wadjet used to answer both the same way, because namesSameColumn tolerates
// one side carrying a qualifier the other omits and the shadowing alias
// therefore "matched" the qualified term. The sort then read the alias's
// column: silently the wrong order, on the single-process pipeline and on the
// stage DAG alike (#488).

// TestQualifiedSortTermBindsTheInputColumn: a shadowing alias must not capture
// a qualified ORDER BY term. The term is materialized as a hidden key over the
// input instead.
func TestQualifiedSortTermBindsTheInputColumn(t *testing.T) {
	plan, err := planOf(t, "SELECT s.b AS a, s.nm FROM s s ORDER BY s.a DESC")
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	sort, child := sortNodeOf(t, plan)
	if len(sort.OrderBy) != 1 {
		t.Fatalf("sort has %d keys, want 1:\n%s", len(sort.OrderBy), plan.PrettyPrint(0))
	}
	key := sort.OrderBy[0].Column
	if !IsHiddenSortColumn(key) {
		t.Fatalf("sort key is %q — the qualified term bound to the SELECT list instead of the input column:\n%s",
			key, plan.PrettyPrint(0))
	}
	// And the materialized projection really reads s.a.
	var found bool
	for _, p := range child.Projections {
		if strings.EqualFold(p.Alias, key) {
			found = true
			if !strings.EqualFold(p.Column, "s.a") && !strings.EqualFold(p.Expr, "s.a") {
				t.Errorf("hidden sort projection reads %q/%q, want s.a", p.Column, p.Expr)
			}
		}
	}
	if !found {
		t.Errorf("no hidden projection named %q on the Sort's input:\n%s", key, plan.PrettyPrint(0))
	}
}

// TestBareSortTermStillBindsTheShadowingAlias is the other half of the rule,
// and the reason the fix cannot be "never match output names": PostgreSQL
// binds the BARE spelling to the alias. This is #327's root-level family and
// it must stay exactly as it is.
func TestBareSortTermStillBindsTheShadowingAlias(t *testing.T) {
	plan, err := planOf(t, "SELECT s.b AS a, s.nm FROM s s ORDER BY a DESC")
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	sort, _ := sortNodeOf(t, plan)
	if len(sort.OrderBy) != 1 || !strings.EqualFold(sort.OrderBy[0].Column, "a") {
		t.Errorf("sort keys %v, want the alias `a` — a bare ORDER BY term binds the SELECT list:\n%s",
			sort.OrderBy, plan.PrettyPrint(0))
	}
}

// TestQualifiedSortTermOverItsOwnColumnStillBindsDirectly: the rule costs
// nothing where the output really IS the column. These shapes must not grow a
// hidden key — the sort reads the column the projection already emits.
func TestQualifiedSortTermOverItsOwnColumnStillBindsDirectly(t *testing.T) {
	for _, sql := range []string{
		// The select item is that very column, qualified the same way.
		"SELECT s.a, s.nm FROM s s ORDER BY s.a DESC",
		// Unqualified select item: a bare column in a SELECT list is only
		// legal when one relation in scope carries the name, so it cannot be
		// the other side of a self-join.
		"SELECT a, nm FROM s s ORDER BY s.a DESC",
		// resolveOrderByColumn maps the term onto the item by its expression
		// text before the output-name test is reached.
		"SELECT s.a AS k, s.nm FROM s s ORDER BY s.a DESC",
	} {
		t.Run(sql, func(t *testing.T) {
			plan, err := planOf(t, sql)
			if err != nil {
				t.Fatalf("BuildFromSelect: %v", err)
			}
			sort, _ := sortNodeOf(t, plan)
			if len(sort.OrderBy) != 1 {
				t.Fatalf("sort has %d keys, want 1", len(sort.OrderBy))
			}
			if IsHiddenSortColumn(sort.OrderBy[0].Column) {
				t.Errorf("sort key %q was materialized; the projection already emits this column:\n%s",
					sort.OrderBy[0].Column, plan.PrettyPrint(0))
			}
		})
	}
}

// TestQualifiedSortTermPicksTheNamedSelfJoinArm: over a self-join both arms
// answer to the same bare column name, so the qualifier is the ONLY thing that
// says which. Binding to the other arm's select item would sort by the wrong
// side — PostgreSQL orders this by n1's column.
func TestQualifiedSortTermPicksTheNamedSelfJoinArm(t *testing.T) {
	plan, err := planOf(t,
		"SELECT n2.nm, n1.r FROM n n1 JOIN n n2 ON n1.r = n2.id ORDER BY n1.nm")
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	sort, child := sortNodeOf(t, plan)
	if len(sort.OrderBy) != 1 || !IsHiddenSortColumn(sort.OrderBy[0].Column) {
		t.Fatalf("sort keys %v — `n1.nm` bound to n2's select item:\n%s",
			sort.OrderBy, plan.PrettyPrint(0))
	}
	for _, p := range child.Projections {
		if strings.EqualFold(p.Alias, sort.OrderBy[0].Column) &&
			!strings.EqualFold(p.Column, "n1.nm") && !strings.EqualFold(p.Expr, "n1.nm") {
			t.Errorf("hidden sort projection reads %q/%q, want n1.nm", p.Column, p.Expr)
		}
	}
}
