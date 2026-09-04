package logical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The predicate pushdown's ROW-FIELD-PATH order, and the ANNOTATION it depends
// on — attempted from both sides (#769).
//
// `projRefs.resolve` asks whether a dotted reference is a field path BEFORE it
// strips the qualifier, because the strip is a fallback meant for a RELATION
// qualifier: over `SELECT c_row AS rw, id AS b`, `rw.b` is the FIELD, which is
// PostgreSQL's `(rw).b` reading and its only anchored one.
//
// It answers that question from `subtreeRowFields`, which reads
// `Node.ScanColFields` — populated by `physical.AnnotateScanColumns`. Where
// nothing below is annotated the map is EMPTY and the resolution keeps the
// pre-#769 order, which is a silent fall back to the old answer rather than a
// failure. That is deliberate — a false field-path reading is worse than the
// old one — and it is the arc's own self-flag, so it gets the fixture the
// flag did not have: BOTH states of the annotation, over one tree, asserted to
// give the two different answers on purpose.
func TestRowFieldPathPushdownFollowsTheAnnotation(t *testing.T) {
	rowFields := []parquet.Column{
		{Name: "a", Type: parquet.TypeString, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
	}
	// SELECT c_row AS rw, id AS b FROM nested   —   WHERE rw.b > 1
	build := func(annotate bool) *Node {
		scan := NewScan("nested", "n")
		scan.ScanColumns = []string{"id", "c_row"}
		if annotate {
			scan.ScanColFields = map[string][]parquet.Column{"c_row": rowFields}
		}
		proj := NewProject(scan, []Projection{
			{Alias: "rw", Column: "c_row", Expr: "c_row"},
			{Alias: "b", Column: "id", Expr: "id"},
		})
		return NewFilter(proj, []Predicate{predGT("rw", "b")})
	}

	// ANNOTATED: the field path is recognised, so the reference is rewritten
	// to the CONTAINER's own name and the field is kept — `c_row.b`.
	got := pushedPredicateText(t, pushdownPredicates(build(true)))
	if !strings.Contains(got, "c_row.b") {
		t.Errorf("with ScanColFields annotated the pushed predicate is %q, want it to name "+
			"the container's field `c_row.b` — the strip is a fallback for a RELATION "+
			"qualifier and must not take a container", got)
	}

	// UNANNOTATED: nothing below says `c_row` is a ROW, so the qualifier is
	// stripped and the bare output `b` — which is `id` — answers. That is the
	// pre-#769 order, kept deliberately, and asserting it is what makes the
	// dependency visible rather than latent.
	got = pushedPredicateText(t, pushdownPredicates(build(false)))
	if strings.Contains(got, "c_row.b") {
		t.Errorf("with no ScanColFields the pushed predicate is %q; the walk cannot know "+
			"`c_row` is a container there, so it must keep the qualifier-stripping order "+
			"rather than guess a field path", got)
	}
	if !strings.Contains(got, "id") {
		t.Errorf("with no ScanColFields the pushed predicate is %q, want the bare output's "+
			"own source column `id`", got)
	}
}

// pushedPredicateText renders the predicates of the FIRST Filter found under
// the result of a pushdown, which is where the rewritten reference lands.
func pushedPredicateText(t *testing.T, n *Node) string {
	t.Helper()
	var out []string
	var walk func(*Node)
	walk = func(cur *Node) {
		if cur == nil {
			return
		}
		if cur.Type == NodeFilter {
			for _, p := range cur.Predicates {
				if p.ASTExpr != nil {
					out = append(out, p.ASTExpr.String())
					continue
				}
				out = append(out, p.Raw)
			}
		}
		for _, c := range cur.Children {
			walk(c)
		}
	}
	walk(n)
	if len(out) == 0 {
		t.Fatalf("the pushdown produced no Filter at all")
	}
	return strings.Join(out, " AND ")
}
