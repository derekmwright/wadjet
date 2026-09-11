package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// refuseOuterLevelReference classifies an unresolved qualified reference in a
// plain derived table's body (#614). Enclosing-query references are legal SQL,
// but unsupported here because correlation analysis cannot see through that
// separate block; refuse 0A000 with predicate-lift/LATERAL workarounds.
// Same-FROM siblings still require LATERAL and keep 42P01: outerDiag contains
// only enclosing levels, never this block's FROM. ADR-0021 §4.
// See docs/internals/derived-table-outer-reference-boundary.md for the design.
func (s *colScope) refuseOuterLevelReference(ref *plansql.ColRef) error {
	if s == nil || s.outerDiag == nil || ref == nil {
		return nil
	}
	if s.outerDiag.quals[strings.ToLower(ref.Table)] == nil {
		return nil
	}
	return sqlerr.New("0A000",
		"a derived table in FROM that references %q from an enclosing query is not supported: "+
			"the reference is legal SQL, and this engine plans a derived table's body as its own "+
			"query block, where %q names nothing — lift the correlated predicate out of the "+
			"derived table, or write the derived table as a LATERAL join",
		ref.Table, ref.Table)
}
