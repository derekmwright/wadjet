package sql

import (
	"strings"
	"testing"
)

// THE INNER RELATION'S SCHEMA DECIDES — #955, at the classifier.
//
// An unqualified name inside a subquery binds the subquery's own FROM when
// that FROM has a column of the name, whatever KIND of relation supplies it.
// These cells drive the classifier directly, one relation shape per cell, so a
// regression names the shape rather than a query's row count.
//
// `i1Catalog` stands for a catalog: it answers for base tables and for nothing
// else, which is exactly what `Planner.subqueryInnerColumns`' base resolver
// does.
func i1Catalog(table string) []string {
	switch strings.ToLower(table) {
	case "typemx":
		return []string{"id", "g", "c_i64"}
	case "decpair":
		return []string{"id", "a", "b", "s"}
	}
	return nil
}

func TestAnUnqualifiedNameBindsTheInnerRelationWhicheverKindItIs(t *testing.T) {
	// The enclosing query is `… FROM decpair WHERE id < 2`, so `id` and `s`
	// are outer names. `id` is ALSO an inner name in most cells below, and
	// that collision is the whole subject.
	outerTables := map[string]bool{"decpair": true}
	outerCols := map[string]string{"id": "decpair", "a": "decpair", "b": "decpair", "s": "decpair"}

	cte := func(name, body string) []CTEDef { return []CTEDef{{Name: name, SQL: body}} }

	for _, tc := range []struct {
		name string
		sql  string
		ctes []CTEDef
		// want is the number of OUTER references the classifier reports. 0
		// means "uncorrelated", which is what SQL says for every shape whose
		// inner FROM carries the name.
		want int
	}{
		{name: "base_table_inner_from",
			sql:  `SELECT MAX(c_i64) FROM typemx WHERE id < 4000`,
			want: 0},
		{name: "cte_reference_inner_from",
			sql:  `SELECT MAX(v) FROM c WHERE id < 4000`,
			ctes: cte("c", `SELECT id, c_i64 AS v FROM typemx`),
			want: 0},
		{name: "cte_reference_under_an_alias",
			sql:  `SELECT MAX(v) FROM c x WHERE id < 4000`,
			ctes: cte("c", `SELECT id, c_i64 AS v FROM typemx`),
			want: 0},
		{name: "cte_with_an_explicit_column_list",
			sql:  `SELECT MAX(vv) FROM c WHERE idd < 4000`,
			ctes: []CTEDef{{Name: "c", SQL: `SELECT id, c_i64 FROM typemx`, Columns: []string{"idd", "vv"}}},
			want: 0},
		{name: "cte_body_is_a_star",
			sql:  `SELECT COUNT(*) FROM c WHERE id < 4000`,
			ctes: cte("c", `SELECT * FROM typemx`),
			want: 0},
		{name: "cte_body_is_a_set_operation",
			sql: `SELECT MAX(v) FROM c WHERE id < 4000`,
			ctes: cte("c", `SELECT id, c_i64 AS v FROM typemx WHERE id < 2000 `+
				`UNION ALL SELECT id, c_i64 FROM typemx WHERE id >= 2000`),
			want: 0},
		{name: "derived_table_inner_from",
			sql:  `SELECT MAX(v) FROM (SELECT id, c_i64 AS v FROM typemx) t WHERE id < 4000`,
			want: 0},
		{name: "derived_table_whose_select_list_is_a_star",
			sql:  `SELECT COUNT(*) FROM (SELECT * FROM typemx) t WHERE id < 4000`,
			want: 0},
		{name: "derived_table_that_is_a_set_operation",
			sql: `SELECT MAX(v) FROM (SELECT id, c_i64 AS v FROM typemx WHERE id < 2000 ` +
				`UNION ALL SELECT id, c_i64 FROM typemx WHERE id >= 2000) t WHERE id < 4000`,
			want: 0},
		{name: "the_cte_reference_is_the_right_side_of_a_join",
			sql:  `SELECT COUNT(*) FROM typemx_dim d JOIN c ON c.g = d.k WHERE id < 10`,
			ctes: cte("c", `SELECT id, g, c_i64 AS v FROM typemx`),
			want: 0},
		{name: "a_subquery_nested_two_deep_over_the_same_cte",
			sql:  `SELECT MAX(v) FROM c WHERE id < (SELECT MAX(id) FROM c WHERE id < 4000)`,
			ctes: cte("c", `SELECT id, c_i64 AS v FROM typemx`),
			want: 0},
		{name: "the_name_is_in_the_HAVING",
			sql:  `SELECT MAX(v) FROM c GROUP BY g HAVING MIN(id) < 1`,
			ctes: cte("c", `SELECT id, g, c_i64 AS v FROM typemx`),
			want: 0},

		// --- the boundary, from the other side ---------------------------
		//
		// A name NO inner relation carries is still an outer reference. Two
		// spellings, because the two arms of walkForOuterRefs are different
		// code: one qualified, one bare.
		{name: "boundary_a_bare_name_only_the_outer_has_is_correlated",
			sql:  `SELECT COUNT(*) FROM c WHERE c.id < 10 AND s = '1.50'`,
			ctes: cte("c", `SELECT id, c_i64 AS v FROM typemx`),
			want: 1},
		{name: "boundary_a_qualified_outer_name_is_correlated",
			sql:  `SELECT COUNT(*) FROM c WHERE c.id < decpair.id`,
			ctes: cte("c", `SELECT id, c_i64 AS v FROM typemx`),
			want: 1},
		// A CTE that is NOT in scope resolves to nothing, so its columns are
		// unknown and the bare name goes back to the outer scope. This is the
		// fallback the fix leaves in place, stated rather than assumed: the
		// classifier never claims a relation it cannot name has a column.
		{name: "boundary_a_relation_the_resolver_cannot_name_falls_back",
			sql:  `SELECT MAX(v) FROM notacte WHERE id < 4000`,
			want: 1},
		// A column-alias list HIDES the names it replaces, so `id` is no
		// longer an inner name here and the reference is the outer one.
		{name: "boundary_a_column_alias_list_hides_the_name_it_replaces",
			sql:  `SELECT MAX(vv) FROM (SELECT id, c_i64 FROM typemx) t(idd, vv) WHERE id < 4000`,
			want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolve := CTEColumns(tc.ctes, i1Catalog)
			refs, err := FindCorrelatedRefsWithScope(tc.sql, outerTables, outerCols, resolve)
			if err != nil {
				t.Fatalf("FindCorrelatedRefsWithScope: %v", err)
			}
			if len(refs) != tc.want {
				t.Fatalf("got %d outer refs %+v, want %d\n  SQL: %s", len(refs), refs, tc.want, tc.sql)
			}
		})
	}
}

// CTEColumns answers the TableColumns contract — the COMPLETE column list or
// nothing — for the shapes a WITH item takes, and terminates on a body that
// names the item it is defining.
func TestCTEColumnsAnswersACompleteListOrNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctes []CTEDef
		ask  string
		want []string
	}{
		{name: "an_explicit_column_list_wins_over_the_body",
			ctes: []CTEDef{{Name: "c", SQL: `SELECT id, c_i64 FROM typemx`, Columns: []string{"idd", "vv"}}},
			ask:  "c", want: []string{"idd", "vv"}},
		{name: "the_bodys_select_list_names_the_columns",
			ctes: []CTEDef{{Name: "c", SQL: `SELECT id, c_i64 AS v FROM typemx`}},
			ask:  "c", want: []string{"id", "v"}},
		{name: "a_star_body_expands_through_the_base_resolver",
			ctes: []CTEDef{{Name: "c", SQL: `SELECT * FROM typemx`}},
			ask:  "c", want: []string{"id", "g", "c_i64"}},
		{name: "a_star_body_over_a_relation_the_base_cannot_name_is_unknown",
			ctes: []CTEDef{{Name: "c", SQL: `SELECT * FROM notatable`}},
			ask:  "c", want: nil},
		{name: "a_set_operation_body_publishes_its_left_arms_names",
			ctes: []CTEDef{{Name: "c", SQL: `SELECT id AS l FROM typemx UNION ALL SELECT id AS r FROM typemx`}},
			ask:  "c", want: []string{"l"}},
		{name: "an_earlier_item_is_visible_to_a_later_ones_body",
			ctes: []CTEDef{
				{Name: "a", SQL: `SELECT id, c_i64 AS v FROM typemx`},
				{Name: "b", SQL: `SELECT * FROM a`},
			},
			ask: "b", want: []string{"id", "v"}},
		// A body that names its OWN item does not resolve against it —
		// PostgreSQL's rule, and what keeps the expansion from recurring
		// without bound. `WITH RECURSIVE r AS (SELECT * FROM r)` reaches the
		// base resolver, which has no such table, so the answer is unknown
		// rather than a stack overflow.
		{name: "a_self_referencing_star_body_terminates_as_unknown",
			ctes: []CTEDef{{Name: "r", SQL: `SELECT * FROM r`, Recursive: true}},
			ask:  "r", want: nil},
		// Two items that name each other cannot be written in SQL (a body
		// sees only EARLIER items), but the resolver is asked about names
		// from parsed text and must terminate on one anyway.
		{name: "mutually_naming_star_bodies_terminate_as_unknown",
			ctes: []CTEDef{
				{Name: "p", SQL: `SELECT * FROM q`},
				{Name: "q", SQL: `SELECT * FROM p`},
			},
			ask: "q", want: nil},
		{name: "a_name_that_is_no_item_goes_to_the_base_resolver",
			ctes: []CTEDef{{Name: "c", SQL: `SELECT id FROM typemx`}},
			ask:  "typemx", want: []string{"id", "g", "c_i64"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := CTEColumns(tc.ctes, i1Catalog)(tc.ask)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("CTEColumns(%q) = %v, want %v", tc.ask, got, tc.want)
			}
		})
	}
}

// CTEColumns with no items is the base resolver itself, and with no base and
// no items it is nil — the callers that have neither (the logical optimizer
// has no catalog) must not be handed a closure that answers for everything.
func TestCTEColumnsWithoutItemsIsTheBaseResolver(t *testing.T) {
	if got := CTEColumns(nil, nil); got != nil {
		t.Fatalf("CTEColumns(nil, nil) = non-nil, want nil")
	}
	got := CTEColumns(nil, i1Catalog)("typemx")
	if strings.Join(got, ",") != "id,g,c_i64" {
		t.Fatalf("CTEColumns(nil, catalog)(typemx) = %v", got)
	}
	// With items but no base, a base table is unknown and an item is not.
	r := CTEColumns([]CTEDef{{Name: "c", SQL: `SELECT id, c_i64 AS v FROM typemx`}}, nil)
	if got := r("typemx"); got != nil {
		t.Fatalf("no base resolver: typemx = %v, want nil", got)
	}
	if got := r("c"); strings.Join(got, ",") != "id,v" {
		t.Fatalf("no base resolver: c = %v, want [id v]", got)
	}
}

// A column-alias list renames the leading outputs POSITIONALLY and hides what
// it replaces; one longer than the relation is PostgreSQL's 42P10, which the
// binder raises and this classifier answers "unknown" for rather than guessing.
func TestAColumnAliasListRenamesPositionally(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want []string
	}{
		{name: "shorter_than_the_relation_renames_the_leading_columns",
			sql: `SELECT 1 FROM (SELECT id, g, c_i64 FROM typemx) t(idd)`, want: []string{"idd", "g", "c_i64"}},
		{name: "exactly_as_long_renames_all_of_them",
			sql: `SELECT 1 FROM (SELECT id, g FROM typemx) t(x, y)`, want: []string{"x", "y"}},
		{name: "longer_than_the_relation_is_unknown",
			sql: `SELECT 1 FROM (SELECT id FROM typemx) t(x, y)`, want: nil},
		{name: "over_a_relation_with_no_known_names_it_is_the_whole_answer",
			sql: `SELECT 1 FROM (SELECT * FROM notatable) t(x, y)`, want: []string{"x", "y"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info, err := parseBlockText(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := sourceColumns(&info.Tables[0], TableColumns(i1Catalog))
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("sourceColumns = %v, want %v\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}
