// Package collide is the COLLIDING-BARE-NAME fixture: three relations whose
// columns are all called c0, c1, c2, the way SQLancer names every schema it
// generates.
//
// It exists because every other corpus in this repository prefixes its column
// names per table — TPC-H's `l_`, `o_`, `c_`, `n_`; the type matrix's `c_i32`,
// `c_str`; multikey's shared `s`, `n`, `d` across four tables of ONE schema —
// so no two DIFFERENT relations in one query ever share a bare column name
// that means different things. That is the blind spot #843 lived in for twelve
// releases: inside a derived table with two or more UNALIASED relations, every
// base scan was re-aliased to the derived table's alias, so `t0.c1` bound to
// whichever relation was planned last and the query answered another table's
// column with no error at all. A fixture whose bare names collide is the only
// shape that can see it, and it belongs in the corpora whether or not any
// particular defect is fixed.
//
// Every Want here was measured on live postgres:17-alpine over these exact
// rows.
package collide

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// The three relations. SQLancer's own names, kept, so a dump from it can be
// replayed against this fixture without renaming anything.
const (
	T0 = "clt0"
	T1 = "clt1"
	T2 = "clt2"
)

// Schema is shared by all three: bare names that mean a DIFFERENT column in
// each table.
func Schema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeBool, Nullable: true},
		{Name: "c1", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c2", Type: parquet.TypeString, Nullable: true},
	}}
}

// Data is one relation's rows. The values are chosen so that a reference
// bound to the WRONG relation cannot answer the right number by accident:
// the three tables' c1 ranges do not overlap, and t0 carries the NULL that
// makes the headline shape's `IS NOT NULL` predicate mean something.
func Data(table string) []map[string]any {
	switch table {
	case T0:
		return []map[string]any{
			{"c0": true, "c1": int32(2145758213), "c2": "t0-a"},
			{"c0": false, "c1": nil, "c2": "t0-b"},
		}
	case T1:
		return []map[string]any{
			{"c0": true, "c1": int32(7), "c2": "t1-a"},
			{"c0": false, "c1": int32(9), "c2": "t1-b"},
		}
	default:
		return []map[string]any{
			{"c0": true, "c1": int32(-1968219095), "c2": "t2-a"},
			{"c0": false, "c1": int32(1737580880), "c2": "t2-b"},
		}
	}
}

// FixtureTable is one loadable table. Every consumer loads Tables(), so a
// relation cannot be added to the corpus and forgotten in one of the gates.
type FixtureTable struct {
	Name   string
	Schema parquet.Schema
	Rows   []map[string]any
}

// Tables is the whole fixture.
func Tables() []FixtureTable {
	return []FixtureTable{
		{T0, Schema(), Data(T0)},
		{T1, Schema(), Data(T1)},
		{T2, Schema(), Data(T2)},
	}
}

// Case is one corpus entry. Want is the ROW LIST PostgreSQL 17 answers,
// rendered one string per row, in the entry's own ORDER when Ordered is set.
type Case struct {
	Name    string
	SQL     string
	Want    []string
	Ordered bool
	// KnownBug pins a divergence from Want. The comparison still runs and
	// Want stays exactly as PostgreSQL wrote it, so a pinned entry that
	// starts AGREEING fails and deleting the pin is the proof of a fix
	// (ADR-0013 §Pins).
	KnownBug string
	Issue    string
}

// Corpus is the shape list. Each entry names a way two relations that share a
// bare column name can be confused for one another.
func Corpus() []Case {
	return []Case{
		{Name: "qualified_ref_no_derived",
			SQL:  "SELECT MIN(" + T0 + ".c1) AS m FROM " + T0 + ", " + T2,
			Want: []string{"m=2145758213"}},
		{Name: "qualified_ref_in_derived_two_relations",
			SQL: "SELECT agg0 FROM (SELECT MIN(" + T0 + ".c1) AS agg0 FROM " + T0 + ", " + T2 +
				") AS asdf",
			Want: []string{"agg0=2145758213"}},
		{Name: "qualified_ref_in_derived_three_relations",
			SQL: "SELECT a, b FROM (SELECT MIN(" + T0 + ".c1) a, MAX(" + T0 + ".c1) b FROM " +
				T0 + ", " + T2 + " CROSS JOIN " + T1 + ") x",
			Want: []string{"a=2145758213 b=2145758213"}},
		{Name: "qualified_ref_in_derived_no_aggregate",
			SQL: "SELECT c FROM (SELECT " + T0 + ".c1 AS c FROM " + T0 + ", " + T2 + ", " + T1 +
				" WHERE " + T0 + ".c1 IS NOT NULL) x ORDER BY c",
			Ordered: true,
			Want: []string{"c=2145758213", "c=2145758213", "c=2145758213",
				"c=2145758213"}},
		{Name: "qualified_ref_in_derived_one_relation",
			SQL:  "SELECT agg0 FROM (SELECT MIN(" + T0 + ".c1) AS agg0 FROM " + T0 + ") AS asdf",
			Want: []string{"agg0=2145758213"}},
		{Name: "qualified_ref_through_a_cte",
			SQL: "WITH asdf AS (SELECT MIN(" + T0 + ".c1) AS agg0 FROM " + T0 + ", " + T2 +
				") SELECT agg0 FROM asdf",
			Want: []string{"agg0=2145758213"}},
		{Name: "predicate_on_the_other_relation",
			SQL: "SELECT c FROM (SELECT " + T0 + ".c2 AS c FROM " + T0 + ", " + T2 + " WHERE " +
				T2 + ".c1 < 0) x ORDER BY c",
			Ordered: true,
			Want:    []string{"c=t0-a", "c=t0-b"}},
		{Name: "both_relations_projected",
			SQL: "SELECT a, b FROM (SELECT " + T0 + ".c2 AS a, " + T2 + ".c2 AS b FROM " + T0 +
				", " + T2 + ") x ORDER BY a, b",
			Ordered: true,
			Want: []string{"a=t0-a b=t2-a", "a=t0-a b=t2-b",
				"a=t0-b b=t2-a", "a=t0-b b=t2-b"}},
		{Name: "join_on_colliding_bare_names",
			SQL: "SELECT c FROM (SELECT " + T0 + ".c2 AS c FROM " + T0 + " JOIN " + T1 +
				" ON " + T0 + ".c0 = " + T1 + ".c0) x ORDER BY c",
			Ordered: true,
			Want:    []string{"c=t0-a", "c=t0-b"}},
		{Name: "nested_derived_tables",
			SQL: "SELECT c FROM (SELECT c FROM (SELECT " + T0 + ".c2 AS c FROM " + T0 + ", " +
				T2 + ") y) x ORDER BY c",
			Ordered: true,
			Want:    []string{"c=t0-a", "c=t0-a", "c=t0-b", "c=t0-b"}},
	}
}

// DuplicateNameCorpus is the part of this fixture whose answer cannot be read
// through a map keyed by column NAME, so it is kept apart from Corpus(): both
// of these project two columns called `c0`, and `map[string]any` holds one of
// them. That losing map is not an accident of the harness — it is the same
// mistake the ENGINE makes in #844, where a UNION ALL branch projecting two
// columns of one bare name binds the first to the LAST one's value. A gate
// that reads rows by name cannot tell the two apart, which is why these
// entries are gated POSITIONALLY (wadjet.QueryResult.Cells) instead.
//
// Want here is one string per row, cells joined by "|", in the entry's own
// ORDER. Measured on live postgres:17-alpine.
func DuplicateNameCorpus() []Case {
	return []Case{
		{Name: "two_same_named_columns_from_two_relations",
			SQL: "SELECT " + T1 + ".c0, " + T2 + ".c1, " + T2 + ".c0 FROM " + T1 + ", " + T2 +
				" ORDER BY 2, 1, 3",
			Ordered: true,
			Want: []string{
				"false|-1968219095|true", "true|-1968219095|true",
				"false|1737580880|false", "true|1737580880|false",
			}},
		{Name: "union_all_branch_with_two_same_named_outputs",
			SQL: "SELECT " + T1 + ".c0, " + T2 + ".c1, " + T2 + ".c0 FROM " + T1 + ", " + T2 +
				" UNION ALL SELECT " + T1 + ".c0, " + T2 + ".c1, " + T2 + ".c0 FROM " + T1 +
				", " + T2 + " ORDER BY 2, 1, 3",
			Ordered: true,
			Want: []string{
				"false|-1968219095|true", "false|-1968219095|true",
				"true|-1968219095|true", "true|-1968219095|true",
				"false|1737580880|false", "false|1737580880|false",
				"true|1737580880|false", "true|1737580880|false",
			}},
	}
}
