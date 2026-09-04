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

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The three relations. SQLancer's own names, kept, so a dump from it can be
// replayed against this fixture without renaming anything.
const (
	T0 = "clt0"
	T1 = "clt1"
	T2 = "clt2"
	// T4 and T5 carry ONE column whose names differ only by CASE. Since an
	// unquoted reference folds (#731) those two names are one name to every
	// resolver, so a qualified reference to either is only resolvable through
	// its RELATION — the identity is (relation, folded name) and never the
	// bare folded name (ADR-0026). Without them nothing in any corpus made
	// that distinction: the arc's round-0 review found `SELECT clt4.MixedCol,
	// clt5.mixedcol FROM clt4, clt5` answering clt5's value TWICE on all four
	// arms.
	T4 = "clt4"
	T5 = "clt5"
	// T6 is the THIRD spelling of that one folded name, all upper case, so a
	// query can name all three relations at once and each reference has two
	// wrong answers available to it rather than one. Round 1's review found
	// single and dag DISAGREEING on exactly that shape.
	T6 = "clt6"
	// TCam is a CamelCase SCHEMA — the shape a parquet-registered table has,
	// and ClickBench's `hits` exactly. It is here because no DAG or worker
	// gate put one through `batch.resolveFoldedIndex`'s case-insensitive arm:
	// the two-path corpora all run on all-lower-case fixtures, so the half of
	// #731 that keeps ClickBench working never executed on a worker.
	TCam = "cltcam"
)

// CamelSchema is TCam's: names a parquet file would carry, not names a DDL
// would fold.
func CamelSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "UserID", Type: parquet.TypeInt64, Nullable: true},
		{Name: "EventDate", Type: parquet.TypeInt32, Nullable: true},
		{Name: "SearchPhrase", Type: parquet.TypeString, Nullable: true},
	}}
}

// CamelData is TCam's rows.
func CamelData() []map[string]any {
	return []map[string]any{
		{"WatchID": int64(1), "UserID": int64(10), "EventDate": int32(20), "SearchPhrase": "abc"},
		{"WatchID": int64(2), "UserID": int64(10), "EventDate": int32(21), "SearchPhrase": "abd"},
		{"WatchID": int64(3), "UserID": int64(11), "EventDate": int32(20), "SearchPhrase": "zzz"},
	}
}

// CaseSchema is T4's and T5's, parameterized by the case-carrying column's
// spelling: `MixedCol` in one relation, `mixedcol` in the other.
func CaseSchema(col string) parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: col, Type: parquet.TypeInt64, Nullable: true},
	}}
}

// CaseData is one case-colliding relation's rows. The three relations' value
// ranges do not overlap, so a reference bound to the wrong one cannot answer
// the right number by accident.
func CaseData(table string) []map[string]any {
	switch table {
	case T4:
		return []map[string]any{
			{"k": int64(1), "MixedCol": int64(100)},
			{"k": int64(2), "MixedCol": int64(200)},
		}
	case T6:
		return []map[string]any{
			{"k": int64(1), "MIXEDCOL": int64(700)},
			{"k": int64(2), "MIXEDCOL": int64(701)},
		}
	default:
		return []map[string]any{
			{"k": int64(1), "mixedcol": int64(900)},
			{"k": int64(2), "mixedcol": int64(901)},
		}
	}
}

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
		{T4, CaseSchema("MixedCol"), CaseData(T4)},
		{T5, CaseSchema("mixedcol"), CaseData(T5)},
		{T6, CaseSchema("MIXEDCOL"), CaseData(T6)},
		{TCam, CamelSchema(), CamelData()},
	}
}

// Case is one corpus entry. Want is the ROW LIST PostgreSQL 17 answers,
// rendered one string per row, in the entry's own ORDER when Ordered is set.
type Case struct {
	Name    string
	SQL     string
	Want    []string
	Ordered bool
	// PGSQL is the spelling PostgreSQL needs for the same question, when the
	// wadjet spelling is one PostgreSQL cannot resolve. It is filled in
	// automatically for every entry naming a CamelCase column of this fixture
	// — see pgSpelling — because that unquoted spelling IS the divergence
	// ADR-0012 records: PostgreSQL matches a folded name exactly against the
	// catalog, so `clt4.MixedCol` is 42703 there and `clt4."MixedCol"` is the
	// question it can answer. Every value this corpus asserts is therefore
	// PostgreSQL's answer to the quoted spelling, which is the contract that
	// entry states.
	PGSQL string
	// Ref is the same question with DISTINCT output names, for the entries
	// whose own names collide. A result read BY NAME cannot represent two
	// columns of one name, so the duplicate spelling is compared against its
	// reference — which is positional by construction and works on the DAG,
	// where no positional accessor reaches the caller.
	Ref string
	// KnownBug pins a divergence from Want. The comparison still runs and
	// Want stays exactly as PostgreSQL wrote it, so a pinned entry that
	// starts AGREEING fails and deleting the pin is the proof of a fix
	// (ADR-0013 §Pins).
	KnownBug string
	Issue    string
}

// camelColumns are this fixture's stored names that are NOT their own folded
// form. A reference to one is resolvable unquoted here and only quoted in
// PostgreSQL.
//
// `MixedCol` is matched CASE-SENSITIVELY because clt5 stores a DIFFERENT
// column spelled `mixedcol`, which needs no quoting and must not be rewritten
// into clt4's — the whole point of the pair. The rest have no lower-case twin,
// so a reference to them in any case names the one stored column.
var camelColumns = []struct {
	name    string
	anyCase bool
}{
	{"MixedCol", false},
	{"MIXEDCOL", false},
	{"WatchID", true}, {"UserID", true}, {"EventDate", true}, {"SearchPhrase", true},
}

// pgSpelling quotes every reference to a CamelCase column of this fixture, in
// whatever case the entry wrote it, so PostgreSQL resolves the same column.
// Text already inside double quotes is left alone.
func pgSpelling(sql string) string {
	isIdentByte := func(c byte) bool {
		return c == '_' || (c >= '0' && c <= '9') ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
	}
	for _, col := range camelColumns {
		name := col.name
		var b strings.Builder
		i, inQuote := 0, false
		for i < len(sql) {
			if sql[i] == '"' {
				inQuote = !inQuote
				b.WriteByte(sql[i])
				i++
				continue
			}
			match := false
			if !inQuote && i+len(name) <= len(sql) &&
				(i == 0 || !isIdentByte(sql[i-1])) &&
				(i+len(name) == len(sql) || !isIdentByte(sql[i+len(name)])) {
				if col.anyCase {
					match = strings.EqualFold(sql[i:i+len(name)], name)
				} else {
					match = sql[i:i+len(name)] == name
				}
			}
			if match {
				b.WriteString(`"` + name + `"`)
				i += len(name)
				continue
			}
			b.WriteByte(sql[i])
			i++
		}
		sql = b.String()
	}
	return sql
}

// withPGSpelling fills in PGSQL for every entry whose own spelling PostgreSQL
// cannot resolve, and leaves an entry that already carries one alone.
func withPGSpelling(cases []Case) []Case {
	for i := range cases {
		if cases[i].PGSQL != "" {
			continue
		}
		if pg := pgSpelling(cases[i].SQL); pg != cases[i].SQL {
			cases[i].PGSQL = pg
		}
	}
	return cases
}

// Corpus is the shape list. Each entry names a way two relations that share a
// bare column name can be confused for one another.
func Corpus() []Case {
	return withPGSpelling([]Case{
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
		{Name: "both_relations_projected_mirrored",
			SQL: "SELECT a, b FROM (SELECT " + T0 + ".c2 AS a, " + T2 + ".c2 AS b FROM " + T2 +
				", " + T0 + ") x ORDER BY a, b",
			Ordered: true,
			Want: []string{"a=t0-a b=t2-a", "a=t0-a b=t2-b",
				"a=t0-b b=t2-a", "a=t0-b b=t2-b"}},
		{Name: "qualified_ref_no_derived_mirrored",
			SQL:  "SELECT MIN(" + T0 + ".c1) AS m FROM " + T2 + ", " + T0,
			Want: []string{"m=2145758213"}},
		{Name: "qualified_ref_in_derived_two_relations_mirrored",
			SQL: "SELECT agg0 FROM (SELECT MIN(" + T0 + ".c1) AS agg0 FROM " + T2 + ", " + T0 +
				") AS asdf",
			Want: []string{"agg0=2145758213"}},
		{Name: "qualified_ref_in_derived_no_aggregate_mirrored",
			SQL: "SELECT c FROM (SELECT " + T0 + ".c1 AS c FROM " + T1 + ", " + T2 + ", " + T0 +
				" WHERE " + T0 + ".c1 IS NOT NULL) x ORDER BY c",
			Ordered: true,
			Want: []string{"c=2145758213", "c=2145758213", "c=2145758213",
				"c=2145758213"}},
		{Name: "predicate_on_the_other_relation_mirrored",
			SQL: "SELECT c FROM (SELECT " + T0 + ".c2 AS c FROM " + T2 + ", " + T0 + " WHERE " +
				T2 + ".c1 < 0) x ORDER BY c",
			Ordered: true,
			Want:    []string{"c=t0-a", "c=t0-b"}},
		{Name: "join_on_colliding_bare_names_mirrored",
			SQL: "SELECT c FROM (SELECT " + T0 + ".c2 AS c FROM " + T1 + " JOIN " + T0 +
				" ON " + T0 + ".c0 = " + T1 + ".c0) x ORDER BY c",
			Ordered: true,
			Want:    []string{"c=t0-a", "c=t0-b"}},
		{Name: "join_on_colliding_bare_names",
			SQL: "SELECT c FROM (SELECT " + T0 + ".c2 AS c FROM " + T0 + " JOIN " + T1 +
				" ON " + T0 + ".c0 = " + T1 + ".c0) x ORDER BY c",
			Ordered: true,
			Want:    []string{"c=t0-a", "c=t0-b"}},
		// A qualified reference to a column two relations spell in DIFFERENT
		// CASES. The fold makes them one NAME; only the relation tells them
		// apart. Every one of these answered the OTHER relation's column
		// until the join's duplicate detector was taught to fold and the
		// resolver was taught to hold a QUALIFIER byte-exact (round-0 B1).
		{Name: "case_colliding_columns_both_projected",
			SQL: "SELECT " + T4 + ".MixedCol AS a, " + T5 + ".mixedcol AS b FROM " + T4 +
				", " + T5 + " WHERE " + T4 + ".k = " + T5 + ".k ORDER BY a",
			Ordered: true,
			Want:    []string{"a=100 b=900", "a=200 b=901"}},
		{Name: "case_colliding_columns_aggregated",
			SQL: "SELECT SUM(" + T4 + ".MixedCol) AS s FROM " + T4 + ", " + T5 +
				" WHERE " + T4 + ".k = " + T5 + ".k",
			Want: []string{"s=300"}},
		{Name: "case_colliding_columns_camel_side_only",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T4 + ", " + T5 +
				" WHERE " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "case_colliding_columns_lower_side_only",
			SQL: "SELECT " + T5 + ".mixedcol AS m FROM " + T4 + ", " + T5 +
				" WHERE " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=900", "m=901"}},
		{Name: "case_colliding_columns_through_a_derived_table",
			SQL: "SELECT x FROM (SELECT a.MixedCol AS x FROM " + T4 + " a, " + T5 +
				" b WHERE a.k = b.k) s ORDER BY x",
			Ordered: true,
			Want:    []string{"x=100", "x=200"}},
		{Name: "ctl_case_colliding_column_one_relation",
			SQL:     "SELECT " + T4 + ".MixedCol AS m FROM " + T4 + " ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "ctl_case_colliding_column_one_relation_upper",
			SQL:     "SELECT " + T6 + ".MIXEDCOL AS m FROM " + T6 + " ORDER BY m",
			Ordered: true,
			Want:    []string{"m=700", "m=701"}},
		// THE MIRROR. Every entry above names the CamelCase relation FIRST in
		// the FROM list, which is the side a join PROBES and publishes
		// UNQUALIFIED — the direction that was already right at the arc's
		// base. Written the other way round the referenced relation is the
		// BUILD side, the join publishes it QUALIFIED because the bare name
		// collides, and the chain from the reference to that column has to
		// carry the relation the whole way: through the join's output filter,
		// which is the list that decides whether the column reaches the
		// consumer at all. It did not, so the reference fell back to the bare
		// name and answered the OTHER relation's value on all four arms
		// (round-1 B1). Ten of these were RIGHT at the base commit.
		{Name: "case_colliding_columns_both_projected_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS a, " + T5 + ".mixedcol AS b FROM " + T5 +
				", " + T4 + " WHERE " + T4 + ".k = " + T5 + ".k ORDER BY a",
			Ordered: true,
			Want:    []string{"a=100 b=900", "a=200 b=901"}},
		{Name: "case_colliding_columns_aggregated_mirrored",
			SQL: "SELECT SUM(" + T4 + ".MixedCol) AS s FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k",
			Want: []string{"s=300"}},
		{Name: "case_colliding_columns_camel_side_only_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "case_colliding_columns_lower_side_only_mirrored",
			SQL: "SELECT " + T5 + ".mixedcol AS m FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=900", "m=901"}},
		{Name: "case_colliding_columns_through_a_derived_table_mirrored",
			SQL: "SELECT x FROM (SELECT a.MixedCol AS x FROM " + T5 + " b, " + T4 +
				" a WHERE a.k = b.k) s ORDER BY x",
			Ordered: true,
			Want:    []string{"x=100", "x=200"}},
		// PostgreSQL's OWN spelling of that reference. `clt4."MixedCol"` is
		// the only way PostgreSQL can name the column at all, and it answered
		// NULL — which is precisely what a bare `MixedCol` does against a
		// schema that publishes the build side qualified.
		{Name: "case_colliding_columns_delimited_reference_mirrored",
			SQL: `SELECT ` + T4 + `."MixedCol" AS m FROM ` + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "case_colliding_columns_delimited_reference",
			SQL: `SELECT ` + T4 + `."MixedCol" AS m FROM ` + T4 + ", " + T5 +
				" WHERE " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		// The reference in a GROUP BY, an aggregate argument and an ORDER BY
		// — the other three places the identity rule has to hold, in the
		// mirrored direction.
		{Name: "case_colliding_columns_group_by_qualified_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS g, COUNT(*) AS n FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k GROUP BY " + T4 + ".MixedCol ORDER BY g",
			Ordered: true,
			Want:    []string{"g=100 n=1", "g=200 n=1"}},
		{Name: "case_colliding_columns_order_by_qualified_mirrored",
			SQL: "SELECT " + T5 + ".mixedcol AS m FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k ORDER BY " + T4 + ".MixedCol DESC",
			Ordered: true,
			Want:    []string{"m=901", "m=900"}},
		// The explicit join spellings, both directions. USING collapses the
		// key to one column, which changes what the join publishes.
		{Name: "case_colliding_columns_using_join",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T4 + " JOIN " + T5 +
				" USING (k) ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "case_colliding_columns_using_join_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T5 + " JOIN " + T4 +
				" USING (k) ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "case_colliding_columns_left_join",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T4 + " LEFT JOIN " + T5 +
				" ON " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "case_colliding_columns_left_join_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T5 + " LEFT JOIN " + T4 +
				" ON " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		// THREE relations spelling one folded name three ways. Each reference
		// now has two wrong answers available to it, and the arms have to
		// agree with each other as well as with PostgreSQL: round 1 found
		// single answering `700|900|700` and the DAG `100|900|100` where
		// PostgreSQL says `100|900|700`.
		{Name: "case_colliding_columns_three_relations",
			SQL: "SELECT " + T4 + ".MixedCol AS a, " + T5 + ".mixedcol AS b, " + T6 +
				".MIXEDCOL AS c FROM " + T4 + ", " + T5 + ", " + T6 + " WHERE " + T4 +
				".k = " + T5 + ".k AND " + T5 + ".k = " + T6 + ".k ORDER BY a",
			Ordered: true,
			Want:    []string{"a=100 b=900 c=700", "a=200 b=901 c=701"}},
		{Name: "case_colliding_columns_three_relations_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS a, " + T5 + ".mixedcol AS b, " + T6 +
				".MIXEDCOL AS c FROM " + T6 + ", " + T5 + ", " + T4 + " WHERE " + T4 +
				".k = " + T5 + ".k AND " + T5 + ".k = " + T6 + ".k ORDER BY a",
			Ordered: true,
			Want:    []string{"a=100 b=900 c=700", "a=200 b=901 c=701"}},
		{Name: "case_colliding_columns_three_relations_delimited",
			SQL: `SELECT ` + T4 + `."MixedCol" AS a, ` + T5 + `."mixedcol" AS b, ` + T6 +
				`."MIXEDCOL" AS c FROM ` + T6 + ", " + T5 + ", " + T4 + " WHERE " + T4 +
				".k = " + T5 + ".k AND " + T5 + ".k = " + T6 + ".k ORDER BY a",
			Ordered: true,
			Want:    []string{"a=100 b=900 c=700", "a=200 b=901 c=701"}},
		{Name: "case_colliding_columns_upper_side_only",
			SQL: "SELECT " + T6 + ".MIXEDCOL AS m FROM " + T4 + ", " + T6 +
				" WHERE " + T4 + ".k = " + T6 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=700", "m=701"}},
		{Name: "case_colliding_columns_upper_side_only_mirrored",
			SQL: "SELECT " + T6 + ".MIXEDCOL AS m FROM " + T6 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T6 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=700", "m=701"}},
		{Name: "case_colliding_columns_camel_against_upper_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T6 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T6 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		// The remaining places a reference is read, all in the mirrored
		// direction: the JOIN KEY itself, HAVING, DISTINCT, a window's
		// ORDER BY, a CTE body, a set-operation arm, a scalar subquery and an
		// arithmetic expression that reads BOTH colliding columns at once.
		{Name: "case_colliding_columns_join_key_predicate_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T5 + " JOIN " + T4 +
				" ON " + T4 + ".k = " + T5 + ".k AND " + T4 + ".MixedCol < " + T5 +
				".mixedcol ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "case_colliding_columns_having_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS g, COUNT(*) AS n FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k GROUP BY " + T4 + ".MixedCol HAVING SUM(" +
				T4 + ".MixedCol) > 150 ORDER BY g",
			Ordered: true,
			Want:    []string{"g=200 n=1"}},
		{Name: "case_colliding_columns_distinct_mirrored",
			SQL: "SELECT DISTINCT " + T4 + ".MixedCol AS m FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200"}},
		{Name: "case_colliding_columns_window_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS m, ROW_NUMBER() OVER (ORDER BY " + T4 +
				".MixedCol) AS r FROM " + T5 + ", " + T4 + " WHERE " + T4 + ".k = " + T5 +
				".k ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100 r=1", "m=200 r=2"}},
		{Name: "case_colliding_columns_through_a_cte_mirrored",
			SQL: "WITH s AS (SELECT " + T4 + ".MixedCol AS x FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k) SELECT x FROM s ORDER BY x",
			Ordered: true,
			Want:    []string{"x=100", "x=200"}},
		{Name: "case_colliding_columns_set_op_arm_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol AS m FROM " + T5 + ", " + T4 + " WHERE " + T4 +
				".k = " + T5 + ".k UNION ALL SELECT " + T6 + ".MIXEDCOL FROM " + T6 +
				" ORDER BY m",
			Ordered: true,
			Want:    []string{"m=100", "m=200", "m=700", "m=701"}},
		{Name: "case_colliding_columns_in_a_scalar_subquery_mirrored",
			SQL: "SELECT (SELECT MAX(" + T4 + ".MixedCol) FROM " + T5 + ", " + T4 +
				" WHERE " + T4 + ".k = " + T5 + ".k) AS m",
			Want: []string{"m=200"}},
		{Name: "case_colliding_columns_both_sides_in_one_expression_mirrored",
			SQL: "SELECT " + T4 + ".MixedCol + " + T5 + ".mixedcol AS s FROM " + T5 + ", " +
				T4 + " WHERE " + T4 + ".k = " + T5 + ".k ORDER BY s",
			Ordered: true,
			Want:    []string{"s=1000", "s=1101"}},
		// A DELIMITED table alias is byte-exact, so `t` and `"T"` are two
		// relations. Folding the qualifier made them one and `t.c1` answered
		// the other relation's column (round-0 B2).
		{Name: "delimited_alias_beside_an_unquoted_one",
			SQL:     `SELECT t.c1 AS x, "T".c1 AS y FROM ` + T1 + ` t, ` + T2 + ` "T" ORDER BY x, y`,
			Ordered: true,
			Want: []string{"x=7 y=-1968219095", "x=7 y=1737580880",
				"x=9 y=-1968219095", "x=9 y=1737580880"}},
		{Name: "delimited_alias_unquoted_side_only",
			SQL:     `SELECT t.c1 AS x FROM ` + T1 + ` t, ` + T2 + ` "T" ORDER BY x`,
			Ordered: true,
			Want:    []string{"x=7", "x=7", "x=9", "x=9"}},
		{Name: "delimited_alias_delimited_side_only",
			SQL:     `SELECT "T".c1 AS y FROM ` + T1 + ` t, ` + T2 + ` "T" ORDER BY y`,
			Ordered: true,
			Want: []string{"y=-1968219095", "y=-1968219095",
				"y=1737580880", "y=1737580880"}},
		{Name: "delimited_alias_beside_an_unquoted_one_mirrored",
			SQL:     `SELECT t.c1 AS x, "T".c1 AS y FROM ` + T2 + ` "T", ` + T1 + ` t ORDER BY x, y`,
			Ordered: true,
			Want: []string{"x=7 y=-1968219095", "x=7 y=1737580880",
				"x=9 y=-1968219095", "x=9 y=1737580880"}},
		{Name: "delimited_alias_unquoted_side_only_mirrored",
			SQL:     `SELECT t.c1 AS x FROM ` + T2 + ` "T", ` + T1 + ` t ORDER BY x`,
			Ordered: true,
			Want:    []string{"x=7", "x=7", "x=9", "x=9"}},
		{Name: "delimited_alias_delimited_side_only_mirrored",
			SQL:     `SELECT "T".c1 AS y FROM ` + T2 + ` "T", ` + T1 + ` t ORDER BY y`,
			Ordered: true,
			Want: []string{"y=-1968219095", "y=-1968219095",
				"y=1737580880", "y=1737580880"}},
		// A positional ORDER BY over a JOIN. On the DAG no stage is emitted
		// for a Project, so the sort stage reads the join's whole output and
		// a select-list POSITION is not a column index there — these sorted
		// by column ONE on both DAG arms (round-0 B4).
		{Name: "ordinal_over_a_comma_join",
			SQL: "SELECT " + T1 + ".c2, " + T2 + ".c1 FROM " + T1 + ", " + T2 +
				" ORDER BY 2, 1",
			Ordered: true,
			Want: []string{"c2=t1-a c1=-1968219095", "c2=t1-b c1=-1968219095",
				"c2=t1-a c1=1737580880", "c2=t1-b c1=1737580880"}},
		{Name: "ordinal_over_a_comma_join_descending",
			SQL: "SELECT " + T1 + ".c2, " + T2 + ".c1 FROM " + T1 + ", " + T2 +
				" ORDER BY 2 DESC, 1",
			Ordered: true,
			Want: []string{"c2=t1-a c1=1737580880", "c2=t1-b c1=1737580880",
				"c2=t1-a c1=-1968219095", "c2=t1-b c1=-1968219095"}},
		{Name: "ordinal_over_an_explicit_join",
			SQL: "SELECT " + T1 + ".c2, " + T2 + ".c1 FROM " + T1 + " JOIN " + T2 +
				" ON " + T1 + ".c0 = " + T2 + ".c0 ORDER BY 2, 1",
			Ordered: true,
			Want:    []string{"c2=t1-a c1=-1968219095", "c2=t1-b c1=1737580880"}},
		{Name: "ctl_ordinal_over_one_relation",
			SQL:     "SELECT " + T1 + ".c2, " + T1 + ".c1 FROM " + T1 + " ORDER BY 2, 1",
			Ordered: true,
			Want:    []string{"c2=t1-a c1=7", "c2=t1-b c1=9"}},
		// A CamelCase SCHEMA, referenced in every case, on every arm. Every
		// output is ALIASED to a lower-case name on purpose: the PostgreSQL
		// oracle declares this fixture with unquoted DDL, so PostgreSQL folds
		// the stored names and only the aliases are comparable between the
		// two engines. What is under test is the RESOLUTION, which is the
		// half of #731 that keeps ClickBench working and which no DAG gate
		// exercised before (round-1 P7).
		{Name: "camel_bare_reference",
			SQL:     "SELECT WatchID AS w FROM " + TCam + " ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1", "w=2", "w=3"}},
		{Name: "camel_folded_reference",
			SQL:     "SELECT watchid AS w FROM " + TCam + " ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1", "w=2", "w=3"}},
		{Name: "camel_upper_reference",
			SQL:     "SELECT WATCHID AS w FROM " + TCam + " ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1", "w=2", "w=3"}},
		{Name: "camel_in_a_filter",
			SQL:     "SELECT UserID AS u FROM " + TCam + " WHERE WatchID > 1 ORDER BY u",
			Ordered: true,
			Want:    []string{"u=10", "u=11"}},
		{Name: "camel_qualified",
			SQL:     "SELECT h.WatchID AS w FROM " + TCam + " h WHERE h.UserID > 0 ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1", "w=2", "w=3"}},
		{Name: "camel_group_by",
			SQL: "SELECT EventDate AS d, COUNT(*) AS n FROM " + TCam +
				" GROUP BY EventDate ORDER BY d",
			Ordered: true,
			Want:    []string{"d=20 n=2", "d=21 n=1"}},
		{Name: "camel_aggregate_input",
			SQL:  "SELECT MAX(WatchID) AS mx, SUM(UserID) AS s FROM " + TCam,
			Want: []string{"mx=3 s=31"}},
		// …and the CamelCase schema JOINED, both directions. Every camel entry
		// above names TCam alone, so no CamelCase column ever went through a
		// join's duplicate detection or its output filter — the gate could
		// not see round-1 B1 from this side either.
		{Name: "camel_joined_as_the_build_side",
			SQL: "SELECT h.WatchID AS w, " + T5 + ".mixedcol AS m FROM " + T5 + ", " + TCam +
				" h WHERE h.WatchID = " + T5 + ".k ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1 m=900", "w=2 m=901"}},
		{Name: "camel_joined_as_the_probe_side",
			SQL: "SELECT h.WatchID AS w, " + T5 + ".mixedcol AS m FROM " + TCam + " h, " + T5 +
				" WHERE h.WatchID = " + T5 + ".k ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1 m=900", "w=2 m=901"}},
		{Name: "camel_sort_key_not_projected",
			SQL:     "SELECT UserID AS u FROM " + TCam + " ORDER BY WatchID DESC",
			Ordered: true,
			Want:    []string{"u=11", "u=10", "u=10"}},
		{Name: "camel_join_key",
			SQL: "SELECT a.WatchID AS w FROM " + TCam + " a JOIN " + TCam +
				" b ON a.WatchID = b.WatchID ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1", "w=2", "w=3"}},
		{Name: "camel_distinct",
			SQL:     "SELECT DISTINCT SearchPhrase AS p FROM " + TCam + " ORDER BY p",
			Ordered: true,
			Want:    []string{"p=abc", "p=abd", "p=zzz"}},
		{Name: "camel_window",
			SQL: "SELECT WatchID AS w, ROW_NUMBER() OVER (PARTITION BY UserID ORDER BY WatchID) " +
				"AS rn FROM " + TCam + " ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1 rn=1", "w=2 rn=2", "w=3 rn=1"}},
		{Name: "camel_derived_table",
			SQL:     "SELECT s.w AS w FROM (SELECT WatchID AS w FROM " + TCam + ") s ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1", "w=2", "w=3"}},
		{Name: "camel_cte",
			SQL: "WITH c AS (SELECT WatchID AS w, UserID AS u FROM " + TCam +
				") SELECT SUM(u) AS s FROM c",
			Want: []string{"s=31"}},
		{Name: "camel_in_subquery",
			SQL: "SELECT WatchID AS w FROM " + TCam + " WHERE UserID IN (SELECT UserID FROM " +
				TCam + ") ORDER BY w",
			Ordered: true,
			Want:    []string{"w=1", "w=2", "w=3"}},
		{Name: "nested_derived_tables",
			SQL: "SELECT c FROM (SELECT c FROM (SELECT " + T0 + ".c2 AS c FROM " + T0 + ", " +
				T2 + ") y) x ORDER BY c",
			Ordered: true,
			Want:    []string{"c=t0-a", "c=t0-a", "c=t0-b", "c=t0-b"}},
	})
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
	return withPGSpelling([]Case{
		{Name: "two_same_named_columns_from_two_relations",
			SQL: "SELECT " + T1 + ".c0, " + T2 + ".c1, " + T2 + ".c0 FROM " + T1 + ", " + T2 +
				" ORDER BY 2, 1, 3",
			Ref: "SELECT " + T1 + ".c0 AS a, " + T2 + ".c1 AS b, " + T2 + ".c0 AS c FROM " +
				T1 + ", " + T2 + " ORDER BY 2, 1, 3",
			Ordered: true,
			Want: []string{
				"false|-1968219095|true", "true|-1968219095|true",
				"false|1737580880|false", "true|1737580880|false",
			}},
		// No Ref: BOTH of this entry's output columns are called c2, so no
		// column of it is uniquely addressable by name and the reference
		// technique cannot read it. It is gated positionally in
		// wadjet/collide_duplicate_names_test.go instead.
		{Name: "two_same_named_columns_ordered_positionally",
			SQL:     "SELECT " + T1 + ".c2, " + T2 + ".c2 FROM " + T1 + ", " + T2 + " ORDER BY 2, 1",
			Ordered: true,
			Want: []string{"t1-a|t2-a", "t1-b|t2-a",
				"t1-a|t2-b", "t1-b|t2-b"}},
		{Name: "union_all_branch_with_two_same_named_outputs",
			SQL: "SELECT " + T1 + ".c0, " + T2 + ".c1, " + T2 + ".c0 FROM " + T1 + ", " + T2 +
				" UNION ALL SELECT " + T1 + ".c0, " + T2 + ".c1, " + T2 + ".c0 FROM " + T1 +
				", " + T2 + " ORDER BY 2, 1, 3",
			Ref: "SELECT " + T1 + ".c0 AS a, " + T2 + ".c1 AS b, " + T2 + ".c0 AS c FROM " +
				T1 + ", " + T2 + " UNION ALL SELECT " + T1 + ".c0, " + T2 + ".c1, " + T2 +
				".c0 FROM " + T1 + ", " + T2 + " ORDER BY 2, 1, 3",
			Ordered: true,
			Want: []string{
				"false|-1968219095|true", "false|-1968219095|true",
				"true|-1968219095|true", "true|-1968219095|true",
				"false|1737580880|false", "false|1737580880|false",
				"true|1737580880|false", "true|1737580880|false",
			}},
	})
}
