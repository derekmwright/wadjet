package tpch

import (
	"strings"
	"testing"
)

// TestPostgresOracleCarriesACamelCaseFixture asserts that the fixture
// createPostgresSchema's comment describes actually exists.
//
// It did not. The comment said "Every fixture but the CamelCase one is lower
// case already … that one is here precisely because its names are not", and
// `oracleTables()` had no column carrying an upper-case letter at all — the
// comment documented a table nobody had written. The consequence is specific:
// the wire arm compares RowDescription names (`wirePropFieldNames`) with no
// pin, so it is the gate for identifier CASE on the wire, and it had nothing
// to compare. Two wire-visible defects of that class shipped past it.
//
// This runs with no server, which is the point: the fixture's EXISTENCE is a
// premise the arm depends on, and a premise that only holds when a container
// happens to be reachable is not gated.
func TestPostgresOracleCarriesACamelCaseFixture(t *testing.T) {
	schema, ok := oracleTables()[pgCaseTable]
	if !ok {
		t.Fatalf("oracleTables() has no %q. The CamelCase fixture is the only place this oracle can "+
			"ask what an identifier's CASE means on the wire; without it the RowDescription "+
			"comparison runs over lower-case names only and can never fail on a fold.", pgCaseTable)
	}

	camel, folded := 0, 0
	seen := make(map[string]string, len(schema.Columns))
	for _, c := range schema.Columns {
		if c.Name == strings.ToLower(c.Name) {
			folded++
		} else {
			camel++
		}
		// The catalog refuses a schema whose columns collide under the fold
		// (catalog.checkDistinctColumnNames), so a fixture carrying two such
		// names would not load at all — it would abort the whole run rather
		// than report anything.
		if prev, dup := seen[strings.ToLower(c.Name)]; dup {
			t.Errorf("%s declares both %q and %q, which fold to one name — catalog.CreateTable "+
				"refuses that schema and the fixture would not load", pgCaseTable, prev, c.Name)
		}
		seen[strings.ToLower(c.Name)] = c.Name
	}
	if camel == 0 {
		t.Errorf("%s has no CamelCase column, so it is not the fixture it exists to be", pgCaseTable)
	}
	if folded == 0 {
		t.Errorf("%s has no already-folded column. The MIXED schema is the load-bearing shape: a "+
			"partial miss, where the folded columns of a row survive and the CamelCase ones do not, "+
			"reads as data rather than as an error", pgCaseTable)
	}

	// Every declared column must be carried by the rows, or the COPY loads a
	// column the corpus can select and always find NULL in — an answer that
	// would agree on both engines and prove nothing.
	rows := pgCaseRows()
	if len(rows) == 0 {
		t.Fatalf("%s has no rows", pgCaseTable)
	}
	for _, c := range schema.Columns {
		if _, ok := rows[0][c.Name]; !ok {
			t.Errorf("%s row 0 has no value for column %q, spelled exactly as the schema declares it",
				pgCaseTable, c.Name)
		}
	}

	// And it must actually be loaded. A schema entry alone creates the table
	// on both sides and leaves it empty.
	if src := readTestSource(t, "postgres_oracle_test.go"); !strings.Contains(src, "sink(pgCaseTable") {
		t.Errorf("postgres_oracle_test.go declares %s but never sinks its rows, so both engines hold "+
			"an empty table and every entry over it agrees vacuously", pgCaseTable)
	}
}
