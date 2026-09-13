package wadjet

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ctasShapeDB is a small fixture with one of everything a query shape needs.
func ctasShapeDB(t *testing.T) (*DB, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "g", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 12, Scale: 3, Nullable: true},
		{Name: "ip", Type: parquet.TypeIPv4, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "shp", schema, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO shp (id, g, s, d, ip) VALUES `+
		`(1, 10, 'a', 1.5, '10.0.0.1'), (2, 10, 'b', 2.25, '10.0.0.2'), `+
		`(3, 20, 'c', 3.125, '10.0.0.3'), (4, 20, NULL, NULL, NULL)`); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}

// A query answers the same thing inside a write as outside it — that is the
// whole invariant of #1024's design, and this is the gate for it.
//
// Each cell runs the SELECT bare, then runs it again as the source of a CTAS,
// and asserts three things: the table's DECLARED columns are the query's
// declared columns (names, types and DECIMAL (p, s)), the table's ROWS are the
// query's rows, and the command tag is PostgreSQL's `SELECT <n>`.
//
// The shapes are the ones whose declared output comes from somewhere other
// than a base table's schema — a join's ordered list, an aggregate's result
// types, a window's, a set operation's arm reconciliation, a CTE's published
// columns, a lateral's, a scalar subquery's — because that is where a CTAS can
// declare a table the query does not answer.
func TestACreatedTableDeclaresWhatTheQueryDeclares(t *testing.T) {
	db, ctx := ctasShapeDB(t)

	cases := []struct {
		name  string
		query string
		cols  []string
	}{
		{"Star", `SELECT * FROM shp`, []string{"id", "g", "s", "d", "ip"}},
		{"Join", `SELECT a.id, b.s FROM shp a JOIN shp b ON b.id = a.id`, []string{"id", "s"}},
		{"GroupBy", `SELECT g, COUNT(*) AS c, SUM(d) AS sd, MIN(ip) AS mi FROM shp GROUP BY g`,
			[]string{"g", "c", "sd", "mi"}},
		{"Window", `SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM shp`,
			[]string{"id", "rn"}},
		{"CTE", `WITH x AS (SELECT id, d FROM shp) SELECT id, d FROM x`, []string{"id", "d"}},
		{"RecursiveCTE", `WITH RECURSIVE r(k) AS (SELECT 1 AS k UNION ALL SELECT k+1 FROM r WHERE k < 4) SELECT k FROM r`,
			[]string{"k"}},
		{"Lateral", `SELECT a.id, x.m FROM shp a, LATERAL (SELECT MAX(b.id) AS m FROM shp b WHERE b.g = a.g) x`,
			[]string{"id", "m"}},
		{"Union", `SELECT id FROM shp UNION SELECT g FROM shp`, []string{"id"}},
		{"UnionAll", `SELECT id, s FROM shp UNION ALL SELECT g, s FROM shp`, []string{"id", "s"}},
		{"Distinct", `SELECT DISTINCT g FROM shp`, []string{"g"}},
		{"OrderByLimit", `SELECT id, s FROM shp ORDER BY id DESC LIMIT 2`, []string{"id", "s"}},
		{"ScalarSubquery", `SELECT id, (SELECT MAX(g) FROM shp) AS mx FROM shp`, []string{"id", "mx"}},
		{"Empty", `SELECT id, s, d FROM shp WHERE 1 = 0`, []string{"id", "s", "d"}},
		{"Unaliased", `SELECT id, g + 1 FROM shp`, []string{"id", "?column?"}},
		{"Expression", `SELECT UPPER(s) AS us, d * 2 AS d2, id % 2 AS m FROM shp`,
			[]string{"us", "d2", "m"}},
		{"NetworkFunction", `SELECT id, ip_to_string(ip) AS h FROM shp`, []string{"id", "h"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bare, err := db.Query(ctx, tc.query)
			if err != nil {
				t.Fatalf("the bare query: %v", err)
			}
			dst := "shape_" + strings.ToLower(tc.name)
			res, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE %s AS %s", dst, tc.query))
			if err != nil {
				t.Fatalf("the same query inside a CTAS: %v", err)
			}
			if got, want := res.Rows[0]["result"], fmt.Sprintf("SELECT %d", len(bare.Rows)); got != want {
				t.Errorf("command tag %v, want %q", got, want)
			}

			meta, err := db.catalog.GetTable(ctx, dst)
			if err != nil {
				t.Fatal(err)
			}
			if got := meta.Schema.ColumnNames(); !ctasEqualStrings(got, tc.cols) {
				t.Errorf("the table declares %v, want %v", got, tc.cols)
			}
			// The DECLARATION is the query's, column for column.
			if len(meta.Schema.Columns) != len(bare.ColumnMetas) {
				t.Fatalf("the table has %d columns, the query declared %d",
					len(meta.Schema.Columns), len(bare.ColumnMetas))
			}
			for i, cm := range bare.ColumnMetas {
				col := meta.Schema.Columns[i]
				if col.Name != cm.Name {
					t.Errorf("column %d named %q, the query named it %q", i, col.Name, cm.Name)
				}
				if col.Type != cm.TypeID {
					t.Errorf("column %q declared %s, the query declared %s", cm.Name, col.Type, cm.TypeID)
				}
				if cm.TypeID == parquet.TypeDecimal && !cm.WireUnconstrained &&
					(col.Precision != cm.Precision || col.Scale != cm.Scale) {
					t.Errorf("column %q DECIMAL(%d,%d), the query declared (%d,%d)",
						cm.Name, col.Precision, col.Scale, cm.Precision, cm.Scale)
				}
				if !col.Nullable {
					t.Errorf("column %q is NOT NULL; a CTAS never infers one", cm.Name)
				}
			}

			// The ROWS, compared as multisets — a CTAS does not promise the
			// query's ORDER, and neither does PostgreSQL's, because a table
			// has none.
			got, err := db.Query(ctx, "SELECT * FROM "+dst)
			if err != nil {
				t.Fatal(err)
			}
			if !sameMultiset(got, bare) {
				t.Errorf("the table's rows are not the query's rows:\n  table %v\n  query %v",
					renderRows(got), renderRows(bare))
			}
		})
	}
}

// The refusals, each one measured on PostgreSQL 17.11 first.
func TestAQuerySourcedWriteRefusesWhatPostgresRefuses(t *testing.T) {
	db, ctx := ctasShapeDB(t)
	if _, err := db.Query(ctx, `CREATE TABLE taken AS SELECT id FROM shp`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, `CREATE TABLE tgt (a INT64, b INT64, c STRING)`); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		sql   string
		state string
		says  string
	}{
		// `relation "taken" already exists` — 42P07, measured.
		{"CreateOverAnExistingName", `CREATE TABLE taken AS SELECT id FROM shp`, "42P07", "already exists"},
		// `column "?column?" specified more than once` — 42701, measured: two
		// unaliased expressions both take PostgreSQL's ?column? name and a
		// relation cannot hold both.
		{"TwoUnnamedExpressions", `CREATE TABLE dup AS SELECT g+1, g+2 FROM shp`, "42701", "more than once"},
		// `too many column names were specified` — 42601, measured.
		{"TooManyRenames", `CREATE TABLE many (x, y, z) AS SELECT id, g FROM shp`, "42601", "too many column names"},
		// `INSERT has more expressions than target columns` — 42601, measured.
		{"MoreExpressionsThanColumns", `INSERT INTO tgt (a, b) SELECT id, g, s FROM shp`,
			"42601", "more expressions than target columns"},
		// `INSERT has more target columns than expressions` — 42601, measured,
		// and ONLY with an explicit list (see the accepted cell below).
		{"FewerExpressionsThanAnExplicitList", `INSERT INTO tgt (a, b) SELECT id FROM shp`,
			"42601", "more target columns than expressions"},
		// `column "a" is of type bigint but expression is of type text` —
		// 42804, measured. PostgreSQL would assignment-cast some pairs this
		// engine refuses; see ADR-0012's divergence list.
		{"TypeMismatch", `INSERT INTO tgt (a) SELECT s FROM shp`, "42804", "is of type"},
		{"RelationDoesNotExist", `INSERT INTO nosuch SELECT id FROM shp`, "42P01", "does not exist"},
		{"ColumnDoesNotExist", `INSERT INTO tgt (nope) SELECT id FROM shp`, "42703", "does not exist"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Query(ctx, tc.sql)
			if err == nil {
				t.Fatalf("%s was accepted; PostgreSQL 17.11 refuses it %s", tc.sql, tc.state)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("SQLSTATE %q, want %q (%v)", got, tc.state, err)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the message does not say %q: %v", tc.says, err)
			}
		})
	}

	// The boundary of the arity rule: fewer expressions with NO column list is
	// ACCEPTED on PostgreSQL 17.11, the unnamed columns taking NULL. Method 10
	// — the corpus attempts the shape just outside the refusal.
	t.Run("FewerExpressionsAndNoList", func(t *testing.T) {
		if _, err := db.Query(ctx, `INSERT INTO tgt SELECT id, g FROM shp`); err != nil {
			t.Fatalf("PostgreSQL accepts this (INSERT 0 4, c NULL): %v", err)
		}
		res, err := db.Query(ctx, `SELECT a, b, c FROM tgt ORDER BY a`)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Rows) != 4 {
			t.Fatalf("%d rows, want 4", len(res.Rows))
		}
		for i := range res.Rows {
			if c := res.Cells(i)[2]; c != nil {
				t.Errorf("row %d: c = %v, want NULL — the column the query did not reach takes its default", i, c)
			}
		}
	})

	// A STAR over a SELF JOIN is the deliberate SUPERSET, and it is the one
	// place this statement answers where PostgreSQL 17.11 refuses. There, both
	// sides publish `id` and `CREATE TABLE … AS SELECT * FROM shp a JOIN shp b`
	// is 42701, `column "id" specified more than once`. Here a join publishes
	// the probe's columns bare and every duplicate build column QUALIFIED by
	// its owning alias (ADR-0026 §8d), so the list has no duplicate at all and
	// the table is created — with `b.id` as a column name, reachable by its
	// delimited spelling. Recorded in ADR-0012's divergence list; the fixture
	// is here because a superset that cannot be READ BACK is not a superset.
	t.Run("StarOverASelfJoinIsASuperset", func(t *testing.T) {
		if _, err := db.Query(ctx, `CREATE TABLE dupstar AS SELECT * FROM shp a JOIN shp b ON b.id = a.id`); err != nil {
			t.Fatalf("the qualified-name superset was refused: %v", err)
		}
		meta, err := db.catalog.GetTable(ctx, "dupstar")
		if err != nil {
			t.Fatal(err)
		}
		if got := meta.Schema.ColumnNames(); !ctasEqualStrings(got,
			[]string{"id", "g", "s", "d", "ip", "b.id", "b.g", "b.s", "b.d", "b.ip"}) {
			t.Fatalf("declared %v", got)
		}
		res, err := db.Query(ctx, `SELECT id, "b.id" FROM dupstar ORDER BY id`)
		if err != nil {
			t.Fatalf("a delimited reference to the qualified column: %v", err)
		}
		if len(res.Rows) != 4 {
			t.Fatalf("%d rows, want 4", len(res.Rows))
		}
		for i := range res.Rows {
			c := res.Cells(i)
			if c[0] != c[1] {
				t.Errorf("row %d: id=%v, \"b.id\"=%v — the self join matched on id", i, c[0], c[1])
			}
		}
		// The column-list form is the way to give them ordinary names, and
		// PostgreSQL accepts that spelling too (measured, 14 names).
		if _, err := db.Query(ctx, `CREATE TABLE dupstar2 (aid, ag, as_, ad, aip, bid, bg, bs, bd, bip) `+
			`AS SELECT * FROM shp a JOIN shp b ON b.id = a.id`); err != nil {
			t.Fatalf("the rename list over a self-join star: %v", err)
		}
		m2, err := db.catalog.GetTable(ctx, "dupstar2")
		if err != nil {
			t.Fatal(err)
		}
		if got := m2.Schema.ColumnNames(); !ctasEqualStrings(got,
			[]string{"aid", "ag", "as_", "ad", "aip", "bid", "bg", "bs", "bd", "bip"}) {
			t.Fatalf("renamed to %v", got)
		}
	})

	// And IF NOT EXISTS over a taken name: no error, PostgreSQL's
	// `CREATE TABLE AS` tag, and the query is NOT run — the table it would
	// have overwritten is untouched.
	t.Run("IfNotExistsOverATakenName", func(t *testing.T) {
		before := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM taken`)
		res, err := db.Query(ctx, `CREATE TABLE IF NOT EXISTS taken AS SELECT id FROM shp WHERE id < 0`)
		if err != nil {
			t.Fatalf("IF NOT EXISTS refused: %v", err)
		}
		if got := res.Rows[0]["result"]; got != CommandCreateTableAs {
			t.Errorf("command tag %v, want %q", got, CommandCreateTableAs)
		}
		if after := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM taken`); after != before {
			t.Errorf("the existing table now holds %d rows, was %d", after, before)
		}
	})
}

// WITH NO DATA creates the table with the query's schema and does NOT run the
// query — which is observable: a query that fails at RUN time still creates the
// table, as it does on PostgreSQL.
func TestWithNoDataDeclaresWithoutRunning(t *testing.T) {
	db, ctx := ctasShapeDB(t)

	// The fixture carries an UNALIASED item on purpose. With every item
	// aliased or a bare column there is no `?column?` for the two arms to
	// disagree about, and they DID disagree: `WITH NO DATA` named this column
	// `"g + 1"` — its expression TEXT, which is what the plan-time walk names
	// it — where `WITH DATA` and PostgreSQL 17.11 both name it `?column?`
	// (round-2 review B7).
	res, err := db.Query(ctx, `CREATE TABLE nodata AS SELECT id, g + 1, d FROM shp WITH NO DATA`)
	if err != nil {
		t.Fatalf("WITH NO DATA: %v", err)
	}
	if got := res.Rows[0]["result"]; got != CommandCreateTableAs {
		t.Errorf("command tag %v, want %q", got, CommandCreateTableAs)
	}
	meta, err := db.catalog.GetTable(ctx, "nodata")
	if err != nil {
		t.Fatal(err)
	}
	if got := meta.Schema.ColumnNames(); !ctasEqualStrings(got, []string{"id", "?column?", "d"}) {
		t.Errorf("declared %v, want [id ?column? d]", got)
	}
	if n := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM nodata`); n != 0 {
		t.Errorf("the table holds %d rows; WITH NO DATA writes none", n)
	}
	// The declaration is the same one the WITH DATA arm takes.
	if _, err := db.Query(ctx, `CREATE TABLE withdata AS SELECT id, g + 1, d FROM shp`); err != nil {
		t.Fatal(err)
	}
	withData, err := db.catalog.GetTable(ctx, "withdata")
	if err != nil {
		t.Fatal(err)
	}
	ctasCompareSchemas(t, withData.Schema.Columns, meta.Schema.Columns)

	// The query is not RUN: one that would fail on a row still declares.
	// `1/0` over four rows is a runtime refusal; PostgreSQL creates the table
	// anyway under WITH NO DATA.
	if _, err := db.Query(ctx, `CREATE TABLE divzero AS SELECT id, id / (id - id) AS q FROM shp WITH NO DATA`); err != nil {
		t.Fatalf("WITH NO DATA over a query that fails at run time: %v", err)
	}
	if _, err := db.catalog.GetTable(ctx, "divzero"); err != nil {
		t.Fatalf("the table was not created: %v", err)
	}
}

func renderRows(res *QueryResult) []string {
	out := make([]string, 0, len(res.Rows))
	for i := range res.Rows {
		out = append(out, fmt.Sprint(res.Cells(i)))
	}
	sort.Strings(out)
	return out
}

func sameMultiset(a, b *QueryResult) bool {
	return ctasEqualStrings(renderRows(a), renderRows(b))
}

func ctasEqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The two arms of one statement declare ONE table (#1024 round-2 review B7).
//
// `WITH DATA` takes its columns from the EXECUTED plan's declared output;
// `WITH NO DATA` takes them from the plan-time walk, because it does not run
// the query. The walk names an unaliased column by its expression TEXT and the
// executed plan gives it PostgreSQL's `?column?` — so the two arms of one
// statement declared two different tables, and with the name went the
// duplicate rule: `SELECT id, n+1, s||'x', 42 … WITH NO DATA` was CREATED where
// the same statement `WITH DATA` and PostgreSQL 17.11 both answer 42701
// (measured).
//
// Every shape here is run BOTH ways and the two declarations compared, and the
// shapes are the ones where a name has to be derived rather than copied.
func TestTheTwoArmsOfACreateDeclareOneTable(t *testing.T) {
	db, ctx := ctasShapeDB(t)

	shapes := []string{
		`SELECT * FROM shp`,
		`SELECT id, g + 1, s FROM shp`,
		`SELECT UPPER(s), d * 2 AS d2, id % 2 AS m FROM shp`,
		`SELECT id, (SELECT MAX(g) FROM shp) FROM shp`,
		`SELECT id, COUNT(*) OVER () FROM shp`,
		`SELECT g, COUNT(*), SUM(d), MIN(ip) FROM shp GROUP BY g`,
		`SELECT a.id, b.s FROM shp a JOIN shp b ON b.id = a.id`,
		`WITH x AS (SELECT id, d FROM shp) SELECT id, d FROM x`,
		`SELECT id FROM shp UNION SELECT g FROM shp`,
		`SELECT DISTINCT g FROM shp`,
		`SELECT id, s FROM shp ORDER BY id DESC LIMIT 2`,
		`SELECT id, s, d FROM shp WHERE 1 = 0`,
		`SELECT CAST(id AS DECIMAL(18,4)), CAST(s AS STRING) FROM shp`,
		// A STAR over a relation that is not a base table, which is where the
		// round-2 fix stopped: `deriveColumns` names the items the OUTER
		// select lists, and a star lists none, so these fell through to the
		// plan-time walk — which spells an INNER unaliased item by its
		// expression TEXT. PostgreSQL 17.11 declares `id, ?column?` for every
		// one of them on BOTH arms (measured). A star over a base table was
		// always fine, which is why the round-2 shapes above could not see it.
		`WITH c AS (SELECT id, g + 1 FROM shp) SELECT * FROM c`,
		`SELECT * FROM (SELECT id, g + 1 FROM shp) x`,
		`SELECT * FROM (SELECT id, s || 'x' FROM shp) x`,
		`WITH c AS (SELECT * FROM (SELECT id, g + 1 FROM shp) y) SELECT * FROM c`,
		`WITH c AS (SELECT id, g + 1 AS np FROM shp) SELECT * FROM c`,
		`WITH c AS (SELECT id, UPPER(s) FROM shp) SELECT * FROM c`,
		`SELECT * FROM (SELECT id, (SELECT MAX(g) FROM shp) FROM shp) x`,
		`WITH c AS (SELECT id, COUNT(*) OVER () FROM shp) SELECT * FROM c`,
	}
	for i, q := range shapes {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			withData, nodata := fmt.Sprintf("wd%d", i), fmt.Sprintf("nd%d", i)
			if _, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE %s AS %s", withData, q)); err != nil {
				t.Fatalf("WITH DATA: %v", err)
			}
			if _, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE %s AS %s WITH NO DATA", nodata, q)); err != nil {
				t.Fatalf("WITH NO DATA: %v", err)
			}
			a, err := db.catalog.GetTable(ctx, withData)
			if err != nil {
				t.Fatal(err)
			}
			b, err := db.catalog.GetTable(ctx, nodata)
			if err != nil {
				t.Fatal(err)
			}
			ctasCompareSchemas(t, a.Schema.Columns, b.Schema.Columns)
		})
	}
}

// …and the duplicate rule travels with the name, on both arms.
func TestTheDuplicateNameRuleHoldsOnBothArms(t *testing.T) {
	db, ctx := ctasShapeDB(t)

	// Each measured on PostgreSQL 17.11: `ERROR: column "?column?" specified
	// more than once`.
	for i, q := range []string{
		`SELECT id, g+1, s||'x', 42 FROM shp`,
		`SELECT g+1, g+2 FROM shp`,
		`SELECT g+1, g+1 FROM shp`,
		// …and through a STAR, where the two unaliased items are the INNER
		// relation's. `WITH NO DATA` CREATED these where `WITH DATA` and
		// PostgreSQL both answer 42701 (round-2 review B1).
		`WITH c AS (SELECT g+1, g+2 FROM shp) SELECT * FROM c`,
		`SELECT * FROM (SELECT g+1, g+2 FROM shp) x`,
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			for _, suffix := range []string{"", " WITH NO DATA"} {
				tbl := fmt.Sprintf("dupname%d%d", i, len(suffix))
				_, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE %s AS %s%s", tbl, q, suffix))
				if err == nil {
					t.Errorf("`%s%s` was created; PostgreSQL 17.11 answers 42701", q, suffix)
					continue
				}
				if got := sqlerr.StateOf(err); got != "42701" {
					t.Errorf("`%s%s` refused %s, want 42701: %v", q, suffix, got, err)
				}
			}
		})
	}
}

// The DECLARED form honours IF NOT EXISTS (#1024 round-2 review B3).
//
// The grammar took the clause in this arc; the executor did not read it, so a
// statement documented as a no-op raised the very 42P07 the documentation said
// it replaced. PostgreSQL 17.11 answers a NOTICE and skips, and it tests
// existence BEFORE the column types — `CREATE TABLE IF NOT EXISTS t (a
// nosuchtype)` over an existing `t` is the skip, not a type error (measured).
func TestIfNotExistsIsHonouredOnBothFormsOfCreateTable(t *testing.T) {
	db, ctx := ctasShapeDB(t)

	for _, form := range []struct{ name, create, again string }{
		{"Declared",
			`CREATE TABLE ine (a INT64, b STRING)`,
			`CREATE TABLE IF NOT EXISTS ine (a INT64, b STRING)`},
		{"AsSelect",
			`CREATE TABLE inect AS SELECT id, s FROM shp`,
			`CREATE TABLE IF NOT EXISTS inect AS SELECT id FROM shp`},
	} {
		t.Run(form.name, func(t *testing.T) {
			if _, err := db.Query(ctx, form.create); err != nil {
				t.Fatal(err)
			}
			before, err := db.catalog.GetTable(ctx, ctasNameOf(form.create))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Query(ctx, form.again); err != nil {
				t.Fatalf("IF NOT EXISTS over a taken name refused: %v", err)
			}
			// The existing table is untouched — its SCHEMA above all, because
			// the second statement declares a different one.
			after, err := db.catalog.GetTable(ctx, ctasNameOf(form.create))
			if err != nil {
				t.Fatal(err)
			}
			if !ctasEqualStrings(before.Schema.ColumnNames(), after.Schema.ColumnNames()) {
				t.Errorf("the no-op replaced the table's schema: %v -> %v",
					before.Schema.ColumnNames(), after.Schema.ColumnNames())
			}
			// Without the clause it is still 42P07.
			if _, err := db.Query(ctx, form.create); sqlerr.StateOf(err) != "42P07" {
				t.Errorf("without IF NOT EXISTS: %s %v, want 42P07", sqlerr.StateOf(err), err)
			}
		})
	}

	// The existence test precedes the column types, as PostgreSQL's does.
	if _, err := db.Query(ctx, `CREATE TABLE IF NOT EXISTS ine (a NOSUCHTYPE)`); err != nil {
		t.Errorf("IF NOT EXISTS over a taken name looked at the types: %v", err)
	}
}

// A column DEFINITION list and a query cannot both be written (round-2 P1).
//
// `CREATE TABLE t (a INT64) AS SELECT 1` created an empty declared table and
// dropped the query on the floor, reporting success. PostgreSQL 17.11:
// `syntax error at or near "AS"` (measured).
func TestADefinitionListAndAQueryCannotBothBeWritten(t *testing.T) {
	db, ctx := ctasShapeDB(t)

	for _, sql := range []string{
		`CREATE TABLE trail (a INT64) AS SELECT 1 AS a`,
		`CREATE TABLE trail (a INT64, b STRING) AS SELECT id, s FROM shp`,
		`CREATE TABLE trail (a INT64) PARTITION BY (a) AS SELECT 1 AS a`,
	} {
		_, err := db.Query(ctx, sql)
		if err == nil {
			t.Errorf("%q was accepted; PostgreSQL 17.11 answers 42601", sql)
			continue
		}
		if got := sqlerr.StateOf(err); got != "42601" {
			t.Errorf("%q refused %s, want 42601: %v", sql, got, err)
		}
		if _, gerr := db.catalog.GetTable(ctx, "trail"); gerr == nil {
			t.Errorf("%q created the table anyway", sql)
		}
	}
	// The boundary from the other side: a RENAME list and a query is the CTAS
	// form and is accepted, and a definition list ALONE is the declared form.
	if _, err := db.Query(ctx, `CREATE TABLE renamed (a, b) AS SELECT id, s FROM shp`); err != nil {
		t.Errorf("the rename list form was refused: %v", err)
	}
	if _, err := db.Query(ctx, `CREATE TABLE declaredonly (a INT64, b STRING)`); err != nil {
		t.Errorf("the declared form was refused: %v", err)
	}
}

// ctasNameOf reads the table name out of a CREATE TABLE statement.
func ctasNameOf(sql string) string {
	f := strings.Fields(sql)
	for i, w := range f {
		if strings.EqualFold(w, "TABLE") && i+1 < len(f) {
			return strings.TrimSuffix(strings.TrimSuffix(f[i+1], "("), ",")
		}
	}
	return ""
}

// ctasReadCounter counts the data objects a statement READS.
//
// The unit is a `Get` of `tables/**.parquet`: that is a row of the source
// table crossing into the engine, and a statement documented not to execute
// its query must cause none.
type ctasReadCounter struct {
	objstore.Store
	gets  atomic.Int64
	bytes atomic.Int64
}

func (s *ctasReadCounter) Get(ctx context.Context, b, k string) (io.ReadCloser, objstore.ObjectInfo, error) {
	rc, info, err := s.Store.Get(ctx, b, k)
	if err == nil && strings.HasPrefix(k, "tables/") && strings.HasSuffix(k, ".parquet") {
		s.gets.Add(1)
		s.bytes.Add(info.Size)
	}
	return rc, info, err
}

func (s *ctasReadCounter) GetReaderAt(ctx context.Context, b, k string) (objstore.ReaderAtCloser, int64, error) {
	ra, ok := s.Store.(objstore.ReaderAtStore)
	if !ok {
		return nil, 0, fmt.Errorf("no ReaderAt")
	}
	r, n, err := ra.GetReaderAt(ctx, b, k)
	if err == nil && strings.HasPrefix(k, "tables/") && strings.HasSuffix(k, ".parquet") {
		s.gets.Add(1)
		s.bytes.Add(n)
	}
	return r, n, err
}

// WITH NO DATA DOES NOT RUN THE QUERY, and this asserts the ABSENCE of the
// execution rather than the outcome (#1024 round-3 review B1/N4).
//
// The outcome — an empty table with the right columns — is the same whether or
// not the rows were read, which is exactly how a round-3 change that ran every
// CTE body and every hash join's build side inside `WITH NO DATA` passed the
// gate above. Planning is not a pure derivation: `physical.Planner.Plan`
// materializes a CTE by RUNNING a pipeline over it and builds a join's build
// side, so a declaration that asks `Plan` for anything reads the table. The
// names come from `physical.PublishedOutputNames`, a walk over the logical
// plan, for that reason.
//
// Three assertions, because each catches a different way of executing: no data
// object is READ; no spill scratch is created; and a row that would make the
// query FAIL does not make the declaration fail — which is the property
// ADR-0036 names as the point of the clause.
func TestWithNoDataReadsNothingAndEvaluatesNothing(t *testing.T) {
	ctx := context.Background()
	scratch := t.TempDir()
	store := &ctasReadCounter{Store: objstore.NewMemStore()}
	db, err := Open(ctx, Config{Store: store, Bucket: "test", SpillDir: scratch})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "g", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "big", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, 4000)
	for i := 0; i < 4000; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "g": int64(i % 7), "s": "x"})
	}
	ing := db.NewIngester("big", schema, nil, ingest.Config{MaxBufferRows: 2500, RowGroupSize: 512})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Every shape the round-3 review measured a read on, plus the controls it
	// measured zero on, so the cell that starts reading is named.
	shapes := []struct{ name, query string }{
		{"Plain", `SELECT id, g + 1 FROM big`},
		{"StarOverDerived", `SELECT * FROM (SELECT id, g + 1 FROM big) x`},
		{"StarOverCTE", `WITH c AS (SELECT id, g + 1 FROM big) SELECT * FROM c`},
		{"CTEListedItems", `WITH c AS (SELECT id, g FROM big) SELECT id, g FROM c`},
		{"CTEUsedTwice", `WITH c AS (SELECT id, g FROM big) SELECT a.id, b.g FROM c a JOIN c b ON b.id = a.id`},
		{"NestedDerivedInCTE", `WITH c AS (SELECT * FROM (SELECT id, g + 1 FROM big) y) SELECT * FROM c`},
		{"Join", `SELECT a.id, b.g FROM big a JOIN big b ON b.id = a.id`},
		{"ScalarSubquery", `SELECT id, (SELECT MAX(g) FROM big) FROM big`},
		{"Aggregate", `SELECT g, COUNT(*) FROM big GROUP BY g`},
		// A row that would FAIL the query. `id = 5` exists, so the query
		// divides by zero the moment it runs — and the declaration must not.
		{"PoisonedRow", `SELECT id, 1 / (id - 5) AS x FROM big`},
		{"PoisonedRowInCTE", `WITH c AS (SELECT id, 1 / (id - 5) AS x FROM big) SELECT * FROM c`},
		{"PoisonedRowInCTEListed", `WITH c AS (SELECT id, 1 / (id - 5) AS x FROM big) SELECT id, x FROM c`},
		{"PoisonedRowInDerived", `SELECT * FROM (SELECT id, 1 / (id - 5) AS x FROM big) y`},
		{"PoisonedCastInCTE", `WITH c AS (SELECT CAST(s AS INT64) AS n FROM big) SELECT * FROM c`},
		{"PoisonedRowInJoin", `SELECT a.id, 1 / (a.id - 5) AS x FROM big a JOIN big b ON b.id = a.id`},
	}

	for i, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			tbl := fmt.Sprintf("nd_%d", i)
			store.gets.Store(0)
			store.bytes.Store(0)

			if _, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE %s AS %s WITH NO DATA", tbl, sh.query)); err != nil {
				t.Fatalf("the declaration FAILED: %v\n  WITH NO DATA must declare a table for a "+
					"query it does not run, including one that would fail on a row", err)
			}
			if n := store.gets.Load(); n != 0 {
				t.Errorf("the declaration READ %d data object(s) (%d bytes). WITH NO DATA does not "+
					"run the query, and planning is not a pure derivation — materializeCTEs RUNS a "+
					"pipeline and buildJoin builds the build side", n, store.bytes.Load())
			}
			if n := ctasScalar(t, ctx, db, "SELECT COUNT(*) AS c FROM "+tbl); n != 0 {
				t.Errorf("the declared table holds %d rows", n)
			}
			// No scratch: a spilling operator that ran would leave one.
			entries, err := os.ReadDir(scratch)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				names := make([]string, 0, len(entries))
				for _, e := range entries {
					names = append(names, e.Name())
				}
				t.Errorf("the declaration left %d scratch entries %v; nothing ran", len(entries), names)
			}
		})
	}

	// The boundary from the other side: the SAME poisoned queries DO fail when
	// the statement is asked to run them, so the cells above are not passing
	// because the expression is harmless.
	for i, q := range []string{
		`SELECT id, 1 / (id - 5) AS x FROM big`,
		`WITH c AS (SELECT id, 1 / (id - 5) AS x FROM big) SELECT * FROM c`,
	} {
		t.Run(fmt.Sprintf("WithDataFails%d", i), func(t *testing.T) {
			if _, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE poisoned_%d AS %s", i, q)); err == nil {
				t.Errorf("%s succeeded WITH DATA; the poisoned cells above prove nothing", q)
			}
		})
	}
}
