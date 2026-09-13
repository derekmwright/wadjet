package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
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

	res, err := db.Query(ctx, `CREATE TABLE nodata AS SELECT id, g + 1 AS gp, d FROM shp WITH NO DATA`)
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
	if got := meta.Schema.ColumnNames(); !ctasEqualStrings(got, []string{"id", "gp", "d"}) {
		t.Errorf("declared %v, want [id gp d]", got)
	}
	if n := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM nodata`); n != 0 {
		t.Errorf("the table holds %d rows; WITH NO DATA writes none", n)
	}
	// The declaration is the same one the WITH DATA arm takes.
	if _, err := db.Query(ctx, `CREATE TABLE withdata AS SELECT id, g + 1 AS gp, d FROM shp`); err != nil {
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
