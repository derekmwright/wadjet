// SPDX-License-Identifier: MIT

package pgwire

// A WINDOW'S TERMS ARE JUDGED LIKE A SELECT ITEM ABOVE A GROUP BY, AND AN
// ITEM MIXING AN AGGREGATE AND A WINDOW FUNCTION IS COMPUTED — ON THE WIRE
// (#1651, #1646): the RowDescription's OIDs and the DataRow bytes over the
// simple protocol, the extended protocol (text) and the extended protocol
// with binary results, or the refusal's SQLSTATE and the column it names.
// At 542b4f37 `SELECT g, MAX(b) * 2 + ROW_NUMBER() OVER (ORDER BY g) … GROUP
// BY g` went out as float8 (OID 701) NULLs where PostgreSQL sends int8 41,
// 102, 123, and `sum(f) OVER (PARTITION BY a.i) … GROUP BY i` as three float8
// NULLs where PostgreSQL raises 42803.
//
// PostgreSQL 17.11's answers (testdata/arc_cw_window_terms_wire_pg17.tsv:
// name, mode, sql, answer) are measured by this test with CWW_PG_DSN naming a
// scratch database, over the same client and the same fixture as DDL.

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

const cwtWireFixture = `DROP TABLE IF EXISTS rv_a;
CREATE TABLE rv_a (id integer, i integer, f double precision);
INSERT INTO rv_a VALUES (1,2,0.5),(2,4,1.5),(3,1,2.5);
DROP TABLE IF EXISTS wd_t;
CREATE TABLE wd_t (id integer, g integer, b bigint);
INSERT INTO wd_t VALUES (1,1,20),(2,2,50),(3,3,60),(4,1,10);`

// cwtWireCells are name, sql; every statement orders its rows.
func cwtWireCells() [][2]string {
	return [][2]string{
		{"1646/mixed", "SELECT g, MAX(b) * 2 + ROW_NUMBER() OVER (ORDER BY g) AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"1646/windowFirst", "SELECT g, ROW_NUMBER() OVER (ORDER BY g) + COUNT(*) AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"1646/coalesceRank", "SELECT g, COALESCE(MAX(b), 0) + RANK() OVER (ORDER BY g) AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"1646/aggPlusLagOfAgg", "SELECT g, MAX(b) + LAG(MAX(b), 1, 0) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"1646/caseOverBoth", "SELECT g, CASE WHEN MAX(b) > 20 THEN ROW_NUMBER() OVER (ORDER BY g) ELSE 0 END AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"1646/implicit", "SELECT count(*) + row_number() OVER () AS w FROM wd_t"},
		{"1646/aggInsideWindow", "SELECT g, sum(MAX(b) * 2) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"1651/arg", "SELECT sum(f) OVER (PARTITION BY a.i) AS w FROM rv_a a GROUP BY i ORDER BY i"},
		{"1651/argQual", "SELECT sum(a.f) OVER (PARTITION BY a.i) FROM rv_a a GROUP BY i ORDER BY i"},
		{"1651/partition", "SELECT sum(i) OVER (PARTITION BY f) FROM rv_a a GROUP BY i ORDER BY i"},
		{"1651/order", "SELECT sum(i) OVER (ORDER BY f) FROM rv_a a GROUP BY i ORDER BY i"},
		{"1651/keyExpr", "SELECT sum(i) OVER () FROM rv_a a GROUP BY i + 1 ORDER BY 1"},
		{"1651/keyExprInside", "SELECT g, rank() OVER (ORDER BY g + b) AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"1651/default", "SELECT g, lag(g, 1, b) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"1651/sortTerm", "SELECT g FROM wd_t GROUP BY g ORDER BY sum(b) OVER (), g"},
		{"ctl/frame", "SELECT sum(i) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM rv_a a GROUP BY i ORDER BY i"},
		{"ctl/keyArg", "SELECT sum(i) OVER (PARTITION BY a.i) AS w FROM rv_a a GROUP BY i ORDER BY i"},
		{"ctl/aggArg", "SELECT i, sum(max(f)) OVER () FROM rv_a a GROUP BY i ORDER BY i"},
		{"ctl/aggPartition", "SELECT i, row_number() OVER (PARTITION BY max(f) ORDER BY i) FROM rv_a a GROUP BY i ORDER BY i"},
		{"ctl/outputAlias", "SELECT i AS k, sum(i) OVER (PARTITION BY k) FROM rv_a a GROUP BY i ORDER BY i"},
		{"ctl/aggExprArgNested", "SELECT g, sum(max(b) * 2) OVER () + 0 AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"ctl/lagAggExprNested", "SELECT g, lag(max(b) * 2, 1, 0) OVER (ORDER BY g) + 0 AS w FROM wd_t GROUP BY g ORDER BY g"},
		{"qualify/mixedAlias", "SELECT g, max(b) + row_number() OVER (ORDER BY g) AS w FROM wd_t GROUP BY g QUALIFY w > 50 ORDER BY g"},
		{"qualify/aggPlusWin", "SELECT g FROM wd_t GROUP BY g QUALIFY max(b) + row_number() OVER (ORDER BY g) > 50 ORDER BY g"},
		{"qualify/lagUngrouped", "SELECT g FROM wd_t GROUP BY g QUALIFY lag(b) OVER (ORDER BY g) IS NULL ORDER BY g"},
		{"qualify/partUngrouped", "SELECT g FROM wd_t GROUP BY g QUALIFY count(*) OVER (PARTITION BY b) = 1 ORDER BY g"},
		{"qualify/rankUngrouped", "SELECT g FROM wd_t GROUP BY g QUALIFY rank() OVER (ORDER BY b) = 1 ORDER BY g"},
	}
}

// cwtWireOracle is the statement PostgreSQL answers in a QUALIFY cell's place
// (PostgreSQL has no QUALIFY): the same filter over a derived table.
var cwtWireOracle = map[string]string{
	"qualify/mixedAlias":    "SELECT g, w FROM (SELECT g, max(b) + row_number() OVER (ORDER BY g) AS w FROM wd_t GROUP BY g) s WHERE w > 50 ORDER BY g",
	"qualify/aggPlusWin":    "SELECT g FROM (SELECT g, max(b) + row_number() OVER (ORDER BY g) AS q FROM wd_t GROUP BY g) s WHERE q > 50 ORDER BY g",
	"qualify/lagUngrouped":  "SELECT g FROM (SELECT g, lag(b) OVER (ORDER BY g) AS q FROM wd_t GROUP BY g) s WHERE q IS NULL ORDER BY g",
	"qualify/partUngrouped": "SELECT g FROM (SELECT g, count(*) OVER (PARTITION BY b) AS q FROM wd_t GROUP BY g) s WHERE q = 1 ORDER BY g",
	"qualify/rankUngrouped": "SELECT g FROM (SELECT g, rank() OVER (ORDER BY b) AS q FROM wd_t GROUP BY g) s WHERE q = 1 ORDER BY g",
}

var cwtWireColumnRe = regexp.MustCompile(`column "([^"]+)"`)

// cwtWireCell is the cell's answer: ssAuditWireRun's rendering, or for a
// refusal its SQLSTATE and the column it names (the bare name: PostgreSQL
// qualifies it with the relation's alias, this engine with what was written).
func cwtWireCell(ctx context.Context, conn *pgx.Conn, sql string, binary bool) string {
	r := ssAuditWireRun(ctx, conn, sql, true, binary)
	rest, ok := strings.CutPrefix(r, "ERR ")
	if !ok {
		return r
	}
	code, msg, _ := strings.Cut(rest, " ")
	if m := cwtWireColumnRe.FindStringSubmatch(msg); m != nil {
		name := m[1]
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			name = name[i+1:]
		}
		return "ERR " + code + " " + strings.ToLower(name)
	}
	return "ERR " + code
}

func TestArcCWWindowTermsOnTheWire(t *testing.T) {
	ctx := context.Background()
	if dsn := os.Getenv("CWW_PG_DSN"); dsn != "" {
		cwtWireMeasure(t, ctx, dsn)
		return
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range strings.Split(cwtWireFixture, ";\n") {
		if strings.HasPrefix(strings.TrimSpace(ddl), "DROP") {
			continue
		}
		if _, err := db.Query(ctx, strings.TrimSuffix(strings.TrimSpace(ddl), ";")); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conns := map[string]*pgx.Conn{}
	for m, qm := range ci2WireModes {
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable&default_query_exec_mode=%s", srv.Addr(), qm))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(ctx) })
		conns[m] = conn
	}
	cells := ssAuditWireTSV(t, "testdata/arc_cw_window_terms_wire_pg17.tsv", 4)
	if want := 3 * len(cwtWireCells()); len(cells) != want {
		t.Fatalf("%d measured cells, want %d (three modes per cell): re-measure", len(cells), want)
	}
	for _, c := range cells {
		name, mode, sql, pg := c[0], c[1], c[2], c[3]
		t.Run(name+"/"+mode, func(t *testing.T) {
			if got := cwtWireCell(ctx, conns[mode], sql, mode == "b"); got != pg {
				t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11 sends)", sql, got, pg)
			}
		})
	}
}

// cwtWireMeasure writes PostgreSQL's answers for every cell and mode.
func cwtWireMeasure(t *testing.T, ctx context.Context, dsn string) {
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 (postgres:17-alpine), measured by TestArcCWWindowTermsOnTheWire with CWW_PG_DSN: name, mode, sql, answer.\n")
	for _, mode := range []string{"s", "e", "b"} {
		conn, err := pgx.Connect(ctx, dsn+"&default_query_exec_mode="+ci2WireModes[mode])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, cwtWireFixture); err != nil {
			t.Fatal(err)
		}
		for _, c := range cwtWireCells() {
			q := c[1]
			if o, ok := cwtWireOracle[c[0]]; ok {
				q = o
			}
			fmt.Fprintf(&b, "%s\t%s\t%s\t%s\n", c[0], mode, c[1], cwtWireCell(ctx, conn, q, mode == "b"))
		}
		conn.Close(ctx)
	}
	if err := os.WriteFile("testdata/arc_cw_window_terms_wire_pg17.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
