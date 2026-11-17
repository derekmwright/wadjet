// SPDX-License-Identifier: MIT

package wadjet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// AN ORDER BY OR GROUP BY POSITION THAT NAMES NO SELECT ITEM (#1700). The
// table (testdata/ordinal_position_pg17.tsv) is position spelling {9 past the
// list, 0, -1, -0, 1.0, 2.5, '1', +1, (1), 1 in range} x where the term sits
// {ORDER BY, GROUP BY, ORDER BY over a GROUP BY, both inside a derived table,
// a scalar subquery, a correlated scalar subquery, EXISTS over GROUP BY, a CTE
// body, a set operation's ORDER BY}. Every want is PostgreSQL 17.11's answer
// (rows sorted, since several cells order by a constant, which orders
// nothing); a fourth field is a RESIDUAL pinned at this engine's answer.
//
// An ORDER BY position past the list, 0 or negative was refused UNCODED and
// reached a client as 42601; it is PostgreSQL's 42P10 now, as the GROUP BY
// position beside it already was. The pins record what this engine keeps
// answering differently, among them the kept SUPERSET: a non-integer constant
// as a GROUP BY or ORDER BY term (`'1'`) answers here where PostgreSQL refuses
// 42601, and `+1` is a position here where PostgreSQL reads an expression.
//
// Regenerate: ORD_PG_DSN=postgres://... go test -run TestOrdinalPositionGenerate ./wadjet/

var ordFixture = []string{
	"CREATE TABLE ot (a INT, b INT)",
	"INSERT INTO ot VALUES (1,20),(2,10),(3,30)",
}

func ordCellSQL() (names, sqls []string) {
	spellings := []struct{ name, k string }{
		{"past", "9"}, {"zero", "0"}, {"negative", "-1"}, {"negZero", "-0"},
		{"float", "1.0"}, {"fraction", "2.5"}, {"string", "'1'"}, {"plus", "+1"},
		{"paren", "(1)"}, {"inRange", "1"},
	}
	places := []struct{ name, tmpl string }{
		{"orderBy", "SELECT a, b FROM ot ORDER BY %s"},
		{"groupBy", "SELECT a, count(*) FROM ot GROUP BY %s"},
		{"orderOverGroup", "SELECT a, count(*) FROM ot GROUP BY a ORDER BY %s"},
		{"derivedOrderBy", "SELECT * FROM (SELECT a, b FROM ot ORDER BY %s) s"},
		{"derivedGroupBy", "SELECT * FROM (SELECT a, count(*) AS c FROM ot GROUP BY %s) s"},
		{"scalarOrderBy", "SELECT (SELECT a FROM ot ORDER BY %s LIMIT 1) AS x"},
		{"correlatedOrderBy", "SELECT o.a, (SELECT i.b FROM ot i WHERE i.a = o.a ORDER BY %s LIMIT 1) AS x FROM ot o"},
		{"existsGroupBy", "SELECT o.a FROM ot o WHERE EXISTS (SELECT i.a, count(*) FROM ot i WHERE i.a = o.a GROUP BY %s)"},
		{"cteOrderBy", "WITH s AS (SELECT a, b FROM ot ORDER BY %s) SELECT * FROM s"},
		{"setOpOrderBy", "SELECT a FROM ot UNION SELECT b FROM ot ORDER BY %s"},
	}
	for _, sp := range spellings {
		for _, pl := range places {
			names = append(names, pl.name+"/"+sp.name)
			sqls = append(sqls, fmt.Sprintf(pl.tmpl, sp.k))
		}
	}
	return names, sqls
}

func ordRender(rows []string) string {
	if len(rows) == 0 {
		return "(none)"
	}
	sort.Strings(rows)
	return strings.Join(rows, " | ")
}

func TestOrdinalPositionGenerate(t *testing.T) {
	dsn := os.Getenv("ORD_PG_DSN")
	if dsn == "" {
		t.Skip("ORD_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, q := range append([]string{"SET statement_timeout = '30s'", "DROP TABLE IF EXISTS ot"}, ordFixture...) {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	pins := map[string]string{}
	if old, err := ordLoad(); err == nil {
		for _, c := range old {
			pins[c.name] = c.pin
		}
	}
	names, sqls := ordCellSQL()
	var b strings.Builder
	b.WriteString("# name\tSQL\tPostgreSQL 17.11 answer\tresidual pin (empty: none)\n")
	for i := range names {
		ans := func() string {
			rows, err := conn.Query(ctx, sqls[i])
			if err != nil {
				return ordPGErr(err)
			}
			defer rows.Close()
			var all []string
			for rows.Next() {
				v, err := rows.Values()
				if err != nil {
					return ordPGErr(err)
				}
				parts := make([]string, len(v))
				for j := range v {
					parts[j] = ordCell(v[j])
				}
				all = append(all, strings.Join(parts, ","))
			}
			if err := rows.Err(); err != nil {
				return ordPGErr(err)
			}
			return ordRender(all)
		}()
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\n", names[i], sqls[i], ans, pins[names[i]])
	}
	if err := os.WriteFile("testdata/ordinal_position_pg17.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func ordCell(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}

func ordPGErr(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}

type ordCellT struct{ name, sql, pg, pin string }

func ordLoad() ([]ordCellT, error) {
	f, err := os.Open("testdata/ordinal_position_pg17.tsv")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []ordCellT
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.Split(line, "\t")
		if len(p) != 4 {
			return nil, fmt.Errorf("malformed line %q", line)
		}
		out = append(out, ordCellT{p[0], p[1], p[2], p[3]})
	}
	return out, sc.Err()
}

// TestOrdinalPositionCodes runs the table on the embedded engine.
// ORD_MEASURE=1 logs every cell instead of asserting.
func TestOrdinalPositionCodes(t *testing.T) {
	ctx := context.Background()
	cells, err := ordLoad()
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range ordFixture {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	measure := os.Getenv("ORD_MEASURE") != ""
	for _, c := range cells {
		got := func() string {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				return "ERR " + sqlerr.StateOf(err)
			}
			rows := make([]string, len(res.Rows))
			for i := range res.Rows {
				cs := res.Cells(i)
				parts := make([]string, len(cs))
				for j := range cs {
					parts[j] = ordCell(cs[j])
				}
				rows[i] = strings.Join(parts, ",")
			}
			return ordRender(rows)
		}()
		switch {
		case measure:
			status := "RIGHT"
			if got != c.pg {
				status = "DIFF"
			}
			t.Logf("CELL\t%s\t%s\tgot=%s\tpg=%s", status, c.name, got, c.pg)
		case c.pin == "" && got != c.pg:
			t.Errorf("%s: %s\n got  %s\n want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.pg)
		case c.pin != "" && got != c.pin:
			t.Errorf("%s: %s\n got    %s\n pinned %s (a residual; PostgreSQL 17.11 answers %s)", c.name, c.sql, got, c.pin, c.pg)
		}
	}
}
