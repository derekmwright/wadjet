// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Arc BY round 2 (#1501, review r1 B2): a BYTES value that re-enters SQL text
// — a scalar subquery's answer, a correlated re-run's outer value — is
// `CAST('\x<hex>' AS BYTES)`, and that spelling has to survive EVERY position
// it can land in, not the window default alone. byR2Positions is the position
// table (X is the substituted value); byR2Values are the bytes that tell a
// spelling from a value: `hi`, a NUL inside, a quote, a backslash, empty.
//
// The same table over a bound parameter, in both formats, is
// pgwire.TestArcBYR2ReentryPositionsOnTheWire.

var byR2Values = []string{"6869", "610062", "27", "5c", ""}

// byR2Positions: each is ONE scalar query over by_one (one row: id 1, b
// `\xff`, s 'z') and by_v (k 1..5 holding byR2Values), with X the value.
var byR2Positions = []struct{ name, q string }{
	{"select", `SELECT encode(X, 'hex') FROM by_one`},
	{"where-eq", `SELECT count(*) FROM by_v WHERE b = X`},
	{"where-rev", `SELECT count(*) FROM by_v WHERE X = b`},
	{"in-list", `SELECT count(*) FROM by_v WHERE b IN (X)`},
	{"between", `SELECT count(*) FROM by_v WHERE b BETWEEN X AND X`},
	{"case", `SELECT encode(CASE WHEN id = 1 THEN X ELSE b END, 'hex') FROM by_one`},
	{"coalesce", `SELECT encode(COALESCE(X, b), 'hex') FROM by_one`},
	{"nullif", `SELECT encode(NULLIF(X, b), 'hex') FROM by_one`},
	{"greatest", `SELECT encode(GREATEST(X, decode('', 'hex')), 'hex') FROM by_one`},
	{"length", `SELECT length(X) FROM by_one`},
	{"concat", `SELECT encode(X || b, 'hex') FROM by_one`},
	{"substring", `SELECT encode(substring(X from 1 for 2), 'hex') FROM by_one`},
	{"like", `SELECT count(*) FROM by_v WHERE b LIKE X`},
	{"lag-value", `SELECT encode(lag(X, 0) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"lag-default", `SELECT encode(lag(b, 1, X) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"lead-default", `SELECT encode(lead(b, 1, X) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"nth-value", `SELECT encode(nth_value(X, 1) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"agg", `SELECT count(DISTINCT X) FROM by_one`},
	{"group-by", `SELECT count(*) FROM by_one GROUP BY X`},
	{"order-by", `SELECT id FROM by_one ORDER BY X`},
	{"join-on", `SELECT count(*) FROM by_v a JOIN by_one c ON a.b = X`},
	{"table-fn", `SELECT count(*) FROM generate_series(1, length(X) + 1)`},
	{"subquery-body", `SELECT (SELECT encode(X, 'hex'))`},
	{"cte", `WITH c AS (SELECT X AS v) SELECT encode(v, 'hex') FROM c`},
}

// byR2Sources: how X is spelled in the statement.
//   - scalar: a scalar subquery answering the value (the DAG substitutes it)
//   - correlated: the outer value of a correlated re-run
func byR2Cells() []struct{ name, sql string } {
	var out []struct{ name, sql string }
	for vi, v := range byR2Values {
		k := vi + 1
		label := v
		if label == "" {
			label = "empty"
		}
		for _, p := range byR2Positions {
			sq := strings.ReplaceAll(p.q, "X", fmt.Sprintf("(SELECT b FROM by_v WHERE k = %d)", k))
			out = append(out, struct{ name, sql string }{"scalar/" + p.name + "/" + label, sq})
			corr := fmt.Sprintf("SELECT o.k, (%s) AS r FROM by_v o WHERE o.k = %d", strings.ReplaceAll(p.q, "X", "o.b"), k)
			out = append(out, struct{ name, sql string }{"correlated/" + p.name + "/" + label, corr})
		}
	}
	return out
}

func byR2Tables() []tmdTable {
	i64 := parquet.Column{Name: "id", Type: parquet.TypeInt64}
	one := tmdTable{name: "by_one", schema: parquet.Schema{Columns: []parquet.Column{
		i64, {Name: "b", Type: parquet.TypeBytes, Nullable: true}, {Name: "s", Type: parquet.TypeString, Nullable: true}}},
		rows: []map[string]any{{"id": int64(1), "b": []byte{0xff}, "s": "z"}}}
	v := tmdTable{name: "by_v", schema: parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeBytes, Nullable: true}}}}
	for i, h := range byR2Values {
		raw, _ := hex.DecodeString(h)
		v.rows = append(v.rows, map[string]any{"k": int64(i + 1), "b": raw})
	}
	return []tmdTable{one, v}
}

const byR2PGFixture = `DROP TABLE IF EXISTS by_one; DROP TABLE IF EXISTS by_v;
CREATE TABLE by_one (id bigint, b bytea, s text); INSERT INTO by_one VALUES (1, '\xff', 'z');
CREATE TABLE by_v (k bigint, b bytea);
INSERT INTO by_v VALUES (1, '\x6869'), (2, '\x610062'), (3, '\x27'), (4, '\x5c'), (5, '\x');`

// byR2Kept is the cells this engine answers differently from PostgreSQL 17.11
// at c67ebf5b and at the round-2 tip alike — none is the re-entry spelling,
// each is a position this engine has no reading for yet, recorded as an arc BY
// filing candidate. ok=false means the cell must equal PostgreSQL. A kept cell
// that starts agreeing FAILS: delete its line.
func byR2Kept(name, arm string) (want, msg, reason string, ok bool) {
	parts := strings.Split(name, "/")
	src, pos, val := parts[0], parts[1], parts[2]
	dag := strings.HasPrefix(arm, "dag")
	switch {
	case src == "correlated" && pos == "agg":
		return "ERR 0A000", "", "an aggregate over only the outer column (PostgreSQL 42803)", true
	case src == "correlated" && pos == "cte":
		return "ERR 42P01", "", "a CTE body is not reached by the correlated re-run (BY-C9)", true
	case src == "correlated" && (pos == "lag-value" || pos == "lag-default" || pos == "lead-default" || pos == "nth-value"):
		return "ERR 0A000", "", "a window call in a correlated re-run (lateral-subqueries)", true
	case pos == "table-fn":
		return "ERR 0A000", "", "a table function takes constant arguments", true
	case pos == "like" && val == "5c":
		if src == "correlated" {
			return "rows=1 4,1", "", "a bytea LIKE pattern ending in its escape answers (PostgreSQL 22025; BY-C10)", true
		}
		if !dag {
			return "rows=1 1", "", "a bytea LIKE pattern ending in its escape answers (PostgreSQL 22025; BY-C10)", true
		}
	case src == "scalar" && pos == "substring":
		return "ERR 42883", "", "substring over a scalar subquery's BYTES declares text (BY-C11)", true
	}
	if src == "scalar" && dag {
		switch pos {
		case "between", "group-by", "lag-default", "lead-default", "like":
			return "ERR ", "subqueries require a SubqueryRunner", "a stage evaluates no scalar subquery in this position (BY-C8)", true
		}
	}
	return "", "", "", false
}

func byR2PGRender(res *pgconn.Result) string {
	var rows []string
	for _, r := range res.Rows {
		var cells []string
		for _, c := range r {
			if c == nil {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, string(c))
			}
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	sort.Strings(rows)
	return fmt.Sprintf("rows=%d %s", len(rows), strings.Join(rows, " | "))
}

func byR2Answers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_by_r2_reentry_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, _ := strings.Cut(line, "\t")
		out[name] = want
	}
	return out
}

// TestArcBYR2ReentryPositionsEveryArm: every (source, position, value) cell
// answers PostgreSQL 17.11's value or SQLSTATE on the five arms.
// BY_PG_DSN=<dsn> prints the answer file instead.
func TestArcBYR2ReentryPositionsEveryArm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	cells := byR2Cells()
	if dsn := os.Getenv("BY_PG_DSN"); dsn != "" {
		conn, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'; "+byR2PGFixture).ReadAll(); err != nil {
			t.Fatal(err)
		}
		for _, c := range cells {
			res := conn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
			if res.Err != nil {
				code := res.Err.Error()
				if pe, ok := res.Err.(*pgconn.PgError); ok {
					code = pe.Code
				}
				fmt.Printf("%s\tERR %s\n", c.name, code)
				continue
			}
			fmt.Printf("%s\t%s\n", c.name, byR2PGRender(res))
		}
		return
	}
	if testing.Short() {
		t.Skip("-short: five arms over the re-entry position table")
	}
	answers := byR2Answers(t)
	arms := byArmsOver(t, ctx, byR2Tables())
	compared := 0
	for _, tc := range cells {
		want, ok := answers[tc.name]
		if !ok {
			t.Fatalf("%s: no PostgreSQL answer", tc.name)
		}
		results := make([]struct {
			res *oracle.Result
			err error
		}, len(arms))
		var wg sync.WaitGroup
		for i, arm := range arms {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i].res, results[i].err = arm.run(tc.sql)
			}()
		}
		wg.Wait()
		for i, arm := range arms {
			res, err := results[i].res, results[i].err
			var got string
			switch {
			case err != nil:
				got = "ERR " + sqlerr.StateOf(err)
			default:
				got = byRender(res)
			}
			if kwant, kmsg, reason, kept := byR2Kept(tc.name, arm.name); kept {
				if got == kwant && (kmsg == "" || (err != nil && strings.Contains(err.Error(), kmsg))) {
					continue
				}
				t.Errorf("%s [%s]: kept cell moved\n  got  %s %v\n  kept %s %s (%s; PostgreSQL 17.11: %s)", tc.name, arm.name, got, err, kwant, kmsg, reason, want)
				continue
			}
			if got != want {
				msg := ""
				if err != nil {
					msg = err.Error()
					if len(msg) > 160 {
						msg = msg[:160]
					}
				}
				t.Errorf("%s [%s]\n  got  %s %s\n  want %s (PostgreSQL 17.11)\n  SQL: %s", tc.name, arm.name, got, msg, want, tc.sql)
				continue
			}
			compared++
		}
	}
	t.Logf("%d (cell, arm) answers equal PostgreSQL", compared)
}
