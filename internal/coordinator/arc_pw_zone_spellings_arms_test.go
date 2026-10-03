// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// pwZoneSpellings is the zone / clock-suffix set the date/time grammar is
// held to: each after `2024-03-04 12:00:00`. It carries the numeric-offset
// digit rule (PostgreSQL's DecodeTimezone reads the digits after the sign as
// one integer, the last two the minute when there is no `:` — `+00130`,
// `+0000130` and `+00000000130` are 01:30, `+000130` 01:30, `+001500` 15:00,
// `+053000` and `+0530:00` hour 530 → 22009, `+05:` +05), the POSIX zone name
// `Z+05` (PostgreSQL: five hours WEST; refused here, temporal r25), and the
// shapes around them.
var pwZoneSpellings = []string{
	"+00130", "+0000130", ",5", "+abc", " +0530", "Z+05", "+05:30:15:00", "+5:3",
	"+05:3", ".5+05", "-00130", "+00000000130", "+1:30", "+001", " z", "+05:",
	"++05", ".5.5", ":00", " 12:00", "-05:00:00", "-1559", ".000000000000001Z", "Zz",
	"+", "-",
	// the digit rule's own cells
	"+000130", "+001500", "+053000", "+0530:00", "+16", "+15:59:59",
}

// TestArcPWZoneSpellingsEveryArm: every zone spelling × {DATE / TIMESTAMP
// literal, CAST from text, INSERT of the quoted text (stored value read back),
// a bound parameter declared date / timestamp / timestamptz} against
// PostgreSQL 17.11's answer, on five doors over pgwire: the embedded engine,
// the embedded engine under a 512 KiB budget, and the coordinator's router
// over the DAG, the shuffled DAG and the DAG with four morsel workers. INSERT
// runs on the two embedded doors only — DML is the embedded engine's on every
// door, and the DAG doors' catalog is the coordinator's.
//
// The answers are testdata/arc_pw_zone_spellings_pg17.tsv (PostgreSQL 17.11,
// TimeZone UTC, re-measured whenever WADJET_PG_DSN names a server: the
// column must equal the live answer). A cell whose fourth column names a
// catalog row is a kept divergence: the engine must answer the third column
// and PostgreSQL must still answer the second — a kept cell that starts
// agreeing FAILS.
//
// At ed8d9c66 the grammar split an un-coloned offset by its length: `+000130`
// read as 00:01:30 (timestamptz 11:58:30 where PostgreSQL stores 10:30),
// `+001500` as 00:15:00, `+053000` answered where PostgreSQL raises 22009,
// and `+00130` / `+0000130` / `+00000000130` / `+05:` were 22007 on every
// DATE door where PostgreSQL and v0.25.3 answer.
func TestArcPWZoneSpellingsEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five doors over pgwire")
	}
	ctx := context.Background()
	type door struct {
		name     string
		embedded bool
		addr     string
	}
	serve := func(db *wadjet.DB, c *Coordinator) string {
		srv := pgwire.NewServer(db, pgwire.Config{}, nil)
		if c != nil {
			srv.SetRouter(NewQueryRouter(c))
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Shutdown() })
		return srv.Addr()
	}
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, []tmdTable{dtTable()})
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	doors := []door{
		{"single", true, serve(dtStandalone(t, ctx, 0), nil)},
		{"spilled512k", true, serve(dtStandalone(t, ctx, 512*1024), nil)},
		{"dag", false, serve(dtStandalone(t, ctx, 0), stand(nil))},
		{"dag-shuffled", false, serve(dtStandalone(t, ctx, 0), stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 }))},
		{"dag-morsel4", false, serve(dtStandalone(t, ctx, 0), stand(func(w *worker.Config) { w.MorselWorkers = 4 }))},
	}

	type cell struct {
		name   string
		insert bool
		run    func(conn *pgconn.PgConn) string
	}
	render := func(rr *pgconn.ResultReader) string {
		res := rr.Read()
		if res.Err != nil {
			return pwDoorErr(res.Err)
		}
		var rows []string
		for _, r := range res.Rows {
			var f []string
			for _, v := range r {
				if v == nil {
					f = append(f, "NULL")
				} else {
					f = append(f, string(v))
				}
			}
			rows = append(rows, strings.Join(f, "|"))
		}
		return "rows=[" + strings.Join(rows, " ; ") + "]"
	}
	simple := func(conn *pgconn.PgConn, sql string) string {
		mrr := conn.Exec(ctx, sql)
		defer mrr.Close()
		out := "rows=[]"
		for mrr.NextResult() {
			out = render(mrr.ResultReader())
		}
		if err := mrr.Close(); err != nil {
			return pwDoorErr(err)
		}
		return out
	}
	var cells []cell
	for k, z := range pwZoneSpellings {
		text := "2024-03-04 12:00:00" + z
		for _, typ := range []string{"DATE", "TIMESTAMP"} {
			lit := fmt.Sprintf("SELECT id, %s '%s' AS v FROM dt_pair WHERE id = 1", typ, text)
			cast := fmt.Sprintf("SELECT id, CAST('%s' AS %s) AS v FROM dt_pair WHERE id = 1", text, typ)
			cells = append(cells,
				cell{name: fmt.Sprintf("literal %s/%s", typ, text), run: func(c *pgconn.PgConn) string { return simple(c, lit) }},
				cell{name: fmt.Sprintf("CAST AS %s/%s", typ, text), run: func(c *pgconn.PgConn) string { return simple(c, cast) }},
			)
			tbl, id := "pwz_"+strings.ToLower(typ), k+1
			ins := fmt.Sprintf("INSERT INTO %s VALUES (%d, '%s')", tbl, id, text)
			back := fmt.Sprintf("SELECT v FROM %s WHERE id = %d", tbl, id)
			cells = append(cells, cell{name: fmt.Sprintf("INSERT %s/%s", typ, text), insert: true, run: func(c *pgconn.PgConn) string {
				if got := simple(c, ins); strings.HasPrefix(got, "ERR") {
					return got
				}
				return simple(c, back)
			}})
		}
		for _, p := range []struct {
			oid uint32
			typ string
		}{{1082, "DATE"}, {1114, "TIMESTAMP"}, {1184, "TIMESTAMP"}} {
			sql := fmt.Sprintf("SELECT id, CAST($1 AS %s) AS v FROM dt_pair WHERE id = 1", p.typ)
			cells = append(cells, cell{name: fmt.Sprintf("$1 %d AS %s/%s", p.oid, p.typ, text), run: func(c *pgconn.PgConn) string {
				return render(c.ExecParams(ctx, sql, [][]byte{[]byte(text)}, []uint32{p.oid}, nil, nil))
			}})
		}
	}
	setup := []string{"SET statement_timeout = '30s'", "SET TimeZone = 'UTC'",
		"CREATE TABLE pwz_date (id BIGINT, v DATE)", "CREATE TABLE pwz_timestamp (id BIGINT, v TIMESTAMP)"}

	// PostgreSQL first.
	pins := pwZonePins(t)
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		pg, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pg.Close(ctx)
		pre := append([]string{"DROP SCHEMA IF EXISTS pwzone CASCADE", "CREATE SCHEMA pwzone", "SET search_path = pwzone"},
			strings.Split(dtPGFixture, "\n")...)
		for _, s := range append(pre, setup...) {
			if _, err := pg.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		var dump []string
		for _, c := range cells {
			got := c.run(pg)
			dump = append(dump, c.name+"\t"+got)
			if p, ok := pins[c.name]; ok && p.pg != got {
				t.Errorf("PostgreSQL %s answered %s, pinned %s", c.name, got, p.pg)
			}
		}
		if f := os.Getenv("PW_ZONE_PG_DUMP"); f != "" {
			if err := os.WriteFile(f, []byte(strings.Join(dump, "\n")+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	seen := map[string]bool{}
	var engineDump []string
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+d.addr+"/test?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			if d.embedded {
				for _, s := range setup[2:] {
					if got := simple(conn, s); strings.HasPrefix(got, "ERR") {
						t.Fatalf("%s: %s", s, got)
					}
				}
			}
			for _, c := range cells {
				if c.insert && !d.embedded {
					continue
				}
				got := c.run(conn)
				if d.name == "single" {
					engineDump = append(engineDump, c.name+"\t"+got)
				}
				p, ok := pins[c.name]
				if !ok {
					t.Errorf("%s: no pinned PostgreSQL answer (engine %s)", c.name, got)
					continue
				}
				seen[c.name] = true
				want := p.pg
				if p.kept != "" {
					want = p.kept
				}
				if got != want {
					t.Errorf("%s on %s\n  got  %s\n  want %s (PostgreSQL 17.11 %s%s)", c.name, d.name, got, want, p.pg, p.row)
				}
			}
		})
	}
	if f := os.Getenv("PW_ZONE_ENGINE_DUMP"); f != "" {
		_ = os.WriteFile(f, []byte(strings.Join(engineDump, "\n")+"\n"), 0o644)
	}
	var stale []string
	for name := range pins {
		if !seen[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	for _, name := range stale {
		t.Errorf("pinned cell %q is no longer generated", name)
	}
}

type pwZonePin struct{ pg, kept, row string }

// pwZonePins reads testdata/arc_pw_zone_spellings_pg17.tsv: cell, PostgreSQL
// 17.11's answer, and for a kept divergence the engine's answer and its
// catalog row.
func pwZonePins(t *testing.T) map[string]pwZonePin {
	t.Helper()
	f, err := os.Open("testdata/arc_pw_zone_spellings_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pins := map[string]pwZonePin{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		for len(cols) < 4 {
			cols = append(cols, "")
		}
		if cols[3] != "" {
			cols[3] = "; kept: " + cols[3]
		}
		pins[cols[0]] = pwZonePin{pg: cols[1], kept: cols[2], row: cols[3]}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return pins
}
