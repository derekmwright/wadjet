// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A TIMESTAMPTZ VALUE AND THE SESSION ZONE ON EVERY DOOR (#1680 line d).
//
// Every door that runs a SELECT reads a timestamptz literal as the instant it
// names (its offset applied: `TIMESTAMPTZ '2024-07-01 12:00:00+05:30'` is
// 06:30 UTC, PostgreSQL 17.11's answer under TimeZone=UTC; it was 12:00 on
// every door), reads a quoted literal with an offset beside a timestamptz —
// written directly or carried by a derived table, a CTE, a UNION ALL arm, a
// scalar subquery or a join — as a timestamptz, and refuses one beside a
// stored timestamp column, whose declaration no door records (round 1 left
// the carried cells answering false, a regression from the base's true).
// pgwire, the only door with a session,
// refuses a SET TimeZone / SET TIME ZONE naming any zone but UTC on both arms
// and both protocols, and the session keeps answering at UTC after it; the
// embedded, HTTP and gRPC doors have no SET and refuse the statement. The
// pgwire column of a timestamptz value is OID 1114 (timestamp) with no
// offset printed — the documented "no timestamptz" divergence, pinned here
// so a change to it is seen.
func TestTimestamptzOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	single, dag, coord, db := owaDoorsOver(t, ctx, vliWriteDates)
	local := httptest.NewServer(New(Config{Addr: ":0", Catalog: db.Catalog()}, nil).Mux())
	t.Cleanup(local.Close)
	dist := httptest.NewServer(New(Config{Addr: ":0", Catalog: db.Catalog(), Coordinator: coord}, nil).Mux())
	t.Cleanup(dist.Close)
	gEmbedded := NewGRPCServer(GRPCConfig{DB: db}, slog.Default())
	gDist := NewGRPCServer(GRPCConfig{Coord: coord}, slog.Default())
	conns := map[string]*pgconn.PgConn{}
	for name, addr := range map[string]string{"pgwire/single": single, "pgwire/dag": dag} {
		c, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", addr))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close(context.Background()) })
		conns[name] = c
	}
	post := func(base, sql string) (map[string]any, string, string) {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		resp, err := http.Post(base+"/v1/queries", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST %q: %v", sql, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out struct {
			Rows     []map[string]any `json:"rows"`
			Error    string           `json:"error"`
			SQLState string           `json:"sqlstate"`
		}
		_ = json.Unmarshal(raw, &out)
		if out.Error == "" && resp.StatusCode >= 400 {
			out.Error = fmt.Sprintf("status %d", resp.StatusCode)
		}
		var row map[string]any
		if len(out.Rows) == 1 {
			row = out.Rows[0]
		}
		return row, out.Error, out.SQLState
	}
	pgState := func(err error) string {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return pe.Code
		}
		return ""
	}

	// The stored columns of rule 3 (validate_timestamptz_lineage.go): a
	// TIMESTAMP column declared through SQL DDL (tzs), through the HTTP and
	// gRPC table-creation doors (tzh, tzg), and a CREATE TABLE AS of a
	// timestamptz (tzc). No door can declare a timestamptz column — each
	// answers TIMESTAMPTZ with 42704 — and none records which declaration a
	// TIMESTAMP column had, so a quoted literal with an offset beside any of
	// them is refused.
	for _, s := range []string{
		"CREATE TABLE tzs (id BIGINT, ts TIMESTAMP)",
		"INSERT INTO tzs VALUES (1, TIMESTAMP '2024-07-01 12:00:00')",
		"CREATE TABLE tzc AS SELECT id, TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS ts FROM vld WHERE id = 1",
	} {
		if _, err := db.Query(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := db.Query(ctx, "CREATE TABLE tzbad (ts TIMESTAMPTZ)"); sqlerr.StateOf(err) != "42704" {
		t.Errorf("SQL DDL TIMESTAMPTZ: %v, want 42704 (no door declares a timestamptz column)", err)
	}
	httpCreate := func(name, typ string) (int, string) {
		nullable := true
		body, _ := json.Marshal(map[string]any{"name": name, "columns": []map[string]any{
			{"name": "id", "type": "INT64", "nullable": &nullable}, {"name": "ts", "type": typ, "nullable": &nullable}}})
		resp, err := http.Post(local.URL+"/v1/tables", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}
	if code, body := httpCreate("tzh", "TIMESTAMP"); code != http.StatusCreated {
		t.Fatalf("HTTP CREATE TABLE tzh: %d %s", code, body)
	}
	if code, body := httpCreate("tzhbad", "TIMESTAMPTZ"); code == http.StatusCreated || !strings.Contains(body, `type \"TIMESTAMPTZ\" does not exist`) {
		t.Errorf("HTTP CREATE TABLE TIMESTAMPTZ: %d %s, want type does not exist (42704)", code, body)
	}
	grpcCols := func(typ string) []*wadjetv1.ColumnDef {
		return []*wadjetv1.ColumnDef{{Name: "id", Type: "INT64", Nullable: true}, {Name: "ts", Type: typ, Nullable: true}}
	}
	gDDL := NewGRPCServer(GRPCConfig{Catalog: db.Catalog()}, slog.Default())
	if _, err := gDDL.CreateTable(ctx, &wadjetv1.CreateTableRequest{Name: "tzg", Columns: grpcCols("TIMESTAMP")}); err != nil {
		t.Fatalf("gRPC CreateTable tzg: %v", err)
	}
	if _, err := gDDL.CreateTable(ctx, &wadjetv1.CreateTableRequest{Name: "tzgbad", Columns: grpcCols("TIMESTAMPTZ")}); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("gRPC CreateTable TIMESTAMPTZ: %v, want type does not exist", err)
	}

	// Values: PostgreSQL 17.11 under UTC, as text (the timestamptz's `+00`
	// is the divergence; the instant is the answer). A bare expression runs
	// as `SELECT CAST(<expr> AS TEXT) AS r FROM vld WHERE id = 1`; a statement
	// (one starting with SELECT or WITH) runs as written.
	const tz = "TIMESTAMPTZ '2024-07-01 12:00:00+05:30'"
	const ofs = "'2024-07-01 12:00:00+05:30'"
	for _, c := range []struct{ expr, want string }{
		{"TIMESTAMPTZ '2024-07-01 12:00:00+05:30'", "2024-07-01 06:30:00"},
		{"CAST('2024-07-01 12:00:00-04' AS TIMESTAMPTZ)", "2024-07-01 16:00:00"},
		{"'2024-07-01 02:00:00+05:30'::timestamptz", "2024-06-30 20:30:00"},
		{"TIMESTAMPTZ '2024-03-10 02:30:00'", "2024-03-10 02:30:00"},
		{"date_trunc('day', TIMESTAMPTZ '2024-07-01 02:00:00+05:30')", "2024-06-30 00:00:00"},
		{"CAST(TIMESTAMPTZ '2024-07-01 02:00:00+05:30' AS DATE)", "2024-06-30"},
		{"extract(timezone_hour FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30')", "0"},
		{"date_part('timezone', TIMESTAMPTZ '2024-07-01 12:00:00+05:30')", "0"},
		{"TIMESTAMPTZ '2024-07-01 12:00:00+00' = TIMESTAMPTZ '2024-07-01 17:30:00+05:30'", "true"},
		{"TIMESTAMP '2024-07-01 12:00:00+05:30'", "2024-07-01 12:00:00"},
		// A quoted literal beside a timestamptz is read as one (its offset
		// applied), directly and through every carrier of the slot.
		{"TIMESTAMPTZ '2024-07-01 12:00:00+00' = '2024-07-01 17:30:00+05:30'", "true"},
		{"greatest(TIMESTAMPTZ '2024-07-01 12:00:00+00', '2024-07-01 15:00:00+05:30')", "2024-07-01 12:00:00"},
		{"coalesce(CAST(NULL AS TIMESTAMPTZ), '2024-07-01 15:00:00+05:30')", "2024-07-01 09:30:00"},
		{"SELECT CAST(count(*) AS TEXT) AS r FROM (SELECT " + tz + " AS x FROM vld WHERE id = 1) s WHERE x = " + ofs, "1"},
		{"WITH c AS (SELECT " + tz + " AS x FROM vld WHERE id = 1) SELECT CAST(count(*) AS TEXT) AS r FROM c WHERE x >= " + ofs, "1"},
		{"SELECT CAST(count(*) AS TEXT) AS r FROM (SELECT " + tz + " AS x FROM vld WHERE id = 1 UNION ALL SELECT TIMESTAMPTZ '2024-07-01 00:00:00+00' FROM vld WHERE id = 2) s WHERE x = " + ofs, "1"},
		{"SELECT CAST(count(*) AS TEXT) AS r FROM vld WHERE id = 1 AND (SELECT " + tz + ") = " + ofs, "1"},
		{"SELECT CAST(count(*) AS TEXT) AS r FROM (SELECT id, " + tz + " AS x FROM vld) a JOIN vld b ON a.id = b.id WHERE a.id = 1 AND a.x = " + ofs, "1"},
		{"SELECT CAST(count(*) AS TEXT) AS r FROM (SELECT CAST(" + ofs + " AS TIMESTAMPTZ) AS x FROM vld WHERE id = 1) q WHERE x = " + ofs, "1"},
		// Beside a plain timestamp by lineage the offset is discarded.
		{"SELECT CAST(count(*) AS TEXT) AS r FROM (SELECT TIMESTAMP '2024-07-01 12:00:00' AS x FROM vld WHERE id = 1) s WHERE x = " + ofs, "1"},
		// A literal without an offset beside a stored column answers.
		{"SELECT CAST(count(*) AS TEXT) AS r FROM tzs WHERE ts = '2024-07-01 12:00:00'", "1"},
		{"SELECT CAST(count(*) AS TEXT) AS r FROM tzc WHERE ts = TIMESTAMPTZ '2024-07-01 12:00:00+05:30'", "1"},
	} {
		sql := c.expr
		if !strings.HasPrefix(sql, "SELECT ") && !strings.HasPrefix(sql, "WITH ") {
			sql = "SELECT CAST(" + c.expr + " AS TEXT) AS r FROM vld WHERE id = 1"
		}
		var got []string
		check := func(door string, v any, err error) {
			if err != nil {
				got = append(got, door+": "+err.Error())
			} else if fmt.Sprint(v) != c.want {
				got = append(got, fmt.Sprintf("%s: %v", door, v))
			}
		}
		res, err := db.Query(ctx, sql)
		if err == nil {
			check("embedded", res.Rows[0]["r"], nil)
		} else {
			check("embedded", nil, err)
		}
		for name, base := range map[string]string{"http/local": local.URL, "http/distributed": dist.URL} {
			row, e, _ := post(base, sql)
			if e != "" {
				check(name, nil, errors.New(e))
			} else {
				check(name, row["r"], nil)
			}
		}
		for name, g := range map[string]*GRPCServer{"grpc/embedded": gEmbedded, "grpc/distributed": gDist} {
			resp, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: sql})
			if err == nil && len(resp.Rows) == 1 {
				check(name, resp.Rows[0].Fields["r"].GetStringValue(), nil)
			} else {
				check(name, nil, fmt.Errorf("%v (%d rows)", err, len(resp.GetRows())))
			}
		}
		for name, conn := range conns {
			r := conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
			if r.Err == nil && len(r.Rows) == 1 {
				check(name, string(r.Rows[0][0]), nil)
			} else {
				check(name, nil, fmt.Errorf("%v (%d rows)", r.Err, len(r.Rows)))
			}
		}
		if len(got) > 0 {
			t.Errorf("%s (PostgreSQL 17.11 under UTC: %s):\n    %s", c.expr, c.want, strings.Join(got, "\n    "))
		}
	}

	// The refusals, by SQLSTATE and sentence on every door: a quoted literal
	// with an offset beside a stored column of unknown declaration (rule 3,
	// the authorized right-to-refused class: PostgreSQL answers 1 over a
	// timestamp column and over a timestamptz one, by different readings),
	// PostgreSQL's own 42883 for a timestamptz beside a text column, and its
	// 0A000 for a time zone field of a plain timestamp.
	const ambiguous = "a quoted literal with a time zone offset beside a stored timestamp column is not supported"
	for _, c := range []struct{ sql, state, sentence string }{
		{"SELECT count(*) AS n FROM tzs WHERE ts = " + ofs, "0A000", ambiguous},
		{"SELECT count(*) AS n FROM tzc WHERE ts >= " + ofs, "0A000", ambiguous},
		{"SELECT count(*) AS n FROM tzh WHERE ts = " + ofs, "0A000", ambiguous},
		{"SELECT count(*) AS n FROM tzg WHERE ts = " + ofs, "0A000", ambiguous},
		{"SELECT count(*) AS n FROM (SELECT ts FROM tzs) q WHERE ts BETWEEN " + ofs + " AND '2024-07-02 00:00:00'", "0A000", ambiguous},
		{"SELECT count(*) AS n FROM vld WHERE " + tz + " = sd", "42883", "operator does not exist: timestamp with time zone = text"},
		{"SELECT date_part('timezone', TIMESTAMP '2024-07-01 12:00:00') AS n FROM vld WHERE id = 1", "0A000", `unit "timezone" not supported for type timestamp without time zone`},
	} {
		var got []string
		_, err := db.Query(ctx, c.sql)
		if sqlerr.StateOf(err) != c.state || !strings.Contains(fmt.Sprint(err), c.sentence) {
			got = append(got, fmt.Sprintf("embedded: %v", err))
		}
		for name, base := range map[string]string{"http/local": local.URL, "http/distributed": dist.URL} {
			if _, e, s := post(base, c.sql); s != c.state || !strings.Contains(e, c.sentence) {
				got = append(got, fmt.Sprintf("%s: %q %q", name, s, e))
			}
		}
		for name, g := range map[string]*GRPCServer{"grpc/embedded": gEmbedded, "grpc/distributed": gDist} {
			if _, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: c.sql}); err == nil || !strings.Contains(err.Error(), c.sentence) {
				got = append(got, fmt.Sprintf("%s: %v", name, err))
			}
		}
		for name, conn := range conns {
			_, err := conn.Exec(ctx, c.sql).ReadAll()
			if pgState(err) != c.state || !strings.Contains(fmt.Sprint(err), c.sentence) {
				got = append(got, fmt.Sprintf("%s/simple: %v", name, err))
			}
			r := conn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
			if pgState(r.Err) != c.state || !strings.Contains(fmt.Sprint(r.Err), c.sentence) {
				got = append(got, fmt.Sprintf("%s/extended: %v", name, r.Err))
			}
		}
		if len(got) > 0 {
			t.Errorf("%s:\n    %s", c.sql, strings.Join(got, "\n    "))
		}
	}
	// DML: the same refusal for a predicate and an assignment, before any
	// row is written (embedded and pgwire, the doors that run DML here).
	for _, s := range []string{
		"UPDATE tzs SET id = 2 WHERE ts = " + ofs,
		"UPDATE tzs SET ts = " + ofs + " WHERE id = 1",
		"DELETE FROM tzs WHERE ts = " + ofs,
		"INSERT INTO tzs VALUES (3, " + ofs + ")",
		"INSERT INTO tzs SELECT 4, " + ofs,
	} {
		if _, err := db.Query(ctx, s); sqlerr.StateOf(err) != "0A000" || !strings.Contains(fmt.Sprint(err), ambiguous) {
			t.Errorf("embedded %s: %v, want the 0A000 refusal", s, err)
		}
		if _, err := conns["pgwire/single"].Exec(ctx, s).ReadAll(); pgState(err) != "0A000" || !strings.Contains(fmt.Sprint(err), ambiguous) {
			t.Errorf("pgwire/single %s: %v, want the 0A000 refusal", s, err)
		}
	}
	if res, err := db.Query(ctx, "SELECT CAST(count(*) AS TEXT) AS r FROM tzs"); err != nil || fmt.Sprint(res.Rows[0]["r"]) != "1" {
		t.Errorf("tzs after the refused DML: %v %v, want the one row it had", res, err)
	}
	// The session zone: pgwire refuses a non-UTC SET on both protocols and
	// keeps answering at UTC; UTC itself is accepted; the declared type of a
	// timestamptz value is timestamp (1114), printed with no offset.
	probe := "SELECT TIMESTAMPTZ '2024-03-10 02:30:00' AS r"
	for name, conn := range conns {
		for _, set := range []string{"SET TimeZone = 'America/New_York'", "SET TIME ZONE 'Asia/Kolkata'"} {
			if _, err := conn.Exec(ctx, set).ReadAll(); pgState(err) != "0A000" {
				t.Errorf("%s/simple %s: %v, want 0A000", name, set, err)
			}
			if r := conn.ExecParams(ctx, set, nil, nil, nil, nil).Read(); pgState(r.Err) != "0A000" {
				t.Errorf("%s/extended %s: %v, want 0A000", name, set, r.Err)
			}
		}
		if _, err := conn.Exec(ctx, "SET TimeZone = 'UTC'").ReadAll(); err != nil {
			t.Errorf("%s: SET TimeZone = 'UTC': %v", name, err)
		}
		show := conn.ExecParams(ctx, "SHOW TimeZone", nil, nil, nil, nil).Read()
		if show.Err != nil || len(show.Rows) != 1 || string(show.Rows[0][0]) != "UTC" {
			t.Errorf("%s: SHOW TimeZone = %v %v, want UTC", name, show.Rows, show.Err)
		}
		r := conn.ExecParams(ctx, probe, nil, nil, nil, nil).Read()
		if r.Err != nil || len(r.Rows) != 1 || len(r.FieldDescriptions) != 1 {
			t.Errorf("%s: %s: %v", name, probe, r.Err)
			continue
		}
		if oid := r.FieldDescriptions[0].DataTypeOID; oid != 1114 || string(r.Rows[0][0]) != "2024-03-10 02:30:00" {
			t.Errorf("%s: %s = %q OID %d; pinned: 2024-03-10 02:30:00 OID 1114 (PostgreSQL under UTC: "+
				"2024-03-10 02:30:00+00, OID 1184 — the documented divergence moved)", name, probe, r.Rows[0][0], oid)
		}
	}
	// No other door has a session: SET is not a statement there.
	set := "SET TimeZone = 'America/New_York'"
	if _, err := db.Query(ctx, set); err == nil {
		t.Errorf("embedded: %s accepted", set)
	}
	for name, base := range map[string]string{"http/local": local.URL, "http/distributed": dist.URL} {
		if _, e, _ := post(base, set); e == "" {
			t.Errorf("%s: %s accepted", name, set)
		}
	}
	for name, g := range map[string]*GRPCServer{"grpc/embedded": gEmbedded, "grpc/distributed": gDist} {
		if _, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: set}); err == nil {
			t.Errorf("%s: %s accepted", name, set)
		}
	}
}
