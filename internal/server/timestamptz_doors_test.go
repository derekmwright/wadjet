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
// every door) and refuses a comparison of a timestamptz with a quoted literal
// spelling a nonzero offset by name. pgwire, the only door with a session,
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

	// Values: PostgreSQL 17.11 under UTC, as text (the timestamptz's `+00`
	// is the divergence; the instant is the answer).
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
	} {
		sql := "SELECT CAST(" + c.expr + " AS TEXT) AS r FROM vld WHERE id = 1"
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

	// The comparison refusal, by its identity on every door.
	refusal := "SELECT count(*) AS n FROM vld WHERE TIMESTAMPTZ '2024-07-01 12:00:00+00' = '2024-07-01 17:30:00+05:30'"
	const sentence = "comparing a timestamptz with a quoted literal that carries a time zone offset is not supported"
	{
		var got []string
		_, err := db.Query(ctx, refusal)
		if sqlerr.StateOf(err) != "0A000" || sqlerr.ReasonOf(err) != "timestamptz-zoned-literal" {
			got = append(got, fmt.Sprintf("embedded: %v", err))
		}
		for name, base := range map[string]string{"http/local": local.URL, "http/distributed": dist.URL} {
			if _, e, s := post(base, refusal); s != "0A000" || !strings.Contains(e, sentence) {
				got = append(got, fmt.Sprintf("%s: %q %q", name, s, e))
			}
		}
		for name, g := range map[string]*GRPCServer{"grpc/embedded": gEmbedded, "grpc/distributed": gDist} {
			if _, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: refusal}); err == nil || !strings.Contains(err.Error(), sentence) {
				got = append(got, fmt.Sprintf("%s: %v", name, err))
			}
		}
		for name, conn := range conns {
			_, err := conn.Exec(ctx, refusal).ReadAll()
			if pgState(err) != "0A000" || !strings.Contains(fmt.Sprint(err), sentence) {
				got = append(got, fmt.Sprintf("%s/simple: %v", name, err))
			}
			r := conn.ExecParams(ctx, refusal, nil, nil, nil, nil).Read()
			if pgState(r.Err) != "0A000" || !strings.Contains(fmt.Sprint(r.Err), sentence) {
				got = append(got, fmt.Sprintf("%s/extended: %v", name, r.Err))
			}
		}
		if len(got) > 0 {
			t.Errorf("%s:\n    %s", refusal, strings.Join(got, "\n    "))
		}
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
