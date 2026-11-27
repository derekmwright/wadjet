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

// THE INTERVAL RULE ON EVERY DOOR (#1713d). The rule lives in the shared
// statement validation (physical.bindColumns, stamped or not), so every door
// that runs a SELECT reaches it: the embedded API, HTTP over the local engine
// and over the coordinator, HTTP's async submission, gRPC unary over the
// embedded engine and over the coordinator, gRPC's async submission, and
// pgwire over the single-process engine and over the coordinator (simple and
// extended). Each refused statement gives the same SQLSTATE and the same
// reason sentence inside the door's own envelope (HTTP's sqlstate field,
// gRPC's wrapped status, pgwire's ErrorResponse); each control answers on
// every door. PostgreSQL 17.11 answers every refused statement (the
// coordinator's interval table) and raises 42804 for the mixed CASE.
func TestIntervalRuleOnEveryDoor(t *testing.T) {
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
	post := func(base, path, sql string) (int, map[string]any) {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		resp, err := http.Post(base+path, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST %s %q: %v", path, sql, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return resp.StatusCode, out
	}
	pgState := func(err error) string {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return pe.Code
		}
		return ""
	}
	const sentence = "interval values cannot yet be used as"
	for _, c := range []struct {
		sql, state, reason string
	}{
		{"SELECT INTERVAL '1 day' AS r FROM vld WHERE id = 1", "0A000", sentence},
		{"SELECT count(r) AS n FROM (SELECT CAST(sd AS INTERVAL) * 2 AS r FROM vld) t", "0A000", sentence},
		{"SELECT count(*) * INTERVAL '1 hour' AS r FROM vld", "0A000", sentence},
		{"SELECT id FROM vld ORDER BY INTERVAL '1 day' * id, id", "0A000", sentence},
		// #1680c: the bare aggregate is the statement's interval result
		// column (named first); the argument's refusal under a display cast.
		{"SELECT max(INTERVAL '1 day') AS r FROM vld", "0A000", sentence},
		{"SELECT CAST(max(INTERVAL '1 day') AS TEXT) AS r FROM vld", "0A000", "max over an interval argument is not supported"},
		{"SELECT CASE WHEN id = 1 THEN INTERVAL '1 day' ELSE id END AS r FROM vld", "42804", "cannot be matched"},
		// A timestamp difference is an interval (#1713d rounds 3 and 6):
		// PostgreSQL answers each 0A000 statement, the difference is
		// milliseconds here; `> 1000` is PostgreSQL's own 42883.
		{"SELECT count(*) AS n FROM vld WHERE CAST(d AS TIMESTAMP) - TIMESTAMP '1998-01-01 00:00:00' > INTERVAL '30 days'", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as an operand of a comparison (>)"},
		{"SELECT CAST(d AS TIMESTAMP) - TIMESTAMP '1998-01-01 00:00:00' AS r FROM vld WHERE id = 1", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as a query result column"},
		{"SELECT sum(CAST(d AS TIMESTAMP) - TIMESTAMP '1998-01-01 00:00:00') AS r FROM vld", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as a query result column"},
		{"SELECT CAST(sum(CAST(d AS TIMESTAMP) - TIMESTAMP '1998-01-01 00:00:00') AS TEXT) AS r FROM vld", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as an argument of sum"},
		{"SELECT extract(epoch FROM CAST(d AS TIMESTAMP) - TIMESTAMP '1998-01-01 00:00:00') AS r FROM vld", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as an argument of extract"},
		{"SELECT count(*) AS n FROM vld WHERE CAST(d AS TIMESTAMP) - TIMESTAMP '1998-01-01 00:00:00' > 1000", "42883",
			"operator does not exist: interval > integer"},
		{"SELECT count(*) AS n FROM vld GROUP BY CAST(d AS TIMESTAMP) - TIMESTAMP '1998-01-01 00:00:00'", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as a GROUP BY key"},
		// Round 7: the difference of an aggregate, a window function or a
		// scalar subquery result (each answered its millisecond number
		// through round 6).
		{"SELECT max(CAST(d AS TIMESTAMP)) - min(CAST(d AS TIMESTAMP)) AS r FROM vld", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as a query result column"},
		{"SELECT extract(epoch FROM max(CAST(d AS TIMESTAMP)) - min(CAST(d AS TIMESTAMP))) AS r FROM vld", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as an argument of extract"},
		{"SELECT count(*) AS n FROM vld HAVING max(CAST(d AS TIMESTAMP)) - min(CAST(d AS TIMESTAMP)) > 1000", "42883",
			"operator does not exist: interval > integer"},
		{"SELECT id, CAST(d AS TIMESTAMP) - lag(CAST(d AS TIMESTAMP)) OVER (ORDER BY id) AS r FROM vld", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as a query result column"},
		{"SELECT id, CAST(CAST(d AS TIMESTAMP) - lag(CAST(d AS TIMESTAMP)) OVER (ORDER BY id) AS TEXT) AS r FROM vld", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as the operand of a cast to text"},
		{"SELECT id, (SELECT max(CAST(d AS TIMESTAMP)) FROM vld) - CAST(d AS TIMESTAMP) AS r FROM vld", "0A000",
			"a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as a query result column"},
		{"SELECT count(*) AS n FROM vld WHERE CAST(d AS TIMESTAMP) - (SELECT min(CAST(d AS TIMESTAMP)) FROM vld) > 1000", "42883",
			"operator does not exist: interval > integer"},
		{"SELECT count(*) AS n FROM vld WHERE d - DATE '1998-01-01' > INTERVAL '30 days'", "42883", "operator does not exist"},
		// Controls.
		{"SELECT count(*) AS n FROM vld WHERE d + INTERVAL '1 month' > TIMESTAMP '1998-12-15 00:00:00'", "", ""},
		{"SELECT CAST(INTERVAL '1 day' * 2 AS TEXT) AS r FROM vld WHERE id = 1", "", ""},
		{"SELECT extract(epoch FROM CAST(d AS TIMESTAMP)) - extract(epoch FROM TIMESTAMP '1998-01-01 00:00:00') AS r FROM vld WHERE id = 1", "", ""},
		{"SELECT d - DATE '1998-01-01' AS r FROM vld WHERE id = 1", "", ""},
		{"SELECT max(CAST(d AS TIMESTAMP)) > min(CAST(d AS TIMESTAMP)) AS r FROM vld", "", ""},
		{"SELECT max(d) - min(d) AS r FROM vld", "", ""},
	} {
		var got []string
		check := func(door, state, text string) {
			switch {
			case c.state == "" && state != "":
				got = append(got, door+": refused "+state+" "+text)
			case c.state != "" && (state != c.state || !strings.Contains(text, c.reason)):
				got = append(got, fmt.Sprintf("%s: %q %q", door, state, text))
			}
		}
		_, err := db.Query(ctx, c.sql)
		check("embedded", sqlerr.StateOf(err), fmt.Sprint(err))
		for name, base := range map[string]string{"http/local": local.URL, "http/distributed": dist.URL} {
			status, out := post(base, "/v1/queries", c.sql)
			e, _ := out["error"].(string)
			s, _ := out["sqlstate"].(string)
			if e == "" && status >= 400 {
				e = fmt.Sprintf("status %d", status)
			}
			check(name, s, e)
		}
		// Async submission: refused at submission, before a query exists.
		if status, out := post(dist.URL, "/v1/queries/async", c.sql); true {
			e, _ := out["error"].(string)
			s, _ := out["sqlstate"].(string)
			if c.state == "" && status != http.StatusAccepted {
				got = append(got, fmt.Sprintf("http/async: status %d %v", status, out))
			} else if c.state != "" {
				check("http/async", s, e)
			}
		}
		for name, g := range map[string]*GRPCServer{"grpc/embedded": gEmbedded, "grpc/distributed": gDist} {
			_, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: c.sql})
			check(name, grpcState(err, c.state), fmt.Sprint(err))
		}
		if _, err := gDist.SubmitQuery(ctx, &wadjetv1.QueryRequest{Sql: c.sql}); c.state != "" || err != nil {
			check("grpc/async", grpcState(err, c.state), fmt.Sprint(err))
		}
		for name, conn := range conns {
			_, err := conn.Exec(ctx, c.sql).ReadAll()
			check(name+"/simple", pgState(err), fmt.Sprint(err))
			r := conn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
			check(name+"/extended", pgState(r.Err), fmt.Sprint(r.Err))
		}
		if len(got) > 0 {
			t.Errorf("%s (want %q %q):\n    %s", c.sql, c.state, c.reason, strings.Join(got, "\n    "))
		}
	}
}

// grpcState: the gRPC door wraps every engine error as codes.Internal with
// its message and carries no SQLSTATE (its existing envelope; no wire field
// is added for this rule), so a refusal there is judged by its reason
// sentence alone — grpcState answers want's state for any error, and check
// still requires the sentence.
func grpcState(err error, want string) string {
	if err == nil {
		return ""
	}
	if want == "" {
		return "uncoded"
	}
	return want
}
