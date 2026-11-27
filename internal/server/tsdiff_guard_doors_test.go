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
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/wadjet"
)

// tgWrite is the reviewers' p_t / p_s fixture (1713d_r6_r7_reviews.md).
func tgWrite(t *testing.T, ctx context.Context, db *wadjet.DB) {
	t.Helper()
	for _, s := range []string{
		"CREATE TABLE tg_t (id BIGINT, ts TIMESTAMP, ts2 TIMESTAMP, d DATE, s TEXT)",
		"INSERT INTO tg_t VALUES (1, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 03:00:00', DATE '2024-01-31', NULL), " +
			"(2, TIMESTAMP '2024-01-01 00:00:00.5', TIMESTAMP '2024-01-01 00:00:01', DATE '2024-02-29', NULL), " +
			"(3, NULL, TIMESTAMP '2024-01-01 00:00:00', DATE '2023-12-31', NULL)",
		"CREATE TABLE tg_s (id BIGINT, s TEXT)",
		"INSERT INTO tg_s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
	} {
		if _, err := db.Query(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// TestTimestampDifferenceGuardOnEveryDoor (#1713d round 8): the statements
// the round-6 and round-7 reviews found still answering, storing or acting
// on a timestamp difference's millisecond number, on every door, twice —
// with the statement rule (PostgreSQL 17.11's diagnostic where PostgreSQL
// refuses: 42883 for a comparison with a number; else the rule's 0A000) and
// with the rule disabled (physical.DisableTimestampDifferenceRuleForTest),
// where the expression guard alone must refuse each with its own 0A000. No
// write lands on either pass. PostgreSQL 17.11 answers each read with an
// interval, stores '1 day 03:00:00' text for the MERGE SET writes and raises
// 42883 for the others (1713d_r8.md).
func TestTimestampDifferenceGuardOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	single, dag, coord, db := owaDoorsOver(t, ctx, tgWrite)
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
	const (
		rule   = "a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as "
		guard  = "a timestamp difference (timestamp - timestamp) cannot yet be computed"
		number = "operator does not exist: interval > integer"
	)
	type cell struct {
		sql, state, reason string
		write              bool
	}
	cells := []cell{
		// Round 7 review, item 2 (reads).
		{"SELECT id, (SELECT tg_t.ts2 FROM tg_s LIMIT 1) - ts AS r FROM tg_t", "0A000", rule + "a query result column", false},
		{"SELECT id, CAST((SELECT tg_t.ts2 FROM tg_s LIMIT 1) - ts AS TEXT) AS r FROM tg_t", "0A000", rule + "the operand of a cast to text", false},
		{"SELECT id, extract(epoch FROM (SELECT tg_t.ts2 FROM tg_s LIMIT 1) - ts) AS r FROM tg_t", "0A000", rule + "an argument of extract", false},
		{"SELECT id, ts2 - (SELECT * FROM (SELECT max(ts) FROM tg_t) z) AS r FROM tg_t", "0A000", rule + "a query result column", false},
		{"SELECT id, ts2 - (SELECT * FROM (SELECT ts FROM tg_t WHERE id = 1) z) AS r FROM tg_t", "0A000", rule + "a query result column", false},
		{"SELECT z.* FROM (SELECT ts2 - (SELECT * FROM (SELECT max(ts) FROM tg_t) w) AS k FROM tg_t) z", "0A000", rule + "a query result column", false},
		// Round 7 review, item 1 (writes).
		{"MERGE INTO tg_s t USING tg_t src ON t.id = src.id WHEN MATCHED THEN UPDATE SET s = CAST((SELECT max(ts2) FROM tg_t) - src.ts AS TEXT)", "0A000", rule + "the operand of a cast to text", true},
		{"MERGE INTO tg_s t USING tg_t src ON t.id = src.id WHEN MATCHED THEN UPDATE SET s = CAST((SELECT ts FROM tg_t WHERE id = 1) - src.ts AS TEXT)", "0A000", rule + "the operand of a cast to text", true},
		{"UPDATE tg_t SET s = 'x' WHERE ts2 - (SELECT min(ts) FROM tg_t) > 1000", "42883", number, true},
		{"DELETE FROM tg_s WHERE id = 3 AND (SELECT max(ts2) FROM tg_t) - (SELECT min(ts) FROM tg_t) > 1000", "42883", number, true},
		{"MERGE INTO tg_s t USING tg_t src ON t.id = src.id WHEN MATCHED AND (SELECT max(ts2) FROM tg_t) - src.ts > 1000 THEN UPDATE SET s = 'x'", "42883", number, true},
		// Round 6 review and the plain forms.
		{"SELECT max(ts2) - min(ts) AS r FROM tg_t", "0A000", rule + "a query result column", false},
		{"SELECT id, ts2 - lag(ts2) OVER (ORDER BY id) AS r FROM tg_t", "0A000", rule + "a query result column", false},
		{"SELECT id, lag(NULL, 1, ts2) OVER (ORDER BY id) - ts AS r FROM tg_t", "0A000", rule + "a query result column", false},
		{"SELECT id, ts2 - ts AS r FROM tg_t", "0A000", rule + "a query result column", false},
		{"SELECT TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00' AS r", "0A000", rule + "a query result column", false},
		{"UPDATE tg_t SET s = CAST(ts2 - ts AS TEXT)", "0A000", rule + "the operand of a cast to text", true},
		// Controls.
		{"SELECT id, CAST(ts + INTERVAL '1 day' AS TEXT) AS r FROM tg_t", "", "", false},
		{"SELECT id, d - DATE '2024-01-01' AS r FROM tg_t", "", "", false},
		{"SELECT id, (ts2 - ts) IS NULL AS r FROM tg_t", "", "", false},
	}
	for _, mode := range []string{"rule", "guard"} {
		restore := func() {}
		if mode == "guard" {
			restore = physical.DisableTimestampDifferenceRuleForTest()
		}
		for _, c := range cells {
			state, reason := c.state, c.reason
			if mode == "guard" && state != "" {
				state, reason = "0A000", guard
				// The window function's default argument is declared STRING
				// by the window evaluator, so the guard sees no TIMESTAMP:
				// with the rule off this answers NULL text, no number
				// (1713d_r8.md, unresolved).
				if strings.Contains(c.sql, "lag(NULL") {
					continue
				}
			}
			var got []string
			check := func(door, st, text string) {
				switch {
				case state == "" && st != "":
					got = append(got, door+": refused "+st+" "+text)
				case state != "" && (st != state || !strings.Contains(text, reason)):
					got = append(got, fmt.Sprintf("%s: %q %q", door, st, text))
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
			if !c.write && state != "" {
				_, out := post(dist.URL, "/v1/queries/async", c.sql)
				e, _ := out["error"].(string)
				s, _ := out["sqlstate"].(string)
				if mode == "rule" {
					check("http/async", s, e)
				} else if s != "" && s != "0A000" {
					// Guard pass: the async door validates at submission and
					// executes later; a refusal at submission must not be
					// another code.
					got = append(got, fmt.Sprintf("http/async: %q %q", s, e))
				}
			}
			for name, g := range map[string]*GRPCServer{"grpc/embedded": gEmbedded, "grpc/distributed": gDist} {
				if c.write && name == "grpc/distributed" {
					continue // that door runs no DML ("UPDATE is not supported")
				}
				_, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: c.sql})
				check(name, grpcState(err, state), fmt.Sprint(err))
			}
			for name, conn := range conns {
				_, err := conn.Exec(ctx, c.sql).ReadAll()
				check(name+"/simple", pgState(err), fmt.Sprint(err))
				r := conn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
				check(name+"/extended", pgState(r.Err), fmt.Sprint(r.Err))
			}
			if len(got) > 0 {
				t.Errorf("[%s] %s (want %q %q):\n    %s", mode, c.sql, state, reason, strings.Join(got, "\n    "))
			}
		}
		restore()
	}
	// No write landed on any door, on either pass.
	for q, want := range map[string]string{
		"SELECT id, s FROM tg_s ORDER BY id": "[1 a]; [2 b]; [3 c]",
		"SELECT id, s FROM tg_t ORDER BY id": "[1 <nil>]; [2 <nil>]; [3 <nil>]",
	} {
		res, err := db.Query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		for i := range res.Rows {
			rows = append(rows, fmt.Sprint(res.Cells(i)))
		}
		if got := strings.Join(rows, "; "); got != want {
			t.Errorf("a refused write landed: %s\n  got  %s\n  want %s", q, got, want)
		}
	}
}
