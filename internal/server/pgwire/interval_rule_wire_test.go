// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// The INTERVAL rule over pgwire (#1713d): simple and extended protocol, a
// parameter typed interval (OID 1186) in text, binary and NULL, and Describe.
// A parameter typed interval stays an INTERVAL (CAST(... AS INTERVAL)) so the
// plan's rule sees it: consumed inline it answers PostgreSQL's value, as a
// result column it is refused 0A000 with the same sentence the other doors
// carry. Binary interval parameters are refused 0A000 (no whole-second
// carrier for microseconds; the text format answers). `CAST($1 AS INTERVAL)`
// is inferred as interval (1186), not DURATION's bigint. PostgreSQL 17.11
// answers every refused statement (the coordinator's interval table).
func TestIntervalRuleOverTheWire(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "ivwire"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())
	for _, s := range []string{"CREATE TABLE iw (id BIGINT, ts TIMESTAMP, s TEXT)",
		"INSERT INTO iw VALUES (1, TIMESTAMP '2024-01-31 10:00:00', '1 day'), (2, TIMESTAMP '2024-02-29 00:00:00', '30 days')"} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	state := func(err error) string {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return pe.Code
		}
		if err != nil {
			return "uncoded: " + err.Error()
		}
		return ""
	}
	const oidInterval = 1186
	// Simple protocol.
	for _, c := range []struct{ sql, state, want string }{
		{"SELECT INTERVAL '1 day' AS a", "0A000", ""},
		{"SELECT CAST(INTERVAL '1 day' * 2 AS TEXT) AS a", "", "2 days"},
		{"SELECT id FROM iw WHERE CAST(s AS INTERVAL) = INTERVAL '24 hours'", "", "1"},
		{"SELECT CAST(ts + INTERVAL '1 month' AS TEXT) AS a FROM iw WHERE id = 1", "", "2024-02-29 10:00:00"},
		{"SELECT count(*) AS n FROM iw GROUP BY CAST(s AS INTERVAL)", "0A000", ""},
	} {
		res, err := conn.Exec(ctx, c.sql).ReadAll()
		if got := state(err); got != c.state {
			t.Errorf("simple %s: SQLSTATE %q (%v), want %q", c.sql, got, err, c.state)
			continue
		}
		if err == nil && (len(res) == 0 || len(res[0].Rows) == 0 || string(res[0].Rows[0][0]) != c.want) {
			t.Errorf("simple %s: answered %v, want %q", c.sql, res, c.want)
		}
		if err != nil && !strings.Contains(err.Error(), "interval values cannot yet be used as") {
			t.Errorf("simple %s: %v does not carry the rule's sentence", c.sql, err)
		}
	}
	// Extended protocol, parameters typed interval.
	for _, c := range []struct {
		name, sql string
		val       []byte
		format    int16
		state     string
		want      string
	}{
		{"text/shift", "SELECT CAST(ts + $1 AS TEXT) AS a FROM iw WHERE id = 1", []byte("1 month"), 0, "", "2024-02-29 10:00:00"},
		{"text/compare", "SELECT id FROM iw WHERE CAST(s AS INTERVAL) = $1", []byte("24 hours"), 0, "", "1"},
		{"text/publish", "SELECT $1 AS a", []byte("1 day"), 0, "0A000", ""},
		{"text/bad", "SELECT id FROM iw WHERE CAST(s AS INTERVAL) = $1", []byte("bogus"), 0, "22007", ""},
		{"null/publish", "SELECT $1 AS a", nil, 0, "0A000", ""},
		{"null/compare", "SELECT count(*) AS n FROM iw WHERE CAST(s AS INTERVAL) = $1", nil, 0, "", "0"},
		{"binary", "SELECT id FROM iw WHERE CAST(s AS INTERVAL) = $1", make([]byte, 16), 1, "0A000", ""},
	} {
		res := conn.ExecParams(ctx, c.sql, [][]byte{c.val}, []uint32{oidInterval}, []int16{c.format}, nil).Read()
		if got := state(res.Err); got != c.state {
			t.Errorf("extended %s: SQLSTATE %q (%v), want %q", c.name, got, res.Err, c.state)
			continue
		}
		if res.Err == nil && (len(res.Rows) == 0 || string(res.Rows[0][0]) != c.want) {
			t.Errorf("extended %s: answered %v, want %q", c.name, res.Rows, c.want)
		}
	}
	// Describe: the parameter of CAST($1 AS INTERVAL) is an interval, and a
	// statement that would publish one is refused at Describe.
	sd, err := conn.Prepare(ctx, "iv1", "SELECT id FROM iw WHERE CAST($1 AS INTERVAL) > INTERVAL '1 day'", nil)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if len(sd.ParamOIDs) != 1 || sd.ParamOIDs[0] != oidInterval {
		t.Errorf("CAST($1 AS INTERVAL) parameter OIDs %v, want [1186]", sd.ParamOIDs)
	}
	// Describe of a statement with an unbound parameter does not plan it
	// here (its RowDescription waits for the bound statement), so the
	// refusal is the Execute's, as for every parameterized statement.
	if _, err := conn.Prepare(ctx, "iv2", "SELECT CAST($1 AS INTERVAL) AS a", nil); err != nil && state(err) != "0A000" {
		t.Errorf("Describe of a published interval: %v, want 0A000 or a deferred plan", err)
	}
	res := conn.ExecPrepared(ctx, "iv2", [][]byte{[]byte("1 day")}, []int16{0}, nil).Read()
	if state(res.Err) != "0A000" {
		t.Errorf("Execute of a published interval: %v, want 0A000", res.Err)
	}
}

// A timestamp difference over pgwire (#1713d round 6): `timestamp -
// timestamp` is an interval on PostgreSQL 17.11 and a millisecond number
// here, so no statement on either protocol, with a parameter typed timestamp
// (OID 1114, text, binary, NULL), untyped (OID 0) or none, and no write
// through the wire, may answer from that number. PostgreSQL answers the
// published and compared forms (`SELECT $1 - ts` is `2 days` there) and
// refuses `> 1000` (42883) and the bigint write (42804).
func TestTimestampDifferenceOverTheWire(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "tdwire"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())
	for _, s := range []string{"CREATE TABLE tw (id BIGINT, ts TIMESTAMP, ts2 TIMESTAMP, n BIGINT, s TEXT)",
		"INSERT INTO tw VALUES (1, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 03:00:00', NULL, NULL)"} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	state := func(err error) string {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return pe.Code
		}
		if err != nil {
			return "uncoded: " + err.Error()
		}
		return ""
	}
	const sentence = "a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as"
	for _, c := range []struct{ sql, state string }{
		{"SELECT ts2 - ts AS d FROM tw", "0A000"},
		{"SELECT sum(ts2 - ts) AS d FROM tw", "0A000"},
		{"SELECT extract(epoch FROM ts2 - ts) AS d FROM tw", "0A000"},
		{"SELECT id FROM tw WHERE ts2 - ts > INTERVAL '1 hour'", "0A000"},
		{"SELECT id FROM tw WHERE ts2 - ts > 1000", "42883"},
		{"INSERT INTO tw (id, s) SELECT 2, ts2 - ts FROM tw", "0A000"},
		{"UPDATE tw SET n = TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'", "42804"},
		{"UPDATE tw SET s = TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'", "0A000"},
		{"CREATE TABLE tw2 AS SELECT ts2 - ts AS d FROM tw", "0A000"},
		// Round 7: an aggregate's, a window function's or a scalar
		// subquery's timestamp.
		{"SELECT max(ts2) - min(ts) AS d FROM tw", "0A000"},
		{"SELECT extract(epoch FROM max(ts2) - min(ts)) AS d FROM tw", "0A000"},
		{"SELECT count(*) AS n FROM tw HAVING max(ts2) - min(ts) > 1000", "42883"},
		{"SELECT id, ts2 - lag(ts2) OVER (ORDER BY id) AS d FROM tw", "0A000"},
		{"SELECT id, (SELECT max(ts2) FROM tw) - ts AS d FROM tw", "0A000"},
		{"CREATE TABLE tw3 AS SELECT max(ts2) - min(ts) AS d FROM tw", "0A000"},
		// Controls.
		{"SELECT max(ts2) > min(ts) AS b FROM tw", ""},
		{"SELECT extract(epoch FROM ts2) - extract(epoch FROM ts) AS d FROM tw", ""},
		{"SELECT id FROM tw WHERE ts2 > ts + INTERVAL '1 hour'", ""},
	} {
		for _, proto := range []string{"simple", "extended"} {
			var err error
			if proto == "simple" {
				_, err = conn.Exec(ctx, c.sql).ReadAll()
			} else {
				err = conn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read().Err
			}
			if got := state(err); got != c.state {
				t.Errorf("%s %s: SQLSTATE %q (%v), want %q", proto, c.sql, got, err, c.state)
			} else if c.state == "0A000" && !strings.Contains(err.Error(), sentence) {
				t.Errorf("%s %s: %v does not name the timestamp difference", proto, c.sql, err)
			}
		}
	}
	// Parameters: typed timestamp (text, binary, NULL) and untyped text.
	const oidTimestamp = 1114
	bin := make([]byte, 8) // 2000-01-01 00:00:00, PostgreSQL's binary epoch
	for _, c := range []struct {
		name, sql string
		val       []byte
		oid       uint32
		format    int16
		state     string
	}{
		{"text/publish", "SELECT $1 - ts AS v FROM tw", []byte("2024-01-03 00:00:00"), oidTimestamp, 0, "0A000"},
		{"text/number", "SELECT id FROM tw WHERE ts2 - $1 > 1000", []byte("2024-01-01 00:00:00"), oidTimestamp, 0, "42883"},
		{"text/compare", "SELECT id FROM tw WHERE ts2 - $1 > INTERVAL '1 hour'", []byte("2024-01-01 00:00:00"), oidTimestamp, 0, "0A000"},
		{"binary/publish", "SELECT ts - $1 AS v FROM tw", bin, oidTimestamp, 1, "0A000"},
		{"null/publish", "SELECT ts - $1 AS v FROM tw", nil, oidTimestamp, 0, "0A000"},
		{"untyped/publish", "SELECT ts2 - $1 AS v FROM tw", []byte("2024-01-01 00:00:00"), 0, 0, "0A000"},
		{"text/control", "SELECT id FROM tw WHERE ts2 > $1", []byte("2024-01-01 00:00:00"), oidTimestamp, 0, ""},
	} {
		res := conn.ExecParams(ctx, c.sql, [][]byte{c.val}, []uint32{c.oid}, []int16{c.format}, nil).Read()
		if got := state(res.Err); got != c.state {
			t.Errorf("extended %s: SQLSTATE %q (%v) rows %q, want %q", c.name, got, res.Err, res.Rows, c.state)
		}
	}
	// Prepared once, executed: refused at Execute as at every door.
	if _, err := conn.Prepare(ctx, "td1", "SELECT ts2 - ts AS d FROM tw", nil); err != nil && state(err) != "0A000" {
		t.Errorf("Describe of a published difference: %v, want 0A000 or a deferred plan", err)
	} else if err == nil {
		if res := conn.ExecPrepared(ctx, "td1", nil, nil, nil).Read(); state(res.Err) != "0A000" {
			t.Errorf("Execute of a published difference: %v rows %q, want 0A000", res.Err, res.Rows)
		}
	}
}
