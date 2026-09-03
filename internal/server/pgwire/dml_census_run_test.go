package pgwire

// The census runner: the fixture, the three doors, and the digest that makes
// their answers comparable. See dml_census_test.go for the corpus itself.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// censusFixture is the dossier's fixture, created through SQL so all three
// doors see byte-identical tables:
//
//	arcb_pr  = 1:10:a | 2:20:b | 3:30:c
//	arcb_src = (1,100,'x'), (4,400,'y')
//	arcb_dup = (2,100,'p'), (2,200,'q')   — two rows matching one arcb_pr row
//	arcb_fl  = f ∈ {2.5,-2.5,0.5,3.5,1.5}, n = 0
//	arcb_ts  = 1 → 2000-01-01 00:00:00
var censusFixture = []string{
	"CREATE TABLE arcb_pr (id INT64 NOT NULL, n INT64, name STRING)",
	"CREATE TABLE arcb_src (id INT64, n INT64, name STRING)",
	"CREATE TABLE arcb_dup (id INT64, n INT64, name STRING)",
	"CREATE TABLE arcb_fl (id INT64, f FLOAT64, n INT64)",
	"CREATE TABLE arcb_ts (id INT64, t TIMESTAMP)",
	// The column families the corpus could not reach — and B5 lived in exactly
	// that gap: `DELETE FROM t WHERE ts > 5` EMPTIED a TIMESTAMP table, and so
	// did the BOOL and IPv4 spellings, where PostgreSQL raises 42883. No cell
	// could have seen it because no fixture had one of those columns.
	"CREATE TABLE arcb_mix (id INT64, ts TIMESTAMP, flag BOOL, ip IPV4, raw BYTES, d DECIMAL(9,2))",
	// An EMPTY table. A plan-time refusal is invisible on a populated one:
	// the runtime raises on the first row either way, so "the statement was
	// refused before it read anything" needs a table with nothing to read.
	"CREATE TABLE arcb_empty (id INT64, n INT64, name STRING)",
}

var censusSeed = []string{
	"INSERT INTO arcb_pr (id, n, name) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')",
	"INSERT INTO arcb_src (id, n, name) VALUES (1, 100, 'x'), (4, 400, 'y')",
	"INSERT INTO arcb_dup (id, n, name) VALUES (2, 100, 'p'), (2, 200, 'q')",
	"INSERT INTO arcb_fl (id, f, n) VALUES (1, 2.5, 0), (2, -2.5, 0), (3, 0.5, 0), (4, 3.5, 0), (5, 1.5, 0)",
	"INSERT INTO arcb_ts (id, t) VALUES (1, '2000-01-01T00:00:00Z')",
	"INSERT INTO arcb_mix (id, ts, flag, ip, raw, d) VALUES (1, '2020-01-01T00:00:00Z', true, '10.0.0.1', 'ab', 1.50), (2, '2021-01-01T00:00:00Z', false, '10.0.0.2', 'cd', 2.50)",
}

// censusDigestSQL reads a fixture table back in a fixed column order.
var censusDigestSQL = map[string]string{
	"pr": "SELECT id, n, name FROM arcb_pr ORDER BY id",
	"fl": "SELECT id, f, n FROM arcb_fl ORDER BY id",
	"ts": "SELECT id, t FROM arcb_ts ORDER BY id",
	"em": "SELECT id, n, name FROM arcb_empty ORDER BY id",
	"mx": "SELECT id, flag FROM arcb_mix ORDER BY id",
}

func newCensusDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	for _, stmt := range censusFixture {
		if _, err := db.Query(ctx, stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}
	for _, stmt := range censusSeed {
		if _, err := db.Execute(ctx, stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}
	return db
}

// TestDMLCensus is the corpus. Every shape, every door, against the recorded
// answer — and against PostgreSQL 17 where wadjet is supposed to agree.
func TestDMLCensus(t *testing.T) {
	record := os.Getenv("WADJET_CENSUS_RECORD") != ""
	var recorded []string

	for _, sh := range censusShapes() {
		sh := sh
		t.Run(sh.name, func(t *testing.T) {
			emb := censusEmbedded(t, sh)
			simple := censusWire(t, sh, pgx.QueryExecModeSimpleProtocol)
			ext := censusWire(t, sh, pgx.QueryExecModeExec)

			if record {
				recorded = append(recorded, fmt.Sprintf("%s\n\temb=%s\n\tsim=%s\n\text=%s", sh.name, emb, simple, ext))
				return
			}

			wantEmb, wantSim, wantExt := sh.doors()
			for _, d := range []struct{ door, got, want string }{
				{"embedded", emb, wantEmb},
				{"simple", simple, wantSim},
				{"extended", ext, wantExt},
			} {
				if d.got != d.want {
					t.Errorf("%s\n  %s door answered %s\n  recorded          %s\n"+
						"  (PostgreSQL 17:  %s)", sh.sql, d.door, d.got, d.want, sh.pg)
				}
			}

			// The pin half. An entry with no `bug` claims every door agrees
			// with PostgreSQL, so a move away from PG is a regression; an
			// entry with one claims some door does not, so agreement means
			// the fix landed and the pin is now the thing that is wrong.
			agrees := wantEmb == sh.pg && wantSim == sh.pg && wantExt == sh.pg
			switch {
			case sh.bug == "" && !agrees:
				t.Errorf("%s\n  recorded as agreeing with PostgreSQL, but\n"+
					"  embedded %s\n  simple   %s\n  extended %s\n  PG 17    %s\n"+
					"  Either this is a regression, or the entry needs a `bug`.",
					sh.sql, wantEmb, wantSim, wantExt, sh.pg)
			case sh.bug != "" && agrees:
				t.Errorf("%s\n  pinned as %s, but every door now answers what PostgreSQL 17 does:\n  %s\n"+
					"  DELETE THE PIN — that is the fix's proof.", sh.sql, sh.bug, sh.pg)
			}
		})
	}

	if record {
		// A RECORDING is not a PASS. Every assertion above is skipped in this
		// mode, so a green run with WADJET_CENSUS_RECORD set says nothing —
		// and a green run is exactly what a tired author or a CI job that
		// inherited the variable would read as one (review P20).
		t.Logf("recorded census:\n%s", strings.Join(recorded, "\n"))
		t.Fatal("WADJET_CENSUS_RECORD was set: this run RECORDED the census and asserted nothing. " +
			"Paste the door lines into censusShapes() and re-run without the variable.")
	}
}

// censusEmbedded runs one shape through wadjet.DB.
func censusEmbedded(t *testing.T, sh censusShape) string {
	t.Helper()
	ctx := context.Background()
	db := newCensusDB(t)

	if sh.tbl == "" {
		res, err := db.Query(ctx, sh.sql)
		if err != nil {
			return "state=" + censusState(err)
		}
		return "rows=" + censusDigest(res.Columns, res.Rows)
	}

	head := ""
	if res, err := db.Execute(ctx, sh.sql); err != nil {
		head = "state=" + censusState(err)
	} else {
		// commandTag, not a fresh format string: the embedded result carries
		// a verb and a count, and PostgreSQL's rendering of an INSERT tag has
		// an oid field in it. Rendering it any other way here would show a
		// door split that is this test's own doing.
		head = "tag=" + commandTag(res.Command, res.RowsAffected)
	}
	after, err := db.Query(ctx, censusDigestSQL[sh.tbl])
	if err != nil {
		t.Fatalf("digest after %q: %v", sh.sql, err)
	}
	return head + " table=" + censusDigest(after.Columns, after.Rows)
}

// censusWire runs one shape through a real pgwire server, in the requested
// protocol mode, over a real pgx connection.
func censusWire(t *testing.T, sh censusShape, mode pgx.QueryExecMode) string {
	t.Helper()
	ctx := context.Background()
	db := newCensusDB(t)
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	if sh.tbl == "" {
		cols, rows, qErr := censusWireRows(ctx, conn, sh.sql, mode)
		if qErr != nil {
			return "state=" + censusState(qErr)
		}
		return "rows=" + censusDigest(cols, rows)
	}

	head := ""
	if tag, execErr := censusWireExec(ctx, conn, sh.sql, mode); execErr != nil {
		head = "state=" + censusState(execErr)
	} else {
		head = "tag=" + tag
	}
	cols, rows, qErr := censusWireRows(ctx, conn, censusDigestSQL[sh.tbl], mode)
	if qErr != nil {
		t.Fatalf("digest after %q: %v", sh.sql, qErr)
	}
	return head + " table=" + censusDigest(cols, rows)
}

// censusWireExec runs one statement and returns its COMMAND TAG.
//
// `conn.Exec` cannot be used for the extended arm: pgx v5 short-circuits a
// zero-argument Exec onto the SIMPLE protocol whatever the mode says, so an
// Exec-based "extended" arm silently measures the simple door and reports it
// as agreement. Query is the entry that honours QueryExecModeExec, and the
// tag it exposes after Close is the CommandComplete this server sent on the
// extended path — which is the protocol pgx, JDBC and every ORM use.
func censusWireExec(ctx context.Context, conn *pgx.Conn, sql string, mode pgx.QueryExecMode) (string, error) {
	if mode == pgx.QueryExecModeSimpleProtocol {
		tag, err := conn.Exec(ctx, sql, mode)
		if err != nil {
			return "", err
		}
		return tag.String(), nil
	}
	rows, err := conn.Query(ctx, sql, mode)
	if err != nil {
		return "", err
	}
	for rows.Next() {
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}
	return rows.CommandTag().String(), nil
}

func censusWireRows(ctx context.Context, conn *pgx.Conn, sql string, mode pgx.QueryExecMode) ([]string, []map[string]any, error) {
	rows, err := conn.Query(ctx, sql, mode)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var cols []string
	for _, fd := range rows.FieldDescriptions() {
		cols = append(cols, fd.Name)
	}
	var out []map[string]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, nil, err
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			if i < len(vals) {
				row[c] = vals[i]
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return cols, out, nil
}

// censusState is the SQLSTATE of a failure on either door. "<none>" is itself
// an answer: several DML failures carry no class at all today (#719).
func censusState(err error) string {
	var pgErr *pgconn.PgError
	if ok := asPgError(err, &pgErr); ok && pgErr.Code != "" {
		return pgErr.Code
	}
	if s := sqlerr.StateOf(err); s != "" {
		return s
	}
	return "<none>"
}

func asPgError(err error, target **pgconn.PgError) bool {
	for e := err; e != nil; {
		if pe, ok := e.(*pgconn.PgError); ok {
			*target = pe
			return true
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}

// censusDigest renders a row set so the three doors are comparable.
//
// They are not comparable as Go values: the same INT64 arrives as an int64
// from the embedded API and from pgx, but a TIMESTAMP arrives as milliseconds
// on one door and a time.Time on the other, and a FLOAT64 as a float64 on one
// and a formatted string on the other. The digest normalizes by VALUE, never
// by Go type, and sorts, so a door's row ORDER cannot mask a wrong row set.
func censusDigest(cols []string, rows []map[string]any) string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		parts := make([]string, 0, len(cols))
		for _, c := range cols {
			parts = append(parts, censusCell(c, r[c]))
		}
		out = append(out, strings.Join(parts, ":"))
	}
	sort.Strings(out)
	return "[" + strings.Join(out, " ") + "]"
}

func censusCell(col string, v any) string {
	switch t := v.(type) {
	case nil:
		return "~"
	case bool:
		return strconv.FormatBool(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32)
	case int64:
		// The one column whose Go box differs by door: a TIMESTAMP comes back
		// as epoch milliseconds from the embedded API and as a time.Time from
		// pgx, so the millisecond form is rendered as the instant it names.
		if col == "t" {
			return time.UnixMilli(t).UTC().Format(time.RFC3339)
		}
		return strconv.FormatInt(t, 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int:
		return strconv.Itoa(t)
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return fmt.Sprintf("%v", v)
	}
}
