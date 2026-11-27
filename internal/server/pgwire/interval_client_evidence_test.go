// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// The client-visible contract for an interval in the execution-only stage
// (#1680b round 2; the architect's ruling 1 and ruling 2), as a reproducible
// transcript in place of round 1's deleted probes: what an extended-protocol
// client sees at Describe (parameter and result OIDs, typmods, lengths),
// from the explicit text cast in text and in a requested binary format, from
// a bare interval in either format, for NULL, for an empty result, for a
// prepared execution with text, NULL and binary interval parameters, for
// Describe before Bind, for a typmod, and for SET IntervalStyle.
//
// The same steps run against PostgreSQL 17.11 (IV80_PG_DSN; writes
// testdata/interval_client_evidence_pg17.tsv) and against this engine's
// embedded server (held against testdata/interval_client_evidence_engine.tsv;
// IV80_RECORD_ENGINE=1 rewrites it). The two files side by side are the
// evidence; the engine file is a baseline, not a claim of agreement.

const ceFixture = "CREATE TABLE ce (id BIGINT, s TEXT)"
const ceRows = "INSERT INTO ce VALUES (1, '1 day'), (2, NULL), (3, '30 days')"

// ceStep is one client step.
type ceStep struct {
	name string
	run  func(ctx context.Context, c *pgconn.PgConn) string
}

func ceErr(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "ERR " + pe.Code
	}
	return "ERR uncoded"
}

func ceFields(fds []pgconn.FieldDescription) string {
	var f []string
	for _, fd := range fds {
		f = append(f, fmt.Sprintf("%s:oid=%d:typmod=%d:len=%d:fmt=%d", fd.Name, fd.DataTypeOID, fd.TypeModifier, fd.DataTypeSize, fd.Format))
	}
	return "[" + strings.Join(f, " ") + "]"
}

// ceRead reads a result keeping its field descriptions for zero rows; a
// value in binary format is hex.
func ceRead(rr *pgconn.ResultReader) string {
	var rows []string
	for rr.NextRow() {
		var vals []string
		for i, v := range rr.Values() {
			switch {
			case v == nil:
				vals = append(vals, "NULL")
			case rr.FieldDescriptions()[i].Format == 1:
				vals = append(vals, "0x"+hex.EncodeToString(v))
			default:
				vals = append(vals, string(v))
			}
		}
		rows = append(rows, strings.Join(vals, ","))
	}
	fds := ceFields(rr.FieldDescriptions())
	if _, err := rr.Close(); err != nil {
		return ceErr(err)
	}
	return "fields=" + fds + " rows=[" + strings.Join(rows, "; ") + "]"
}

func ceDescribe(name, sql string, paramOIDs []uint32) ceStep {
	return ceStep{name, func(ctx context.Context, c *pgconn.PgConn) string {
		sd, err := c.Prepare(ctx, name, sql, paramOIDs)
		if err != nil {
			return ceErr(err)
		}
		return fmt.Sprintf("params=%v fields=%s", sd.ParamOIDs, ceFields(sd.Fields))
	}}
}

func ceExec(name, sql string, params [][]byte, oids []uint32, formats []int16, result []int16) ceStep {
	return ceStep{name, func(ctx context.Context, c *pgconn.PgConn) string {
		return ceRead(c.ExecParams(ctx, sql, params, oids, formats, result))
	}}
}

func ceExecPrepared(name, stmt string, params [][]byte, formats []int16, result []int16) ceStep {
	return ceStep{name, func(ctx context.Context, c *pgconn.PgConn) string {
		return ceRead(c.ExecPrepared(ctx, stmt, params, formats, result))
	}}
}

func ceSimple(name, sql string) ceStep {
	return ceStep{name, func(ctx context.Context, c *pgconn.PgConn) string {
		mrr := c.Exec(ctx, sql)
		var out []string
		for mrr.NextResult() {
			out = append(out, ceRead(mrr.ResultReader()))
		}
		if err := mrr.Close(); err != nil {
			return ceErr(err)
		}
		return strings.Join(out, " || ")
	}}
}

// oneDayBinary is PostgreSQL's binary interval for '1 day': time int64 0,
// day int32 1, month int32 0, big-endian.
var oneDayBinary = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0}

func ceSteps() []ceStep {
	const oidInterval = 1186
	bare := "SELECT CAST(s AS INTERVAL) AS v FROM ce ORDER BY id"
	text := "SELECT CAST(CAST(s AS INTERVAL) AS TEXT) AS t FROM ce ORDER BY id"
	return []ceStep{
		{"connect/IntervalStyle", func(_ context.Context, c *pgconn.PgConn) string {
			return "ParameterStatus=" + c.ParameterStatus("IntervalStyle")
		}},
		ceDescribe("describe/bare", bare, nil),
		ceDescribe("describe/text", text, nil),
		ceExecPrepared("execute/text", "describe/text", nil, nil, []int16{0}),
		ceExecPrepared("execute/textAsBinary", "describe/text", nil, nil, []int16{1}),
		ceExecPrepared("execute/bare", "describe/bare", nil, nil, []int16{0}),
		ceExec("execute/bareBinary", bare, nil, nil, nil, []int16{1}),
		ceSimple("simple/bare", bare),
		ceSimple("simple/text", text),
		ceExec("null/text", "SELECT CAST(CAST(NULL AS INTERVAL) AS TEXT) AS t", nil, nil, nil, nil),
		ceExec("null/bare", "SELECT CAST(NULL AS INTERVAL) AS v", nil, nil, nil, nil),
		ceDescribe("empty/describeText", "SELECT CAST(CAST(s AS INTERVAL) AS TEXT) AS t FROM ce WHERE false", nil),
		ceExecPrepared("empty/executeText", "empty/describeText", nil, nil, nil),
		ceDescribe("empty/describeBare", "SELECT CAST(s AS INTERVAL) AS v FROM ce WHERE false", nil),
		ceExecPrepared("empty/executeBare", "empty/describeBare", nil, nil, nil),
		ceSimple("empty/simpleBare", "SELECT CAST(s AS INTERVAL) AS v FROM ce WHERE false"),
		ceDescribe("param/describe", "SELECT id FROM ce WHERE CAST(s AS INTERVAL) = $1 ORDER BY id", []uint32{oidInterval}),
		ceExecPrepared("param/text", "param/describe", [][]byte{[]byte("24 hours")}, []int16{0}, nil),
		ceExecPrepared("param/textMultiField", "param/describe", [][]byte{[]byte("1 mon")}, []int16{0}, nil),
		ceExecPrepared("param/null", "param/describe", [][]byte{nil}, []int16{0}, nil),
		ceExecPrepared("param/binary", "param/describe", [][]byte{oneDayBinary}, []int16{1}, nil),
		ceDescribe("param/describeUntyped", "SELECT id FROM ce WHERE CAST(s AS INTERVAL) = $1 ORDER BY id", nil),
		ceDescribe("publishParam/describe", "SELECT CAST($1 AS INTERVAL) AS a", nil),
		ceExecPrepared("publishParam/execute", "publishParam/describe", [][]byte{[]byte("1 day")}, []int16{0}, nil),
		ceDescribe("publishParamText/describe", "SELECT CAST(CAST($1 AS INTERVAL) AS TEXT) AS a", nil),
		ceExecPrepared("publishParamText/execute", "publishParamText/describe", [][]byte{[]byte("1 day 02:00:00")}, []int16{0}, nil),
		ceDescribe("typmod/describe", "SELECT CAST(INTERVAL '1.123456 seconds' AS INTERVAL(3)) AS a", nil),
		ceDescribe("typmod/describeDay", "SELECT CAST(INTERVAL '1 day 02:00:00' AS INTERVAL DAY) AS a", nil),
		ceSimple("intervalstyle/set", "SET IntervalStyle = 'iso_8601'"),
		ceSimple("intervalstyle/show", "SHOW IntervalStyle"),
		ceSimple("intervalstyle/text", "SELECT CAST(INTERVAL '1 day' AS TEXT) AS t"),
		ceSimple("intervalstyle/reset", "RESET IntervalStyle"),
		ceSimple("intervalstyle/afterReset", "SELECT CAST(INTERVAL '1 day' AS TEXT) AS t"),
		// The door assertions of the stage manifest's IntervalStyle cells
		// (#1680b round 3): the corpus applies a cell's IntervalStyle on the
		// reference only, so the engine side of such a cell is decided here,
		// by what SET answers at the door (the error's sentence kept, so the
		// manifest can check the named reason).
		ceSetStyle("door/IntervalStyle/postgres_verbose", "postgres_verbose"),
		ceSetStyle("door/IntervalStyle/sql_standard", "sql_standard"),
		ceSetStyle("door/IntervalStyle/iso_8601", "iso_8601"),
		ceSetStyle("door/IntervalStyle/postgres", "postgres"),
	}
}

// ceSetStyle is SET IntervalStyle to style, answered with the error's
// SQLSTATE and sentence, then RESET.
func ceSetStyle(name, style string) ceStep {
	return ceStep{name, func(ctx context.Context, c *pgconn.PgConn) string {
		_, err := c.Exec(ctx, "SET IntervalStyle = '"+style+"'").ReadAll()
		out := "ok"
		var pe *pgconn.PgError
		switch {
		case errors.As(err, &pe):
			out = "ERR " + pe.Code + ": " + pe.Message
		case err != nil:
			out = "ERR : " + err.Error()
		}
		if _, err := c.Exec(ctx, "RESET IntervalStyle").ReadAll(); err != nil {
			return out + " (RESET " + ceErr(err) + ")"
		}
		return out
	}}
}

// ceTranscript runs every step on one connection (fixture first).
func ceTranscript(t *testing.T, c *pgconn.PgConn) string {
	t.Helper()
	ctx := context.Background()
	for _, s := range []string{ceFixture, ceRows} {
		if _, err := c.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	var b strings.Builder
	for _, s := range ceSteps() {
		b.WriteString(s.name + "\t" + s.run(ctx, c) + "\n")
	}
	return b.String()
}

func TestIntervalClientEvidence(t *testing.T) {
	const pgPath = "testdata/interval_client_evidence_pg17.tsv"
	const enginePath = "testdata/interval_client_evidence_engine.tsv"
	header := "# #1680b client evidence (interval_client_evidence_test.go): step<TAB>what the client saw.\n"
	if dsn := os.Getenv("IV80_PG_DSN"); dsn != "" {
		ctx := context.Background()
		c, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(ctx)
		if _, err := c.Exec(ctx, "DROP TABLE IF EXISTS ce").ReadAll(); err != nil {
			t.Fatal(err)
		}
		ver := ceRead(c.ExecParams(ctx, "SELECT version()", nil, nil, nil, nil))
		body := header + "# PostgreSQL 17.11 (postgres:17-alpine, initdb --locale=C --encoding=UTF8, --memory=4g): " + ver + "\n" + ceTranscript(t, c)
		if err := os.WriteFile(pgPath, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "ivce"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	got := header + "# this engine's embedded server over pgwire\n" + ceTranscript(t, connectPgconn(t, srv.Addr()))
	if os.Getenv("IV80_RECORD_ENGINE") != "" {
		if err := os.WriteFile(enginePath, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(enginePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Errorf("the client transcript moved:\nrecorded:\n%s\nnow:\n%s", want, got)
	}
	// The PostgreSQL transcript has every step.
	pg, err := os.ReadFile(pgPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range ceSteps() {
		if !strings.Contains(string(pg), "\n"+s.name+"\t") {
			t.Errorf("the PostgreSQL transcript lacks step %s", s.name)
		}
	}
}
