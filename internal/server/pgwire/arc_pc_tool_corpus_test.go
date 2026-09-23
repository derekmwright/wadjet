// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The catalog queries real tools send (#1251, arc PC), answered by the
// engine, compared with PostgreSQL 17.11 over the same two tables.
//
// testdata/pc_tool_corpus.json is the corpus and PostgreSQL's answers: psql
// 17's \dt \d \d+ \dn \df \du \dv \di \l statements captured with `psql -E`,
// SQLAlchemy 1.4's inspector statements captured from the server log (the
// reflection Superset runs), pgJDBC's DatabaseMetaData and TypeInfoCache
// statements from its source (what DataGrip, DBeaver and Metabase read
// through), DataGrip's recorded opening statements, the information_schema
// reads BI tools issue, and each catalog relation composed with a filter, a
// join, an aggregate and an ORDER BY/LIMIT. The `pg` answers are MEASURED:
// `PC_RECORD_PG_DSN=postgres://wadjet:…@host/wadjet go test -run
// TestArcPCRecordToolCorpus` rewrites them from a server holding the fixture
// below, connected as an ordinary (non-superuser) role named wadjet.
//
// OIDs are this server's own and need only be self-consistent across the
// catalog (the brief's rule), so an integer at or above 16384 — a user
// object's OID on both servers — renders as <oid>; the system relations' and
// types' OIDs are PostgreSQL's and are compared as they are.

const pcCorpusFile = "testdata/pc_tool_corpus.json"

type pcCorpusEntry struct {
	Name string     `json:"name"`
	SQL  string     `json:"sql"`
	PG   *pcOutcome `json:"pg,omitempty"`
}

type pcOutcome struct {
	Cols []string   `json:"cols,omitempty"`
	Rows [][]string `json:"rows,omitempty"`
	Err  string     `json:"err,omitempty"` // the SQLSTATE of a refusal
}

// pcPins are the statements whose answer is a RECORDED divergence, each with
// its mechanism. A pin that starts agreeing with PostgreSQL fails — deleting
// it is the proof.
var pcPins = map[string]string{
	"psql \\l #1": "E'…' escape-string literals are not in this parser's grammar " +
		"(42601); psql's \\l renders the ACL with E'\\n'",
	"psql \\d o #2": "pg_class.relam is 0 and pg_am is empty: a stored table here " +
		"has no PostgreSQL access method, so \\d prints no 'Access method: heap' line",
	"psql \\d+ o #2": "as psql \\d o #2 (relam)",
	"sqlalchemy 13": "a set-returning function in the SELECT list (unnest, " +
		"generate_subscripts) is not implemented by the engine: SQLAlchemy's " +
		"get_pk_constraint refuses 42883 where PostgreSQL answers no rows",
	"pgjdbc getPrimaryKeys o": "information_schema._pg_expandarray and the field " +
		"access over its composite are not implemented: 42883 where PostgreSQL " +
		"answers no rows",
	"pgjdbc TypeInfoCache getSQLType": "a table function's arguments are literals in " +
		"this grammar: generate_series(1, array_upper(…)) is 42601 where PostgreSQL " +
		"answers",
	"datagrip databases": "this server is ONE database: PostgreSQL also lists " +
		"template0, template1 and postgres",
	"pgjdbc getCatalogs": "as datagrip databases (one database)",
	"information_schema schemata": "one role owns everything here; PostgreSQL's " +
		"system schemas are owned by the bootstrap superuser and public by " +
		"pg_database_owner",
	"psql \\dn #1": "as information_schema schemata (public's owner)",
	"psql \\du #1": "one role: PostgreSQL also lists its bootstrap superuser",
	"pgjdbc TypeInfoCache getPGType": "current_schemas(true) answers the TEXT " +
		"'{public}' — not a name[] with pg_catalog in it — so `= ANY(…)` is false " +
		"(filing candidate)",
	"regclass round trip": "a string literal cast to regclass is READ to its OID " +
		"(buildCast), so `'ev'::regclass::text` prints the OID where PostgreSQL " +
		"prints ev",
	"sqlalchemy 11": "pg_type lists no DOMAIN types: information_schema's " +
		"domains (cardinal_number, …) are carried as their base types here",
}

// pcCorpusDB builds the fixture: the two tables PostgreSQL holds, in the
// types this engine stores them as.
func pcCorpusDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	oSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "customer", Type: parquet.TypeString, Nullable: true},
		{Name: "total", Type: parquet.TypeFloat64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "o", oSchema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("o", oSchema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "customer": "a", "total": 2.5},
		{"id": int64(2), "customer": "b", "total": 3.5},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	evSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "amt", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "flag", Type: parquet.TypeBool, Nullable: true},
		{Name: "u", Type: parquet.TypeUUID, Nullable: true},
		{Name: "k", Type: parquet.TypeInt32, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "ev", evSchema, nil); err != nil {
		t.Fatal(err)
	}
	return db
}

func pcLoadCorpus(t *testing.T) []pcCorpusEntry {
	t.Helper()
	raw, err := os.ReadFile(pcCorpusFile)
	if err != nil {
		t.Fatal(err)
	}
	var corpus []pcCorpusEntry
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	return corpus
}

var pcOIDPattern = regexp.MustCompile(`^[0-9]{5,10}$`)

// pcRender is one value as the corpus records it: PostgreSQL's text form for
// the value, whatever Go type a door handed it over as.
func pcRender(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if x {
			return "t"
		}
		return "f"
	case string:
		return pcOID(x)
	case []byte:
		return pcOID(string(x))
	case int64:
		return pcOID(strconv.FormatInt(x, 10))
	case int32:
		return pcOID(strconv.FormatInt(int64(x), 10))
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int:
		return pcOID(strconv.Itoa(x))
	case uint32:
		return pcOID(strconv.FormatUint(uint64(x), 10))
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		return x.Format("2006-01-02 15:04:05")
	case pgtype.Numeric:
		if !x.Valid {
			return "NULL"
		}
		f := new(big.Float).SetInt(x.Int)
		return f.Text('f', 0) + "e" + strconv.Itoa(int(x.Exp))
	case [16]byte:
		return fmt.Sprintf("%x", x)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = pcRender(e)
		}
		return "{" + strings.Join(parts, ",") + "}"
	case map[string]any:
		return fmt.Sprint(x)
	}
	return fmt.Sprint(v)
}

// pcOID renders an integer at or above 16384 — a user object's OID — as <oid>.
func pcOID(s string) string {
	if pcOIDPattern.MatchString(s) {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n >= 16384 {
			return "<oid>"
		}
	}
	return s
}

// pcResolve replaces the corpus's {OID} with relation o's OID on this server,
// the way psql feeds each \d statement the OID the first one returned.
func pcResolve(sql, oid string) string { return strings.ReplaceAll(sql, "{OID}", oid) }

// pcRawText runs one statement on the simple protocol and keeps every value
// as the TEXT the server sent, undecoded — PostgreSQL's own rendering, which
// is what psql prints (a "char" is its character, not a number).
func pcRawText(ctx context.Context, conn *pgx.Conn, sql string) *pcOutcome {
	mrr := conn.PgConn().Exec(ctx, sql)
	defer mrr.Close()
	var out *pcOutcome
	results := 0
	for mrr.NextResult() {
		results++
		rr := mrr.ResultReader()
		o := &pcOutcome{}
		for rr.NextRow() {
			vals := rr.Values()
			row := make([]string, len(vals))
			for i, v := range vals {
				if v == nil {
					row[i] = "NULL"
					continue
				}
				row[i] = pcOID(string(v))
			}
			o.Rows = append(o.Rows, row)
		}
		// Read after the rows: the RowDescription has arrived by then, for
		// a result with no rows as well.
		for _, fd := range rr.FieldDescriptions() {
			o.Cols = append(o.Cols, fd.Name)
		}
		if _, err := rr.Close(); err != nil {
			return &pcOutcome{Err: pcState(err)}
		}
		out = o
	}
	if err := mrr.Close(); err != nil {
		return &pcOutcome{Err: pcState(err)}
	}
	if results != 1 || out == nil {
		return &pcOutcome{Err: fmt.Sprintf("%d results", results)}
	}
	return out
}

func pcPgxRun(ctx context.Context, conn *pgx.Conn, sql string, mode pgx.QueryExecMode) *pcOutcome {
	rows, err := conn.Query(ctx, sql, mode)
	if err != nil {
		return &pcOutcome{Err: pcState(err)}
	}
	defer rows.Close()
	out := &pcOutcome{}
	for _, fd := range rows.FieldDescriptions() {
		out.Cols = append(out.Cols, fd.Name)
	}
	for rows.Next() {
		vals, verr := rows.Values()
		if verr != nil {
			return &pcOutcome{Err: "values: " + verr.Error()}
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			row[i] = pcRender(v)
		}
		out.Rows = append(out.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return &pcOutcome{Err: pcState(err)}
	}
	return out
}

func pcState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	if s := sqlerr.StateOf(err); s != "" {
		return s
	}
	return "error: " + err.Error()
}

// pcSame compares two outcomes: a refusal by its SQLSTATE, an answer by its
// columns and its rows — in order when the statement orders them, as a set
// otherwise.
func pcSame(sql string, a, b *pcOutcome) bool {
	if a.Err != "" || b.Err != "" {
		return a.Err == b.Err
	}
	if strings.Join(a.Cols, "\x00") != strings.Join(b.Cols, "\x00") || len(a.Rows) != len(b.Rows) {
		return false
	}
	ra, rb := pcRowKeys(a.Rows), pcRowKeys(b.Rows)
	if !strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		sort.Strings(ra)
		sort.Strings(rb)
	}
	return strings.Join(ra, "\n") == strings.Join(rb, "\n")
}

func pcRowKeys(rows [][]string) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = strings.Join(r, "|")
	}
	return out
}

func pcShow(o *pcOutcome) string {
	if o.Err != "" {
		return "ERROR " + o.Err
	}
	return fmt.Sprintf("%v %v", o.Cols, o.Rows)
}

// TestArcPCRecordToolCorpus rewrites the corpus's PostgreSQL answers. It runs
// only when PC_RECORD_PG_DSN names a PostgreSQL server holding the fixture.
func TestArcPCRecordToolCorpus(t *testing.T) {
	dsn := os.Getenv("PC_RECORD_PG_DSN")
	if dsn == "" {
		t.Skip("PC_RECORD_PG_DSN not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
		t.Fatal(err)
	}
	var oid uint32
	if err := conn.QueryRow(ctx, "SELECT 'o'::regclass::oid").Scan(&oid); err != nil {
		t.Fatal(err)
	}
	corpus := pcLoadCorpus(t)
	for i := range corpus {
		corpus[i].PG = pcRawText(ctx, conn, pcResolve(corpus[i].SQL, strconv.FormatUint(uint64(oid), 10)))
	}
	raw, err := json.MarshalIndent(corpus, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pcCorpusFile, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestArcPCToolCatalogQueriesAnswerAsPostgreSQL is the gate: every corpus
// statement, on the wire in text (the simple protocol, psql's) and in binary
// (the extended protocol with binary results, pgx's and pgJDBC's), and on the
// embedded door, answers what PostgreSQL 17.11 answers — or is a pin above.
func TestArcPCToolCatalogQueriesAnswerAsPostgreSQL(t *testing.T) {
	db := pcCorpusDB(t)
	srv := startTestServer(t, db)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	res, err := db.Query(ctx, "SELECT oid FROM pg_class WHERE relname = 'o'")
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("resolving o's OID: %v %v", res, err)
	}
	oid := fmt.Sprint(res.Rows[0]["oid"])

	doors := []struct {
		name string
		run  func(sql string) *pcOutcome
	}{
		{"pgwire/text", func(sql string) *pcOutcome {
			return pcRawText(ctx, conn, sql)
		}},
		{"pgwire/binary", func(sql string) *pcOutcome {
			return pcPgxRun(ctx, conn, sql, pgx.QueryExecModeCacheDescribe)
		}},
		{"embedded", func(sql string) *pcOutcome {
			r, err := db.Query(ctx, sql)
			if err != nil {
				return &pcOutcome{Err: pcState(err)}
			}
			out := &pcOutcome{Cols: r.Columns}
			vals := r.RowValues
			if vals == nil {
				for _, m := range r.Rows {
					row := make([]any, len(r.Columns))
					for i, c := range r.Columns {
						row[i] = m[c]
					}
					vals = append(vals, row)
				}
			}
			for _, v := range vals {
				row := make([]string, len(v))
				for i, x := range v {
					row[i] = pcRender(x)
				}
				out.Rows = append(out.Rows, row)
			}
			return out
		}},
	}
	corpus := pcLoadCorpus(t)
	if len(corpus) < 60 {
		t.Fatalf("the corpus holds %d statements; it was enumerated at 70", len(corpus))
	}
	agreed := 0
	for _, entry := range corpus {
		if entry.PG == nil {
			t.Fatalf("%s: no recorded PostgreSQL answer", entry.Name)
		}
		sql := pcResolve(entry.SQL, oid)
		for _, door := range doors {
			// The embedded door is the engine; the wire's two formats add
			// only the rendering. psql's \l is 42601 at the parser on all.
			got := door.run(sql)
			same := pcSame(sql, got, entry.PG)
			if reason, pinned := pcPins[entry.Name]; pinned {
				if same {
					t.Errorf("%s / %s: the pin agrees with PostgreSQL 17.11 now — delete it (%s)",
						entry.Name, door.name, reason)
				}
				continue
			}
			if !same {
				t.Errorf("%s / %s:\n  wadjet     %s\n  PostgreSQL %s\n  %s",
					entry.Name, door.name, pcShow(got), pcShow(entry.PG), sql)
				continue
			}
			agreed++
		}
	}
	t.Logf("%d (statement, door) pairs agree with PostgreSQL 17.11; %d statements pinned",
		agreed, len(pcPins))
}
