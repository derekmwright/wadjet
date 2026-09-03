package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// The DML corpus through BOTH doors, asserting the SAME table state, the SAME
// command tag and the SAME SQLSTATE.
//
// Until #815 this server carried its own INSERT/UPDATE/DELETE executors over
// the catalog — a line-for-line second copy of wadjet/dml.go's that had
// drifted into the same defects (an unresolved INSERT column list, the
// marker/manifest window, the literal kind discarded in SET) and carried NO
// MERGE at all: `MERGE` was routed into handleDML and then refused with
// HTTP 400 while the same statement succeeded on the embedded and pgwire
// doors. Every fix therefore had to be written twice or left one door wrong,
// and nothing in the tree compared the two.
//
// This is that comparison, and it is the gate every later fix in the DML arc
// rides on: a fix that lands in wadjet/dml.go alone is now visible here.
func TestEveryDMLDoorAnswersTheSame(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		// pgTag is PostgreSQL 17's CommandComplete for this statement,
		// MEASURED. Comparing the two doors to each other could never fail on
		// the tag, because both built it the same way; comparing them to
		// PostgreSQL is what makes the doc line "the command tag does not
		// depend on which door it arrived by" checkable (review B8).
		pgTag string
	}{
		// The four verbs, firing.
		{name: "INSERT", sql: "INSERT INTO pr815 (id, n, name) VALUES (9, 90, 'z')", pgTag: "INSERT 0 1"},
		{name: "INSERT no column list", sql: "INSERT INTO pr815 VALUES (8, 80, 'y')", pgTag: "INSERT 0 1"},
		{name: "INSERT multi row", sql: "INSERT INTO pr815 (id, n, name) VALUES (7, 70, 'w'), (6, 60, 'v')", pgTag: "INSERT 0 2"},
		{name: "UPDATE", sql: "UPDATE pr815 SET n = 99 WHERE id = 1", pgTag: "UPDATE 1"},
		{name: "UPDATE all rows", sql: "UPDATE pr815 SET n = 0 WHERE id > 0", pgTag: "UPDATE 3"},
		{name: "UPDATE expression over the column", sql: "UPDATE pr815 SET n = n + 1 WHERE id = 2", pgTag: "UPDATE 1"},
		{name: "DELETE", sql: "DELETE FROM pr815 WHERE id = 1", pgTag: "DELETE 1"},
		{name: "DELETE many", sql: "DELETE FROM pr815 WHERE id > 1", pgTag: "DELETE 2"},
		{name: "MERGE update", sql: "MERGE INTO pr815 AS t USING src815 AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n"},
		{name: "MERGE delete", sql: "MERGE INTO pr815 AS t USING src815 AS s ON t.id = s.id WHEN MATCHED THEN DELETE"},
		{name: "MERGE insert", sql: "MERGE INTO pr815 AS t USING src815 AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)"},

		// The four verbs, matching nothing: the count is the whole answer.
		{name: "UPDATE no match", sql: "UPDATE pr815 SET n = 99 WHERE id = 99", pgTag: "UPDATE 0"},
		{name: "DELETE no match", sql: "DELETE FROM pr815 WHERE id = 99", pgTag: "DELETE 0"},
		{name: "MERGE no match", sql: "MERGE INTO pr815 AS t USING src815 AS s ON t.id = s.id WHEN MATCHED AND s.n > 100000 THEN DELETE"},

		// Refusals: the SQLSTATE is the answer, and the table must be
		// untouched on both doors.
		{name: "DELETE unknown table", sql: "DELETE FROM nosuch815 WHERE id = 1"},
		{name: "UPDATE unknown table", sql: "UPDATE nosuch815 SET n = 1 WHERE id = 1"},
		{name: "INSERT unknown table", sql: "INSERT INTO nosuch815 (id) VALUES (1)"},
		{name: "MERGE unknown target", sql: "MERGE INTO nosuch815 AS t USING src815 AS s ON t.id = s.id WHEN MATCHED THEN DELETE"},
		{name: "DELETE unknown column", sql: "DELETE FROM pr815 WHERE nosuchcol = 1"},
		{name: "UPDATE unknown SET target", sql: "UPDATE pr815 SET nosuchcol = 1 WHERE id = 1"},
		{name: "DELETE wrong-typed literal", sql: "DELETE FROM pr815 WHERE id = 'abc'"},
		{name: "UPDATE table-qualified WHERE under an alias", sql: "UPDATE pr815 AS a SET n = 1 WHERE pr815.id = 1"},
		{name: "INSERT value count mismatch", sql: "INSERT INTO pr815 (id, n) VALUES (5)"},
		{name: "MERGE unknown ON column", sql: "MERGE INTO pr815 AS t USING src815 AS s ON t.nosuchcol = s.id WHEN MATCHED THEN DELETE"},

		// A TRAILING COMMENT IS NOT A SECOND STATEMENT. `…; -- query tag` is
		// what ORMs, migration runners and hand-typed psql produce constantly,
		// and treating the comment as a second statement made every
		// one-statement door — this one included — refuse the whole string
		// with 42601 (#711 review B1). PostgreSQL 17 runs all four, measured.
		{name: "DELETE with a trailing line comment",
			sql: "DELETE FROM pr815 WHERE id = 1; -- audit note", pgTag: "DELETE 1"},
		{name: "DELETE with a trailing block comment",
			sql: "DELETE FROM pr815 WHERE id = 1; /* audit */", pgTag: "DELETE 1"},
		{name: "UPDATE with a trailing comment on its own line",
			sql: "UPDATE pr815 SET n = 99 WHERE id = 1;\n-- audit note", pgTag: "UPDATE 1"},
		{name: "INSERT with a trailing comment",
			sql: "INSERT INTO pr815 (id, n, name) VALUES (9, 90, 'z'); -- note", pgTag: "INSERT 0 1"},
		{name: "a leading comment",
			sql: "-- lead\nDELETE FROM pr815 WHERE id = 1", pgTag: "DELETE 1"},
		// And a genuine second statement is still refused on this door.
		{name: "two statements are refused",
			sql: "DELETE FROM pr815 WHERE id = 1; DELETE FROM pr815 WHERE id = 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			embedded := embeddedDoorAnswer(t, tc.sql)
			over := httpDoorAnswer(t, tc.sql)
			if embedded != over {
				t.Errorf("%s\n  embedded door: %s\n  HTTP door:     %s", tc.sql, embedded, over)
			}
			// The third comparison, and the only one that can fail on a tag
			// both doors render identically and wrongly (review B8).
			if tc.pgTag != "" {
				want := "tag=" + tc.pgTag + " "
				for _, got := range []string{embedded, over} {
					if !strings.HasPrefix(got, want) {
						t.Errorf("%s\n  answered %s\n  PostgreSQL 17 CommandComplete: %s",
							tc.sql, got, tc.pgTag)
					}
				}
			}
		})
	}
}

// doorFixture is the same three-row table on both doors, created through SQL
// so nothing about the fixture itself can differ between them.
const (
	doorCreate    = "CREATE TABLE pr815 (id INT64, n INT64, name STRING)"
	doorCreateSrc = "CREATE TABLE src815 (id INT64, n INT64, name STRING)"
	doorSeed      = "INSERT INTO pr815 (id, n, name) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')"
	doorSeedSrc   = "INSERT INTO src815 (id, n, name) VALUES (1, 100, 'x'), (4, 400, 'y')"
	doorDigestSQL = "SELECT id, n, name FROM pr815 ORDER BY id"
)

// answer renders everything a client can observe about one statement: the
// command tag with its count, the SQLSTATE of a refusal, and the table state
// afterwards as a full-row digest. A tag alone cannot see a wrong WRITE, which
// is most of what this arc is about.
func doorAnswer(tag, state, digest string) string {
	if state != "" {
		return fmt.Sprintf("state=%s table=%s", state, digest)
	}
	return fmt.Sprintf("tag=%s table=%s", tag, digest)
}

func embeddedDoorAnswer(t *testing.T, sql string) string {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range []string{doorCreate, doorCreateSrc} {
		if _, err := db.Query(ctx, stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}
	for _, stmt := range []string{doorSeed, doorSeedSrc} {
		if _, err := db.Execute(ctx, stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}

	tag, state := "", ""
	res, execErr := db.Execute(ctx, sql)
	if execErr != nil {
		state = doorStateOrNone(execErr)
	} else {
		// res.Tag(), not a local format — the gate used to build the tag the
		// same way BOTH doors did, so it could not see that the HTTP door
		// dropped INSERT's oid field while pgwire and PostgreSQL keep it
		// (review B8).
		tag = res.Tag()
	}
	return doorAnswer(tag, state, embeddedDigest(t, db))
}

func embeddedDigest(t *testing.T, db *wadjet.DB) string {
	t.Helper()
	res, err := db.Query(context.Background(), doorDigestSQL)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	return doorRowsDigest(res.Rows)
}

func httpDoorAnswer(t *testing.T, sql string) string {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := New(Config{Addr: ":0", Catalog: cat}, logger)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	for _, stmt := range []string{doorCreate, doorCreateSrc, doorSeed, doorSeedSrc} {
		if _, _, _, err := postDoorSQL(ts.URL, stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}

	tag, state, _, err := postDoorSQL(ts.URL, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}

	// The digest comes back through the same HTTP door the statement went
	// through, so a write this door cannot see is a divergence here too.
	_, _, rows, err := postDoorSQL(ts.URL, doorDigestSQL)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	return doorAnswer(tag, state, doorRowsDigest(rows))
}

// postSQL runs one statement over the REST door and returns (command tag,
// SQLSTATE, rows). Exactly one of tag / state is set for a DML statement.
func postDoorSQL(base, sql string) (string, string, []map[string]any, error) {
	buf, err := json.Marshal(map[string]any{"sql": sql})
	if err != nil {
		return "", "", nil, err
	}
	resp, err := http.Post(base+"/v1/queries", "application/json", bytes.NewReader(buf))
	if err != nil {
		return "", "", nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", nil, err
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error    string `json:"error"`
			SQLState string `json:"sqlstate"`
		}
		if err := json.Unmarshal(body, &e); err != nil {
			return "", "", nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
		}
		if e.SQLState == "" {
			// The door reported no class at all. Recording that as "<none>"
			// rather than failing is deliberate: several statements carry no
			// SQLSTATE on EITHER door today, and this gate is about the two
			// doors agreeing, not about which class is right (#719 owns that).
			return "", "<none>", nil, nil
		}
		return "", e.SQLState, nil, nil
	}
	var q QueryResponse
	if err := json.Unmarshal(body, &q); err != nil {
		return "", "", nil, err
	}
	// A DML statement comes back as one row {"result": "DELETE 2"}.
	if len(q.Columns) == 1 && q.Columns[0] == "result" && len(q.Rows) == 1 {
		if s, ok := q.Rows[0]["result"].(string); ok {
			return s, "", nil, nil
		}
	}
	return "", "", q.Rows, nil
}

func doorStateOrNone(err error) string {
	if s := sqlerr.StateOf(err); s != "" {
		return s
	}
	return "<none>"
}

func doorRowsDigest(rows []map[string]any) string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf("%v:%v:%v", doorCell(r["id"]), doorCell(r["n"]), doorCell(r["name"])))
	}
	sort.Strings(out)
	return "[" + strings.Join(out, " ") + "]"
}

// cell renders a value so the two doors are comparable: the HTTP door's JSON
// round-trip turns every integer into a float64 and a SQL NULL into nil, so
// the numbers are rendered from their VALUE and never from their Go type.
func doorCell(v any) string {
	switch t := v.(type) {
	case nil:
		return "~"
	case float64:
		return fmt.Sprintf("%d", int64(t))
	case float32:
		return fmt.Sprintf("%d", int64(t))
	case int64:
		return fmt.Sprintf("%d", t)
	case int32:
		return fmt.Sprintf("%d", t)
	case int:
		return fmt.Sprintf("%d", t)
	default:
		return fmt.Sprintf("%v", v)
	}
}
