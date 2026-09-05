package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// #860: `ALTER TABLE t ADD COLUMN c BIGINT` was refused with
// `no SELECT info in parsed query` — an internal invariant's wording, naming
// nothing the client wrote — with NO sqlstate on the HTTP door and the
// blanket 42000 through pgwire. The parser accepts the statement into a
// ParsedQuery whose type no branch of any door handles, so every door fell
// through to plansql.ExtractSelect, which reports the absence of a SELECT.
//
// The disposition is now one dispatch point per door: after a door's own
// statement handlers have had their turn, a parsed statement that is not a
// query is refused 0A000 (feature_not_supported, this engine's class for a
// shape it parses and cannot run) with a message naming the STATEMENT.
//
// The corpus is enumerated FROM THE PARSER, not from the issue: every
// QueryType the parser can produce appears below with the disposition each
// door owes it, and TestEveryParsedStatementTypeIsInTheDoorCensus fails when
// the parser grows a type this census does not name.
//
// Three doors: the embedded API (wadjet.DB.Query), the PostgreSQL wire
// protocol, and the HTTP query endpoint.

// doorCensusEntry is one statement and what each door does with it.
type doorCensusEntry struct {
	name string
	sql  string
	// typ is the QueryType the parser produces. Its presence here is what
	// makes the census enumerable from the parser rather than from memory.
	typ plansql.QueryType
	// refusal is the exact message every door must answer with, or "" when
	// the statement is one the doors execute (a control cell).
	refusal string
	// httpRefusal is the HTTP door's message for a statement the OTHER two
	// doors have a handler for. otherDoorsSay is a substring their answer must
	// carry — the reason their handler declined in this rig — and it is what
	// makes the claim "those doors handle it" an assertion rather than a
	// comment: the dispatch refusal would say "is not supported", and a
	// handler that declined for its own reason says something else.
	httpRefusal   string
	otherDoorsSay string
}

func statementDoorCensus() []doorCensusEntry {
	return []doorCensusEntry{
		// ---- refused on every door: no handler anywhere -------------------
		{name: "alter_table_add_column", sql: "ALTER TABLE hd ADD COLUMN z BIGINT",
			typ: plansql.QueryAlterTable, refusal: "ALTER TABLE is not supported"},
		{name: "alter_table_drop_column", sql: "ALTER TABLE hd DROP COLUMN s",
			typ: plansql.QueryAlterTable, refusal: "ALTER TABLE is not supported"},
		{name: "create_view", sql: "CREATE VIEW hd_v AS SELECT id FROM hd",
			typ: plansql.QueryCreateView, refusal: "CREATE VIEW is not supported"},
		{name: "create_or_replace_view", sql: "CREATE OR REPLACE VIEW hd_v AS SELECT id FROM hd",
			typ: plansql.QueryCreateView, refusal: "CREATE VIEW is not supported"},
		{name: "drop_view", sql: "DROP VIEW hd_v",
			typ: plansql.QueryDropView, refusal: "DROP VIEW is not supported"},
		{name: "explain_alter_table", sql: "EXPLAIN ALTER TABLE hd ADD COLUMN z BIGINT",
			typ: plansql.QueryExplain, refusal: "EXPLAIN ALTER TABLE is not supported"},
		{name: "explain_create_view", sql: "EXPLAIN CREATE VIEW hd_v AS SELECT id FROM hd",
			typ: plansql.QueryExplain, refusal: "EXPLAIN CREATE VIEW is not supported"},
		{name: "explain_drop_table", sql: "EXPLAIN DROP TABLE hd",
			typ: plansql.QueryExplain, refusal: "EXPLAIN DROP TABLE is not supported"},

		// CREATE SNAPSHOT has exactly ONE handler in the tree —
		// Coordinator.ExecuteSQL — and neither wadjet.DB.Query nor this door
		// is it, so all three refuse it like any other unhandled type. It sat
		// in the httpOnly group below for a round, under a comment saying the
		// other two doors run it; they do not. Measured on the embedded door:
		// 0A000 `CREATE SNAPSHOT is not supported`, with EnableAlerts on or
		// off (round-1 B2).
		{name: "create_snapshot", sql: "CREATE SNAPSHOT",
			typ: plansql.QueryCreateSnapshot, refusal: "CREATE SNAPSHOT is not supported"},

		// ---- refused on the HTTP door only: it has no such handler --------
		// The embedded API and pgwire DO have alert handlers — they run these
		// statements when Config.EnableAlerts is set, which
		// TestTheAlertStatementsRunOnTheDoorsThatHandleThem asserts. This rig
		// leaves alerts off, so those two doors decline for their own reason,
		// and the cells below assert that reason rather than assuming it.
		{name: "drop_alert", sql: "DROP ALERT a1",
			typ: plansql.QueryDropAlert, httpRefusal: "DROP ALERT is not supported",
			otherDoorsSay: "alerts are disabled"},
		{name: "alter_alert", sql: "ALTER ALERT a1 DISABLE",
			typ: plansql.QueryAlterAlert, httpRefusal: "ALTER ALERT is not supported",
			otherDoorsSay: "alerts are disabled"},
		{name: "create_alert",
			sql: "CREATE ALERT a1 AS SELECT id FROM hd EVERY 5 MINUTES WEBHOOK 'https://x.invalid'",
			typ: plansql.QueryCreateAlert, httpRefusal: "CREATE ALERT is not supported",
			otherDoorsSay: "alerts are disabled"},

		// ---- controls: statements every door DOES execute -----------------
		// Their presence is what makes this a census rather than a list of
		// refusals: a dispatch guard that refused one of these would be a
		// right→loud move, and these cells are where it shows.
		{name: "select", sql: "SELECT id FROM hd", typ: plansql.QuerySelect},
		{name: "explain_select", sql: "EXPLAIN SELECT id FROM hd", typ: plansql.QueryExplain},
		{name: "describe", sql: "DESCRIBE hd", typ: plansql.QueryDescribe},
		{name: "show_tables", sql: "SHOW TABLES", typ: plansql.QueryShowTables},
		{name: "show_functions", sql: "SHOW FUNCTIONS", typ: plansql.QueryShowFunctions},
		{name: "show_columns", sql: "SHOW COLUMNS FROM hd", typ: plansql.QueryDescribe},
		{name: "create_table", sql: "CREATE TABLE hd_ctl (a BIGINT)", typ: plansql.QueryCreateTable},
		{name: "analyze", sql: "ANALYZE hd", typ: plansql.QueryAnalyzeTable},
		{name: "create_function", sql: "CREATE OR REPLACE FUNCTION hdfn(x) AS (x + 1)",
			typ: plansql.QueryCreateFunction},
		{name: "drop_function", sql: "DROP FUNCTION IF EXISTS hdfn", typ: plansql.QueryDropFunction},
		{name: "insert", sql: "INSERT INTO hd (id, s, d) VALUES (7, 'g', 1.00)", typ: plansql.QueryInsert},
		{name: "update", sql: "UPDATE hd SET s = 'h' WHERE id = 7", typ: plansql.QueryUpdate},
		{name: "delete", sql: "DELETE FROM hd WHERE id = 7", typ: plansql.QueryDelete},
		{name: "merge",
			sql: "MERGE INTO hd USING (SELECT 1 AS id) src ON hd.id = src.id " +
				"WHEN MATCHED THEN UPDATE SET s = 'm'",
			typ: plansql.QueryMerge},
		{name: "drop_table", sql: "DROP TABLE hd_ctl", typ: plansql.QueryDropTable},
	}
}

// TestEveryUnsupportedStatementRefusesTheSameWayOnEveryDoor is #860's gate.
func TestEveryUnsupportedStatementRefusesTheSameWayOnEveryDoor(t *testing.T) {
	base, conn, db := hdSetupWithDB(t)
	ctx := context.Background()

	for _, tc := range statementDoorCensus() {
		t.Run(tc.name, func(t *testing.T) {
			// The parser really does produce the type this entry claims. The
			// census is only enumerable from the parser if this holds.
			parsed, err := plansql.Parse(tc.sql)
			if err != nil {
				t.Fatalf("the parser refuses %q outright (%v); this census is about "+
					"statements it ACCEPTS", tc.sql, err)
			}
			if parsed.Type != tc.typ {
				t.Fatalf("%q parses as %s, the census says %s", tc.sql, parsed.Type, tc.typ)
			}

			if tc.refusal == "" && tc.httpRefusal == "" {
				// Control: a statement every door executes. It may still
				// fail for its own reasons (a name already taken on the
				// second door to run it), but never with the dispatch
				// refusal — that would be a right→loud move.
				controlIsNotRefusedAtDispatch(t, base, conn, db, tc.sql)
				return
			}

			// --- the HTTP door ------------------------------------------
			want := tc.refusal
			if want == "" {
				want = tc.httpRefusal
			}
			status, state, msg := hdPost(t, base, tc.sql)
			if state != "0A000" {
				t.Errorf("HTTP door reported sqlstate %q for %q, want 0A000 (%q)",
					state, tc.sql, msg)
			}
			if msg != want {
				t.Errorf("HTTP door message for %q:\n got  %q\n want %q", tc.sql, msg, want)
			}
			if status != 400 {
				t.Errorf("HTTP door answered %d for %q, want 400", status, tc.sql)
			}

			if tc.httpRefusal != "" {
				// The other two doors HANDLE this statement, so the dispatch
				// guard must NOT be what answers there. Asserted, not assumed:
				// the round-1 review found this branch returning here, which
				// is how a false claim about CREATE SNAPSHOT survived.
				// Each door reports its SQLSTATE its own way, so the check
				// takes both the CLASS and the message rather than matching
				// the message's shape. A suffix test was the first spelling
				// and pgconn defeated it: its Error() ends
				// `… (SQLSTATE 0A000)`, so only the embedded door was ever
				// checked (round-2 P8).
				for _, d := range []struct {
					name string
					run  func(string) (state string, err error)
				}{
					{"pgwire", func(sql string) (string, error) {
						res := conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
						if pe, ok := res.Err.(*pgconn.PgError); ok {
							return pe.Code, res.Err
						}
						return "", res.Err
					}},
					{"embedded", func(sql string) (string, error) {
						_, err := db.Query(ctx, sql)
						return sqlerr.StateOf(err), err
					}},
				} {
					state, err := d.run(tc.sql)
					if err == nil {
						continue // the handler ran it; that is the claim too
					}
					// The dispatch refusal is 0A000 carrying this statement's
					// name and that phrasing — `CREATE SNAPSHOT is not
					// supported`. A handler's own refusal may be 0A000 too
					// (`alerts are disabled`), which is why the message half
					// is the STATEMENT NAME and not just the class.
					if state == "0A000" && strings.Contains(err.Error(), tc.httpRefusal) {
						t.Errorf("the %s door answers %q with the DISPATCH refusal (%s %v); "+
							"this cell claims that door has a handler for it",
							d.name, tc.sql, state, err)
					}
					if !strings.Contains(err.Error(), tc.otherDoorsSay) {
						t.Errorf("the %s door answers %q with %v; want its handler's own "+
							"reason, containing %q", d.name, tc.sql, err, tc.otherDoorsSay)
					}
				}
				return
			}

			// --- pgwire ---------------------------------------------------
			res := conn.ExecParams(ctx, tc.sql, nil, nil, nil, nil).Read()
			if res.Err == nil {
				t.Fatalf("pgwire ANSWERED %q, which every door must refuse", tc.sql)
			}
			pe, ok := res.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("pgwire error for %q is not a PgError: %v", tc.sql, res.Err)
			}
			if pe.Code != "0A000" {
				t.Errorf("pgwire reported %s for %q, want 0A000 (%q)", pe.Code, tc.sql, pe.Message)
			}
			// No stage prefix: the refusal happens at DISPATCH, before any
			// planner code, so nothing has a stage to name. This is the
			// assertion that fails if the guard is reverted and the refusal
			// falls back through ExtractSelect ("extracting SELECT: …").
			if pe.Message != tc.refusal {
				t.Errorf("pgwire message for %q:\n got  %q\n want %q", tc.sql, pe.Message, tc.refusal)
			}

			// --- the embedded API ----------------------------------------
			_, err = db.Query(ctx, tc.sql)
			if err == nil {
				t.Fatalf("the embedded API ANSWERED %q, which every door must refuse", tc.sql)
			}
			if got := sqlerr.StateOf(err); got != "0A000" {
				t.Errorf("the embedded API reported %q for %q, want 0A000 (%v)", got, tc.sql, err)
			}
			if err.Error() != tc.refusal {
				t.Errorf("the embedded API message for %q:\n got  %q\n want %q",
					tc.sql, err.Error(), tc.refusal)
			}
		})
	}
}

// TestTheAlertStatementsRunOnTheDoorsThatHandleThem is what makes the census's
// httpRefusal cells a claim about HANDLERS rather than about a configuration.
//
// The census rig leaves alerts off, so the embedded and pgwire doors decline
// CREATE / ALTER / DROP ALERT with `alerts are disabled` — which distinguishes
// them from the dispatch refusal but does not by itself show a handler exists.
// With Config.EnableAlerts set, they run. The HTTP query endpoint refuses them
// either way: it has no alert handler at all.
//
// The same measurement is what corrected the docs about CREATE SNAPSHOT
// (round-1 B2): with alerts ON, the embedded door still answers
// `CREATE SNAPSHOT is not supported`, because its only handler is
// Coordinator.ExecuteSQL.
func TestTheAlertStatementsRunOnTheDoorsThatHandleThem(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: store, Bucket: "test", EnableAlerts: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.CreateTable(ctx, "alerthd",
		wadjet.Schema{Columns: []wadjet.Column{{Name: "id", Type: wadjet.TypeInt64}}}, nil); err != nil {
		t.Fatal(err)
	}

	srv := New(Config{Addr: ":0", Catalog: db.Catalog()}, nil)
	hs := httptest.NewServer(srv.Mux())
	t.Cleanup(hs.Close)

	for _, sql := range []string{
		"CREATE ALERT f5a AS SELECT id FROM alerthd EVERY 5 MINUTES WEBHOOK 'https://x.invalid'",
		"ALTER ALERT f5a DISABLE",
		"DROP ALERT f5a",
	} {
		if _, err := db.Query(ctx, sql); err != nil {
			t.Errorf("the embedded door refused %q with alerts ENABLED: %v\n"+
				"the census claims this door has a handler for it", sql, err)
		}
		_, state, msg := hdPost(t, hs.URL, sql)
		if state != "0A000" || !strings.HasSuffix(msg, "is not supported") {
			t.Errorf("the HTTP door answered %q with %s %q; it has no alert handler and "+
				"must refuse at dispatch", sql, state, msg)
		}
	}

	// CREATE SNAPSHOT, for contrast: no handler on this door either, alerts
	// on or off.
	if _, err := db.Query(ctx, "CREATE SNAPSHOT"); err == nil {
		t.Error("the embedded door RAN CREATE SNAPSHOT; if it has a handler now, the " +
			"census cell and docs/api-reference.md both have to say so")
	} else if !strings.HasSuffix(err.Error(), "CREATE SNAPSHOT is not supported") {
		t.Errorf("the embedded door refused CREATE SNAPSHOT with %v, want the dispatch refusal", err)
	}
}

// TestEveryParsedStatementTypeIsInTheDoorCensus keeps the census honest.
//
// The corpus above is enumerated from the parser, and the way that claim stays
// true is a walk over the QueryType constants: a type the parser gains without
// a cell here fails this, and so does one whose StatementName was never
// written (RefuseUnsupportedStatement would then name it "this statement").
func TestEveryParsedStatementTypeIsInTheDoorCensus(t *testing.T) {
	covered := make(map[plansql.QueryType]bool)
	for _, tc := range statementDoorCensus() {
		covered[tc.typ] = true
	}
	// QueryUnsupported is the parser's own sentinel and is produced by no
	// parse path; it has no statement to write.
	for typ := plansql.QuerySelect; typ < plansql.QueryUnsupported; typ++ {
		name := typ.StatementName()
		if name == "this statement" || strings.TrimSpace(name) == "" {
			t.Errorf("QueryType %d has no StatementName; a refusal naming it would say "+
				"\"this statement is not supported\"", int(typ))
		}
		if !covered[typ] {
			t.Errorf("QueryType %s (%d) is produced by the parser and no cell of the door "+
				"census names it — add one saying what each door does with it", name, int(typ))
		}
	}
}

// controlIsNotRefusedAtDispatch asserts that a statement the doors execute is
// not what the #860 dispatch guard answers, on any of the three.
func controlIsNotRefusedAtDispatch(t *testing.T, base string, conn *pgconn.PgConn, db *wadjet.DB, sql string) {
	t.Helper()
	const marker = "is not supported"

	_, state, msg := hdPost(t, base, sql)
	if state == "0A000" && strings.HasSuffix(msg, marker) {
		t.Errorf("the HTTP door refuses %q at dispatch (%q); it is a statement this door runs", sql, msg)
	}

	if res := conn.ExecParams(context.Background(), sql, nil, nil, nil, nil).Read(); res.Err != nil {
		if pe, ok := res.Err.(*pgconn.PgError); ok &&
			pe.Code == "0A000" && strings.HasSuffix(pe.Message, marker) {
			t.Errorf("pgwire refuses %q at dispatch (%q); it is a statement this door runs",
				sql, pe.Message)
		}
	}

	if _, err := db.Query(context.Background(), sql); err != nil {
		if sqlerr.StateOf(err) == "0A000" && strings.HasSuffix(err.Error(), marker) {
			t.Errorf("the embedded API refuses %q at dispatch (%v); it is a statement this door runs",
				sql, err)
		}
	}
}
