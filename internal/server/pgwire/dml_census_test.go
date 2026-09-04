package pgwire

// The DML census: every shape in the Arc B round-0 dossier, on all THREE
// doors a user reaches, with PostgreSQL 17's answer recorded beside wadjet's.
//
// Nine of the thirteen issues in the DML cluster had no pin at all when this
// was written — no test anywhere attempted their shape, which is why several
// of them are silent WRONG WRITES that survived twelve releases. A fix with no
// gate on the parent commit cannot be shown to have fixed anything, so the
// corpus comes first and every later fix in the arc is a door's line moving to
// `pg` with its `bug` cleared.
//
// Three doors, because they are three different code paths and they disagree:
//
//	embedded  wadjet.DB.Execute / DB.Query          (the library, and what pgwire calls)
//	simple    pgx QueryExecModeSimpleProtocol       (psql)
//	extended  pgx Query with QueryExecModeExec      (pgx, JDBC, psycopg, every ORM)
//
// Each cell records the COMMAND TAG (or the SQLSTATE) *and* the table state
// afterwards as a full-row digest, because five of the issues are wrong writes
// that report success and a tag alone cannot see one.
//
// `pg` is PostgreSQL 17.11, measured — not remembered. Every `pg` string here
// was produced by TestDMLCensusMatchesPostgres against a live server, and that
// test re-verifies them whenever an oracle is reachable. `emb`/`sim`/`ext` are
// what wadjet answers, one per door. `bug` names the issue; an entry with no
// `bug` is one where every door already agrees with PostgreSQL, and it is a
// REGRESSION GATE — the census fails if such an entry ever stops matching. An
// entry WITH a `bug` fails when it STARTS agreeing, which is the fix's proof:
// delete the pin.
//
// To re-record after a fix:
//
//	WADJET_CENSUS_RECORD=1 go test -run TestDMLCensus ./internal/server/pgwire/ -v
//
// and paste the door lines back in. Never edit an `emb`/`sim`/`ext` string to
// make a test pass without looking at what moved: a right→wrong move here is
// exactly the thing the census exists to catch.

// censusShape is one statement and what each door answers for it.
type censusShape struct {
	name string
	sql  string
	// tbl names the fixture table whose state is digested afterwards.
	// "" means the statement is a QUERY and its own rows are the answer.
	tbl string
	pg  string // PostgreSQL 17, measured
	// One field per door, because the doors CAN disagree and recording that is
	// half of what this corpus is for. Two splits it was built to hold are
	// now closed — the extended protocol's `SELECT 1` for every DML statement
	// (#816) and pgwire's blanket 42000 over an error the engine gave no class
	// (#719) — so every one of the entries below currently records the same
	// answer on all three doors. The fields stay per-door because a split is
	// exactly what a regression here would look like. An empty `sim` means
	// "the same as emb"; an empty `ext` means "the same as sim".
	emb string
	sim string
	ext string
	// pgOne is PostgreSQL's answer through an entry point that carries ONE
	// statement — the extended protocol, a prepared statement, PQexecParams —
	// when that differs from `pg`, which is measured on the SIMPLE protocol.
	//
	// For every shape but one family the two are the same and this is empty.
	// The exception is a MULTI-STATEMENT string: PostgreSQL accepts one ONLY
	// on the simple query protocol and answers 42601 `cannot insert multiple
	// commands into a prepared statement` everywhere else. The census's
	// premise — one PostgreSQL answer, three wadjet doors — is false exactly
	// there, and recording it as a wadjet door split would have blamed wadjet
	// for reproducing PostgreSQL faithfully.
	//
	// The embedded door is held to pgOne because `wadjet.DB.Execute` and
	// `wadjet.DB.Query` return exactly ONE result, like a prepared statement;
	// pgwire's simple door is held to `pg`, because that is the door
	// PostgreSQL gives multi-statement to.
	pgOne string
	bug   string // issue number, or "" when every door already agrees with PG
}

// doors returns the recorded answer for each door, defaults resolved.
func (s censusShape) doors() (emb, sim, ext string) {
	emb = s.emb
	sim = s.sim
	if sim == "" {
		sim = emb
	}
	ext = s.ext
	if ext == "" {
		ext = sim
	}
	return emb, sim, ext
}

// pgDoors returns the PostgreSQL answer each door is held to.
func (s censusShape) pgDoors() (emb, sim, ext string) {
	one := s.pgOne
	if one == "" {
		one = s.pg
	}
	return one, s.pg, one
}

func censusShapes() []censusShape {
	return []censusShape{
		// ---------------------------------------------------------------
		// #814 — INSERT does not resolve its column list.
		// ---------------------------------------------------------------
		{name: "#814 insert names only an unknown column", tbl: "pr",
			sql: "INSERT INTO arcb_pr (nosuchcol) VALUES (1)",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#814 insert drops the unknown column and writes the row", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, nosuchcol) VALUES (9, 1)",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#814 insert with every real column plus an unknown", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name, nosuchcol) VALUES (9, 9, 'z', 1)",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#814 insert unknown column with unparseable text", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, nosuchcol) VALUES (8, 'zz')",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#814 insert violating NOT NULL", tbl: "pr",
			sql: "INSERT INTO arcb_pr (n) VALUES (1)",
			pg:  "state=23502 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=23502 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#814 insert value count mismatch", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n) VALUES (5)",
			pg:  "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42601 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control DELETE names an unknown column", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE nosuchcol = 1",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control UPDATE SET names an unknown column", tbl: "pr",
			sql: "UPDATE arcb_pr SET nosuchcol = 1",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},

		// ---------------------------------------------------------------
		// #721 — a WHERE comparing a column to a literal of another type.
		// ---------------------------------------------------------------
		{name: "#721 delete int column = text garbage", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = 'abc'",
			pg:  "state=22P02 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=22P02 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#721 delete int column > text garbage", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id > 'abc'",
			pg:  "state=22P02 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=22P02 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#721 update int column = text garbage", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 1 WHERE id = 'abc'",
			pg:  "state=22P02 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=22P02 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#721 delete text column = number", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE name = 5",
			pg:  "state=42883 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42883 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#721 delete text column > number", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE name > 5",
			pg:  "state=42883 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42883 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#721 delete text column = fractional number", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE name = 5.5",
			pg:  "state=42883 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42883 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#721 delete int column = boolean", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = true",
			pg:  "state=42883 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42883 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#721 select text column > number", sql: "SELECT count(*) AS c FROM arcb_pr WHERE name > 5",
			pg:  "state=42883",
			emb: "rows=[3]",
			bug: "#721"},
		{name: "#721 select int column = boolean", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id = true",
			pg:  "state=42883",
			emb: "state=22P02",
			bug: "#721"},
		{name: "#721 select int column = text garbage", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id = 'abc'",
			pg:  "state=22P02",
			emb: "state=22P02"},
		// #721's BOUNDARY. The refusal acts on three pairs and no others, and
		// every one of these is just outside it: a pair that must keep working
		// is as much a claim as a pair that must be refused (correctness-fix
		// protocol rule 11).
		{name: "#721 boundary text column = quoted text", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE name = 'a'",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "#721 boundary text column > quoted text", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE name > 'b'",
			pg:  "tag=DELETE 1 table=[1:10:a 2:20:b]",
			emb: "tag=DELETE 1 table=[1:10:a 2:20:b]"},
		{name: "#721 boundary int column = number", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = 2",
			pg:  "tag=DELETE 1 table=[1:10:a 3:30:c]",
			emb: "tag=DELETE 1 table=[1:10:a 3:30:c]"},
		{name: "#721 boundary timestamp column = quoted timestamp", tbl: "ts",
			sql: "DELETE FROM arcb_ts WHERE t = '2000-01-01T00:00:00Z'",
			pg:  "tag=DELETE 1 table=[]",
			emb: "tag=DELETE 1 table=[]"},
		{name: "#721 boundary bool literal against a bool column", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE (id = 1) = true",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "#721 plan-time refusal over an EMPTY table", tbl: "em",
			sql: "DELETE FROM arcb_empty WHERE id = 'abc'",
			pg:  "state=22P02 table=[]",
			emb: "state=22P02 table=[]"},
		{name: "#721 plan-time text-vs-number over an EMPTY table", tbl: "em",
			sql: "DELETE FROM arcb_empty WHERE name > 5",
			pg:  "state=42883 table=[]",
			emb: "state=42883 table=[]"},
		{name: "#721 update refuses the pair too", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 1 WHERE name > 5",
			pg:  "state=42883 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42883 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control delete int column = quoted number", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = '1'",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},

		// ---------------------------------------------------------------
		// #689 — MERGE, a target row two source rows match.
		// ---------------------------------------------------------------
		{name: "#689 two source rows update one target", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_dup AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n",
			pg:  "state=21000 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=21000 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#689 two source rows delete one target", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_dup AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=21000 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=21000 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#689 unknown target column in ON", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.nosuchcol = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#689 unknown source column in ON", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.nosuchcol WHEN MATCHED THEN DELETE",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#689 unknown source column in ON, subquery source", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n FROM arcb_src) AS s ON t.id = s.nosuchcol WHEN MATCHED THEN DELETE",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#689 unknown target column in ON, subquery source", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n FROM arcb_src) AS s ON t.nosuchcol = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		// The BOUNDARY of the subquery-source ON fix. Knowing the source's
		// column NAMES is enough to resolve an ON key and not enough to
		// EVALUATE a SET expression — a subquery's output has no declared
		// types, and inferring them from boxed values is how a DECIMAL or a
		// DATE (both boxed as strings) gets silently mistyped. The first
		// shape must keep working, the second must keep REFUSING.
		{name: "#689 subquery source, ON resolves and DELETE fires", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n FROM arcb_src) AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[2:20:b 3:30:c]"},
		{name: "#689 subquery source, a bare source reference in SET", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n FROM arcb_src) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n",
			pg:  "tag=MERGE 1 table=[1:100:a 2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[1:100:a 2:20:b 3:30:c]"},
		{name: "#689 subquery source, a COMPUTED SET expression still refuses", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n FROM arcb_src) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n + 1",
			pg:  "tag=MERGE 1 table=[1:101:a 2:20:b 3:30:c]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#689"},
		{name: "control merge upsert", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED THEN UPDATE SET n = s.n " +
				"WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)",
			pg:  "tag=MERGE 2 table=[1:100:a 2:20:b 3:30:c 4:400:y]",
			emb: "tag=MERGE 2 table=[1:100:a 2:20:b 3:30:c 4:400:y]"},

		// ---------------------------------------------------------------
		// #690 — a string literal that spells NULL, and doubled apostrophes.
		// ---------------------------------------------------------------
		{name: "#690 update SET a literal spelled NULL", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = 'NULL' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:NULL 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:NULL 2:20:b 3:30:c]"},
		{name: "#690 update SET a literal spelled null", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = 'null' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:null 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:null 2:20:b 3:30:c]"},
		{name: "#690 update SET a quoted apostrophe literal", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = '''a''' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:'a' 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:'a' 2:20:b 3:30:c]"},
		{name: "#690 insert a literal spelled NULL", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (9, 9, 'NULL')",
			pg:  "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 9:9:NULL]",
			emb: "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 9:9:NULL]"},
		{name: "#690 insert a literal spelled null", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (6, 6, 'null')",
			pg:  "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 6:6:null]",
			emb: "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 6:6:null]"},
		{name: "#690 insert a quoted apostrophe literal", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (7, 7, '''a''')",
			pg:  "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 7:7:'a']",
			emb: "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 7:7:'a']"},
		{name: "#690 merge SET a literal spelled NULL", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = 'NULL'",
			pg:  "tag=MERGE 1 table=[1:10:NULL 2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[1:10:NULL 2:20:b 3:30:c]"},
		{name: "control update SET the NULL keyword", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = NULL WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:~ 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:~ 2:20:b 3:30:c]"},
		{name: "control update SET a literal with a comma", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = 'a,b' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:a,b 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:a,b 2:20:b 3:30:c]"},
		{name: "control update SET an escaped apostrophe", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = 'O''Brien' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:O'Brien 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:O'Brien 2:20:b 3:30:c]"},

		// ---------------------------------------------------------------
		// #722 / #690 bullet 3 — a CASE inside a MERGE clause.
		// ---------------------------------------------------------------
		{name: "#722 CASE in the WHEN MATCHED AND condition", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED AND CASE WHEN s.n > 1 THEN true ELSE false END THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[2:20:b 3:30:c]"},
		{name: "#722 CASE in the UPDATE SET action", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED THEN UPDATE SET n = CASE WHEN s.n > 50 THEN 1 ELSE 2 END",
			pg:  "tag=MERGE 1 table=[1:1:a 2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[1:1:a 2:20:b 3:30:c]"},
		{name: "#722 CASE in the INSERT VALUES action", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, CASE WHEN s.n > 50 THEN 1 ELSE 2 END, s.name)",
			pg:  "tag=MERGE 1 table=[1:10:a 2:20:b 3:30:c 4:1:y]",
			emb: "tag=MERGE 1 table=[1:10:a 2:20:b 3:30:c 4:1:y]"},
		// The ON scan no longer truncates this, so the statement now reaches
		// the executor — where MERGE's ON is equality-between-the-two-relations
		// and nothing else, and says so with 0A000. The residual is that
		// restriction, not the parser's.
		{name: "#722 CASE in the ON condition", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s " +
				"ON CASE WHEN t.id = s.id THEN true ELSE false END WHEN MATCHED THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#722"},
		{name: "control merge AND condition without a CASE", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED AND s.n > 1 THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[2:20:b 3:30:c]"},

		// ---------------------------------------------------------------
		// #692 — TIMESTAMP literals.
		// ---------------------------------------------------------------
		{name: "#692 insert a timestamp carrying an offset", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-01-01T05:30:00+05:30')",
			pg:  "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T05:30:00Z]",
			emb: "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T05:30:00Z]"},
		{name: "#692 update to a timestamp carrying an offset", tbl: "ts",
			sql: "UPDATE arcb_ts SET t = '2020-01-01T05:30:00+05:30' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:2020-01-01T05:30:00Z]",
			emb: "tag=UPDATE 1 table=[1:2020-01-01T05:30:00Z]"},
		{name: "#692 insert an impossible day", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-02-30T00:00:00')",
			pg:  "state=22008 table=[1:2000-01-01T00:00:00Z]",
			emb: "state=22008 table=[1:2000-01-01T00:00:00Z]"},
		{name: "#692 insert an impossible month", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-13-01T00:00:00')",
			pg:  "state=22008 table=[1:2000-01-01T00:00:00Z]",
			emb: "state=22008 table=[1:2000-01-01T00:00:00Z]"},
		{name: "#692 insert an impossible hour", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-01-01T25:00:00')",
			pg:  "state=22008 table=[1:2000-01-01T00:00:00Z]",
			emb: "state=22008 table=[1:2000-01-01T00:00:00Z]"},
		{name: "#692 insert text that is not a timestamp", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, 'not-a-timestamp')",
			pg:  "state=22007 table=[1:2000-01-01T00:00:00Z]",
			emb: "state=22007 table=[1:2000-01-01T00:00:00Z]"},
		// The pin is GONE: this cell carried `emb: "rows=[~]"` and
		// `bug: "#692"` because the CAST answered NULL. #836 routed the
		// temporal conversion through the per-row error channel the numeric
		// casts already used, so both engines now raise 22008 and the cell is
		// an ordinary agreement.
		{name: "#692 cast an impossible day", sql: "SELECT CAST('2020-02-30T00:00:00' AS TIMESTAMP) AS c",
			pg:  "state=22008",
			emb: "state=22008"},
		{name: "#836 cast text that is not a timestamp", sql: "SELECT CAST('not-a-timestamp' AS TIMESTAMP) AS c",
			pg:  "state=22007",
			emb: "state=22007"},
		{name: "#840 cast text that is not a date", sql: "SELECT CAST('not-a-date' AS DATE) AS c",
			pg:  "state=22007",
			emb: "state=22007"},
		{name: "#840 cast an impossible day to date", sql: "SELECT CAST('2020-02-30' AS DATE) AS c",
			pg:  "state=22008",
			emb: "state=22008"},
		{name: "control insert a UTC timestamp", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-01-01T05:30:00Z')",
			pg:  "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T05:30:00Z]",
			emb: "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T05:30:00Z]"},

		// ---------------------------------------------------------------
		// #699 — the assignment cast from float8 to an integer column.
		// ---------------------------------------------------------------
		{name: "#699 update an integer column from a float column", tbl: "fl",
			sql: "UPDATE arcb_fl SET n = f",
			pg:  "tag=UPDATE 5 table=[1:2.5:2 2:-2.5:-2 3:0.5:0 4:3.5:4 5:1.5:2]",
			emb: "tag=UPDATE 5 table=[1:2.5:2 2:-2.5:-2 3:0.5:0 4:3.5:4 5:1.5:2]"},
		{name: "#699 update an integer column from a float8 literal", tbl: "fl",
			sql: "UPDATE arcb_fl SET n = 2.5::float8 WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:2.5:2 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]",
			emb: "tag=UPDATE 1 table=[1:2.5:2 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]"},
		{name: "control update an integer column from a numeric literal", tbl: "fl",
			sql: "UPDATE arcb_fl SET n = 2.5 WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:2.5:3 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]",
			emb: "tag=UPDATE 1 table=[1:2.5:3 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]"},
		{name: "control update an integer column from a negative numeric literal", tbl: "fl",
			sql: "UPDATE arcb_fl SET n = 0 - 2.5 WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:2.5:-3 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]",
			emb: "tag=UPDATE 1 table=[1:2.5:-3 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]"},

		// ---------------------------------------------------------------
		// #710 — `= ANY(ARRAY[…])` and row-value comparison, both paths.
		// ---------------------------------------------------------------
		{name: "#710 delete = ANY(ARRAY[…])", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = ANY(ARRAY[1,2])",
			pg:  "tag=DELETE 2 table=[3:30:c]",
			emb: "tag=DELETE 2 table=[3:30:c]"},
		{name: "#710 delete a row-value comparison", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE (id, n) = (1, 10)",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "#710 update = ANY(ARRAY[…])", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 0 WHERE id = ANY(ARRAY[1,2])",
			pg:  "tag=UPDATE 2 table=[1:0:a 2:0:b 3:30:c]",
			emb: "tag=UPDATE 2 table=[1:0:a 2:0:b 3:30:c]"},
		{name: "#710 update a row-value comparison", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 0 WHERE (id, n) = (1, 10)",
			pg:  "tag=UPDATE 1 table=[1:0:a 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:0:a 2:20:b 3:30:c]"},
		{name: "#710 delete <> ALL(ARRAY[…])", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id <> ALL(ARRAY[1,2])",
			pg:  "tag=DELETE 1 table=[1:10:a 2:20:b]",
			emb: "tag=DELETE 1 table=[1:10:a 2:20:b]"},
		{name: "#710 select = ANY(ARRAY[…])", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id = ANY(ARRAY[1,2])",
			pg:  "rows=[2]",
			emb: "rows=[2]"},
		{name: "#710 select a row-value comparison", sql: "SELECT count(*) AS c FROM arcb_pr WHERE (id, n) = (1, 10)",
			pg:  "rows=[1]",
			emb: "rows=[1]"},
		{name: "#710 select a row-value IN list", sql: "SELECT count(*) AS c FROM arcb_pr WHERE (id, n) IN ((1,10),(2,20))",
			pg:  "rows=[2]",
			emb: "rows=[2]"},
		{name: "#710 select = ANY(subquery)", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id = ANY(SELECT id FROM arcb_src)",
			pg:  "rows=[1]",
			emb: "rows=[1]"},
		{name: "#710 select <> ALL(ARRAY[…])", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id <> ALL(ARRAY[1,2])",
			pg:  "rows=[1]",
			emb: "rows=[1]"},
		{name: "#710 select a row-value ordering", sql: "SELECT count(*) AS c FROM arcb_pr WHERE (id, n) < (2, 0)",
			pg:  "rows=[1]",
			emb: "rows=[1]"},
		{name: "control select IN list", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id IN (1,2)",
			pg:  "rows=[2]",
			emb: "rows=[2]"},

		// ---------------------------------------------------------------
		// #688 — a subquery in a DML predicate.
		//
		// These five were the deferral's pins, each carrying PostgreSQL's
		// answer beside an 0A000. They ANSWER now, and the pins are deleted
		// rather than edited: a `bug:` entry that starts agreeing FAILS, so
		// removing it is what the fix has to earn.
		//
		// The mechanism is one sentence: the predicate is still COMPILED and
		// not planned (ADR-0031), but the compile is given a runner AND the
		// outer scope a DML statement has — one relation, the target, under
		// its alias or its name. The scope is what the bounded repair ADR-0031
		// forbade was missing, and it is why the CORRELATED EXISTS below is
		// here rather than still pinned.
		// ---------------------------------------------------------------
		{name: "#688 delete IN (SELECT …)", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id IN (SELECT id FROM arcb_src)",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "#688 delete correlated EXISTS", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE EXISTS (SELECT 1 FROM arcb_src s WHERE s.id = arcb_pr.id)",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "#688 update IN (SELECT …)", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 0 WHERE id IN (SELECT id FROM arcb_src)",
			pg:  "tag=UPDATE 1 table=[1:0:a 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:0:a 2:20:b 3:30:c]"},
		{name: "#688 delete NOT IN (SELECT …)", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id NOT IN (SELECT id FROM arcb_src)",
			pg:  "tag=DELETE 2 table=[1:10:a]",
			emb: "tag=DELETE 2 table=[1:10:a]"},
		{name: "#688 delete against a scalar subquery", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE n < (SELECT max(n) FROM arcb_src)",
			pg:  "tag=DELETE 3 table=[]",
			emb: "tag=DELETE 3 table=[]"},
		// The MERGE spelling. docs/sql-reference.md's limitation bullet names
		// it and the corpus did not, so the documented refusal set and the
		// pinned one disagreed by one shape (review P3). A WHEN condition is
		// compiled by a second site (mergeEvaluator.compile), whose outer
		// scope is the MERGED row rather than the target alone.
		{name: "#688 merge WHEN MATCHED AND a subquery", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED AND t.id IN (SELECT id FROM arcb_src) THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[2:20:b 3:30:c]"},

		// The shapes the five pins did not reach, each measured on live
		// PostgreSQL 17 for this fixture.
		//
		// THE SNAPSHOT, which is the one a merge-on-read engine can get wrong
		// in a way no other cell would see: the subquery reads the TARGET
		// TABLE, and PostgreSQL answers it from the PRE-STATEMENT state. Here
		// `n > 15` selects rows 2 and 3 and both are deleted; an engine that
		// let its own markers become visible mid-statement would delete
		// fewer.
		{name: "#688 delete IN a subquery over the TARGET table", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id IN (SELECT id FROM arcb_pr WHERE n > 15)",
			pg:  "tag=DELETE 2 table=[1:10:a]",
			emb: "tag=DELETE 2 table=[1:10:a]"},
		{name: "#688 delete correlated NOT EXISTS", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE NOT EXISTS (SELECT 1 FROM arcb_src s WHERE s.id = arcb_pr.id)",
			pg:  "tag=DELETE 2 table=[1:10:a]",
			emb: "tag=DELETE 2 table=[1:10:a]"},
		// The ALIASED correlation. An alias HIDES the table name on this door
		// (`DELETE FROM pr AS a WHERE pr.id = 1` is 42P01), so the outer scope
		// the compiler is given has to carry the alias or the correlation is
		// not recognised at all.
		{name: "#688 delete correlated EXISTS under an alias", tbl: "pr",
			sql: "DELETE FROM arcb_pr AS a WHERE EXISTS (SELECT 1 FROM arcb_src s WHERE s.id = a.id)",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		// A correlated EXISTS whose subquery also carries an INNER-only
		// predicate, so "the correlation was dropped" cannot pass as this: if
		// it were, every row would match and the table would be emptied.
		{name: "#688 delete correlated EXISTS with an inner predicate", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE EXISTS (SELECT 1 FROM arcb_src s " +
				"WHERE s.id = arcb_pr.id AND s.n > 200)",
			pg:  "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]",
			emb: "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#688 update against a correlated scalar subquery", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 0 WHERE n < (SELECT max(s.n) FROM arcb_src s WHERE s.id = arcb_pr.id)",
			pg:  "tag=UPDATE 1 table=[1:0:a 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:0:a 2:20:b 3:30:c]"},
		// A subquery that cannot RUN fails the STATEMENT and writes nothing.
		// On a WRITE door that is the difference between refusing and deleting
		// the wrong rows, and it is ADR-0021 §1c's rule reaching this door
		// with everything else.
		{name: "#688 delete IN a subquery over an unknown relation", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id IN (SELECT id FROM arcb_nosuch)",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42P01 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#688 delete IN a subquery that fails at run time", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id IN (SELECT id/0 FROM arcb_src)",
			pg:  "state=22012 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=22012 table=[1:10:a 2:20:b 3:30:c]"},
		// A SCALAR SUBQUERY IS AT MOST ONE ROW, and the second row is 21000 —
		// never the first row (ADR-0021 §5). This is the cell the `max(n)`
		// spelling above could not be: that subquery is single-row by
		// construction, so the corpus could not see what a multi-row one did.
		// It took the first row the runner returned and DELETED THE WHOLE
		// TABLE, on all three doors, where PostgreSQL raises and deletes
		// nothing. The write door is where it mattered; the read doors had
		// the same gap and are censused in
		// coordinator.TestArcD5CorrelationMatchesPostgres.
		{name: "#688 delete against a MULTI-ROW scalar subquery", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE n < (SELECT n FROM arcb_src)",
			pg:  "state=21000 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=21000 table=[1:10:a 2:20:b 3:30:c]"},

		// A CORRELATED subquery over the TARGET TABLE. It refused with 42P01
		// naming a table that plainly exists, because the subquery reads the
		// target under an ALIAS and the correlation analysis let that alias
		// lend its table's name to the outer reference — so `arcb_pr.n` was
		// read as the INNER relation's own column. An alias hides the table
		// name now (plansql.collectInnerTables), which is ADR-0021 §5a's
		// classifier repair; these two are its DML spelling.
		{name: "#688 delete correlated EXISTS over the TARGET table", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE EXISTS (SELECT 1 FROM arcb_pr b WHERE b.n > arcb_pr.n)",
			pg:  "tag=DELETE 2 table=[3:30:c]",
			emb: "tag=DELETE 2 table=[3:30:c]"},
		{name: "#688 delete correlated IN over the TARGET table", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id IN (SELECT b.id FROM arcb_pr b WHERE b.n > arcb_pr.n)",
			pg:  "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]",
			emb: "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]"},

		// THE #686 RULE, INSIDE A SUBQUERY. An alias hides the table name, so
		// a subquery that spells the target by its table name names no
		// relation in scope. Outside a subquery this door has refused it
		// since #686 — `DELETE FROM pr AS a WHERE pr.id = 1` is 42P01, and
		// checkDMLColumns says why: accepting both spellings would let one
		// statement mean two things depending on which resolved. Putting BOTH
		// into the subquery's outer scope held the rule outside and broke it
		// inside, so this ANSWERED where PostgreSQL raises.
		{name: "#688 delete AS alias, subquery names the target by table name", tbl: "pr",
			sql: "DELETE FROM arcb_pr AS a WHERE EXISTS (SELECT 1 FROM arcb_src s WHERE s.id = arcb_pr.id)",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42P01 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#688 control delete AS alias, subquery names the ALIAS", tbl: "pr",
			sql: "DELETE FROM arcb_pr AS a WHERE EXISTS (SELECT 1 FROM arcb_src s WHERE s.id = a.id)",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},

		// STILL REFUSED, and pinned rather than described: a subquery in the
		// SET LIST. It is a different site — ResolveDMLSetClauses, which
		// resolves an assignment against the target column's declaration —
		// and this arc did not touch it.
		{name: "#688 update SET from a subquery", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = (SELECT max(n) FROM arcb_src) WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:400:a 2:20:b 3:30:c]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#688"},

		// ---------------------------------------------------------------
		// #718 — MERGE WHEN NOT MATCHED BY SOURCE / BY TARGET.
		//
		// DEFERRED, and these cells are the deferral's record: every one
		// carries PostgreSQL 17's own answer beside wadjet's 0A000, so the
		// arc that implements the clause kinds has the semantics measured
		// rather than remembered — including the two spellings PostgreSQL
		// answers with a SYNTAX error rather than a feature refusal, the
		// TARGET-ONLY scope of a BY SOURCE clause, and the fact that "matched
		// by source" is decided by the ON condition and NOT by whether a WHEN
		// MATCHED clause fired. ADR-0031's neighbour in the parser
		// (internal/planner/sql/parser.go) carries the mechanism.
		// ---------------------------------------------------------------
		{name: "#718 BY SOURCE THEN DELETE", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE",
			pg:  "tag=MERGE 2 table=[1:10:a]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 BY SOURCE THEN UPDATE", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET n = 0",
			pg:  "tag=MERGE 2 table=[1:10:a 2:0:b 3:0:c]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 BY SOURCE with a condition on the target", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE AND t.n > 25 THEN DELETE",
			pg:  "tag=MERGE 1 table=[1:10:a 2:20:b]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 BY SOURCE with a bare column name", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE AND n > 25 THEN DELETE",
			pg:  "tag=MERGE 1 table=[1:10:a 2:20:b]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 BY SOURCE cannot see the source", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET n = s.n",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 two BY SOURCE clauses, the first firing wins", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE AND t.n > 25 THEN DELETE " +
				"WHEN NOT MATCHED BY SOURCE THEN UPDATE SET n = 0",
			pg: "tag=MERGE 2 table=[1:10:a 2:0:b]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 a MATCHED clause that does not fire still matched by source", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED AND t.n > 99 THEN DELETE " +
				"WHEN NOT MATCHED BY SOURCE THEN UPDATE SET n = 0",
			pg: "tag=MERGE 2 table=[1:10:a 2:0:b 3:0:c]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 BY SOURCE THEN INSERT is a syntax error", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN INSERT (id, n, name) VALUES (1, 1, 'z')",
			pg:  "state=42601 table=[1:10:a 2:20:b 3:30:c]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 BY TARGET THEN INSERT", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY TARGET THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)",
			pg:  "tag=MERGE 1 table=[1:10:a 2:20:b 3:30:c 4:400:y]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 BY TARGET THEN DELETE is a syntax error", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN NOT MATCHED BY TARGET THEN DELETE",
			pg:  "state=42601 table=[1:10:a 2:20:b 3:30:c]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},
		{name: "#718 the full-sync upsert", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n " +
				"WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name) " +
				"WHEN NOT MATCHED BY SOURCE THEN DELETE",
			pg: "tag=MERGE 4 table=[1:100:a 4:400:y]", emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]", bug: "#718"},

		// ---------------------------------------------------------------
		// #837 — a MERGE whose two relations have the SAME EXPOSED NAME.
		//
		// The rule PostgreSQL applies is over EXPOSED names — the alias where
		// one is written, the relation's own name otherwise — and the last two
		// entries are the controls that say so: two DIFFERENT aliases over the
		// SAME table is legal, and a source aliased with the target's TABLE
		// NAME is legal when the target itself is aliased to something else.
		// A rule spelled "the source is not the target table" would refuse
		// both, and PostgreSQL answers both.
		// ---------------------------------------------------------------
		{name: "#837 source alias equals the target name", tbl: "pr",
			sql: "MERGE INTO arcb_pr USING arcb_src AS arcb_pr ON arcb_pr.id = arcb_pr.id WHEN MATCHED THEN DELETE",
			pg:  "state=42712 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42712 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#837 target alias equals the source name", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS arcb_src USING arcb_src ON arcb_src.id = arcb_src.id WHEN MATCHED THEN DELETE",
			pg:  "state=42712 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42712 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#837 self-merge with no aliases", tbl: "pr",
			sql: "MERGE INTO arcb_pr USING arcb_pr ON arcb_pr.id = arcb_pr.id WHEN MATCHED THEN DELETE",
			pg:  "state=42712 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42712 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#837 both relations aliased to the same name", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS x USING arcb_src AS x ON x.id = x.id WHEN MATCHED THEN DELETE",
			pg:  "state=42712 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42712 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#837 subquery source aliased with the target name", tbl: "pr",
			sql: "MERGE INTO arcb_pr USING (SELECT id, n, name FROM arcb_src) AS arcb_pr ON arcb_pr.id = arcb_pr.id WHEN MATCHED THEN DELETE",
			pg:  "state=42712 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42712 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#837 subquery source aliased with the target alias", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n, name FROM arcb_src) AS t ON t.id = t.id WHEN MATCHED THEN DELETE",
			pg:  "state=42712 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42712 table=[1:10:a 2:20:b 3:30:c]"},
		// An UNQUOTED alias folds, so this one collides in both engines. The
		// comparison itself is byte-exact; the folding is the parser's.
		{name: "#837 an unquoted alias folds and collides", tbl: "pr",
			sql: "MERGE INTO arcb_pr USING arcb_src AS ARCB_PR ON arcb_pr.id = arcb_pr.id WHEN MATCHED THEN DELETE",
			pg:  "state=42712 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42712 table=[1:10:a 2:20:b 3:30:c]"},
		// A DELIMITED alias does NOT fold, so it does not collide — PostgreSQL
		// runs this one (measured: MERGE 1) and so does wadjet, on all three
		// doors. This cell was PINNED `#837-folding` and the pin said it would
		// fail the day folding preserved quoting; Arc D4 made the lexer fold
		// unquoted identifiers and leave delimited ones alone (#731), the
		// MERGE parser stopped lower-casing its target, source and alias
		// reads, and the pin started agreeing. Deleting it is that fix's
		// proof, and the cell is promoted to PostgreSQL's own answer.
		{name: "#837 a delimited alias does not fold and is legal", tbl: "pr",
			sql: `MERGE INTO arcb_pr USING arcb_src AS "ARCB_PR" ON arcb_pr.id = "ARCB_PR".id WHEN MATCHED THEN DELETE`,
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[2:20:b 3:30:c]"},
		{name: "#837 the UPDATE arm is refused before it writes", tbl: "pr",
			sql: "MERGE INTO arcb_pr USING arcb_src AS arcb_pr ON arcb_pr.id = arcb_pr.id WHEN MATCHED THEN UPDATE SET n = 0",
			pg:  "state=42712 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42712 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control #837 self-merge with DISTINCT aliases is legal", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_pr AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "tag=MERGE 3 table=[]",
			emb: "tag=MERGE 3 table=[]"},
		{name: "control #837 source aliased with the target's TABLE name is legal", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS arcb_pr ON t.id = arcb_pr.id WHEN MATCHED THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[2:20:b 3:30:c]"},

		// ---------------------------------------------------------------
		// #719 — the SQLSTATE a DML runtime failure carries.
		// ---------------------------------------------------------------
		{name: "#719 delete from a table that does not exist", tbl: "pr",
			sql: "DELETE FROM nosuchtable815 WHERE id = 1",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42P01 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#719 update a table that does not exist", tbl: "pr",
			sql: "UPDATE nosuchtable815 SET n = 1 WHERE id = 1",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42P01 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#719 insert into a table that does not exist", tbl: "pr",
			sql: "INSERT INTO nosuchtable815 (id) VALUES (1)",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42P01 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#719 merge into a table that does not exist", tbl: "pr",
			sql: "MERGE INTO nosuchtable815 AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42P01 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control merge FROM a table that does not exist", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING nosuchtable815 AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42P01 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control select from a table that does not exist", sql: "SELECT * FROM nosuchtable815",
			pg:  "state=42P01",
			emb: "state=42P01"},

		// ---------------------------------------------------------------
		// ---------------------------------------------------------------
		// ROUND 1 — one cell per adversarial-review finding. Each was a
		// silent wrong write, a wrong answer or a right→wrong regression that
		// every gate in the tree was structurally unable to see.
		// ---------------------------------------------------------------

		// B1 — a PADDED quoted literal. The #690 commit moved TrimSpace to
		// the outside of the quotes, so `'  7  '` reached strconv and an
		// INSERT/UPDATE that PostgreSQL and this engine's own base commit
		// answer was REFUSED. Whitespace is data for TEXT and ignorable for
		// every other type, which is PostgreSQL's rule.
		{name: "B1 padded numeric literal in UPDATE", tbl: "fl",
			sql: "UPDATE arcb_fl SET n = '  7  ' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:2.5:7 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]",
			emb: "tag=UPDATE 1 table=[1:2.5:7 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]"},
		{name: "B1 padded float literal in UPDATE", tbl: "fl",
			sql: "UPDATE arcb_fl SET f = '  7.5  ' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:7.5:0 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]",
			emb: "tag=UPDATE 1 table=[1:7.5:0 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]"},
		{name: "B1 padded literal in INSERT", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES ('  9  ', '  90  ', '  padded  ')",
			pg:  "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 9:90:  padded  ]",
			emb: "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 9:90:  padded  ]"},
		{name: "B1 padded bool literal", tbl: "mx",
			sql: "UPDATE arcb_mix SET flag = '  true  ' WHERE id = 2",
			pg:  "tag=UPDATE 1 table=[1:true 2:true]",
			emb: "tag=UPDATE 1 table=[1:true 2:true]"},

		// B2 — the round trip. A row must be findable by the literal that
		// wrote it; the #692 commit fixed the write path and left the
		// comparison path applying the offset, so it was not.
		{name: "B2 offset literal round trip, insert", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-06-01T12:00:00+05:30')",
			pg:  "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-06-01T12:00:00Z]",
			emb: "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-06-01T12:00:00Z]"},
		{name: "B2 offset literal round trip, select", sql: "SELECT count(*) AS c FROM arcb_ts WHERE t = '2000-01-01 00:00:00+00:00'",
			pg:  "rows=[1]",
			emb: "rows=[1]"},
		{name: "B2 space-separated offset literal", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-01-01 12:00:00-08:00')",
			pg:  "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T12:00:00Z]",
			emb: "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T12:00:00Z]"},
		{name: "B2 two-digit offset literal", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-01-01 12:00:00-08')",
			pg:  "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T12:00:00Z]",
			emb: "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T12:00:00Z]"},
		{name: "P9 hour 24 is the next day", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-01-01 24:00:00')",
			pg:  "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-02T00:00:00Z]",
			emb: "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-02T00:00:00Z]"},
		{name: "P9 second 60 is the next minute", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-01-01 23:59:60')",
			pg:  "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-02T00:00:00Z]",
			emb: "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-02T00:00:00Z]"},

		// B5 — the DECLARED #721 exclusion. Every one of these EMPTIED its
		// table; PostgreSQL refuses the operator categorically.
		{name: "B5 timestamp column > number", tbl: "mx",
			sql: "DELETE FROM arcb_mix WHERE ts > 5",
			pg:  "state=42883 table=[1:true 2:false]",
			emb: "state=42883 table=[1:true 2:false]"},
		{name: "B5 bool column > number", tbl: "mx",
			sql: "DELETE FROM arcb_mix WHERE flag > 0",
			pg:  "state=42883 table=[1:true 2:false]",
			emb: "state=42883 table=[1:true 2:false]"},
		{name: "B5 bool column = number", tbl: "mx",
			sql: "DELETE FROM arcb_mix WHERE flag = 1",
			pg:  "state=42883 table=[1:true 2:false]",
			emb: "state=42883 table=[1:true 2:false]"},
		{name: "B5 inet column > number", tbl: "mx",
			sql: "DELETE FROM arcb_mix WHERE ip > 5",
			pg:  "state=42883 table=[1:true 2:false]",
			emb: "state=42883 table=[1:true 2:false]"},
		{name: "B5 bytes column > number", tbl: "mx",
			sql: "DELETE FROM arcb_mix WHERE raw > 5",
			pg:  "state=42883 table=[1:true 2:false]",
			emb: "state=42883 table=[1:true 2:false]"},
		{name: "B5 boundary decimal column > number", tbl: "mx",
			sql: "DELETE FROM arcb_mix WHERE d > 5",
			pg:  "tag=DELETE 0 table=[1:true 2:false]",
			emb: "tag=DELETE 0 table=[1:true 2:false]"},
		{name: "B5 boundary int column > number", tbl: "mx",
			sql: "DELETE FROM arcb_mix WHERE id > 5",
			pg:  "tag=DELETE 0 table=[1:true 2:false]",
			emb: "tag=DELETE 0 table=[1:true 2:false]"},

		// B4 — #721 reaching MERGE's WHEN … AND, the fourth DML verb.
		{name: "B4 MERGE WHEN condition, text column > number", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED AND t.name > 5 THEN DELETE",
			pg:  "state=42883 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42883 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "B4 MERGE WHEN condition, text column > number, UPDATE arm", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED AND t.name > 5 THEN UPDATE SET n = 999",
			pg:  "state=42883 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42883 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "B4 boundary MERGE WHEN condition that must keep working", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED AND s.n > 1 THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[2:20:b 3:30:c]"},

		// B6 — a subquery source's unknown column, on the two arms that WRITE.
		{name: "B6 subquery source, unknown column in SET", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n FROM arcb_src) AS s ON t.id = s.id " +
				"WHEN MATCHED THEN UPDATE SET n = s.nosuchcol",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "B6 subquery source, unknown column in INSERT VALUES", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n FROM arcb_src) AS s ON t.id = s.id " +
				"WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.nosuchcol)",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},

		// B7 — an ordering quantifier over a subquery, and a row arity
		// mismatch. The refusal now REACHES the client instead of being
		// discarded so the raw-string fallback could guess; what remains is
		// that PostgreSQL SUPPORTS the ordering quantifier (recorded as a
		// limitation in docs/sql-reference.md) and words the arity mismatch
		// 42883 where this says 42601. Both loud, both pinned under #710.
		// mismatch: the compiler refused both and the planner threw the
		// refusal away, then guessed with the predicate TEXT.
		{name: "B7 ordering quantifier over a subquery", sql: "SELECT count(*) AS c FROM arcb_pr WHERE name > ANY (SELECT name FROM arcb_src)",
			pg:  "rows=[0]",
			emb: "state=0A000",
			bug: "#710"},
		{name: "B7 ordering quantifier over a subquery, numeric column", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id > ANY (SELECT id FROM arcb_src)",
			pg:  "rows=[2]",
			emb: "state=0A000",
			bug: "#710"},
		{name: "B7 row arity mismatch", sql: "SELECT count(*) AS c FROM arcb_pr WHERE (id, n) = (1)",
			pg:  "state=42883",
			emb: "state=42601",
			bug: "#710"},

		// P6 / P18 — INSERT gets the assignment cast and its classes.
		{name: "P6 INSERT rounds a fractional literal into an integer column", tbl: "fl",
			sql: "INSERT INTO arcb_fl (id, f, n) VALUES (20, 0.0, 2.5)",
			pg:  "tag=INSERT 0 1 table=[1:2.5:0 20:0:3 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]",
			emb: "tag=INSERT 0 1 table=[1:2.5:0 20:0:3 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]"},
		{name: "P18 INSERT of unreadable text carries its class", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (9, 'abc', 'z')",
			pg:  "state=22P02 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=22P02 table=[1:10:a 2:20:b 3:30:c]"},

		// P5 — an explicit CAST decides the assignment-cast family.
		{name: "P5 float column cast to numeric rounds half away", tbl: "fl",
			sql: "UPDATE arcb_fl SET n = f::numeric",
			pg:  "tag=UPDATE 5 table=[1:2.5:3 2:-2.5:-3 3:0.5:1 4:3.5:4 5:1.5:2]",
			emb: "tag=UPDATE 5 table=[1:2.5:3 2:-2.5:-3 3:0.5:1 4:3.5:4 5:1.5:2]"},

		// ---------------------------------------------------------------
		// #711 — a string carrying MORE THAN ONE statement.
		//
		// This is the one family where PostgreSQL's own answer depends on the
		// protocol, so the entries carry `pgOne` beside `pg`: the simple query
		// protocol runs the sequence and reports the LAST statement's tag,
		// and every one-statement entry point answers 42601. Both halves are
		// measured, the second through pgx in QueryExecModeExec.
		//
		// What was here before was a pin recording the silent half: `INSERT …;
		// INSERT …` ran the first statement, DROPPED the second, and reported
		// the first one's tag as success.
		// ---------------------------------------------------------------
		{name: "#711 two INSERTs in one message", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (7, 70, 'w'); " +
				"INSERT INTO arcb_pr (id, n, name) VALUES (8, 80, 'x')",
			pg:    "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 7:70:w 8:80:x]",
			pgOne: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb:   "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			sim:   "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 7:70:w 8:80:x]",
			ext:   "state=42601 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#711 two DELETEs in one message", tbl: "pr",
			sql:   "DELETE FROM arcb_pr WHERE id = 1; DELETE FROM arcb_pr WHERE id = 2",
			pg:    "tag=DELETE 1 table=[3:30:c]",
			pgOne: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb:   "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			sim:   "tag=DELETE 1 table=[3:30:c]",
			ext:   "state=42601 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#711 DELETE then SELECT", tbl: "pr",
			sql:   "DELETE FROM arcb_pr WHERE id = 1; SELECT id FROM arcb_pr",
			pg:    "tag=SELECT 2 table=[2:20:b 3:30:c]",
			pgOne: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb:   "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			sim:   "tag=SELECT 2 table=[2:20:b 3:30:c]",
			ext:   "state=42601 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#711 SELECT then DELETE", tbl: "pr",
			sql:   "SELECT id FROM arcb_pr; DELETE FROM arcb_pr WHERE id = 1",
			pg:    "tag=DELETE 1 table=[2:20:b 3:30:c]",
			pgOne: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb:   "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			sim:   "tag=DELETE 1 table=[2:20:b 3:30:c]",
			ext:   "state=42601 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#711 a DELETE with no WHERE then a DELETE", tbl: "pr",
			sql:   "DELETE FROM arcb_pr; DELETE FROM arcb_pr WHERE id = 2",
			pg:    "tag=DELETE 0 table=[]",
			pgOne: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb:   "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			sim:   "tag=DELETE 0 table=[]",
			ext:   "state=42601 table=[1:10:a 2:20:b 3:30:c]"},
		// PARSE THE WHOLE STRING FIRST: the INSERT does NOT run, on any door.
		// This is the silent half of #711 and no cell recorded it.
		{name: "#711 a statement then garbage runs nothing", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (7, 70, 'w'); ZZZ NOT SQL",
			pg:  "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42601 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "#711 garbage then a statement runs nothing", tbl: "pr",
			sql: "ZZZ NOT SQL; INSERT INTO arcb_pr (id, n, name) VALUES (7, 70, 'w')",
			pg:  "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42601 table=[1:10:a 2:20:b 3:30:c]"},
		// A semicolon inside a literal is not a separator, and a trailing one
		// is not a second statement. These are the boundary from the other
		// side: a splitter that cut on them would refuse statements that work.
		{name: "control #711 a semicolon inside a string literal", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = 'a;b' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:a;b 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:a;b 2:20:b 3:30:c]"},
		{name: "control #711 a trailing semicolon", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = 1;",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		// A TRAILING COMMENT IS NOT A SECOND STATEMENT EITHER, and this is the
		// cell that was missing: the splitter trimmed whitespace only, so
		// `…; -- note` became a two-statement string and every door refused it
		// 42601 where PostgreSQL runs it (review B1).
		{name: "control #711 a trailing line comment", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = 1; -- audit note",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "control #711 a trailing block comment", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = 1; /* audit */",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "control #711 a bare trailing --", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id = 1; --",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "control #711 a leading comment", tbl: "pr",
			sql: "-- lead\nDELETE FROM arcb_pr WHERE id = 1",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		// A comment-only piece BETWEEN two statements is dropped, and the two
		// statements still run in sequence on the door that runs sequences.
		{name: "#711 a comment-only piece in the middle", tbl: "pr",
			sql:   "DELETE FROM arcb_pr WHERE id = 1; -- middle\n; DELETE FROM arcb_pr WHERE id = 2",
			pg:    "tag=DELETE 1 table=[3:30:c]",
			pgOne: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			emb:   "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			sim:   "tag=DELETE 1 table=[3:30:c]",
			ext:   "state=42601 table=[1:10:a 2:20:b 3:30:c]"},

		// The verbs, working. These carry no bug and are the regression
		// half of the census: they fail if a later fix moves a RIGHT answer.
		// ---------------------------------------------------------------
		{name: "control delete one row", tbl: "pr", sql: "DELETE FROM arcb_pr WHERE id = 1",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 1 table=[2:20:b 3:30:c]"},
		{name: "control delete no rows", tbl: "pr", sql: "DELETE FROM arcb_pr WHERE id = 99",
			pg:  "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]",
			emb: "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control update one row", tbl: "pr", sql: "UPDATE arcb_pr SET n = 99 WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:99:a 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:99:a 2:20:b 3:30:c]"},
		{name: "control update no rows", tbl: "pr", sql: "UPDATE arcb_pr SET n = 99 WHERE id = 99",
			pg:  "tag=UPDATE 0 table=[1:10:a 2:20:b 3:30:c]",
			emb: "tag=UPDATE 0 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control insert one row", tbl: "pr", sql: "INSERT INTO arcb_pr (id, n, name) VALUES (9, 90, 'z')",
			pg:  "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 9:90:z]",
			emb: "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 9:90:z]"},
		{name: "control merge deletes one row", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[2:20:b 3:30:c]"},
		{name: "control select every row", sql: "SELECT id, n, name FROM arcb_pr ORDER BY id",
			pg:  "rows=[1:10:a 2:20:b 3:30:c]",
			emb: "rows=[1:10:a 2:20:b 3:30:c]"},
	}
}
