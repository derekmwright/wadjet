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
	// One field per door, because the doors DISAGREE and the disagreement is
	// half of what this corpus is for: the extended protocol reports
	// `SELECT 1` for every DML statement (#816) and the wire manufactures a
	// blanket 42000 for an error the engine gave no class (#719). An empty
	// `sim` means "the same as emb"; an empty `ext` means "the same as sim".
	emb string
	sim string
	ext string
	bug string // issue number, or "" when every door already agrees with PG
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
			emb: "tag=MERGE 2 table=[1:10:a 2:100:b 2:200:b 3:30:c]",
			bug: "#689"},
		{name: "#689 two source rows delete one target", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_dup AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=21000 table=[1:10:a 2:20:b 3:30:c]",
			emb: "tag=MERGE 2 table=[1:10:a 3:30:c]",
			bug: "#689"},
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
			emb: "tag=MERGE 0 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#689"},
		{name: "#689 unknown target column in ON, subquery source", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING (SELECT id, n FROM arcb_src) AS s ON t.nosuchcol = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=42703 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42703 table=[1:10:a 2:20:b 3:30:c]"},
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
			emb: "tag=UPDATE 1 table=[1:10:~ 2:20:b 3:30:c]",
			bug: "#690"},
		{name: "#690 update SET a literal spelled null", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = 'null' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:null 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:~ 2:20:b 3:30:c]",
			bug: "#690"},
		{name: "#690 update SET a quoted apostrophe literal", tbl: "pr",
			sql: "UPDATE arcb_pr SET name = '''a''' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:10:'a' 2:20:b 3:30:c]",
			emb: "tag=UPDATE 1 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#690"},
		{name: "#690 insert a literal spelled NULL", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (9, 9, 'NULL')",
			pg:  "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 9:9:NULL]",
			emb: "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 9:9:~]",
			bug: "#690"},
		{name: "#690 insert a literal spelled null", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (6, 6, 'null')",
			pg:  "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 6:6:null]",
			emb: "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 6:6:~]",
			bug: "#690"},
		{name: "#690 insert a quoted apostrophe literal", tbl: "pr",
			sql: "INSERT INTO arcb_pr (id, n, name) VALUES (7, 7, '''a''')",
			pg:  "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 7:7:'a']",
			emb: "tag=INSERT 0 1 table=[1:10:a 2:20:b 3:30:c 7:7:a]",
			bug: "#690"},
		{name: "#690 merge SET a literal spelled NULL", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = 'NULL'",
			pg:  "tag=MERGE 1 table=[1:10:NULL 2:20:b 3:30:c]",
			emb: "tag=MERGE 1 table=[1:10:~ 2:20:b 3:30:c]",
			bug: "#690"},
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
			emb: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#722"},
		{name: "#722 CASE in the UPDATE SET action", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED THEN UPDATE SET n = CASE WHEN s.n > 50 THEN 1 ELSE 2 END",
			pg:  "tag=MERGE 1 table=[1:1:a 2:20:b 3:30:c]",
			emb: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#722"},
		{name: "#722 CASE in the INSERT VALUES action", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, CASE WHEN s.n > 50 THEN 1 ELSE 2 END, s.name)",
			pg:  "tag=MERGE 1 table=[1:10:a 2:20:b 3:30:c 4:1:y]",
			emb: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#722"},
		{name: "#722 CASE in the ON condition", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s " +
				"ON CASE WHEN t.id = s.id THEN true ELSE false END WHEN MATCHED THEN DELETE",
			pg:  "tag=MERGE 1 table=[2:20:b 3:30:c]",
			emb: "state=42601 table=[1:10:a 2:20:b 3:30:c]",
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
			emb: "tag=INSERT 0 1 table=[1:2000-01-01T00:00:00Z 2:2020-01-01T00:00:00Z]",
			bug: "#692"},
		{name: "#692 update to a timestamp carrying an offset", tbl: "ts",
			sql: "UPDATE arcb_ts SET t = '2020-01-01T05:30:00+05:30' WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:2020-01-01T05:30:00Z]",
			emb: "tag=UPDATE 1 table=[1:2020-01-01T00:00:00Z]",
			bug: "#692"},
		{name: "#692 insert an impossible day", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-02-30T00:00:00')",
			pg:  "state=22008 table=[1:2000-01-01T00:00:00Z]",
			emb: "state=22007 table=[1:2000-01-01T00:00:00Z]",
			bug: "#692"},
		{name: "#692 insert an impossible month", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-13-01T00:00:00')",
			pg:  "state=22008 table=[1:2000-01-01T00:00:00Z]",
			emb: "state=22007 table=[1:2000-01-01T00:00:00Z]",
			bug: "#692"},
		{name: "#692 insert an impossible hour", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, '2020-01-01T25:00:00')",
			pg:  "state=22008 table=[1:2000-01-01T00:00:00Z]",
			emb: "state=22007 table=[1:2000-01-01T00:00:00Z]",
			bug: "#692"},
		{name: "#692 insert text that is not a timestamp", tbl: "ts",
			sql: "INSERT INTO arcb_ts (id, t) VALUES (2, 'not-a-timestamp')",
			pg:  "state=22007 table=[1:2000-01-01T00:00:00Z]",
			emb: "state=22007 table=[1:2000-01-01T00:00:00Z]"},
		{name: "#692 cast an impossible day", sql: "SELECT CAST('2020-02-30T00:00:00' AS TIMESTAMP) AS c",
			pg:  "state=22008",
			emb: "rows=[~]",
			bug: "#692"},
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
			emb: "tag=UPDATE 5 table=[1:2.5:3 2:-2.5:-3 3:0.5:1 4:3.5:4 5:1.5:2]",
			bug: "#699"},
		{name: "#699 update an integer column from a float8 literal", tbl: "fl",
			sql: "UPDATE arcb_fl SET n = 2.5::float8 WHERE id = 1",
			pg:  "tag=UPDATE 1 table=[1:2.5:2 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]",
			emb: "tag=UPDATE 1 table=[1:2.5:3 2:-2.5:0 3:0.5:0 4:3.5:0 5:1.5:0]",
			bug: "#699"},
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
			emb: "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#710"},
		{name: "#710 delete a row-value comparison", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE (id, n) = (1, 10)",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#710"},
		{name: "#710 update = ANY(ARRAY[…])", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 0 WHERE id = ANY(ARRAY[1,2])",
			pg:  "tag=UPDATE 2 table=[1:0:a 2:0:b 3:30:c]",
			emb: "tag=UPDATE 0 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#710"},
		{name: "#710 update a row-value comparison", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 0 WHERE (id, n) = (1, 10)",
			pg:  "tag=UPDATE 1 table=[1:0:a 2:20:b 3:30:c]",
			emb: "tag=UPDATE 0 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#710"},
		{name: "#710 delete <> ALL(ARRAY[…])", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id <> ALL(ARRAY[1,2])",
			pg:  "tag=DELETE 1 table=[1:10:a 2:20:b]",
			emb: "tag=DELETE 0 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#710"},
		{name: "#710 select = ANY(ARRAY[…])", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id = ANY(ARRAY[1,2])",
			pg:  "rows=[2]",
			emb: "rows=[0]",
			bug: "#710"},
		{name: "#710 select a row-value comparison", sql: "SELECT count(*) AS c FROM arcb_pr WHERE (id, n) = (1, 10)",
			pg:  "rows=[1]",
			emb: "rows=[0]",
			bug: "#710"},
		{name: "#710 select a row-value IN list", sql: "SELECT count(*) AS c FROM arcb_pr WHERE (id, n) IN ((1,10),(2,20))",
			pg:  "rows=[2]",
			emb: "rows=[0]",
			bug: "#710"},
		{name: "#710 select = ANY(subquery)", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id = ANY(SELECT id FROM arcb_src)",
			pg:  "rows=[1]",
			emb: "rows=[0]",
			bug: "#710"},
		{name: "#710 select <> ALL(ARRAY[…])", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id <> ALL(ARRAY[1,2])",
			pg:  "rows=[1]",
			emb: "rows=[0]",
			bug: "#710"},
		{name: "#710 select a row-value ordering", sql: "SELECT count(*) AS c FROM arcb_pr WHERE (id, n) < (2, 0)",
			pg:  "rows=[1]",
			emb: "rows=[0]",
			bug: "#710"},
		{name: "control select IN list", sql: "SELECT count(*) AS c FROM arcb_pr WHERE id IN (1,2)",
			pg:  "rows=[2]",
			emb: "rows=[2]"},

		// ---------------------------------------------------------------
		// #688 — a subquery in a DML predicate.
		// ---------------------------------------------------------------
		{name: "#688 delete IN (SELECT …)", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id IN (SELECT id FROM arcb_src)",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#688"},
		{name: "#688 delete correlated EXISTS", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE EXISTS (SELECT 1 FROM arcb_src s WHERE s.id = arcb_pr.id)",
			pg:  "tag=DELETE 1 table=[2:20:b 3:30:c]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#688"},
		{name: "#688 update IN (SELECT …)", tbl: "pr",
			sql: "UPDATE arcb_pr SET n = 0 WHERE id IN (SELECT id FROM arcb_src)",
			pg:  "tag=UPDATE 1 table=[1:0:a 2:20:b 3:30:c]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#688"},
		{name: "#688 delete NOT IN (SELECT …)", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE id NOT IN (SELECT id FROM arcb_src)",
			pg:  "tag=DELETE 2 table=[1:10:a]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#688"},
		{name: "#688 delete against a scalar subquery", tbl: "pr",
			sql: "DELETE FROM arcb_pr WHERE n < (SELECT max(n) FROM arcb_src)",
			pg:  "tag=DELETE 3 table=[]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#688"},

		// ---------------------------------------------------------------
		// #718 — MERGE WHEN NOT MATCHED BY SOURCE / BY TARGET.
		// ---------------------------------------------------------------
		{name: "#718 WHEN NOT MATCHED BY SOURCE THEN DELETE", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN NOT MATCHED BY SOURCE THEN DELETE",
			pg:  "tag=MERGE 2 table=[1:10:a]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#718"},
		{name: "#718 WHEN NOT MATCHED BY TARGET THEN INSERT", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN NOT MATCHED BY TARGET THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)",
			pg:  "tag=MERGE 1 table=[1:10:a 2:20:b 3:30:c 4:400:y]",
			emb: "state=0A000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#718"},

		// ---------------------------------------------------------------
		// #719 — the SQLSTATE a DML runtime failure carries.
		// ---------------------------------------------------------------
		{name: "#719 delete from a table that does not exist", tbl: "pr",
			sql: "DELETE FROM nosuchtable815 WHERE id = 1",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=<none> table=[1:10:a 2:20:b 3:30:c]",
			sim: "state=42000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#719"},
		{name: "#719 update a table that does not exist", tbl: "pr",
			sql: "UPDATE nosuchtable815 SET n = 1 WHERE id = 1",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=<none> table=[1:10:a 2:20:b 3:30:c]",
			sim: "state=42000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#719"},
		{name: "#719 insert into a table that does not exist", tbl: "pr",
			sql: "INSERT INTO nosuchtable815 (id) VALUES (1)",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=<none> table=[1:10:a 2:20:b 3:30:c]",
			sim: "state=42000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#719"},
		{name: "#719 merge into a table that does not exist", tbl: "pr",
			sql: "MERGE INTO nosuchtable815 AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=<none> table=[1:10:a 2:20:b 3:30:c]",
			sim: "state=42000 table=[1:10:a 2:20:b 3:30:c]",
			bug: "#719"},
		{name: "control merge FROM a table that does not exist", tbl: "pr",
			sql: "MERGE INTO arcb_pr AS t USING nosuchtable815 AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
			pg:  "state=42P01 table=[1:10:a 2:20:b 3:30:c]",
			emb: "state=42P01 table=[1:10:a 2:20:b 3:30:c]"},
		{name: "control select from a table that does not exist", sql: "SELECT * FROM nosuchtable815",
			pg:  "state=42P01",
			emb: "state=42P01"},

		// ---------------------------------------------------------------
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
