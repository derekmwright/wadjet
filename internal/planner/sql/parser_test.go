package sql

import (
	"testing"
)

func TestParse_Select(t *testing.T) {
	tests := []struct {
		sql     string
		wantErr bool
	}{
		{"SELECT * FROM events", false},
		{"SELECT id, name FROM users WHERE id > 5", false},
		{"SELECT user_id, SUM(amount) FROM events GROUP BY user_id", false},
		{"SELECT * FROM events ORDER BY ts DESC LIMIT 10", false},
		{"SELECT e.user_id, SUM(e.amount) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.ts >= '2026-03-01' GROUP BY e.user_id ORDER BY SUM(e.amount) DESC LIMIT 20", false},
		{"INVALID SQL", true},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Type != QuerySelect {
				t.Fatalf("expected SELECT, got %v", parsed.Type)
			}
		})
	}
}

func TestExtractSelect(t *testing.T) {
	parsed, err := Parse("SELECT user_id, SUM(amount) as total FROM events WHERE year = '2026' GROUP BY user_id ORDER BY total DESC LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.Tables) != 1 || info.Tables[0].Name != "events" {
		t.Fatalf("expected table 'events', got %v", info.Tables)
	}

	if len(info.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(info.Columns))
	}

	if !info.Columns[1].IsAgg || info.Columns[1].AggFunc != "sum" {
		t.Fatalf("expected SUM aggregate, got %v", info.Columns[1])
	}

	if len(info.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY, got %d", len(info.GroupBy))
	}

	if len(info.OrderBy) != 1 || !info.OrderBy[0].Desc {
		t.Fatalf("expected 1 ORDER BY DESC, got %v", info.OrderBy)
	}

	if info.Limit != "10" {
		t.Fatalf("expected LIMIT 10, got %s", info.Limit)
	}
}

func TestExtractJoin(t *testing.T) {
	parsed, err := Parse("SELECT e.user_id FROM events e JOIN users u ON e.user_id = u.user_id")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(info.Joins))
	}

	if info.Joins[0].RightTable != "users" {
		t.Fatalf("expected right table 'users', got %s", info.Joins[0].RightTable)
	}
}

func TestParseExplain(t *testing.T) {
	parsed, err := Parse("EXPLAIN SELECT * FROM events WHERE id > 5")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Type != QueryExplain {
		t.Fatalf("expected QueryExplain, got %v", parsed.Type)
	}
	if parsed.Explain == nil {
		t.Fatal("Explain info is nil")
	}
	if parsed.Explain.Verbose {
		t.Error("should not be verbose")
	}
	if parsed.Explain.InnerSQL != "SELECT * FROM events WHERE id > 5" {
		t.Errorf("unexpected inner SQL: %s", parsed.Explain.InnerSQL)
	}
	// SelectInfo should be populated for the inner SELECT
	if parsed.SelectInfo == nil {
		t.Error("SelectInfo should contain inner SELECT")
	}
}

func TestParseExplainVerbose(t *testing.T) {
	parsed, err := Parse("EXPLAIN VERBOSE SELECT id FROM events")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Type != QueryExplain {
		t.Fatalf("expected QueryExplain, got %v", parsed.Type)
	}
	if !parsed.Explain.Verbose {
		t.Error("should be verbose")
	}
}

func TestParseDescribe(t *testing.T) {
	tests := []struct {
		sql       string
		tableName string
	}{
		{"DESCRIBE events", "events"},
		{"DESC events", "events"},
		{"describe users;", "users"},
		{"SHOW COLUMNS FROM events", "events"},
		{"show columns from users;", "users"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Type != QueryDescribe {
				t.Fatalf("expected QueryDescribe, got %v", parsed.Type)
			}
			if parsed.Describe == nil {
				t.Fatal("Describe info is nil")
			}
			if parsed.Describe.TableName != tt.tableName {
				t.Errorf("expected table %q, got %q", tt.tableName, parsed.Describe.TableName)
			}
		})
	}
}

func TestParseDescribeEmpty(t *testing.T) {
	_, err := Parse("DESCRIBE ")
	if err == nil {
		t.Error("expected error for empty DESCRIBE")
	}
}

func TestParseCreateFunction(t *testing.T) {
	tests := []struct {
		sql     string
		name    string
		params  []string
		body    string
		replace bool
		locked  bool
	}{
		{
			sql:    "CREATE FUNCTION double(x) AS x * 2",
			name:   "double",
			params: []string{"x"},
			body:   "x * 2",
		},
		{
			sql:     "CREATE OR REPLACE FUNCTION weighted(val, weight) AS val * weight + 10",
			name:    "weighted",
			params:  []string{"val", "weight"},
			body:    "val * weight + 10",
			replace: true,
		},
		{
			sql:    "CREATE FUNCTION classify(score) AS CASE WHEN score >= 90 THEN 'A' ELSE 'B' END WITH LOCK",
			name:   "classify",
			params: []string{"score"},
			body:   "CASE WHEN score >= 90 THEN 'A' ELSE 'B' END",
			locked: true,
		},
		{
			sql:    "CREATE FUNCTION no_params() AS 42;",
			name:   "no_params",
			params: nil,
			body:   "42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if parsed.Type != QueryCreateFunction {
				t.Fatalf("expected QueryCreateFunction, got %v", parsed.Type)
			}
			cf := parsed.CreateFunction
			if cf == nil {
				t.Fatal("CreateFunction is nil")
			}
			if cf.Name != tt.name {
				t.Errorf("name: got %q, want %q", cf.Name, tt.name)
			}
			if len(cf.Params) != len(tt.params) {
				t.Fatalf("params: got %v, want %v", cf.Params, tt.params)
			}
			for i := range cf.Params {
				if cf.Params[i] != tt.params[i] {
					t.Errorf("params[%d]: got %q, want %q", i, cf.Params[i], tt.params[i])
				}
			}
			if cf.Body != tt.body {
				t.Errorf("body: got %q, want %q", cf.Body, tt.body)
			}
			if cf.Replace != tt.replace {
				t.Errorf("replace: got %v, want %v", cf.Replace, tt.replace)
			}
			if cf.Locked != tt.locked {
				t.Errorf("locked: got %v, want %v", cf.Locked, tt.locked)
			}
		})
	}
}

func TestParseDropFunction(t *testing.T) {
	tests := []struct {
		sql      string
		name     string
		ifExists bool
	}{
		{"DROP FUNCTION my_func", "my_func", false},
		{"DROP FUNCTION IF EXISTS my_func", "my_func", true},
		{"DROP FUNCTION my_func;", "my_func", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if parsed.Type != QueryDropFunction {
				t.Fatalf("expected QueryDropFunction, got %v", parsed.Type)
			}
			df := parsed.DropFunction
			if df.Name != tt.name {
				t.Errorf("name: got %q, want %q", df.Name, tt.name)
			}
			if df.IfExists != tt.ifExists {
				t.Errorf("ifExists: got %v, want %v", df.IfExists, tt.ifExists)
			}
		})
	}
}

func TestParseShowFunctions(t *testing.T) {
	for _, sql := range []string{"SHOW FUNCTIONS", "SHOW FUNCTIONS;"} {
		parsed, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", sql, err)
		}
		if parsed.Type != QueryShowFunctions {
			t.Fatalf("expected QueryShowFunctions, got %v", parsed.Type)
		}
	}
}

func TestParseUnion(t *testing.T) {
	tests := []struct {
		sql     string
		wantAll bool
	}{
		{"SELECT id FROM events UNION SELECT id FROM users", false},
		{"SELECT id FROM events UNION ALL SELECT id FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Type != QuerySelect {
				t.Fatalf("expected QuerySelect, got %v", parsed.Type)
			}
		})
	}
}

func TestExtractUnion(t *testing.T) {
	parsed, err := Parse("SELECT id, name FROM events UNION SELECT id, name FROM users")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if info.Union == nil {
		t.Fatal("expected Union to be non-nil")
	}
	if info.Union.All {
		t.Error("expected UNION (not UNION ALL)")
	}

	// Left side should have table "events"
	if len(info.Union.Left.Tables) != 1 || info.Union.Left.Tables[0].Name != "events" {
		t.Errorf("left tables: got %v, want [events]", info.Union.Left.Tables)
	}

	// Right side should have table "users"
	if len(info.Union.Right.Tables) != 1 || info.Union.Right.Tables[0].Name != "users" {
		t.Errorf("right tables: got %v, want [users]", info.Union.Right.Tables)
	}
}

func TestExtractUnionAll(t *testing.T) {
	parsed, err := Parse("SELECT id FROM events UNION ALL SELECT id FROM users")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if info.Union == nil {
		t.Fatal("expected Union to be non-nil")
	}
	if !info.Union.All {
		t.Error("expected UNION ALL")
	}
}

func TestExtractUnionWithOrderByAndLimit(t *testing.T) {
	parsed, err := Parse("SELECT id FROM events UNION SELECT id FROM users ORDER BY id LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if info.Union == nil {
		t.Fatal("expected Union to be non-nil")
	}

	if len(info.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY, got %d", len(info.OrderBy))
	}
	if info.Limit != "10" {
		t.Errorf("expected LIMIT 10, got %s", info.Limit)
	}
}

func TestExtractNestedUnion(t *testing.T) {
	// A UNION B UNION ALL C is parsed as (A UNION B) UNION ALL C
	parsed, err := Parse("SELECT id FROM a UNION SELECT id FROM b UNION ALL SELECT id FROM c")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if info.Union == nil {
		t.Fatal("expected outer Union to be non-nil")
	}
	if !info.Union.All {
		t.Error("expected outer to be UNION ALL")
	}

	// The left side of the outer UNION ALL should itself be a UNION
	if info.Union.Left.Union == nil {
		t.Fatal("expected inner Union to be non-nil on the left side")
	}
	if info.Union.Left.Union.All {
		t.Error("expected inner to be UNION (not ALL)")
	}
}

func TestParseIntersect(t *testing.T) {
	tests := []struct {
		sql    string
		wantOp SetOp
		all    bool
	}{
		{"SELECT id FROM events INTERSECT SELECT id FROM users", SetOpIntersect, false},
		{"SELECT id FROM events INTERSECT ALL SELECT id FROM users", SetOpIntersect, true},
		{"SELECT id FROM events EXCEPT SELECT id FROM users", SetOpExcept, false},
		{"SELECT id FROM events EXCEPT ALL SELECT id FROM users", SetOpExcept, true},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatal(err)
			}
			if info.Union == nil {
				t.Fatal("expected Union (set op) to be non-nil")
			}
			if info.Union.Op != tt.wantOp {
				t.Errorf("op: got %q, want %q", info.Union.Op, tt.wantOp)
			}
			if info.Union.All != tt.all {
				t.Errorf("all: got %v, want %v", info.Union.All, tt.all)
			}
		})
	}
}

func TestParseIntersectWithOrderByAndLimit(t *testing.T) {
	parsed, err := Parse("SELECT id FROM events INTERSECT SELECT id FROM users ORDER BY id DESC LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if info.Union == nil {
		t.Fatal("expected set op to be non-nil")
	}
	if info.Union.Op != SetOpIntersect {
		t.Errorf("op: got %q, want INTERSECT", info.Union.Op)
	}
	if len(info.OrderBy) != 1 || !info.OrderBy[0].Desc {
		t.Errorf("expected ORDER BY id DESC, got %v", info.OrderBy)
	}
	if info.Limit != "5" {
		t.Errorf("expected LIMIT 5, got %s", info.Limit)
	}
}

func TestParseMixedSetOps(t *testing.T) {
	// SELECT a UNION SELECT b EXCEPT SELECT c => ((a UNION b) EXCEPT c)
	parsed, err := Parse("SELECT id FROM a UNION SELECT id FROM b EXCEPT SELECT id FROM c")
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if info.Union == nil {
		t.Fatal("expected outer set op")
	}
	if info.Union.Op != SetOpExcept {
		t.Errorf("outer op: got %q, want EXCEPT", info.Union.Op)
	}
	// Left side should be a UNION
	if info.Union.Left.Union == nil {
		t.Fatal("expected inner set op on left")
	}
	if info.Union.Left.Union.Op != SetOpUnion {
		t.Errorf("inner op: got %q, want UNION", info.Union.Left.Union.Op)
	}
}

// Edge cases that the old substring-slicing parser would fail on.

func TestParseCreateFunctionParensInBody(t *testing.T) {
	// Old parser: strings.Index(rest, ")") found the ) from f(score), not the param list close
	sql := "CREATE FUNCTION classify(score) AS CASE WHEN score >= 90 THEN f(score) ELSE 'low' END WITH LOCK"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cf := parsed.CreateFunction
	if cf.Name != "classify" {
		t.Errorf("name: got %q, want 'classify'", cf.Name)
	}
	if len(cf.Params) != 1 || cf.Params[0] != "score" {
		t.Errorf("params: got %v, want [score]", cf.Params)
	}
	expected := "CASE WHEN score >= 90 THEN f(score) ELSE 'low' END"
	if cf.Body != expected {
		t.Errorf("body: got %q, want %q", cf.Body, expected)
	}
	if !cf.Locked {
		t.Error("expected Locked=true")
	}
}

func TestParseCreateFunctionWithLockInStringLiteral(t *testing.T) {
	// Old parser: strings.HasSuffix(upperBody, " WITH LOCK") would false-positive
	sql := "CREATE FUNCTION flagged(x) AS CASE WHEN x = 'WITH LOCK' THEN 1 ELSE 0 END"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cf := parsed.CreateFunction
	expected := "CASE WHEN x = 'WITH LOCK' THEN 1 ELSE 0 END"
	if cf.Body != expected {
		t.Errorf("body: got %q, want %q", cf.Body, expected)
	}
	if cf.Locked {
		t.Error("expected Locked=false (WITH LOCK was inside a string literal)")
	}
}

func TestParseCreateFunctionEscapedQuotes(t *testing.T) {
	sql := "CREATE FUNCTION greet(name) AS 'hello, it''s ' || name"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cf := parsed.CreateFunction
	if cf.Name != "greet" {
		t.Errorf("name: got %q, want 'greet'", cf.Name)
	}
	expected := "'hello, it''s ' || name"
	if cf.Body != expected {
		t.Errorf("body: got %q, want %q", cf.Body, expected)
	}
}

func TestParseCreateFunctionNestedParens(t *testing.T) {
	sql := "CREATE FUNCTION deep(x) AS f(g(h(x, 1), 2), 3);"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cf := parsed.CreateFunction
	if cf.Body != "f(g(h(x, 1), 2), 3)" {
		t.Errorf("body: got %q", cf.Body)
	}
}

func TestParseCreateFunctionErrors(t *testing.T) {
	tests := []string{
		"CREATE FUNCTION",
		"CREATE FUNCTION AS x",
		"CREATE FUNCTION fn AS x",     // no parens
		"CREATE FUNCTION fn(x) x * 2", // missing AS
		"CREATE FUNCTION fn(x) AS",    // empty body
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err == nil {
				t.Errorf("expected error for %q", sql)
			}
		})
	}
}

func TestParseCreateTable(t *testing.T) {
	tests := []struct {
		sql           string
		name          string
		cols          int
		partKeys      int
		firstColName  string
		firstColType  string
		firstNullable bool
	}{
		{
			sql:           "CREATE TABLE events (id BIGINT NOT NULL, name VARCHAR, score DOUBLE)",
			name:          "events",
			cols:          3,
			partKeys:      0,
			firstColName:  "id",
			firstColType:  "BIGINT",
			firstNullable: false,
		},
		{
			sql:           "CREATE TABLE flow_logs (src_ip IPV4 NOT NULL, dst_ip IPV4, port PORT, proto PROTOCOL) PARTITION BY (date)",
			name:          "flow_logs",
			cols:          4,
			partKeys:      1,
			firstColName:  "src_ip",
			firstColType:  "IPV4",
			firstNullable: false,
		},
		{
			sql:           "CREATE TABLE simple (val INT)",
			name:          "simple",
			cols:          1,
			partKeys:      0,
			firstColName:  "val",
			firstColType:  "INT",
			firstNullable: true,
		},
		{
			sql:           "CREATE TABLE multi_part (id BIGINT, year INT, month INT) PARTITION BY (year, month)",
			name:          "multi_part",
			cols:          3,
			partKeys:      2,
			firstColName:  "id",
			firstColType:  "BIGINT",
			firstNullable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if parsed.Type != QueryCreateTable {
				t.Fatalf("expected QueryCreateTable, got %v", parsed.Type)
			}
			ct := parsed.CreateTable
			if ct == nil {
				t.Fatal("CreateTable is nil")
			}
			if ct.Name != tt.name {
				t.Errorf("name: got %q, want %q", ct.Name, tt.name)
			}
			if len(ct.Columns) != tt.cols {
				t.Fatalf("columns: got %d, want %d", len(ct.Columns), tt.cols)
			}
			if len(ct.PartitionKeys) != tt.partKeys {
				t.Fatalf("partition keys: got %d, want %d", len(ct.PartitionKeys), tt.partKeys)
			}
			if ct.Columns[0].Name != tt.firstColName {
				t.Errorf("first col name: got %q, want %q", ct.Columns[0].Name, tt.firstColName)
			}
			if ct.Columns[0].Type != tt.firstColType {
				t.Errorf("first col type: got %q, want %q", ct.Columns[0].Type, tt.firstColType)
			}
			if ct.Columns[0].Nullable != tt.firstNullable {
				t.Errorf("first col nullable: got %v, want %v", ct.Columns[0].Nullable, tt.firstNullable)
			}
		})
	}
}

func TestParseCreateTableErrors(t *testing.T) {
	tests := []struct {
		sql  string
		desc string
	}{
		{"CREATE TABLE", "missing table name"},
		{"CREATE TABLE events", "missing columns"},
		{"CREATE TABLE events ()", "empty columns"},
		{"CREATE TABLE events (id)", "missing type"},
		{"CREATE TABLE events (id BIGINT) PARTITION", "missing BY"},
		{"CREATE TABLE events (id BIGINT) PARTITION BY", "missing partition parens"},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err == nil {
				t.Errorf("expected error for %q", tt.sql)
			}
		})
	}
}

func TestParseDropTable(t *testing.T) {
	tests := []struct {
		sql      string
		name     string
		ifExists bool
	}{
		{"DROP TABLE events", "events", false},
		{"DROP TABLE IF EXISTS events", "events", true},
		{"DROP TABLE events;", "events", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if parsed.Type != QueryDropTable {
				t.Fatalf("expected QueryDropTable, got %v", parsed.Type)
			}
			dt := parsed.DropTable
			if dt.Name != tt.name {
				t.Errorf("name: got %q, want %q", dt.Name, tt.name)
			}
			if dt.IfExists != tt.ifExists {
				t.Errorf("ifExists: got %v, want %v", dt.IfExists, tt.ifExists)
			}
		})
	}
}

func TestParseShowTables(t *testing.T) {
	for _, sql := range []string{"SHOW TABLES", "SHOW TABLES;", "show tables"} {
		parsed, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", sql, err)
		}
		if parsed.Type != QueryShowTables {
			t.Fatalf("expected QueryShowTables, got %v", parsed.Type)
		}
	}
}

func TestParseCreateTableAllTypes(t *testing.T) {
	sql := `CREATE TABLE network_logs (
		id BIGINT NOT NULL,
		src_ip IPV4 NOT NULL,
		dst_ip IPV6,
		src_port PORT,
		proto PROTOCOL,
		mac_addr MAC,
		subnet CIDR,
		request_id UUID,
		event_date DATE,
		event_ts TIMESTAMP,
		duration DURATION,
		payload BYTES,
		active BOOLEAN,
		score FLOAT,
		amount DOUBLE,
		count INTEGER
	) PARTITION BY (event_date)`
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ct := parsed.CreateTable
	if len(ct.Columns) != 16 {
		t.Fatalf("expected 16 columns, got %d", len(ct.Columns))
	}
	if len(ct.PartitionKeys) != 1 || ct.PartitionKeys[0] != "event_date" {
		t.Fatalf("expected partition key [event_date], got %v", ct.PartitionKeys)
	}
}

func TestParseWindowFunction(t *testing.T) {
	tests := []struct {
		sql     string
		winFunc string
		partBy  int // number of partition by cols
		orderBy int // number of order by cols
	}{
		{"SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t", "row_number", 0, 1},
		{"SELECT SUM(amount) OVER (PARTITION BY dept ORDER BY id) AS running FROM t", "sum", 1, 1},
		{"SELECT RANK() OVER (PARTITION BY dept, team ORDER BY score DESC NULLS LAST) AS r FROM t", "rank", 2, 1},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatal(err)
			}
			if len(info.Windows) != 1 {
				t.Fatalf("expected 1 window, got %d", len(info.Windows))
			}
			ws := info.Windows[0]
			if ws.FuncName != tt.winFunc {
				t.Errorf("func: got %q, want %q", ws.FuncName, tt.winFunc)
			}
			if len(ws.PartitionBy) != tt.partBy {
				t.Errorf("partition by: got %d, want %d", len(ws.PartitionBy), tt.partBy)
			}
			if len(ws.OrderBy) != tt.orderBy {
				t.Errorf("order by: got %d, want %d", len(ws.OrderBy), tt.orderBy)
			}
		})
	}
}

func TestParseWindowFrame(t *testing.T) {
	sql := "SELECT SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rolling FROM t"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Windows) != 1 {
		t.Fatalf("expected 1 window, got %d", len(info.Windows))
	}
	ws := info.Windows[0]
	if ws.Frame == nil {
		t.Fatal("expected frame spec")
	}
	if ws.Frame.Mode != FrameRows {
		t.Errorf("expected ROWS frame, got %v", ws.Frame.Mode)
	}
}

func TestParseNullsFirstLast(t *testing.T) {
	tests := []struct {
		sql        string
		nullsFirst *bool
	}{
		{"SELECT * FROM t ORDER BY id NULLS FIRST", boolPtr(true)},
		{"SELECT * FROM t ORDER BY id NULLS LAST", boolPtr(false)},
		{"SELECT * FROM t ORDER BY id DESC NULLS FIRST", boolPtr(true)},
		{"SELECT * FROM t ORDER BY id", nil},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatal(err)
			}
			if len(info.OrderBy) != 1 {
				t.Fatalf("expected 1 ORDER BY, got %d", len(info.OrderBy))
			}
			ob := info.OrderBy[0]
			if tt.nullsFirst == nil && ob.NullsFirst != nil {
				t.Errorf("expected NullsFirst=nil, got %v", *ob.NullsFirst)
			}
			if tt.nullsFirst != nil {
				if ob.NullsFirst == nil {
					t.Fatalf("expected NullsFirst=%v, got nil", *tt.nullsFirst)
				}
				if *ob.NullsFirst != *tt.nullsFirst {
					t.Errorf("expected NullsFirst=%v, got %v", *tt.nullsFirst, *ob.NullsFirst)
				}
			}
		})
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func TestPositionalGroupByOrderBy(t *testing.T) {
	tests := []struct {
		sql      string
		groupBy  []string
		orderCol string
	}{
		{
			sql:     "SELECT user_id, SUM(amount) AS total FROM events GROUP BY 1",
			groupBy: []string{"user_id"},
		},
		{
			sql:      "SELECT user_id, name FROM users ORDER BY 1 DESC",
			orderCol: "user_id",
		},
		{
			sql:      "SELECT user_id, SUM(amount) AS total FROM events GROUP BY 1 ORDER BY 2 DESC",
			groupBy:  []string{"user_id"},
			orderCol: "total",
		},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatal(err)
			}
			if tt.groupBy != nil {
				if len(info.GroupBy) != len(tt.groupBy) {
					t.Fatalf("GROUP BY: got %v, want %v", info.GroupBy, tt.groupBy)
				}
				for i, want := range tt.groupBy {
					if info.GroupBy[i] != want {
						t.Errorf("GROUP BY[%d]: got %q, want %q", i, info.GroupBy[i], want)
					}
				}
			}
			if tt.orderCol != "" {
				if len(info.OrderBy) != 1 || info.OrderBy[0].Column != tt.orderCol {
					t.Errorf("ORDER BY: got %v, want column=%q", info.OrderBy, tt.orderCol)
				}
			}
		})
	}
}

func TestCTEColumnList(t *testing.T) {
	sql := "WITH t(a, b) AS (SELECT id, name FROM users) SELECT a, b FROM t"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(parsed.CTEs))
	}
	cte := parsed.CTEs[0]
	if cte.Name != "t" {
		t.Errorf("CTE name: got %q, want %q", cte.Name, "t")
	}
	if len(cte.Columns) != 2 || cte.Columns[0] != "a" || cte.Columns[1] != "b" {
		t.Errorf("CTE columns: got %v, want [a b]", cte.Columns)
	}
}

func TestParseTableFunction(t *testing.T) {
	sql := "SELECT * FROM read_json('https://example.com/data.json')"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(info.Tables))
	}
	tr := info.Tables[0]
	if !tr.IsFunction {
		t.Error("expected IsFunction=true")
	}
	if tr.Name != "read_json" {
		t.Errorf("expected name=read_json, got %q", tr.Name)
	}
	if len(tr.FuncArgs) != 1 || tr.FuncArgs[0] != "https://example.com/data.json" {
		t.Errorf("expected args=[https://example.com/data.json], got %v", tr.FuncArgs)
	}
}

func TestParseTableFunctionWithAlias(t *testing.T) {
	sql := "SELECT j.name FROM read_json('/tmp/data.jsonl') AS j WHERE j.age > 30"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(info.Tables))
	}
	tr := info.Tables[0]
	if !tr.IsFunction {
		t.Error("expected IsFunction=true")
	}
	if tr.Alias != "j" {
		t.Errorf("expected alias=j, got %q", tr.Alias)
	}
	if info.Where == "" {
		t.Error("expected WHERE clause")
	}
}

func TestParseTableFunctionNamedArgs(t *testing.T) {
	sql := "SELECT * FROM read_csv('/tmp/data.csv', delimiter='|', header=false)"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(info.Tables))
	}
	tr := info.Tables[0]
	if !tr.IsFunction {
		t.Error("expected IsFunction=true")
	}
	if tr.Name != "read_csv" {
		t.Errorf("expected name=read_csv, got %q", tr.Name)
	}
	if len(tr.FuncArgs) != 1 || tr.FuncArgs[0] != "/tmp/data.csv" {
		t.Errorf("expected positional args=[/tmp/data.csv], got %v", tr.FuncArgs)
	}
	if tr.FuncNamedArgs["delimiter"] != "|" {
		t.Errorf("expected delimiter=|, got %q", tr.FuncNamedArgs["delimiter"])
	}
	if tr.FuncNamedArgs["header"] != "FALSE" {
		t.Errorf("expected header=FALSE, got %q", tr.FuncNamedArgs["header"])
	}
}

func TestParseTableFunctionMixedArgs(t *testing.T) {
	sql := "SELECT * FROM read_json('data.json') AS j"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	tr := info.Tables[0]
	if len(tr.FuncNamedArgs) != 0 {
		t.Errorf("expected no named args, got %v", tr.FuncNamedArgs)
	}
	if tr.Alias != "j" {
		t.Errorf("expected alias=j, got %q", tr.Alias)
	}
}
