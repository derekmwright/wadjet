package sql

import "testing"

// TestParseNiladicSessionFuncs pins the parenless spellings of the session
// information functions. PostgreSQL clients send `current_user` bare (DataGrip
// opens every connection with `select current_database(), current_schema(),
// current_user`), and before these were recognised the bare name parsed as a
// column reference and the statement failed with "unknown column".
func TestParseNiladicSessionFuncs(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantFunc string
	}{
		{"current_user", "SELECT current_user", "current_user"},
		{"CURRENT_USER upper", "SELECT CURRENT_USER", "current_user"},
		{"session_user", "SELECT session_user", "session_user"},
		{"user", "SELECT user", "user"},
		{"current_role", "SELECT current_role", "current_role"},
		{"current_catalog", "SELECT current_catalog", "current_catalog"},
		{"current_schema", "SELECT current_schema", "current_schema"},
		{"current_date still works", "SELECT current_date", "current_date"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.sql, err)
			}
			col := parsed.SelectInfo.Columns[0]
			fn, ok := col.ASTExpr.(*FuncCallNode)
			if !ok {
				t.Fatalf("expression is %T, want a zero-arg function call", col.ASTExpr)
			}
			if fn.Name != tt.wantFunc {
				t.Errorf("function name: got %q, want %q", fn.Name, tt.wantFunc)
			}
			if len(fn.Args) != 0 {
				t.Errorf("args: got %d, want 0", len(fn.Args))
			}
			if col.ColumnRef != "" {
				t.Errorf("parsed as a column reference to %q", col.ColumnRef)
			}
		})
	}
}

// TestParseNiladicParenthesizedSpelling covers the same names written with an
// empty argument list — the niladic shortcut must not swallow the name and
// leave the parens stranded.
func TestParseNiladicParenthesizedSpelling(t *testing.T) {
	for _, sql := range []string{
		"SELECT current_user()",
		"SELECT current_schema()",
		"SELECT current_date()",
		"SELECT current_timestamp()",
	} {
		parsed, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", sql, err)
		}
		if _, ok := parsed.SelectInfo.Columns[0].ASTExpr.(*FuncCallNode); !ok {
			t.Errorf("%q: expression is %T, want a function call",
				sql, parsed.SelectInfo.Columns[0].ASTExpr)
		}
	}
}

// TestParseNiladicQuotedStaysColumnRef pins #304 semantics: inside double
// quotes these spellings are ordinary column names, which is also the escape
// hatch for a table that really has a column called `user`.
func TestParseNiladicQuotedStaysColumnRef(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT "current_user" FROM t`, "current_user"},
		{`SELECT "user" FROM t`, "user"},
		{`SELECT "current_schema" FROM t`, "current_schema"},
	} {
		parsed, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.sql, err)
		}
		col := parsed.SelectInfo.Columns[0]
		if col.ColumnRef != tc.want {
			t.Errorf("%q: ColumnRef = %q, want %q", tc.sql, col.ColumnRef, tc.want)
		}
		if _, ok := col.ASTExpr.(*ColRef); !ok {
			t.Errorf("%q: expression is %T, want *ColRef", tc.sql, col.ASTExpr)
		}
	}
}

// TestParseNiladicQualifiedReference keeps `user` usable as a table qualifier:
// a following dot means the name introduces a qualified column reference, not
// the niladic function.
func TestParseNiladicQualifiedReference(t *testing.T) {
	parsed, err := Parse("SELECT user.name FROM accounts AS user")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	col := parsed.SelectInfo.Columns[0]
	ref, ok := col.ASTExpr.(*ColRef)
	if !ok {
		t.Fatalf("expression is %T, want *ColRef", col.ASTExpr)
	}
	if ref.Table != "user" || ref.Column != "name" {
		t.Errorf("got %s.%s, want user.name", ref.Table, ref.Column)
	}
}

// TestSessionFuncOutputLabel pins the PostgreSQL output column name: clients
// read these results by label, and `current_user()` is not the label pgJDBC
// expects for `SELECT current_user`.
func TestSessionFuncOutputLabel(t *testing.T) {
	tests := []struct {
		sql       string
		wantAlias string
	}{
		{"SELECT current_user", "current_user"},
		{"SELECT current_schema()", "current_schema"},
		{"SELECT current_database()", "current_database"},
		{"SELECT current_schemas(false)", "current_schemas"},
		{"SELECT version()", "version"},
		{"SELECT current_user AS whoami", "whoami"},
	}

	for _, tt := range tests {
		parsed, err := Parse(tt.sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tt.sql, err)
		}
		if got := parsed.SelectInfo.Columns[0].Alias; got != tt.wantAlias {
			t.Errorf("%q: output label = %q, want %q", tt.sql, got, tt.wantAlias)
		}
	}
}

// TestParseDataGripOpeningQuery is the statement from the DataGrip 2026.1.3
// debug log that motivated niladic support.
func TestParseDataGripOpeningQuery(t *testing.T) {
	parsed, err := Parse("select current_database(), current_schema(), current_user")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cols := parsed.SelectInfo.Columns
	if len(cols) != 3 {
		t.Fatalf("got %d columns, want 3", len(cols))
	}
	want := []string{"current_database", "current_schema", "current_user"}
	for i, w := range want {
		if cols[i].Alias != w {
			t.Errorf("column %d label = %q, want %q", i, cols[i].Alias, w)
		}
		if _, ok := cols[i].ASTExpr.(*FuncCallNode); !ok {
			t.Errorf("column %d is %T, want a function call", i, cols[i].ASTExpr)
		}
	}
}
