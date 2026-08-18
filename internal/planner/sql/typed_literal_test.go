package sql

import "testing"

// A typed literal is a type name followed by a string; a bare type name is
// still an ordinary identifier. `SELECT date FROM t` must keep parsing as a
// column reference, and `SELECT DATE '1992-01-01'` must not.
func TestTypedLiteralVsColumnNamed(t *testing.T) {
	for _, tt := range []struct {
		name, sql string
		wantCast  bool
	}{
		{"date literal", "SELECT DATE '1992-01-01' FROM t", true},
		{"timestamp literal", "SELECT TIMESTAMP '1992-01-01 10:00:00' FROM t", true},
		{"time literal", "SELECT TIME '10:00:00' FROM t", true},
		{"column named date", "SELECT date FROM t", false},
		{"column named time", "SELECT time, date FROM t", false},
		{"qualified column named date", "SELECT t.date FROM t", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if len(info.Columns) == 0 {
				t.Fatal("no columns")
			}
			_, isCast := info.Columns[0].ASTExpr.(*CastNode)
			if isCast != tt.wantCast {
				t.Fatalf("%s: cast=%v want %v (expr %T: %v)",
					tt.sql, isCast, tt.wantCast, info.Columns[0].ASTExpr, info.Columns[0].ASTExpr)
			}
		})
	}
}
