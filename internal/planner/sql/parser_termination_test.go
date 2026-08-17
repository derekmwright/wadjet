package sql

import (
	"testing"
	"time"
)

// Regression: incomplete DML must return a parse error and terminate, not
// spin. These inputs previously left a token loop with no exit at EOF
// (INSERT column list / VALUES row, and the collectUntil paren collector
// used by UPDATE ... SET). Surfaced by FuzzParseSQL.
func TestParserTerminatesOnIncompleteInput(t *testing.T) {
	cases := []string{
		"INSERT INTO t( VA",
		"INSERT INTO t(",
		"INSERT INTO t(a,",
		"INSERT INTO t (a) VALUES (1",
		"INSERT INTO t (a) VALUES (1,",
		"UPDATE A SET A=(",
		"UPDATE A SET A=((((",
		"DELETE FROM t WHERE a=(",
	}
	for _, sql := range cases {
		done := make(chan struct{})
		go func(q string) {
			defer close(done)
			defer func() { _ = recover() }()
			_, _ = Parse(q) // error is fine; not returning is not
		}(sql)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("Parse did not return on %q", sql)
		}
	}
}
