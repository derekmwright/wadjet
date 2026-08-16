package clickbench

import (
	"bufio"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestClickBenchQueriesParse is the arc's coverage floor: every one of the
// 43 ClickBench queries (standard-SQL/DuckDB dialect, queries.sql) must at
// least PARSE. Failures here enumerate the parser gaps to close before any
// execution work; planner/function binding gaps surface later in the
// execution harness, not here.
func TestClickBenchQueriesParse(t *testing.T) {
	f, err := os.Open("queries.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	n, failed := 0, 0
	for sc.Scan() {
		q := sc.Text()
		if q == "" {
			continue
		}
		n++
		if _, err := sql.Parse(q); err != nil {
			failed++
			t.Errorf("Q%d does not parse: %v\n  %s", n, err, q)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("clickbench parse coverage: %d/%d", n-failed, n)
}
