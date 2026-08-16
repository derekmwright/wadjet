package main

import (
	"os"
	"testing"
)

// The embedded queries.sql is a copy of benchmarks/clickbench/queries.sql
// (go:embed cannot reach outside the package directory). This test keeps
// the copy in sync — the correctness gate runs against the benchmarks copy,
// and the bench must run the exact same 43 queries.
func TestEmbeddedQueriesInSync(t *testing.T) {
	canonical, err := os.ReadFile("../../benchmarks/clickbench/queries.sql")
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != embeddedQueries {
		t.Fatal("cmd/clickbench-bench/queries.sql is out of sync with benchmarks/clickbench/queries.sql — copy it over")
	}
}
