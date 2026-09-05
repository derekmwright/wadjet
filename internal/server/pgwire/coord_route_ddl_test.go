package pgwire

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// `shouldRouteThroughCoord` is the fact three documents now rest on: the
// PostgreSQL wire protocol sends only SELECT and WITH to the coordinator, so a
// statement whose ONLY handler is `Coordinator.ExecuteSQL` is unreachable from
// psql however the server was started.
//
// That is why `CREATE SNAPSHOT` is documented as reachable through the gRPC
// `Query` RPC and nowhere else (round-1 B2), and why `--enable-alerts` makes
// the ALERT statements reachable over gRPC only (round-2 P9): in standalone
// mode the coordinator owns the alert scheduler, and the DB this door is built
// over is opened without `EnableAlerts`, so it answers `alerts are disabled`.
//
// A change that routed DDL here would make all three of those doc lines wrong
// with nothing to notice. This is what notices.
func TestThePgwireDoorRoutesOnlyQueriesToTheCoordinator(t *testing.T) {
	// Every statement the parser accepts, with the routing this door owes it.
	for _, tc := range []struct {
		sql   string
		coord bool
	}{
		{"SELECT 1", true},
		{"select n_nationkey FROM nation", true},
		{"  (SELECT 1)", true},
		{"WITH q AS (SELECT 1) SELECT * FROM q", true},

		{"CREATE SNAPSHOT", false},
		{"CREATE ALERT a AS SELECT id FROM t EVERY 5 MINUTES WEBHOOK 'https://x'", false},
		{"DROP ALERT a", false},
		{"ALTER ALERT a DISABLE", false},
		{"ALTER TABLE t ADD COLUMN z BIGINT", false},
		{"CREATE VIEW v AS SELECT 1", false},
		{"DROP VIEW v", false},
		{"CREATE TABLE t (a BIGINT)", false},
		{"DROP TABLE t", false},
		{"ANALYZE t", false},
		{"DESCRIBE t", false},
		{"SHOW TABLES", false},
		{"EXPLAIN SELECT 1", false},
		{"INSERT INTO t VALUES (1)", false},
		{"UPDATE t SET a = 1", false},
		{"DELETE FROM t", false},
		{"MERGE INTO t USING (SELECT 1 AS i) s ON t.a = s.i WHEN MATCHED THEN UPDATE SET a = 2", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			if got := shouldRouteThroughCoord(tc.sql); got != tc.coord {
				t.Errorf("shouldRouteThroughCoord(%q) = %v, want %v.\n"+
					"If DDL now routes to the coordinator, docs/sql-reference.md, "+
					"docs/api-reference.md and docs/disaster-recovery.md all say it does "+
					"not — CREATE SNAPSHOT and the ALERT statements are documented as "+
					"reachable through the gRPC Query RPC only, because of this function.",
					tc.sql, got, tc.coord)
			}
		})
	}
}

// TestNoCoordinatorOnlyStatementIsRoutedHere states the same thing over the
// PARSER's own statement list rather than a corpus: whatever `plansql` learns
// to parse, this door still sends only queries to the coordinator.
func TestNoCoordinatorOnlyStatementIsRoutedHere(t *testing.T) {
	for _, sql := range []string{
		"CREATE SNAPSHOT",
		"DROP ALERT a",
		"ALTER ALERT a DISABLE",
	} {
		parsed, err := plansql.Parse(sql)
		if err != nil {
			t.Fatalf("the parser refuses %q: %v", sql, err)
		}
		if parsed.SelectInfo != nil {
			t.Fatalf("%q parses as a query; this list is for statements that do not", sql)
		}
		if shouldRouteThroughCoord(sql) {
			t.Errorf("%q routes to the coordinator, which is the only place its handler "+
				"lives — if that is now true, the door attribution in the docs is stale "+
				"and should be widened", sql)
		}
	}
}
