package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The gate for the RESERVED SLOT NAMESPACE.
//
// The planner materializes its own values into hidden slots — a window's
// output (`__win_N`), a materialized window key (`__winkey_N`), an ORDER BY
// term (`__sortkey_N`), a computed GROUP BY key (`__gb_expr_N`) — and every
// consumer reads one BY NAME off a batch. The names are ordinary identifiers,
// so a user can write them, and then the planner's value and the user's column
// are the same name:
//
//	SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a, b AS __win_0 FROM decpair) x
//	-- PostgreSQL 52.99 on every row; wadjet answered decpair.b, on every arm
//
// That is #694 re-created under the slot's own name, and #694's fix is what
// made it reachable for the bare spelling — but `__winkey_0` had the same
// defect before it, so the namespace and not the window is the mechanism.
//
// The namespace is reserved: a query that spells one is REFUSED (42601). That
// is a deliberate divergence from PostgreSQL, which answers these queries, and
// it is the right trade because the alternative is not answering them either —
// it is answering them wrongly.
func TestReservedSlotNamespaceIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		// slot is the family named in the refusal.
		slot string
	}{
		{
			// The witness: without the refusal this answered decpair.b.
			name: "a derived table publishing a window slot's name",
			sql: "SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a, b AS __win_0 FROM " +
				dbpTable + ") x ORDER BY id",
			slot: "__win_",
		},
		{
			// The TEXT column version, where the wrong answer was also the
			// wrong TYPE.
			name: "a derived table publishing a window slot's name over TEXT",
			sql: "SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a, s AS __win_0 FROM " +
				dbpTable + ") x ORDER BY id",
			slot: "__win_",
		},
		{
			// The window KEY family, which had the same defect before #694
			// existed: this answered NULL on every arm.
			name: "a derived table publishing a window key slot's name",
			sql: "SELECT id, SUM(a * 2) OVER () AS w FROM (SELECT id, a, s AS __winkey_0 FROM " +
				dbpTable + ") x ORDER BY id",
			slot: "__winkey_",
		},
		{
			name: "a top-level alias in the sort-key family",
			sql:  "SELECT a AS __sortkey_0 FROM " + dbpTable + " ORDER BY 1",
			slot: "__sortkey_",
		},
		{
			name: "a CTE column in the group-key family",
			sql: "WITH c AS (SELECT a AS __gb_expr_0 FROM " + dbpTable + ") " +
				"SELECT COUNT(*) AS n FROM c",
			slot: "__gb_expr_",
		},
		{
			name: "an aggregate-argument slot's name",
			sql:  "SELECT COUNT(*) AS n FROM (SELECT a AS __agg_expr_0 FROM " + dbpTable + ") x",
			slot: "__agg_",
		},
		{
			name: "a scalar-subquery slot's name",
			sql:  "SELECT id AS __scalar_0 FROM " + dbpTable + " ORDER BY 1",
			slot: "__scalar_",
		},
		{
			// Case-insensitively, because column resolution is: a consumer
			// looking up `__win_0` finds a column spelled `__WIN_0`.
			name: "the same name in a different case",
			sql: "SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a, b AS __WIN_0 FROM " +
				dbpTable + ") x ORDER BY id",
			slot: "__win_",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range sfcArms(ctx, single, coord) {
				_, err := arm.run(tc.sql)
				if err == nil {
					t.Fatalf("%s arm ANSWERED a query spelling a reserved slot name; the "+
						"planner's own value and the user's column share it and the answer "+
						"cannot be right\n  SQL: %s", arm.name, tc.sql)
				}
				if !strings.Contains(err.Error(), tc.slot) {
					t.Errorf("%s arm refused with %q, which does not name the %q family — the "+
						"refusal has to say which name collided\n  SQL: %s",
						arm.name, err, tc.slot, tc.sql)
				}
			}
		})
	}
}

// TestReservedSlotNamespaceAdmitsEverythingElse is the other half, and the one
// that keeps the refusal from being a blanket ban on leading underscores.
func TestReservedSlotNamespaceAdmitsEverythingElse(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, sql := range []string{
		// A single leading underscore is not the reserved prefix.
		"SELECT id, a AS _win_0 FROM " + dbpTable + " ORDER BY id",
		// A double underscore that names no slot family.
		"SELECT id, a AS __mycol FROM " + dbpTable + " ORDER BY id",
		"SELECT id, a AS __window FROM " + dbpTable + " ORDER BY id",
		// And the ordinary shapes the corpus is built on, so a refusal that
		// fired too widely is visible here rather than as 200 failures
		// elsewhere.
		"SELECT id, SUM(a) OVER () AS w FROM " + dbpTable + " ORDER BY id",
		"SELECT COUNT(*) AS n FROM " + typematrix.Table,
	} {
		for _, arm := range sfcArms(ctx, single, coord) {
			if _, err := arm.run(sql); err != nil {
				t.Errorf("%s arm refused a query that spells no reserved slot: %v\n  SQL: %s",
					arm.name, err, sql)
			}
		}
	}
}
