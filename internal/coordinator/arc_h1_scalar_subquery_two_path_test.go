package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// A SUBQUERY'S RESULT IS ITS SELECT LIST — #875, on FOUR ARMS with REPLICATES.
//
// `SELECT id, (SELECT c_ts FROM typemx ORDER BY id LIMIT 1) FROM …` answered a
// DIFFERENT VALUE run to run: about one run in five it came back as `id`'s
// value (`0` over typemx, `1` over decpair), on the single-process pipeline,
// the spilled one and BOTH DAG arms.
//
// The mechanism is two facts meeting. The logical builder materializes an
// ORDER BY term the SELECT list does not carry as a HIDDEN projection
// (`__sortkey_0`), and `Plan` drops it again before rows reach the client
// (#320) — but `buildSubqueryPipelineFor` had no such trim, so a SUBQUERY's
// rows arrived carrying the hidden key beside the one column selected. Every
// consumer that reduces a subquery row to ONE value then picks it out of a Go
// MAP: `expr.ScalarSubqueryValue`'s `for _, v := range rows[0]`,
// `InSubquery.resolveSlow`'s "first column only", and
// `CorrelatedInSubquery`'s. Map iteration order is randomized per range
// statement, which is the whole of the run-to-run divergence.
//
// This is NOT one of ADR-0013's eight classes of legal nondeterminism: the SQL
// carries a total order over a unique key, so the answer is determinate, and
// what changed was a VALUE.
//
// Every Want below is live PostgreSQL 17 over the same rows (the h1_ fixtures
// in this arc's ROUND0). Ten replicates per arm per shape, because a defect
// that fires one run in five passes a single-run gate four times out of five.
func TestArcH1AScalarSubqueryIsItsSelectList(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	const reps = 10
	for _, tc := range []struct {
		name string
		sql  string
		want string
		// routes, when true, asserts that each DAG arm's
		// ScalarProjectionLocalRoutes moved for this shape: the SELECT-list
		// lowering DECLINES a `LIMIT 1` item (it is not provably one row) and
		// the coordinator answers locally. Rows alone cannot tell "the DAG
		// executed it" from "the DAG refused it and the local pipeline
		// answered", and this shape's whole history is on the local route.
		routes bool
	}{
		// The hidden ORDER BY key is the trigger, so every shape here orders
		// by a column the SELECT list does NOT carry.
		{name: "timestamp", routes: true,
			sql:  `SELECT id, (SELECT c_ts FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,1700000000000 | 1,1700000000000`},
		{name: "uuid", routes: true,
			sql: `SELECT id, (SELECT c_uuid FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,00000000-0000-4000-8000-000000000000 | ` +
				`1,00000000-0000-4000-8000-000000000000`},
		{name: "decimal-9-2", routes: true,
			sql:  `SELECT id, (SELECT a FROM decpair ORDER BY id LIMIT 1) AS v FROM decpair WHERE id < 3 ORDER BY id`,
			want: `id,v | 1,12.75 | 2,12.75`},
		{name: "decimal-18-4", routes: true,
			sql:  `SELECT id, (SELECT b FROM decpair ORDER BY id LIMIT 1) AS v FROM decpair WHERE id < 3 ORDER BY id`,
			want: `id,v | 1,12.7500 | 2,12.7500`},
		{name: "string", routes: true,
			sql:  `SELECT id, (SELECT c_str FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,s-000000 | 1,s-000000`},
		// BYTES renders as the byte slice a BYTES column boxes as, which is
		// what the PLAIN spelling of the same value answers
		// (TestArcH1AScalarSubqueryAnswersItsOwnType asserts that pairing
		// directly). Before #874 the item fell to the STRING fallback and
		// this cell read `bytes-000000-`; the DIGITS are the same value,
		// under the type a bytea has.
		{name: "bytes", routes: true,
			sql: `SELECT id, (SELECT c_bytes FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,[98 121 116 101 115 45 48 48 48 48 48 48 45] | ` +
				`1,[98 121 116 101 115 45 48 48 48 48 48 48 45]`},
		{name: "date", routes: true,
			sql:  `SELECT id, (SELECT c_date FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,2010-01-01 | 1,2010-01-01`},
		{name: "ipv4", routes: true,
			sql:  `SELECT id, (SELECT c_ipv4 FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,10.0.0.0 | 1,10.0.0.0`},
		{name: "cidr", routes: true,
			sql:  `SELECT id, (SELECT c_cidr FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,192.168.0.0/24 | 1,192.168.0.0/24`},
		{name: "aliased-select-item", routes: true,
			sql:  `SELECT id, (SELECT c_str AS s FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,s-000000 | 1,s-000000`},
		{name: "descending-hidden-key", routes: true,
			sql:  `SELECT id, (SELECT c_str FROM typemx ORDER BY id DESC LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,s-004999 | 1,s-004999`},
		{name: "hidden-key-over-a-cte", routes: true,
			sql: `WITH c AS (SELECT id, c_i64 AS w FROM typemx) ` +
				`SELECT id, (SELECT w FROM c ORDER BY id LIMIT 1) AS v FROM decpair WHERE id < 2`,
			want: `id,v | 1,0`},

		// THE SIBLING SITE. An IN-subquery reduces each of its rows to one
		// value through the same map iteration ("first column only"), so a
		// hidden ORDER BY key inside an IN-subquery is the same defect one
		// construct over. PostgreSQL: the five lowest ids, so four of
		// decpair's nine match.
		{name: "in-subquery-with-a-hidden-key",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN ` +
				`(SELECT id FROM typemx ORDER BY c_str LIMIT 5)`,
			want: `n | 4`},
		{name: "not-in-subquery-with-a-hidden-key",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id NOT IN ` +
				`(SELECT id FROM typemx ORDER BY c_str LIMIT 5)`,
			want: `n | 5`},

		// A CORRELATED scalar subquery takes ScalarSubqueryValue too, and its
		// SQL is rebuilt per outer row — so the trim has to be on the BUILD,
		// which is where it is.
		{name: "correlated-with-a-hidden-key",
			sql: `SELECT d.id AS did, (SELECT t.c_str FROM typemx t WHERE t.id = d.id ` +
				`ORDER BY t.c_i64 LIMIT 1) AS s FROM decpair d WHERE d.id < 4 ORDER BY d.id`,
			want: `did,s | 1,s-000001 | 2,s-000002 | 3,s-000003`},

		{name: "hidden-key-under-a-filter", routes: true,
			sql: `SELECT id, (SELECT c_str FROM typemx WHERE id < 3 ORDER BY c_i64 LIMIT 1) AS v ` +
				`FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,s-000000 | 1,s-000000`},

		// THE CONTROLS, and they bound the fix from the other side: a
		// subquery whose ORDER BY term IS in its SELECT list has no hidden
		// column, so the trim must not fire and the answer must not move.
		{name: "ctl-order-by-the-selected-column", routes: true,
			sql:  `SELECT id, (SELECT c_ts FROM typemx ORDER BY c_ts LIMIT 1) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,1700000000000 | 1,1700000000000`},
		{name: "ctl-an-aggregate-has-no-order-by",
			sql:  `SELECT id, (SELECT MAX(c_i64) FROM typemx) AS v FROM typemx WHERE id < 2 ORDER BY id`,
			want: `id,v | 0,4999014997 | 1,4999014997`},
		{name: "ctl-in-subquery-without-a-hidden-key",
			sql: `SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN ` +
				`(SELECT id FROM typemx ORDER BY id LIMIT 5)`,
			want: `n | 4`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := int64(0)
				if arm.coord != nil {
					before = arm.coord.ScalarProjectionLocalRoutes()
				}
				seen := map[string]int{}
				for i := 0; i < reps; i++ {
					cols, rows, err := arm.run(tc.sql)
					if err != nil {
						t.Fatalf("%s arm refused replicate %d: %v\n  SQL: %s",
							arm.name, i, err, tc.sql)
					}
					seen[e3Render(cols, rows)]++
				}
				if len(seen) != 1 {
					keys := make([]string, 0, len(seen))
					for k, n := range seen {
						keys = append(keys, strings.TrimSpace(k)+"  (x"+itoa(n)+")")
					}
					sort.Strings(keys)
					t.Fatalf("%s arm answered %d DIFFERENT results over %d replicates — a scalar "+
						"subquery is ONE value, and this SQL carries a total order:\n    %s\n  SQL: %s",
						arm.name, len(seen), reps, strings.Join(keys, "\n    "), tc.sql)
				}
				for got := range seen {
					if got != tc.want {
						t.Fatalf("%s arm: %s\n  want (live PostgreSQL 17): %s\n  SQL: %s",
							arm.name, got, tc.want, tc.sql)
					}
				}
				if tc.routes && arm.coord != nil {
					if got := arm.coord.ScalarProjectionLocalRoutes() - before; got != int64(reps) {
						t.Errorf("%s arm: ScalarProjectionLocalRoutes moved by %d over %d "+
							"replicates, want %d — this shape declines the SELECT-list "+
							"producer lowering and is answered on the coordinator-local "+
							"route, which is the route #875 is about\n  SQL: %s",
							arm.name, got, reps, reps, tc.sql)
					}
				}
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
