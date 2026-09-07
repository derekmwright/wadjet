package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// j1Arms is e3Arms plus a FIFTH arm that FORCES the spill.
//
// The `spilled` arm e3Arms builds runs at a 512 KiB budget, and the LATERAL
// fixture is four rows: at that size nothing reaches a spill threshold, so
// that arm is a second copy of `single` for these cells. A spill is a
// CONDITION, not a query shape (ADR-0027) — the only way a gate can claim the
// spilled path answered is to ARM the condition and assert it engaged.
func j1Arms(t *testing.T, ctx context.Context) []e3Arm {
	t.Helper()
	arms := e3Arms(t, ctx)
	forced := e3BudgetedStandalone(t, ctx)
	before := exec.ForcedAggDrains.Load()
	arms = append(arms, e3Arm{
		name: "spill-forced (" + e3ArmBudgetName + ", drain every batch)",
		run: func(sql string) ([]string, [][]any, error) {
			restore := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(512)
			cols, rows, err := e3PosSingle(ctx, forced, sql)
			restoreRuns()
			exec.ForceAggDrainEvery(restore)
			return cols, rows, err
		},
	})
	t.Cleanup(func() {
		if exec.ForcedAggDrains.Load() > before {
			return
		}
		// Every cell here holds an aggregate, so the drain knob has something
		// to drain on the cells themselves. If it never fired, the arm ran
		// in-memory and proved nothing.
		t.Errorf("the forced-spill arm never drained a partial aggregate — it is a " +
			"second copy of `single` and this gate's spilled column is worthless " +
			"(ADR-0027 §6)")
	})
	return arms
}

// AN UNGROUPED AGGREGATE'S EMPTY-INPUT VALUE IS RIGHT FOR EVERY TYPE FAMILY,
// BECAUSE IT IS AN EXPRESSION AND NOT A STAMPED VALUE (#977, arc J1 round 5).
//
// Round 4 carried the default as TEXT and wrote it into the padded row's
// vector in place, through a hand-written type switch. That cannot be right
// for a varlen or a container vector and it was not:
// `CAST(COUNT(*) AS VARCHAR)` answered `2 | "" | "20"` — a MATCHED row emptied
// and the padded one holding two values concatenated — and the star spellings
// crashed in `slice bounds out of range`.
//
// The default is now a compiled PROJECTION EXPRESSION,
// `CASE WHEN <marker> IS NULL THEN <the item over an empty input> ELSE <col> END`,
// built as an AST at plan time and compiled through `internal/engine/expr`. It
// takes the engine's own typed kernel for all 22 types and writes a NEW
// vector, which is what every other computed column does. There is no
// per-type stamp left to get wrong, and this gate is the proof: one cell per
// storage family, each holding all THREE row kinds at once.
//
// The fixture makes that possible. `amount > 60` leaves Alice ONE item, Bob
// TWO and Carol NONE, so `CASE WHEN COUNT(*) = 1 THEN NULL ELSE <item> END`
// gives:
//
//   - Alice — a MATCHED row whose own value is NULL. Round 3 stamped these.
//   - Bob   — a MATCHED row with a value. Round 4 emptied these.
//   - Carol — the PAD, whose value is the item over an empty input: the
//     aggregate is 0 there, so `CAST(COUNT(*) AS VARCHAR)` is '0'.
//
// ROW and MAP have no cell because no SELECT item can produce one: `ROW(...)`
// is a parse error and there is no map constructor (`map_from_entries` needs
// the ROW literal). Recorded for filing, not a hole this gate can close.
func TestArcJ1TheEmptyInputDefaultIsRightForEveryTypeFamily(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := j1Arms(t, ctx)

	// The lateral: Alice one item over 60, Bob two, Carol none.
	lat := func(item string) string {
		return `FROM lat_ord o LEFT JOIN LATERAL (SELECT ` + item + ` AS n FROM lat_item ` +
			`WHERE order_id = o.id AND amount > 60) s ON true ORDER BY o.id`
	}
	// nullThen is the uniform shape: a matched NULL, then the item.
	nullThen := func(item string) string {
		return `CASE WHEN COUNT(*) = 1 THEN NULL ELSE ` + item + ` END`
	}
	// twoBranch is for the families no count can be cast into: the empty-input
	// branch is chosen BY the substituted aggregate, which is the whole point.
	twoBranch := func(pad, matched string) string {
		return `CASE WHEN COUNT(*) = 1 THEN NULL WHEN COUNT(*) = 0 THEN ` + pad +
			` ELSE ` + matched + ` END`
	}

	for _, fam := range []struct {
		// name is the storage family this cell stands for.
		name string
		// item is the lateral's SELECT item.
		item string
		// want is `c,n | Alice,<matched NULL> | Bob,<matched> | Carol,<pad>`.
		want string
	}{
		{"string", nullThen(`CAST(COUNT(*) AS VARCHAR)`), `Alice,NULL | Bob,2 | Carol,0`},
		{"string-concat", nullThen(`'n=' || CAST(COUNT(*) AS VARCHAR)`),
			`Alice,NULL | Bob,n=2 | Carol,n=0`},
		{"bytes", nullThen(`CAST(CAST(COUNT(*) AS VARCHAR) AS BYTES)`),
			`Alice,NULL | Bob,2 | Carol,0`},
		{"int64", nullThen(`COUNT(*) + 1`), `Alice,NULL | Bob,3 | Carol,1`},
		{"int32", nullThen(`CAST(COUNT(*) AS INT32)`), `Alice,NULL | Bob,2 | Carol,0`},
		{"float64", nullThen(`COALESCE(SUM(amount), 0)`), `Alice,NULL | Bob,200 | Carol,0`},
		{"decimal", nullThen(`CAST(COUNT(*) AS DECIMAL(9,2))`),
			`Alice,NULL | Bob,2.00 | Carol,0.00`},
		{"bool", nullThen(`COUNT(*) = 0`), `Alice,NULL | Bob,false | Carol,true`},
		{"array-int", nullThen(`ARRAY[COUNT(*)]`), `Alice,NULL | Bob,[2] | Carol,[0]`},
		{"array-string", nullThen(`ARRAY[CAST(COUNT(*) AS VARCHAR)]`),
			`Alice,NULL | Bob,[2] | Carol,[0]`},
		{"vector", nullThen(`CAST(ARRAY[COUNT(*)] AS VECTOR(1))`),
			`Alice,NULL | Bob,[2] | Carol,[0]`},
		{"date", twoBranch(`CAST('2020-01-01' AS DATE)`, `CAST('2021-01-01' AS DATE)`),
			`Alice,NULL | Bob,2021-01-01 | Carol,2020-01-01`},
		{"timestamp", twoBranch(`CAST('2020-01-01 00:00:00' AS TIMESTAMP)`,
			`CAST('2021-01-01 00:00:00' AS TIMESTAMP)`),
			`Alice,NULL | Bob,1609459200000 | Carol,1577836800000`},
		{"uuid", twoBranch(`CAST('00000000-0000-0000-0000-000000000000' AS UUID)`,
			`CAST('11111111-1111-1111-1111-111111111111' AS UUID)`),
			`Alice,NULL | Bob,11111111-1111-1111-1111-111111111111 | ` +
				`Carol,00000000-0000-0000-0000-000000000000`},
		{"ipv4", twoBranch(`CAST('10.0.0.1' AS IPV4)`, `CAST('10.0.0.2' AS IPV4)`),
			`Alice,NULL | Bob,10.0.0.2 | Carol,10.0.0.1`},
		{"ipv6", twoBranch(`CAST('::1' AS IPV6)`, `CAST('::2' AS IPV6)`),
			`Alice,NULL | Bob,::2 | Carol,::1`},
		{"cidr", twoBranch(`CAST('10.0.0.0/8' AS CIDR)`, `CAST('192.168.0.0/16' AS CIDR)`),
			`Alice,NULL | Bob,192.168.0.0/16 | Carol,10.0.0.0/8`},
		{"mac", twoBranch(`CAST('00:00:00:00:00:00' AS MAC)`, `CAST('11:11:11:11:11:11' AS MAC)`),
			`Alice,NULL | Bob,11:11:11:11:11:11 | Carol,00:00:00:00:00:00`},
		{"duration", twoBranch(`CAST('1s' AS DURATION)`, `CAST('2s' AS DURATION)`),
			`Alice,NULL | Bob,2s | Carol,1s`},
		{"port", twoBranch(`CAST(80 AS PORT)`, `CAST(443 AS PORT)`),
			`Alice,NULL | Bob,443 | Carol,80`},
	} {
		fam := fam
		t.Run(fam.name, func(t *testing.T) {
			// THE NAMED SPELLING answers on every arm.
			named := `SELECT o.customer AS c, s.n AS n ` + lat(fam.item)
			for _, arm := range arms {
				cols, rows, err := arm.run(named)
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s", arm.name, err, named)
				}
				if got := e3Render(cols, rows); got != "c,n | "+fam.want {
					t.Fatalf("%s arm: %s\n  want c,n | %s\n  SQL: %s",
						arm.name, got, fam.want, named)
				}
			}
			// THE STAR SPELLINGS take the same operator and must publish the
			// same value under the lateral's own name, with the correlation
			// slot gone. The DAG arms are PINNED: a lateral whose item is a
			// COMPUTED expression has no stage that materializes it (a Project
			// emits no stage), so the join's build files carry the aggregate's
			// raw slot while the empty-build task carries the DECLARED schema,
			// and the two disagree under ADR-0010. That is not this operator's
			// doing — it fails identically with the operator removed — and it
			// is not about laterals either; see
			// TestArcJ1TheDagStarOverADerivedSideLosesAColumn.
			starWant := strings.ReplaceAll(fam.want, "Alice,", "1,Alice,150,")
			starWant = strings.ReplaceAll(starWant, "Bob,", "2,Bob,200,")
			starWant = strings.ReplaceAll(starWant, "Carol,", "3,Carol,0,")
			for _, sp := range []struct{ name, sql string }{
				{"star", `SELECT * ` + lat(fam.item)},
				{"derived-star", `SELECT * FROM (SELECT * ` + lat(fam.item) + `) x`},
			} {
				for _, arm := range arms {
					cols, rows, err := arm.run(sp.sql)
					if arm.coord != nil {
						if err == nil {
							t.Fatalf("%s/%s ANSWERED %s where the pin says the stage "+
								"model refuses it — the stage carries the computed "+
								"column now, so delete the pin\n  SQL: %s",
								sp.name, arm.name, e3Render(cols, rows), sp.sql)
						}
						if !strings.Contains(err.Error(), "one stage's files describe one relation") {
							t.Errorf("%s/%s failed by a DIFFERENT sentence: %v\n  SQL: %s",
								sp.name, arm.name, err, sp.sql)
						}
						continue
					}
					if err != nil {
						t.Fatalf("%s/%s: %v\n  SQL: %s", sp.name, arm.name, err, sp.sql)
					}
					if got := e3Render(cols, rows); got != "id,customer,total,n | "+starWant {
						t.Fatalf("%s/%s: %s\n  want id,customer,total,n | %s\n  SQL: %s",
							sp.name, arm.name, got, starWant, sp.sql)
					}
				}
			}
		})
	}
}

// A PUBLISHED CORRELATION KEY IS A USER COLUMN, ONCE OR THREE TIMES, UNDER ANY
// ALIAS (#956, arc J1 round 5).
//
// The lowering MINTS a hidden slot for the correlation key and drops it above
// the join. What it must never drop is a column the QUERY published: the key
// spelled in the lateral's own SELECT list is an output column like any other,
// and the drop is by the slot's POSITION on the side the lowering built, never
// by its name. A query may publish that key once, twice or three times, under
// its own name or under aliases, and every one of those is a column of the
// answer under the name the query gave it.
//
// The DAG's `SELECT *` is pinned, and the pin is NOT about laterals: the same
// loss reproduces over a plain join whose right side is a derived table that
// publishes a column twice — see
// TestArcJ1TheDagStarOverADerivedSideLosesAColumn, which has no LATERAL, no
// aggregate and no hidden slot in it at all.
func TestArcJ1APublishedKeyIsAUserColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	lat := func(items string) string {
		return `FROM lat_ord o JOIN LATERAL (SELECT ` + items + `, COUNT(*) AS n ` +
			`FROM lat_item WHERE order_id = o.id GROUP BY order_id) s ON true ORDER BY o.id`
	}

	for _, tc := range []struct {
		name, sql, want string
		// pinDAG is what the DISTRIBUTED arms answer today where that is NOT
		// `want`: a KNOWN DEFECT, pinned so it cannot get worse silently and
		// so a fix deletes the pin as its proof.
		pinDAG string
	}{
		// THE NAMED SPELLINGS are right on every arm — the key is published
		// under the name the query gave it, as many times as it gave it.
		{name: "named/once-under-its-own-name",
			sql:  `SELECT o.customer AS c, s.order_id AS k, s.n AS n ` + lat(`order_id`),
			want: `c,k,n | Alice,1,2 | Bob,2,2`},
		{name: "named/once-under-an-alias",
			sql:  `SELECT o.customer AS c, s.oid AS k, s.n AS n ` + lat(`order_id AS oid`),
			want: `c,k,n | Alice,1,2 | Bob,2,2`},
		{name: "named/twice",
			sql: `SELECT o.customer AS c, s.order_id AS a, s.oid AS b, s.n AS n ` +
				lat(`order_id, order_id AS oid`),
			want: `c,a,b,n | Alice,1,1,2 | Bob,2,2,2`},
		{name: "named/three-times",
			sql: `SELECT o.customer AS c, s.order_id AS a, s.oid AS b, s.oid2 AS d, ` +
				`s.n AS n ` + lat(`order_id, order_id AS oid, order_id AS oid2`),
			want: `c,a,b,d,n | Alice,1,1,1,2 | Bob,2,2,2,2`},
		{name: "named/the-alias-only",
			sql:  `SELECT o.customer AS c, s.oid AS k ` + lat(`order_id, order_id AS oid`),
			want: `c,k | Alice,1 | Bob,2`},

		// THE STAR SPELLINGS publish every one of them, under its own name,
		// and never the slot. The DAG shows the STAGE's stream instead.
		{name: "star/once-under-its-own-name", sql: `SELECT * ` + lat(`order_id`),
			want:   `id,customer,total,order_id,n | 1,Alice,150,1,2 | 2,Bob,200,2,2`,
			pinDAG: `order_id,n,id,customer,total | 1,2,1,Alice,150 | 2,2,2,Bob,200`},
		{name: "star/once-under-an-alias", sql: `SELECT * ` + lat(`order_id AS oid`),
			want:   `id,customer,total,oid,n | 1,Alice,150,1,2 | 2,Bob,200,2,2`,
			pinDAG: `order_id,n,id,customer,total | 1,2,1,Alice,150 | 2,2,2,Bob,200`},
		{name: "star/twice", sql: `SELECT * ` + lat(`order_id, order_id AS oid`),
			want: `id,customer,total,order_id,oid,n | 1,Alice,150,1,1,2 | ` +
				`2,Bob,200,2,2,2`,
			pinDAG: `order_id,n,id,customer,total | 1,2,1,Alice,150 | 2,2,2,Bob,200`},
		{name: "star/three-times",
			sql: `SELECT * ` + lat(`order_id, order_id AS oid, order_id AS oid2`),
			want: `id,customer,total,order_id,oid,oid2,n | 1,Alice,150,1,1,1,2 | ` +
				`2,Bob,200,2,2,2,2`,
			pinDAG: `order_id,n,id,customer,total | 1,2,1,Alice,150 | 2,2,2,Bob,200`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, tc.want, tc.sql)
				}
				got := e3Render(cols, rows)
				want := tc.want
				if tc.pinDAG != "" && arm.coord != nil {
					if got == tc.want {
						t.Fatalf("%s arm ANSWERED PostgreSQL's %s where the pin says it "+
							"loses a column — the stage publishes the projection now, "+
							"so delete the pin\n  SQL: %s", arm.name, got, tc.sql)
					}
					want = tc.pinDAG
				}
				if got != want {
					t.Fatalf("%s arm: %s\n  want %s\n  SQL: %s",
						arm.name, got, want, tc.sql)
				}
			}
		})
	}
}

// THE DAG'S `SELECT *` OVER A DERIVED SIDE LOSES A COLUMN, WITH NO LATERAL IN
// THE QUERY (arc J1 round 5, recorded for filing).
//
// This is the MECHANISM behind every DAG pin in this arc, isolated: a plain
// inner join whose right side is a derived table publishing `order_id` twice.
// No LATERAL, no aggregate, no correlation, no hidden slot — and the
// single-process arms answer PostgreSQL's five columns while the DAG arms
// answer four. A Project emits no stage, so `SELECT *` on the distributed path
// shows the STAGE's stream rather than the query's projection; a column the
// projection introduces is not in that stream.
//
// It is here so the pins in this arc cannot be read as a lateral defect, and
// so the day the DAG publishes a projection this test fails and every pin
// beside it can be deleted together.
func TestArcJ1TheDagStarOverADerivedSideLosesAColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	const sql = `SELECT * FROM lat_ord o JOIN (SELECT order_id, order_id AS oid ` +
		`FROM lat_item) s ON s.order_id = o.id ORDER BY o.id`
	const want = `order_id,oid,id,customer,total | 1,1,1,Alice,150 | 1,1,1,Alice,150 | ` +
		`2,2,2,Bob,200 | 2,2,2,Bob,200`
	const pinDAG = `order_id,id,customer,total | 1,1,Alice,150 | 1,1,Alice,150 | ` +
		`2,2,Bob,200 | 2,2,Bob,200`

	for _, arm := range arms {
		cols, rows, err := arm.run(sql)
		if err != nil {
			t.Fatalf("%s arm: %v\n  SQL: %s", arm.name, err, sql)
		}
		got := e3Render(cols, rows)
		if arm.coord == nil {
			if got != want {
				t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
					arm.name, got, want, sql)
			}
			continue
		}
		if got == want {
			t.Fatalf("%s arm ANSWERED PostgreSQL's %s — the stage publishes the "+
				"projection now. DELETE this pin and every `pinDAG` beside it in "+
				"this arc\n  SQL: %s", arm.name, got, sql)
		}
		if got != pinDAG {
			t.Fatalf("%s arm: %s\n  pinned at %s\n  SQL: %s", arm.name, got, pinDAG, sql)
		}
	}
}

// AN `ON` CONDITION OVER A DEFAULTED COLUMN IS EITHER RIGHT OR LOUD, NEVER
// NULL WHERE POSTGRESQL SAYS 0 (arc J1 round 5).
//
// PostgreSQL evaluates the LATERAL per outer row and applies the ON AFTER it,
// so a row the subquery produced from NO INPUT still faces the condition, and
// `ON s.n = 0` keeps Carol WITH her zero. This engine decorrelates into a
// join, so the ON is applied by the join — below the operator that writes the
// empty-input value. Where the two orders agree, the query answers; where they
// do not, it refuses.
//
// The three dispositions, and why each is the one it is:
//
//   - INNER `JOIN LATERAL … ON s.n = 0` ANSWERS. `lateralPadThenFilter` moves
//     the written ON into the enclosing WHERE, which is evaluated ABOVE the
//     default — PostgreSQL's order exactly.
//   - LEFT `… ON s.n > 1` ANSWERS. The condition REJECTS the padded row, and a
//     rejected row on an outer join is kept with NULLs, which is what padding
//     first also produces. The two orders agree, and the fold proves it.
//   - LEFT `… ON s.n = 0` REFUSES (0A000). The condition would have ACCEPTED
//     the padded row, so PostgreSQL keeps Carol with 0 and this order would
//     answer NULL. A wrong number is not an option, so it is one sentence.
//
// WHERE over a defaulted column was already right and stays right: the filter
// sits above the join, and the optimizer is forbidden to push it through.
func TestArcJ1AnOnConditionOverADefaultedColumnIsRightOrLoud(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := j1Arms(t, ctx)

	const cnt = `LATERAL (SELECT COUNT(*) AS n FROM lat_item WHERE order_id = o.id) s`

	for _, tc := range []struct{ name, sql, want, wantErrLike string }{
		{name: "inner-on-accepts-the-pad",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o JOIN ` + cnt +
				` ON s.n = 0 ORDER BY 1`,
			want: `c,n | Carol,0`},
		{name: "left-on-rejects-the-pad",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o LEFT JOIN ` + cnt +
				` ON s.n > 1 ORDER BY 1`,
			want: `c,n | Alice,2 | Bob,2 | Carol,NULL`},
		{name: "left-on-accepts-the-pad-is-refused",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o LEFT JOIN ` + cnt +
				` ON s.n = 0 ORDER BY 1`,
			wantErrLike: `cannot be answered`},
		// A join matches only on TRUE, so an ON that folds to UNKNOWN rejects
		// the padded row exactly as FALSE does, and this answers.
		{name: "left-on-is-unknown-over-the-pad",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o LEFT JOIN ` + cnt +
				` ON s.n = NULL ORDER BY 1`,
			want: `c,n | Alice,NULL | Bob,NULL | Carol,NULL`},
		{name: "left-on-false-rejects-everything",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o LEFT JOIN ` + cnt +
				` ON 1 = 0 ORDER BY 1`,
			want: `c,n | Alice,NULL | Bob,NULL | Carol,NULL`},
		// AN ON OVER AN OUTER COLUMN IS REFUSED TOO, and this is the cell
		// that made the first cut of the refusal wrong: it only looked at ONs
		// naming the LATERAL, and `ON o.id > 1` names none. PostgreSQL keeps
		// Carol at 0 — the pair (Carol, the empty-input row) passes `3 > 1` —
		// and this engine answered `Carol, NULL` in silence. Whether the
		// padded row passes depends on the OUTER row, so nothing can be
		// folded and nothing can be proven.
		{name: "left-on-over-an-outer-column-is-refused",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o LEFT JOIN ` + cnt +
				` ON o.id > 1 ORDER BY 1`,
			wantErrLike: `cannot be answered`},
		{name: "left-on-true-is-the-plain-shape",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o LEFT JOIN ` + cnt +
				` ON true ORDER BY 1`,
			want: `c,n | Alice,2 | Bob,2 | Carol,0`},
		// WHERE is evaluated ABOVE the join and stays right.
		{name: "where-over-the-defaulted-column",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o LEFT JOIN ` + cnt +
				` ON true WHERE s.n = 0 ORDER BY 1`,
			want: `c,n | Carol,0`},
		{name: "where-rejects-the-pad",
			sql: `SELECT o.customer AS c, s.n AS n FROM lat_ord o LEFT JOIN ` + cnt +
				` ON true WHERE s.n > 1 ORDER BY 1`,
			want: `c,n | Alice,2 | Bob,2`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(tc.sql)
				if tc.wantErrLike != "" {
					if err == nil {
						t.Fatalf("%s arm ANSWERED %s where the refusal is the "+
							"disposition — a NULL where PostgreSQL says 0 is the "+
							"defect this refuses\n  SQL: %s",
							arm.name, e3Render(cols, rows), tc.sql)
					}
					if !strings.Contains(err.Error(), tc.wantErrLike) {
						t.Fatalf("%s arm refused by a DIFFERENT sentence: %v\n  SQL: %s",
							arm.name, err, tc.sql)
					}
					if n := strings.Count(err.Error(), "\n"); n > 0 {
						t.Errorf("%s arm's refusal is %d lines; it must be ONE "+
							"sentence: %v", arm.name, n+1, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s arm: %v\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, err, tc.want, tc.sql)
				}
				if got := e3Render(cols, rows); got != tc.want {
					t.Fatalf("%s arm: %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
