package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// AN OUTER EXPRESSION OVER A PUBLISHED SLOT IS MATERIALIZED AT ITS OWN TYPE
// (#831, #645).
//
// A SELECT item wrapping an aggregate or a window — `CAST(MAX(c) AS STRING)`,
// `CASE WHEN SUM(x) > 0 THEN 'a' ELSE 'b' END` — is evaluated at the GATHER,
// from the `__agg_N` / `__win_N` column the producing stage publishes. The
// gather built EVERY such column into a float64 vector, with a runtime escape
// for an exact DECIMAL and one for an integer, and a `default: SetNull` for
// everything else. So a STRING result — and a DATE, a TIMESTAMP or a network
// address a CAST or a CASE can produce — came back NULL on both DAG arms while
// the single-process path rendered the value. Every type, silently, right
// column shape and no data.
//
// The CAST kernel was never the defect and neither was a respell pass: the
// DERIVED-TABLE spelling of the same query gets a real `project` stage and is
// right on every arm. What was missing is a DECLARATION. A column the gather
// computes exists in no catalog, so its declared type IS its runtime type
// (ADR-0025), and `OutputRename` now carries the plan's inference — the same
// `inferProjectionDeclType` call `attachScanSelectProjections` makes for a
// SELECT item a FRAGMENT computes. One rule for a computed column's type,
// whichever operator ends up computing it.
//
// The census below is the whole matrix the fix has to cover: every FLAT type
// in the type matrix × six consumer classes × {aggregate, window} ×
// {ungrouped, grouped}, on both DAG arms. Its assertion is that each DAG arm
// answers what the SINGLE arm answers, box type included — that is this arc's
// charter, and for the eighteen types it is also the only oracle that covers
// PORT, PROTOCOL and DURATION, which PostgreSQL has no analog for. The
// PostgreSQL-verified VALUES are asserted separately, below, for the rows
// PostgreSQL can answer.
func gxtConsumers() []struct {
	name string
	// tmpl renders the SELECT item for the value expression %s.
	tmpl string
} {
	return []struct {
		name string
		tmpl string
	}{
		{"cast_string", `CAST(%s AS STRING)`},
		{"concat", `CAST(%s AS STRING) || 'z'`},
		{"case_string", `CASE WHEN %s IS NULL THEN 'n' ELSE 'y' END`},
		{"coalesce", `COALESCE(CAST(%s AS STRING), 'z')`},
		{"string_fn", `UPPER(CAST(%s AS STRING))`},
		{"comparison", `%s IS NULL`},
		// The four classes whose RESULT is the slot's OWN type. The six above
		// all dodge it — four CAST to string and two return a bool or a
		// literal — so no cell among the first 432 asked the declaration for
		// anything but STRING or BOOL, which is exactly the case it got
		// wrong: `inferRenameExprDecl` typed the AGGREGATE scope through a
		// walk with no Aggregate arm, fell to its STRING fallback, declared
		// it anyway, and `evalDeclaredColumn` rendered a DATE as its epoch
		// day `16195` on both DAG arms (#831 review B1). A census whose
		// templates cannot reach the failing case is not a census.
		//
		// `%[1]s` twice rather than a separate boolean condition: the
		// condition then has the same shape as the value (an aggregate stays
		// an aggregate, a window a window), so a cell never mixes the two
		// slot families by accident.
		{"case_own_type", `CASE WHEN %[1]s IS NOT NULL THEN %[1]s ELSE NULL END`},
		{"coalesce_own_type", `COALESCE(%[1]s, %[1]s)`},
		{"greatest_own_type", `GREATEST(%[1]s, %[1]s)`},
		{"arith_plus_zero", `%s + 0`},
	}
}

func TestAnOuterExpressionOverAPublishedSlotAgreesOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	cells := 0
	for _, col := range typematrix.Columns() {
		if !col.Flat {
			continue // a container is not a MIN/MAX input
		}
		for _, cons := range gxtConsumers() {
			for _, over := range []struct{ name, value string }{
				{"aggregate", "MAX(" + col.Name + ")"},
				{"window", "MAX(" + col.Name + ") OVER ()"},
			} {
				item := fmt.Sprintf(cons.tmpl, over.value)
				var sqls []struct{ name, sql string }
				if over.name == "aggregate" {
					sqls = []struct{ name, sql string }{
						{"ungrouped", fmt.Sprintf(
							`SELECT %s AS v FROM typemx WHERE id < 40`, item)},
						{"grouped", fmt.Sprintf(
							`SELECT g, %s AS v FROM typemx WHERE id < 40 GROUP BY g ORDER BY g`, item)},
					}
				} else {
					sqls = []struct{ name, sql string }{
						{"ungrouped", fmt.Sprintf(
							`SELECT id, %s AS v FROM typemx WHERE id < 8 ORDER BY id`, item)},
						{"grouped", fmt.Sprintf(
							`SELECT g, %s AS v FROM typemx WHERE id < 8 GROUP BY g ORDER BY g`,
							fmt.Sprintf(cons.tmpl, "MAX("+col.Name+") OVER (PARTITION BY g)"))},
					}
				}
				for _, q := range sqls {
					name := col.Name + "/" + cons.name + "/" + over.name + "/" + q.name
					cells++
					t.Run(name, func(t *testing.T) {
						want, werr := na2Run(tmdRunSingle(ctx, single, q.sql))
						for _, arm := range []struct {
							name string
							c    *Coordinator
						}{{"dag", coord}, {"dag-shuffled", coordB}} {
							got, err := na2Run(tmdRunDAG(ctx, arm.c, q.sql))
							// A shape the single arm REFUSES is asserted as
							// refused on the DAG too, rather than SKIPPED: a
							// skip is a cell that cannot fail, and a consumer
							// class no type can reach would then pass for the
							// wrong reason (`arith_plus_zero` is refused over
							// most of the 22 types, which is the answer this
							// records rather than hides).
							if werr != nil {
								if err == nil {
									t.Errorf("%s arm ANSWERED %v where the single-process arm "+
										"refuses (%v). One engine answering what the other "+
										"refuses is the divergence this gate exists for.\n  SQL: %s",
										arm.name, got, werr, q.sql)
								}
								continue
							}
							if err != nil {
								t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, q.sql)
								continue
							}
							if strings.Join(got, "\n") != strings.Join(want, "\n") {
								t.Errorf("%s arm disagrees with the single-process arm\n"+
									"  got  %v\n  want %v\n"+
									"  (the single arm is PostgreSQL's answer where PostgreSQL has\n"+
									"  the type; for PORT, PROTOCOL and DURATION it is the only\n"+
									"  oracle there is)\n  SQL: %s",
									arm.name, got, want, q.sql)
							}
						}
					})
				}
			}
		}
	}
	if cells == 0 {
		t.Fatal("the type matrix produced no flat column, so this gate cannot fail")
	}
	t.Logf("%d cells", cells)
}

// The PostgreSQL-verified half: the exact VALUES #831 tabulates, read off
// postgres:17-alpine over the same fixture. Cross-arm agreement cannot see a
// value both engines get wrong.
func TestAnOuterExpressionOverAPublishedSlotMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	cells := []struct {
		name, sql string
		want      []string
		pgSays    string
	}{
		{"cast_max_timestamp", `SELECT CAST(MAX(c_ts) AS STRING) AS v FROM typemx WHERE id < 5`,
			[]string{"v=2023-11-14 22:17:24"}, "2023-11-14 22:17:24"},
		{"cast_max_int32", `SELECT CAST(MAX(c_i32) AS STRING) AS v FROM typemx WHERE id < 5`,
			[]string{"v=12"}, "12"},
		{"cast_max_date", `SELECT CAST(MAX(c_date) AS STRING) AS v FROM typemx WHERE id < 5`,
			[]string{"v=2014-05-05"}, "2014-05-05"},
		// PostgreSQL renders an inet as `10.0.0.4/32`; wadjet's IPv4 is a HOST
		// address and renders `10.0.0.4`. A deliberate divergence of the
		// network-native types, not of this fix — what this cell asserts is
		// that the DAG renders what the single arm renders.
		{"cast_max_ipv4", `SELECT CAST(MAX(c_ipv4) AS STRING) AS v FROM typemx WHERE id < 5`,
			[]string{"v=10.0.0.4"}, "10.0.0.4/32 (inet); wadjet's IPv4 is a host address"},
		{"cast_max_grouped", `SELECT g, CAST(MAX(c_date) AS STRING) AS v FROM typemx ` +
			`WHERE id < 5 GROUP BY g ORDER BY g`,
			[]string{"g=int32:0|v=2010-01-01", "g=int32:1|v=2011-02-02", "g=int32:2|v=2012-03-03",
				"g=int32:3|v=2013-04-04", "g=int32:4|v=2014-05-05"}, "five dates"},
		{"case_over_aggregate_to_string",
			`SELECT CASE WHEN MAX(c_i32) > 0 THEN 'a' ELSE 'b' END AS v FROM typemx WHERE id < 5`,
			[]string{"v=a"}, "a"},
		{"coalesce_over_aggregate",
			`SELECT COALESCE(CAST(MAX(c_i32) AS STRING), 'z') AS v FROM typemx WHERE id < 5`,
			[]string{"v=12"}, "12"},
		{"string_function_over_aggregate",
			`SELECT UPPER(MAX(c_str)) AS v FROM typemx WHERE id < 5`,
			[]string{"v=S-000004"}, "S-000004"},
		{"concat_over_aggregate",
			`SELECT MAX(c_str) || 'z' AS v FROM typemx WHERE id < 5`,
			[]string{"v=s-000004z"}, "s-000004z"},
		{"case_over_window_to_string",
			`SELECT id, CASE WHEN ROW_NUMBER() OVER (ORDER BY id) = 1 THEN 'a' ELSE 'b' END AS w ` +
				`FROM typemx WHERE id < 5 ORDER BY id`,
			[]string{"id=int64:0|w=a", "id=int64:1|w=b", "id=int64:2|w=b",
				"id=int64:3|w=b", "id=int64:4|w=b"}, "a,b,b,b,b"},
		{"case_over_grouped_aggregate_to_string",
			`SELECT g, CASE WHEN SUM(id) > 0 THEN 'a' ELSE 'b' END AS w FROM typemx ` +
				`WHERE id < 20 GROUP BY g ORDER BY g`,
			[]string{"g=NULL|w=a", "g=int32:0|w=a", "g=int32:1|w=a", "g=int32:2|w=a",
				"g=int32:3|w=a", "g=int32:4|w=a", "g=int32:5|w=a", "g=int32:6|w=a"},
			"eight rows, all 'a'"},
		// The controls: the arms that were already right and must stay right.
		// The derived-table spelling is the one #831 names as the proof the
		// CAST kernel is innocent; the arithmetic, integer-CAST, DECIMAL and
		// comparison arms are the four the gather already materialized.
		// NAMED for what it actually controls. It exonerates the CAST kernel
		// for the CAST-to-STRING spelling and for nothing wider: with the
		// result typed as the aggregate's OWN type the same derived-table
		// spelling is WRONG on both DAG arms, which
		// `TestTheDerivedTableSpellingIsNotAControlForTheOwnTypeCase` pins.
		{"ctl_derived_table_spelling_cast_to_string",
			`SELECT CAST(m AS STRING) AS v FROM (SELECT MAX(c_ts) AS m FROM typemx WHERE id < 5) d`,
			[]string{"v=2023-11-14 22:17:24"}, "2023-11-14 22:17:24"},
		{"ctl_arithmetic_over_aggregate",
			`SELECT MAX(c_i32) + 1 AS v FROM typemx WHERE id < 5`,
			[]string{"v=int64:13"}, "13 (integer)"},
		{"ctl_integer_cast_over_aggregate",
			`SELECT CAST(MAX(c_i32) AS BIGINT) AS v FROM typemx WHERE id < 5`,
			[]string{"v=int64:12"}, "12"},
		{"ctl_decimal_over_aggregate",
			`SELECT SUM(a) * 2 AS v FROM decpair`,
			[]string{"v=105.98"}, "105.98 exact"},
		{"ctl_comparison_over_aggregate",
			`SELECT MAX(c_i32) > 0 AS v FROM typemx WHERE id < 5`,
			[]string{"v=bool:true"}, "t"},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				run  func() ([]string, error)
			}{
				{"single", func() ([]string, error) { return na2Run(tmdRunSingle(ctx, single, tc.sql)) }},
				{"dag", func() ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, tc.sql)) }},
				{"dag-shuffled", func() ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, tc.sql)) }},
			} {
				got, err := arm.run()
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				want := append([]string(nil), tc.want...)
				sort.Strings(want)
				if strings.Join(got, "\n") != strings.Join(want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v (live PostgreSQL 17: %s)\n  SQL: %s",
						arm.name, got, want, tc.pgSays, tc.sql)
				}
			}
		})
	}
}

// THE DERIVED-TABLE SPELLING IS NOT A CONTROL FOR THE OWN-TYPE CASE.
//
// #831's argument, and this file's, is "the derived-table spelling of the same
// question is right on every arm, so the CAST kernel is innocent". True of the
// CAST-to-STRING spelling; FALSE when the result is the aggregate's OWN type —
// a DATE comes back as its epoch day on both DAG arms. Pinned rather than
// reworded away, because the sentence it qualifies is load-bearing for two
// issues and because a pin FAILS when it starts agreeing.
//
// It is a DIFFERENT SITE from the one this branch fixes, and the plan says so.
// The aggregate spelling reaches the gather as an `OutputRename` carrying an
// `Expr`, typed by `inferRenameExprDecl` — which asks the EMITTED scope now, so
// the aggregate's `__agg_N` outputs are declared and the family is right. This
// spelling gets a real `project` stage whose `ProjectExprSpec.Type` comes from
// `attachScanSelectProjections`' `inputColDecls(projNode.Children[0])`, and
// `inputColTypes` has arms for Scan, Filter/Sort/Limit/Distinct, Window and
// Join and NONE for Project or Aggregate — so above either of those it answers
// nothing and the spec takes its STRING fallback. Byte-identical at `fd679ae9`;
// this branch does not touch that walk.
//
// The fix is to give that walk the answer `emittedColTypes` already has, which
// means merging the two walks — numeric-typing territory, not this arc's.
func TestTheDerivedTableSpellingIsNotAControlForTheOwnTypeCase(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	const sql = `SELECT CASE WHEN m > 0 THEN d ELSE NULL END AS v FROM ` +
		`(SELECT MAX(id) AS m, MAX(c_date) AS d FROM typemx WHERE id < 5) x`

	got, err := na2Run(tmdRunSingle(ctx, single, sql))
	if err != nil {
		t.Fatalf("single arm: %v", err)
	}
	if strings.Join(got, "\n") != "v=2014-05-05" {
		t.Errorf("single arm got %v, want [v=2014-05-05] (live PostgreSQL 17: 2014-05-05)", got)
	}
	for _, arm := range []struct {
		name string
		c    *Coordinator
	}{{"dag", coord}, {"dag-shuffled", coordB}} {
		dgot, derr := na2Run(tmdRunDAG(ctx, arm.c, sql))
		if derr != nil {
			t.Errorf("%s arm: %v", arm.name, derr)
			continue
		}
		if strings.Join(dgot, "\n") == "v=2014-05-05" {
			t.Errorf("%s arm now AGREES — the second site is fixed, so delete this pin and "+
				"move the shape into TestAnOuterExpressionOverAPublishedSlotMatchesPostgres "+
				"as a control", arm.name)
			continue
		}
		if strings.Join(dgot, "\n") != "v=16195" {
			t.Errorf("%s arm got %v, want [v=16195] — the pinned wrong answer moved without "+
				"becoming right, which is a change nobody recorded", arm.name, dgot)
		}
	}
}
