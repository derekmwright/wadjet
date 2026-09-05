package coordinator

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Arc F3's shapes on the DISTRIBUTED arms.
//
// Each of them reaches a mechanism the single-process path does not have. The
// aggregate declarations (#867) are computed in the DAG from `AggSpec` rather
// than from `AggColumn`, and the arithmetic above the aggregate is a GATHER
// projection there. The network and bytea literal refusals (#627, #582) are
// raised at PLAN time now, so the arms answer whether the refusal survives the
// coordinator's own planning rather than the worker's. The BYTES literal has
// to reach a WORKER's scan, whose row-group prune is the fourth site that
// reads it. And a timestamp-valued function (#868) declares its type through
// the gather stage's own output schema, not through CollectSink's hint.
//
// Every expectation is live PostgreSQL 17.11 (the arc's ROUND0 carries the
// transcripts), or — where the claim is "one value, two spellings" — the other
// spelling of the same query, which cannot inherit a wrong number from a wrong
// engine because a divergence between the two IS the failure.
func TestArcF3ExprTypingOnEveryArm(t *testing.T) {
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

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
		{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
	}

	// ---------------------------------------------------------------- #867
	// Arithmetic over an aggregate whose ARGUMENT is computed. The value is
	// PostgreSQL's; before the fix it was a float64, and the first cell's
	// outer `+ 1` was not representable in one, so the answer came back BELOW
	// the sum it was added to.
	for _, tc := range []struct{ name, sql, want string }{
		{"agg_wide_product", `SELECT SUM(c_i64 * 3000000) + 1 AS v FROM typemx`,
			"v=36280278840510000001"},
		{"agg_sum_of_a_sum", `SELECT SUM(c_i64 + 0) + 1 AS v FROM typemx`, "v=12093426280171"},
		{"agg_decimal_product", `SELECT SUM(c_dec * 2) + 1 AS v FROM typemx`, "v=24750123.7648"},
		{"agg_max_of_a_product", `SELECT MAX(c_i64 * 3000000) + 1 AS v FROM typemx`,
			"v=int64:14997044991000001"},
		{"agg_int4_product", `SELECT SUM(c_i32 * 2) + 1 AS v FROM typemx`, "v=int64:72397261"},
		// The control that says a regression is in the walk over the
		// aggregate and not in its own output.
		{"agg_alone", `SELECT SUM(c_i64 * 3000000) AS v FROM typemx`, "v=36280278840510000000"},
		{"agg_bare_argument", `SELECT SUM(c_i64) + 1 AS v FROM typemx`, "v=12093426280171"},
	} {
		f3RunOnEveryArm(t, arms, tc.name, tc.sql, []string{tc.want})
	}

	// ---------------------------------------------------------------- #628
	// A COMPUTED boolean against a quoted literal, which reaches the row
	// evaluator on every arm (no kernel keys on a derived value).
	f3AgreeOnEveryArm(t, arms, "bool_not_yes_equals_true",
		`SELECT COUNT(*) AS n FROM typemx WHERE (NOT c_bool) = 'yes'`,
		`SELECT COUNT(*) AS n FROM typemx WHERE (NOT c_bool) = 'true'`)
	f3AgreeOnEveryArm(t, arms, "bool_coalesce_f_equals_false",
		`SELECT COUNT(*) AS n FROM typemx WHERE COALESCE(c_bool, FALSE) = 'f'`,
		`SELECT COUNT(*) AS n FROM typemx WHERE COALESCE(c_bool, FALSE) = 'false'`)
	f3RefuseOnEveryArm(t, arms, "bool_bogus_literal",
		`SELECT COUNT(*) AS n FROM typemx WHERE (NOT c_bool) = 'bogus'`,
		"invalid input syntax for type boolean")

	// ---------------------------------------------------------------- #627
	// PostgreSQL's abbreviated cidr spelling names the same value as the
	// canonical one, so the two queries are one query.
	f3AgreeOnEveryArm(t, arms, "cidr_abbreviated_spelling",
		`SELECT COUNT(*) AS n FROM typemx WHERE c_cidr = '192.168'`,
		`SELECT COUNT(*) AS n FROM typemx WHERE c_cidr = '192.168.0.0/24'`)
	f3AgreeOnEveryArm(t, arms, "cidr_abbreviated_with_mask",
		`SELECT COUNT(*) AS n FROM typemx WHERE c_cidr = '192.168.4/24'`,
		`SELECT COUNT(*) AS n FROM typemx WHERE c_cidr = '192.168.4.0/24'`)
	// The refusal is decided at PLAN time now, so it fires over a scan the
	// filter never reaches a row of. `id < 0` is what makes that the claim:
	// a data-dependent refusal answers zero rows here.
	f3RefuseOnEveryArm(t, arms, "cidr_garbage_literal_over_an_empty_scan",
		`SELECT COUNT(*) AS n FROM typemx WHERE id < 0 AND c_cidr = 'zzz'`,
		"invalid input syntax for type cidr")
	f3RefuseOnEveryArm(t, arms, "uuid_garbage_literal_over_an_empty_scan",
		`SELECT COUNT(*) AS n FROM typemx WHERE id < 0 AND c_uuid = 'nope'`,
		"invalid input syntax for type uuid")
	f3RefuseOnEveryArm(t, arms, "mac_garbage_literal_over_an_empty_scan",
		`SELECT COUNT(*) AS n FROM typemx WHERE id < 0 AND c_mac = 'nope'`,
		"invalid input syntax for type macaddr")

	// The literal that is NOT an address, even though PostgreSQL's abbreviated
	// cidr grammar would read it as one. A site that does not know the
	// column's type must read `'3.1'` and `'2'` as numbers; reading them as
	// 3.1.0.0/16 and 2.0.0.0/8 ordered a DOUBLE column's rows as addresses.
	// The bare predicate goes through the KERNEL and the CASE-wrapped one
	// through the compiled comparison, which is why both spellings are here.
	f3AgreeOnEveryArm(t, arms, "quoted_number_is_not_an_address_float",
		`SELECT COUNT(*) AS n FROM typemx WHERE CASE WHEN c_f64 < '3.1' THEN 1 ELSE 0 END = 1`,
		`SELECT COUNT(*) AS n FROM typemx WHERE c_f64 < '3.1'`)
	f3AgreeOnEveryArm(t, arms, "quoted_number_is_not_an_address_int",
		`SELECT COUNT(*) AS n FROM typemx WHERE CASE WHEN c_i64 > '2' THEN 1 ELSE 0 END = 1`,
		`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 > '2'`)

	// The NETWORK-LITERAL CENSUS (#627 round 2, B1). A literal's
	// classification happens ONCE, at typing time, and every arm and every
	// SITE gets the same answer. Before this the 0A000 lived in one evaluator
	// — reachable only when the vectorized filter declined to build a kernel —
	// so `c_ipv4 = '10/8'` refused in a WHERE clause on the single arm,
	// ANSWERED 0 inside a CASE on that same arm, and on the DAG answered
	// 4916 for `>` because the widened parser read the prefix as the address
	// ZERO. Three dispositions for one query.
	//
	// Two classes, and they are different answers:
	//   a network PREFIX on a bare-address column   0A000  (valid text, the
	//                                                       TYPE is the limit)
	//   text that names no address at all           22P02  (PostgreSQL's own)
	for _, tc := range []struct{ col, lit, state string }{
		{"c_ipv4", "10/8", "0A000"},
		{"c_ipv4", "10.0.1/24", "0A000"},
		{"c_ipv4", "192.168/16", "0A000"},
		{"c_ipv6", "::1/64", "0A000"},
		{"c_ipv6", "2001:db8::/32", "0A000"},
		{"c_ipv4", "zzz", "22P02"},
		{"c_ipv6", "zzz", "22P02"},
		{"c_cidr", "zzz", "22P02"},
	} {
		for _, site := range f3NetSites(tc.col, tc.lit) {
			f3RefuseOnEveryArmWithState(t, arms,
				"network_literal/"+tc.col+"/"+tc.lit+"/"+site.name, site.sql, tc.state)
		}
	}
	// The other side of that boundary: a literal the column CAN hold answers,
	// at every site and on every arm. A refusal that swallowed these would be
	// the ADR-0012 item 1 violation the census exists to prevent.
	for _, tc := range []struct{ col, lit string }{
		{"c_ipv4", "10.0.0.1"},
		{"c_ipv4", "10.0.0.1/32"},
		{"c_ipv6", "2001:db8::1"},
		{"c_ipv6", "2001:db8::1/128"},
		{"c_cidr", "10/8"},
		{"c_cidr", "192.168"},
		{"c_cidr", "239"},
	} {
		for _, site := range f3NetSites(tc.col, tc.lit) {
			// PRE-EXISTING, pinned fail-on-agree: `c_ipv4 IN (<any host
			// literal>)` answers on the single arm and ZERO on both DAG arms.
			// Measured identically at dea4e795 with none of this arc's
			// commits — `IN ('10.0.0.1')` is single 1 / dag 0 there — so it is
			// not #627's, and the `=` spelling of the same predicate agrees on
			// every arm. Filed with the arc's report; the cell stays here
			// because this census is where the split is visible, and it FAILS
			// when the two arms start agreeing.
			if tc.col == "c_ipv4" && site.name == "in" {
				f3SplitOnTheDAG(t, arms,
					"network_literal_ok/"+tc.col+"/"+tc.lit+"/"+site.name, site.sql)
				continue
			}
			f3AnswersOnEveryArm(t, arms,
				"network_literal_ok/"+tc.col+"/"+tc.lit+"/"+site.name, site.sql)
		}
	}

	// ---------------------------------------------------------------- #582
	// A bytea literal in both of byteain's spellings, against the same row.
	// The hex form is computed from the fixture's own value rather than
	// transcribed, so the two queries cannot drift apart.
	val := "bytes-000001-x"
	f3AgreeOnEveryArm(t, arms, "bytes_hex_spelled_literal",
		fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE c_bytes = '\x%s'`,
			hex.EncodeToString([]byte(val))),
		fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE c_bytes = '%s'`, val))
	f3RunOnEveryArm(t, arms, "bytes_hex_spelled_literal_finds_the_row",
		fmt.Sprintf(`SELECT id AS v FROM typemx WHERE c_bytes = '\x%s'`,
			hex.EncodeToString([]byte(val))), []string{"v=int64:1"})

	// ---------------------------------------------------------------- #583
	// `bytea || bytea` and `substring(bytea, …)` are bytea, and the byte
	// indexing is what makes the second cell's answer a value the column
	// holds rather than the UTF-8 replacement character.
	f3RunOnEveryArm(t, arms, "bytes_substring_is_bytes",
		`SELECT SUBSTRING(c_bytes, 1, 5) AS v FROM typemx WHERE id = 1`,
		[]string{"v=[]uint8:[98 121 116 101 115]"})
	f3RunOnEveryArm(t, arms, "bytes_length_is_the_byte_count",
		`SELECT LENGTH(c_bytes) AS v FROM typemx WHERE id = 1`, []string{"v=int32:14"})

	// ---------------------------------------------------------------- #580
	// A v4-mapped IPv6 address prints as PostgreSQL's inet prints it. The
	// fixture holds no such value, so the claim is made through the two
	// functions that render one.
	f3RunOnEveryArm(t, arms, "ipv6_v4_mapped_rendering",
		`SELECT IPV6_COMPRESS('::ffff:10.0.0.1') AS v FROM typemx WHERE id = 1`,
		[]string{"v=::ffff:10.0.0.1"})
	f3RunOnEveryArm(t, arms, "ipv6_column_rendering_unchanged",
		`SELECT c_ipv6 AS v FROM typemx WHERE id = 1`, []string{"v=2001:db8::1"})

	// ---------------------------------------------------------------- #635
	// element_at over a container expression that is not a bare column.
	f3AgreeOnEveryArm(t, arms, "element_at_map_under_coalesce",
		`SELECT COUNT(*) AS n FROM typemx_nested WHERE ELEMENT_AT(COALESCE(c_map, c_map), 'k1') IS NOT NULL`,
		`SELECT COUNT(*) AS n FROM typemx_nested WHERE ELEMENT_AT(c_map, 'k1') IS NOT NULL`)

	// ---------------------------------------------------------------- #868
	// A timestamp-valued function declares TIMESTAMP, and its VALUE is the
	// instant — the same box the column itself produces, which is what the
	// gather stage has to carry through its own output schema.
	f3AgreeOnEveryArm(t, arms, "date_trunc_declares_the_instant",
		`SELECT DATE_TRUNC('day', c_ts) AS v FROM typemx WHERE id = 1`,
		`SELECT FROM_UNIXTIME(1699920000) AS v FROM typemx WHERE id = 1`)
	f3RunOnEveryArm(t, arms, "date_trunc_value",
		`SELECT DATE_TRUNC('day', c_ts) AS v FROM typemx WHERE id = 1`,
		[]string{"v=int64:1699920000000"})
	// Rendered back to text at a SITE that renders it, which must still be
	// PostgreSQL's own spelling.
	f3RunOnEveryArm(t, arms, "date_trunc_rendered",
		`SELECT CAST(DATE_TRUNC('day', c_ts) AS STRING) AS v FROM typemx WHERE id = 1`,
		[]string{"v=2023-11-14 00:00:00"})
}

// f3NetSites is the site list a network literal has to be classified the same
// way at: the two the pre-round-2 refusal reached, the four it did not, and the
// EMPTY-SCAN shape, which is the one that separates a plan-time decision from a
// per-row one.
func f3NetSites(col, lit string) []struct{ name, sql string } {
	q := "'" + lit + "'"
	return []struct{ name, sql string }{
		{"eq", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s = %s`, col, q)},
		{"ne", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s <> %s`, col, q)},
		{"gt", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s > %s`, col, q)},
		{"lt", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s < %s`, col, q)},
		{"in", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s IN (%s)`, col, q)},
		{"case", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE CASE WHEN %s = %s THEN 1 ELSE 0 END = 1`, col, q)},
		{"greatest", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE GREATEST(%s, %s) = %s`, col, q, col)},
		{"projection", fmt.Sprintf(`SELECT COUNT(*) AS n FROM (SELECT %s = %s AS b FROM typemx) x WHERE b`, col, q)},
		{"empty_scan", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE id < 0 AND %s = %s`, col, q)},
	}
}

// f3RefuseOnEveryArmWithState asserts a refusal AND its SQLSTATE on every arm:
// two arms refusing with two different classes is the same defect as one
// refusing and one answering.
func f3RefuseOnEveryArmWithState(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sql, state string) {
	t.Helper()
	for _, arm := range arms {
		t.Run(name+"/"+arm.name, func(t *testing.T) {
			rows, err := arm.run(sql)
			if err == nil {
				t.Fatalf("answered %v; want %s\n  SQL: %s", rows, state, sql)
			}
			if got := sqlerr.StateOf(err); got != state {
				t.Errorf("SQLSTATE %q, want %q\n  err: %v\n  SQL: %s", got, state, err, sql)
			}
		})
	}
}

// f3AnswersOnEveryArm asserts a query is NOT refused, and that every arm
// agrees on the number.
func f3AnswersOnEveryArm(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sql string) {
	t.Helper()
	var first string
	for i, arm := range arms {
		t.Run(name+"/"+arm.name, func(t *testing.T) {
			got, err := arm.run(sql)
			if err != nil {
				t.Fatalf("REFUSED a literal this column can hold: %v\n  SQL: %s", err, sql)
			}
			j := strings.Join(got, ";")
			if i == 0 {
				first = j
			} else if j != first {
				t.Errorf("%s answers %s where the single arm answers %s\n  SQL: %s",
					arm.name, j, first, sql)
			}
		})
	}
}

// f3SplitOnTheDAG pins a PRE-EXISTING two-path split: the single arm answers
// and both DAG arms answer ZERO. It fails when they agree, which is what makes
// deleting it the proof that whoever owns the defect closed it.
func f3SplitOnTheDAG(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sql string) {
	t.Helper()
	t.Run(name+"/pinned_dag_split", func(t *testing.T) {
		var single string
		for _, arm := range arms {
			got, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s REFUSED: %v\n  SQL: %s", arm.name, err, sql)
			}
			j := strings.Join(got, ";")
			if arm.name == "single" {
				single = j
				if j == "n=int64:0" {
					t.Errorf("the single arm answers 0 too; this pin records that it "+
						"answers and the DAG does not\n  SQL: %s", sql)
				}
				continue
			}
			if j != "n=int64:0" {
				t.Errorf("%s answers %s where this pin records 0 — the split is CLOSED, "+
					"so delete the pin and let the census assert agreement (single=%s)"+
					"\n  SQL: %s", arm.name, j, single, sql)
			}
		}
	})
}

// f3RunOnEveryArm asserts one query's rows on all three arms.
func f3RunOnEveryArm(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sql string, want []string) {
	t.Helper()
	for _, arm := range arms {
		t.Run(name+"/"+arm.name, func(t *testing.T) {
			got, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, sql)
			}
			if strings.Join(got, ";") != strings.Join(want, ";") {
				t.Errorf("= %v, want %v (live PostgreSQL 17.11)\n  SQL: %s", got, want, sql)
			}
		})
	}
}

// f3AgreeOnEveryArm asserts that two SPELLINGS of one query answer the same
// thing, on every arm — and that the answer is not the empty one, which every
// pair of broken spellings would also agree on.
func f3AgreeOnEveryArm(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sqlA, sqlB string) {
	t.Helper()
	for _, arm := range arms {
		t.Run(name+"/"+arm.name, func(t *testing.T) {
			a, err := arm.run(sqlA)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, sqlA)
			}
			b, err := arm.run(sqlB)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, sqlB)
			}
			ja, jb := strings.Join(a, ";"), strings.Join(b, ";")
			if ja != jb {
				t.Errorf("two spellings of one query answer differently:\n  %s => %s\n  %s => %s",
					sqlA, ja, sqlB, jb)
			}
			for _, zero := range []string{"n=int64:0", "v=NULL", ""} {
				if ja == zero {
					t.Errorf("both spellings answer %q, which two broken ones would also "+
						"agree on — the fixture cannot separate the rules\n  SQL: %s", zero, sqlA)
				}
			}
		})
	}
}

// f3RefuseOnEveryArm asserts a query is REFUSED with the same message on every
// arm. The refusal travels out of a worker's evaluator, or is raised at plan
// time before any worker runs; both must reach the client as the error rather
// than as a stalled query or an empty answer.
func f3RefuseOnEveryArm(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sql, want string) {
	t.Helper()
	for _, arm := range arms {
		t.Run(name+"/"+arm.name, func(t *testing.T) {
			rows, err := arm.run(sql)
			if err == nil {
				t.Fatalf("answered %v; PostgreSQL 17.11 refuses with %q\n  SQL: %s", rows, want, sql)
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%v, want a refusal containing %q\n  SQL: %s", err, want, sql)
			}
		})
	}
}
