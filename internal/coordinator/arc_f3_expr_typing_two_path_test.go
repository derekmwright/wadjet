package coordinator

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
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
//
// The ACCEPTED literals carry PostgreSQL's own COUNT for every site
// (f3PGCounts), measured over this fixture rebuilt on the server with the type
// mapping the differential oracle uses — comparing the arms only to each other
// is what let a value all of them got wrong pass this census (round-3 review
// P-A).
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

	// The FOURTH arm is the spilled one, and it is a condition rather than a
	// shape (ADR-0027): a 512 KiB budget alone moves no engagement counter on
	// a COUNT(*) over 5000 rows, so the DRAIN is forced on this arm and left
	// disarmed on every other — arming both sides cancels a defect that lives
	// in the drain (#790). It is the arm this census owed since round 2's P-7.
	spilled := na2Standalone(t, ctx, 512*1024)
	var f3Drains int
	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
		{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		{"spilled", func(sql string) ([]string, error) {
			before := exec.ForcedAggDrains.Load()
			restore := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(512)
			out, err := na2Run(tmdRunSingle(ctx, spilled, sql))
			restoreRuns()
			exec.ForceAggDrainEvery(restore)
			if exec.ForcedAggDrains.Load() > before {
				f3Drains++
			}
			return out, err
		}},
	}
	t.Cleanup(func() {
		// Every census cell is a `COUNT(*)` — one group, no hash table — so
		// the drain knob has nothing to drain on the cells themselves, and
		// claiming per-cell engagement for them would be false. What IS
		// asserted is that this arm's DB and knob really do spill: one probe
		// with a real hash aggregate, run through the same door with the same
		// knob armed. Without it the arm would be a second copy of `single`
		// and would prove nothing (ADR-0027 §6).
		before := exec.ForcedAggDrains.Load()
		restore := exec.ForceAggDrainEvery(1)
		restoreRuns := exec.ForceSmallSpillRuns(512)
		_, err := na2Run(tmdRunSingle(ctx, spilled,
			`SELECT c_str, COUNT(*) AS n FROM typemx GROUP BY c_str`))
		restoreRuns()
		exec.ForceAggDrainEvery(restore)
		if err != nil {
			t.Errorf("the spilled arm's engagement probe failed: %v", err)
		}
		if exec.ForcedAggDrains.Load() == before {
			t.Errorf("the spilled arm forced no drain even on a hash aggregate: " +
				"the arm is a second copy of `single` and proves nothing")
		}
		t.Logf("spilled arm: engagement probe drained; %d census cells drained on their own",
			f3Drains)
	})

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
		// The DERIVED-TABLE spellings. The DAG answered these exactly all
		// along and the single arm did not, so before the fix this was a
		// two-path split as well as a wrong number; the cells are here
		// because that is the shape only these arms can show.
		{"agg_under_a_derived_rename",
			`SELECT SUM(v * 3000000) + 1 AS v FROM (SELECT c_i64 AS v FROM typemx) x`,
			"v=36280278840510000001"},
		{"agg_under_a_cte",
			`WITH c AS (SELECT c_i64 AS v FROM typemx) SELECT SUM(v * 3000000) + 1 AS v FROM c`,
			"v=36280278840510000001"},
	} {
		f3RunOnEveryArm(t, arms, tc.name, tc.sql, []string{tc.want})
	}

	// The SET-OPERATION face of the same walk, on every arm. The two shapes
	// whose arms agree ANSWER — that is #867's fix crossing a UNION — and the
	// shape whose arms DISAGREE is pinned fail-on-agree beside them: the walk
	// types a column only when every arm declares it identically, and a name
	// left out of that map takes nodeDeclaredType's float fall-through, so
	// `… UNION ALL SELECT NULL` goes out as float8/OID 701 where PostgreSQL
	// sends numeric/OID 1700 with the outer `+ 1` intact. Pre-existing and
	// filed; `wadjet.TestArithmeticOverAComputedAggregateCarriesTheAggregatesType`
	// carries the declaration half.
	//
	// The census had no set-operation cell at all before this, so neither
	// side of that boundary was gated on the distributed arms (round-4 review
	// P-B-1).
	f3RunOnEveryArm(t, arms, "setop_agg_same_typed_arms",
		`SELECT SUM(v * 3000000) + 1 AS v FROM (SELECT c_i64 AS v FROM typemx `+
			`UNION ALL SELECT c_i64 FROM typemx) x`,
		[]string{"v=72560557681020000001"})
	f3RunOnEveryArm(t, arms, "setop_agg_decimal_arms",
		`SELECT SUM(v * 2) + 1 AS v FROM (SELECT c_dec AS v FROM typemx `+
			`UNION ALL SELECT c_dec FROM typemx) x`,
		[]string{"v=49500246.5296"})
	f3PinnedFloatOnEveryArm(t, arms, "residual_setop_agg_null_arm",
		`SELECT SUM(v * 3000000) + 1 AS v FROM (SELECT c_i64 AS v FROM typemx `+
			`UNION ALL SELECT NULL) x`, "36280278840510000001")

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
	// PostgreSQL's abbreviated INET spelling names the same value as the
	// canonical one, so the two queries are one query. The mask is not
	// optional: `'192.168'` beside a cidr column is 22P02 on the server (the
	// pair resolves through `=(inet, inet)` and inet has no classful
	// inference), and it is in the refusal census below for that reason.
	f3AgreeOnEveryArm(t, arms, "cidr_abbreviated_spelling",
		`SELECT COUNT(*) AS n FROM typemx WHERE c_cidr = '192.168.0/24'`,
		`SELECT COUNT(*) AS n FROM typemx WHERE c_cidr = '192.168.0.0/24'`)
	f3AgreeOnEveryArm(t, arms, "cidr_abbreviated_with_mask",
		`SELECT COUNT(*) AS n FROM typemx WHERE c_cidr = '192.168.4/24'`,
		`SELECT COUNT(*) AS n FROM typemx WHERE c_cidr = '192.168.4.0/24'`)
	// The refusal is decided at PLAN time now, so it fires over a scan the
	// filter never reaches a row of. `id < 0` is what makes that the claim:
	// a data-dependent refusal answers zero rows here.
	// `inet`, not `cidr`: the server reads a literal beside a network column
	// with inet's parser and its own message names that type, and a wadjet
	// CIDR column IS an inet — it holds host bits under a mask, which
	// PostgreSQL's cidr refuses (round-3 review P-E).
	f3RefuseOnEveryArm(t, arms, "cidr_garbage_literal_over_an_empty_scan",
		`SELECT COUNT(*) AS n FROM typemx WHERE id < 0 AND c_cidr = 'zzz'`,
		"invalid input syntax for type inet")
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
		// The v6 MASK grammar, which round 3 did not have: the prefix
		// predicate answered `mask != "128"` without parsing the mask or
		// checking the family. `'10.0.0.1/128'` then had its mask stripped
		// and ANSWERED 0 rows where the base and the server both raise
		// (128 does not fit a v4 address), and `/129`, `/abc`, `/0128` were
		// refused with 0A000 — a class that asserts the text IS
		// PostgreSQL-valid and only the TYPE is the limit, which none of them
		// is. Measured: PostgreSQL's inet6 mask takes decimal digits with NO
		// leading zeros, 0-128 (round-3 review B3-3).
		{"c_ipv6", "10.0.0.1/128", "22P02"},
		{"c_ipv6", "2001:db8::1/129", "22P02"},
		{"c_ipv6", "2001:db8::1/abc", "22P02"},
		{"c_ipv6", "2001:db8::1/0128", "22P02"},
		{"c_ipv6", "2001:db8::1/064", "22P02"},
		{"c_ipv6", "2001:db8::1/", "22P02"},
		// The literals the CIDR TYPE accepts and no COMPARISON does. `cidr`
		// carries no operators of its own, so `c_cidr = '<literal>'` resolves
		// through `=(inet, inet)` and the literal is read by inet's parser,
		// which has no classful inference and no hex input:
		//
		//	SELECT '239'::cidr                      -> 239.0.0.0/8
		//	SELECT ... WHERE c_cidr = '239'
		//	   ERROR: invalid input syntax for type inet: "239"
		//
		// Round 2 of this arc read the cidr grammar at these sites and
		// answered a row here — an undeclared accept-where-PostgreSQL-rejects
		// superset (round-2 review P-6), and the same reading is what made
		// `'1/0'` look like a regression when it is the server's own answer.
		{"c_cidr", "192.168", "22P02"},
		{"c_cidr", "239", "22P02"},
		{"c_cidr", "10", "22P02"},
		{"c_cidr", "224", "22P02"},
		{"c_cidr", "0x10", "22P02"},
		// A mask may not name a byte the literal never wrote: '10/15' is a
		// value and '10/16' is 22P02, measured over all four octet counts.
		{"c_cidr", "10/16", "22P02"},
		{"c_cidr", "192.168/24", "22P02"},
		// The FOLD sites refuse these too, and that is AGREEMENT rather than
		// the divergence round 3 recorded. `GREATEST`/`LEAST`/`COALESCE`
		// unify their arguments' types, so beside a PostgreSQL `cidr` column
		// they read the CIDR parser — but a wadjet CIDR column is not one and
		// cannot be: it holds host bits under a mask, and
		// `'192.168.5.7/24'::cidr` is 22P02 while `::inet` is a value. This
		// repository's own oracle maps IPV4/IPV6/CIDR to `inet` for exactly
		// that reason, and over an inet column every fold site refuses '239',
		// '192.168' and 'zzz' as this engine does (measured; ADR-0012).
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
		{"c_cidr", "192.168.0/24"},
		// The two WHOLE-QUAD spellings Go's parser refuses and PostgreSQL's
		// reads: a leading zero and a trailing dot. Round 3 widened the
		// PARSER for them and left the type-blind gate in front of it on
		// net.ParseCIDR, so the same literal answered on the single arm at
		// `=`, refused inside a CASE on that same arm, and refused on both
		// DAG arms — three dispositions for one literal, which is the class
		// the census was built to forbid (round-3 review B3-1).
		{"c_cidr", "010.1.2.3"},
		{"c_cidr", "10.1.2.3."},
		// The four literals the round-2 review read as a regression. Every
		// one is a value on the server — inet KEEPS the bits to the right of
		// the mask, which is the check the CIDR type makes and inet does not
		// — and every one answers zero rows both there and here:
		//
		//	'1/0'::inet 1.0.0.0/0   '255/1'::inet 255.0.0.0/1
		//	'10.1/8'    10.1.0.0/8  '172.31/12'   172.31.0.0/12
		//
		// The base refused all four with 22P02, which is the disagreement
		// this arc closed; the census carries them so a return to the CIDR
		// grammar's `addressOK` check fails here.
		{"c_cidr", "1/0"},
		{"c_cidr", "255/1"},
		{"c_cidr", "10.1/8"},
		{"c_cidr", "172.31/12"},
	} {
		for _, site := range f3NetSites(tc.col, tc.lit) {
			f3AnswersPGCountOnEveryArm(t, arms,
				"network_literal_ok/"+tc.col+"/"+tc.lit+"/"+site.name, site.sql,
				f3PGCount(t, tc.col, tc.lit, site.name))
		}
	}

	// The IN-list family, which was a PRE-EXISTING two-path split until this
	// round and is anchored to PostgreSQL now rather than pinned. Measured on
	// the reconstructed fixture (f3PGCounts' transcript):
	//
	//	                                        PG   base   round 3   now
	//	c_ipv4 IN ('10.0.0.1')                   1    dag 0   dag 0     1
	//	c_mac  IN ('aa:bb:cc:00:00:01')          1    dag 0   dag 0     1
	//	c_ipv4 BETWEEN '10.0.0.1' AND same       1    dag 0   dag 0     1
	//	c_ipv4 NOT IN ('10.0.0.1')            4915    dag 4916 dag 4916 4915
	//	CASE WHEN c_ipv4 IN (...) THEN 1 ...     1    all 0    all 0    1
	//
	// One mechanism closed all five: an IPV4 column boxes as the RAW int64 it
	// stores on the paths these take, so comparing it against a quoted literal
	// fell to compare(), which reads a dotted quad as the number ZERO. The
	// boxed-pair layer has a boxIPv4 kind now, the way CIDR and IPv6 have had
	// one since #565, and the pins that recorded the split are deleted — which
	// is the proof.
	for _, tc := range []struct {
		name, sql string
		want      int64
	}{
		{"in_family/mac_in", `SELECT COUNT(*) AS n FROM typemx WHERE c_mac IN ('aa:bb:cc:00:00:01')`, 1},
		{"in_family/between", `SELECT COUNT(*) AS n FROM typemx WHERE c_ipv4 BETWEEN '10.0.0.1' AND '10.0.0.1'`, 1},
		{"in_family/not_in", `SELECT COUNT(*) AS n FROM typemx WHERE c_ipv4 NOT IN ('10.0.0.1')`, 4915},
		{"in_family/inside_a_case",
			`SELECT COUNT(*) AS n FROM typemx WHERE CASE WHEN c_ipv4 IN ('10.0.0.1') THEN 1 ELSE 0 END = 1`, 1},
	} {
		f3AnswersPGCountOnEveryArm(t, arms, tc.name, tc.sql, tc.want)
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
// way at: the two the pre-round-2 refusal reached, the five it did not, and the
// EMPTY-SCAN shape, which is the one that separates a plan-time decision from a
// per-row one.
//
// COALESCE joined the list in round 3. It was the last site outside the claim
// — `COALESCE(<network col>, 'zzz')` ANSWERED on every arm for all five types
// where PostgreSQL raises 22P02, because COALESCE folds its arguments to one
// type at parse analysis and the plan-time validator did not treat it as a
// fold.
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
		{"least", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE LEAST(%s, %s) = %s`, col, q, col)},
		{"coalesce", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE COALESCE(%s, %s) = %s`, col, q, col)},
		{"projection", fmt.Sprintf(`SELECT COUNT(*) AS n FROM (SELECT %s = %s AS b FROM typemx) x WHERE b`, col, q)},
		// The two ORDERINGS of the same conjunction. `id < 0` reaches no row,
		// so a refusal that fires there is a PLAN-time one; the other
		// ordering reaches every row, so a refusal that fires only there is
		// per-ROW. A literal whose disposition depends on which one you write
		// is the data-dependent refusal this census exists to forbid, and
		// round 3 shipped exactly that for '010.1.2.3' (review B3-2).
		{"empty_scan", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE id < 0 AND %s = %s`, col, q)},
		{"row_reaching", fmt.Sprintf(`SELECT COUNT(*) AS n FROM typemx WHERE %s = %s AND id >= 0`, col, q)},
	}
}

// f3PGCounts is `SELECT COUNT(*)` on PostgreSQL 17.11 for every accepted
// literal at every site — the number this engine has to produce, not merely
// the number its other arms produce.
//
// It exists because `f3AnswersOnEveryArm` compares the arms TO EACH OTHER: a
// value all four get wrong passes it silently, and one did. `GREATEST(c_ipv4,
// '10.0.0.1') = c_ipv4` counted 4916 here and 4915 there, and `LEAST` counted
// 0 where the server counts 2 — an IPV4 column boxes as its RENDERED dotted
// quad, whose byte order is not the address's (round-3 review P-A).
//
// The fixture is `internal/oracle/typematrix`'s 5000 rows rebuilt in
// PostgreSQL with the SAME type mapping the differential oracle uses for these
// columns: IPV4, IPV6 and CIDR all map to `inet`, because inet is the only
// PostgreSQL type that holds what this fixture carries — `'192.168.5.7/24'`
// (host bits under a mask) is 22P02 for `cidr` and a value for `inet`
// (`benchmarks/tpch/postgres_oracle_test.go`'s postgresType, re-measured
// here). Transcript: scratchpad/arcs8/f3_expr_typing3/r4/mkfixture.sql and
// anchor.sql.
var f3PGCounts = map[string]int64{
	"c_cidr|010.1.2.3|case":               0,
	"c_cidr|010.1.2.3|coalesce":           4926,
	"c_cidr|010.1.2.3|empty_scan":         0,
	"c_cidr|010.1.2.3|eq":                 0,
	"c_cidr|010.1.2.3|greatest":           3696,
	"c_cidr|010.1.2.3|gt":                 3696,
	"c_cidr|010.1.2.3|in":                 0,
	"c_cidr|010.1.2.3|least":              1230,
	"c_cidr|010.1.2.3|lt":                 1230,
	"c_cidr|010.1.2.3|ne":                 4926,
	"c_cidr|010.1.2.3|projection":         0,
	"c_cidr|010.1.2.3|row_reaching":       0,
	"c_cidr|1/0|case":                     0,
	"c_cidr|1/0|coalesce":                 4926,
	"c_cidr|1/0|empty_scan":               0,
	"c_cidr|1/0|eq":                       0,
	"c_cidr|1/0|greatest":                 4926,
	"c_cidr|1/0|gt":                       4926,
	"c_cidr|1/0|in":                       0,
	"c_cidr|1/0|least":                    0,
	"c_cidr|1/0|lt":                       0,
	"c_cidr|1/0|ne":                       4926,
	"c_cidr|1/0|projection":               0,
	"c_cidr|1/0|row_reaching":             0,
	"c_cidr|10.1.2.3.|case":               0,
	"c_cidr|10.1.2.3.|coalesce":           4926,
	"c_cidr|10.1.2.3.|empty_scan":         0,
	"c_cidr|10.1.2.3.|eq":                 0,
	"c_cidr|10.1.2.3.|greatest":           3696,
	"c_cidr|10.1.2.3.|gt":                 3696,
	"c_cidr|10.1.2.3.|in":                 0,
	"c_cidr|10.1.2.3.|least":              1230,
	"c_cidr|10.1.2.3.|lt":                 1230,
	"c_cidr|10.1.2.3.|ne":                 4926,
	"c_cidr|10.1.2.3.|projection":         0,
	"c_cidr|10.1.2.3.|row_reaching":       0,
	"c_cidr|10.1/8|case":                  0,
	"c_cidr|10.1/8|coalesce":              4926,
	"c_cidr|10.1/8|empty_scan":            0,
	"c_cidr|10.1/8|eq":                    0,
	"c_cidr|10.1/8|greatest":              4863,
	"c_cidr|10.1/8|gt":                    4863,
	"c_cidr|10.1/8|in":                    0,
	"c_cidr|10.1/8|least":                 63,
	"c_cidr|10.1/8|lt":                    63,
	"c_cidr|10.1/8|ne":                    4926,
	"c_cidr|10.1/8|projection":            0,
	"c_cidr|10.1/8|row_reaching":          0,
	"c_cidr|10/8|case":                    0,
	"c_cidr|10/8|coalesce":                4926,
	"c_cidr|10/8|empty_scan":              0,
	"c_cidr|10/8|eq":                      0,
	"c_cidr|10/8|greatest":                4926,
	"c_cidr|10/8|gt":                      4926,
	"c_cidr|10/8|in":                      0,
	"c_cidr|10/8|least":                   0,
	"c_cidr|10/8|lt":                      0,
	"c_cidr|10/8|ne":                      4926,
	"c_cidr|10/8|projection":              0,
	"c_cidr|10/8|row_reaching":            0,
	"c_cidr|172.31/12|case":               0,
	"c_cidr|172.31/12|coalesce":           4926,
	"c_cidr|172.31/12|empty_scan":         0,
	"c_cidr|172.31/12|eq":                 0,
	"c_cidr|172.31/12|greatest":           3696,
	"c_cidr|172.31/12|gt":                 3696,
	"c_cidr|172.31/12|in":                 0,
	"c_cidr|172.31/12|least":              1230,
	"c_cidr|172.31/12|lt":                 1230,
	"c_cidr|172.31/12|ne":                 4926,
	"c_cidr|172.31/12|projection":         0,
	"c_cidr|172.31/12|row_reaching":       0,
	"c_cidr|192.168.0/24|case":            20,
	"c_cidr|192.168.0/24|coalesce":        4926,
	"c_cidr|192.168.0/24|empty_scan":      0,
	"c_cidr|192.168.0/24|eq":              20,
	"c_cidr|192.168.0/24|greatest":        2463,
	"c_cidr|192.168.0/24|gt":              2443,
	"c_cidr|192.168.0/24|in":              20,
	"c_cidr|192.168.0/24|least":           2483,
	"c_cidr|192.168.0/24|lt":              2463,
	"c_cidr|192.168.0/24|ne":              4906,
	"c_cidr|192.168.0/24|projection":      20,
	"c_cidr|192.168.0/24|row_reaching":    20,
	"c_cidr|255/1|case":                   0,
	"c_cidr|255/1|coalesce":               4926,
	"c_cidr|255/1|empty_scan":             0,
	"c_cidr|255/1|eq":                     0,
	"c_cidr|255/1|greatest":               3696,
	"c_cidr|255/1|gt":                     3696,
	"c_cidr|255/1|in":                     0,
	"c_cidr|255/1|least":                  1230,
	"c_cidr|255/1|lt":                     1230,
	"c_cidr|255/1|ne":                     4926,
	"c_cidr|255/1|projection":             0,
	"c_cidr|255/1|row_reaching":           0,
	"c_ipv4|10.0.0.1|case":                1,
	"c_ipv4|10.0.0.1|coalesce":            4916,
	"c_ipv4|10.0.0.1|empty_scan":          0,
	"c_ipv4|10.0.0.1|eq":                  1,
	"c_ipv4|10.0.0.1|greatest":            4915,
	"c_ipv4|10.0.0.1|gt":                  4914,
	"c_ipv4|10.0.0.1|in":                  1,
	"c_ipv4|10.0.0.1|least":               2,
	"c_ipv4|10.0.0.1|lt":                  1,
	"c_ipv4|10.0.0.1|ne":                  4915,
	"c_ipv4|10.0.0.1|projection":          1,
	"c_ipv4|10.0.0.1|row_reaching":        1,
	"c_ipv4|10.0.0.1/32|case":             1,
	"c_ipv4|10.0.0.1/32|coalesce":         4916,
	"c_ipv4|10.0.0.1/32|empty_scan":       0,
	"c_ipv4|10.0.0.1/32|eq":               1,
	"c_ipv4|10.0.0.1/32|greatest":         4915,
	"c_ipv4|10.0.0.1/32|gt":               4914,
	"c_ipv4|10.0.0.1/32|in":               1,
	"c_ipv4|10.0.0.1/32|least":            2,
	"c_ipv4|10.0.0.1/32|lt":               1,
	"c_ipv4|10.0.0.1/32|ne":               4915,
	"c_ipv4|10.0.0.1/32|projection":       1,
	"c_ipv4|10.0.0.1/32|row_reaching":     1,
	"c_ipv6|2001:db8::1|case":             1,
	"c_ipv6|2001:db8::1|coalesce":         4919,
	"c_ipv6|2001:db8::1|empty_scan":       0,
	"c_ipv6|2001:db8::1|eq":               1,
	"c_ipv6|2001:db8::1|greatest":         4918,
	"c_ipv6|2001:db8::1|gt":               4917,
	"c_ipv6|2001:db8::1|in":               1,
	"c_ipv6|2001:db8::1|least":            2,
	"c_ipv6|2001:db8::1|lt":               1,
	"c_ipv6|2001:db8::1|ne":               4918,
	"c_ipv6|2001:db8::1|projection":       1,
	"c_ipv6|2001:db8::1|row_reaching":     1,
	"c_ipv6|2001:db8::1/128|case":         1,
	"c_ipv6|2001:db8::1/128|coalesce":     4919,
	"c_ipv6|2001:db8::1/128|empty_scan":   0,
	"c_ipv6|2001:db8::1/128|eq":           1,
	"c_ipv6|2001:db8::1/128|greatest":     4918,
	"c_ipv6|2001:db8::1/128|gt":           4917,
	"c_ipv6|2001:db8::1/128|in":           1,
	"c_ipv6|2001:db8::1/128|least":        2,
	"c_ipv6|2001:db8::1/128|lt":           1,
	"c_ipv6|2001:db8::1/128|ne":           4918,
	"c_ipv6|2001:db8::1/128|projection":   1,
	"c_ipv6|2001:db8::1/128|row_reaching": 1,
}

// f3PGCount returns the measured PostgreSQL count for one census cell.
func f3PGCount(t *testing.T, col, lit, site string) int64 {
	t.Helper()
	n, ok := f3PGCounts[col+"|"+lit+"|"+site]
	if !ok {
		t.Fatalf("no PostgreSQL count measured for %s / %q / %s — a census cell "+
			"without an anchor compares the arms only to each other", col, lit, site)
	}
	return n
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
// f3AnswersPGCountOnEveryArm asserts the number PostgreSQL 17.11 answers, on
// every arm — not merely that the arms agree with each other. Four arms
// agreeing on a wrong number is what let `GREATEST(c_ipv4, '10.0.0.1')` count
// 4916 rows through this census (round-3 review P-A).
func f3AnswersPGCountOnEveryArm(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sql string, want int64) {
	t.Helper()
	wantRow := "n=int64:" + strconv.FormatInt(want, 10)
	for _, arm := range arms {
		t.Run(name+"/"+arm.name, func(t *testing.T) {
			got, err := arm.run(sql)
			if err != nil {
				t.Fatalf("REFUSED a literal this column can hold: %v\n  SQL: %s", err, sql)
			}
			if j := strings.Join(got, ";"); j != wantRow {
				t.Errorf("%s answers %s, PostgreSQL 17.11 answers %s\n  SQL: %s",
					arm.name, j, wantRow, sql)
			}
		})
	}
}

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

// f3ArmsDisagree pins a PRE-EXISTING two-path split whose DAG answer is not
// zero: the single arm and both DAG arms answer, and they answer DIFFERENTLY.
// It fails when they agree, which is what makes deleting it the fix's proof.
func f3ArmsDisagree(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sql string) {
	t.Helper()
	t.Run(name+"/pinned_arm_disagreement", func(t *testing.T) {
		var single string
		for _, arm := range arms {
			got, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s REFUSED: %v\n  SQL: %s", arm.name, err, sql)
			}
			j := strings.Join(got, ";")
			if arm.name == "single" {
				single = j
				continue
			}
			if j == single {
				t.Errorf("%s answers %s, the same as the single arm — the split is "+
					"CLOSED, so delete this pin\n  SQL: %s", arm.name, j, sql)
			}
		}
	})
}

// f3SpellingsDisagree pins a PRE-EXISTING disagreement between two spellings of
// ONE predicate, on every arm. Nothing about it is distributed: it is the same
// "one predicate, two dispositions" class the census closed for the refusal,
// still open for the IN list's VALUE.
func f3SpellingsDisagree(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, a, b string) {
	t.Helper()
	t.Run(name+"/pinned_spelling_disagreement", func(t *testing.T) {
		for _, arm := range arms {
			ga, err := arm.run(a)
			if err != nil {
				t.Fatalf("%s REFUSED: %v\n  SQL: %s", arm.name, err, a)
			}
			gb, err := arm.run(b)
			if err != nil {
				t.Fatalf("%s REFUSED: %v\n  SQL: %s", arm.name, err, b)
			}
			if strings.Join(ga, ";") == strings.Join(gb, ";") {
				t.Errorf("%s answers %v for both spellings — the disagreement is CLOSED, "+
					"so delete this pin\n  A: %s\n  B: %s", arm.name, ga, a, b)
			}
		}
	})
}

// f3PinnedFloatOnEveryArm pins a shape this engine answers as a FLOAT where
// PostgreSQL answers an exact number: every arm must come back with a `float:`
// box, and the pin fails the day any of them stops doing so — which is what
// makes deleting it the fix's proof.
//
// It asserts the BOX and not the digits on purpose: a float sum's last ulp
// moves with the order the arms are aggregated in, on one arm as much as
// between two (ADR-0013's legal nondeterminism), and pinning that would be
// pinning noise.
func f3PinnedFloatOnEveryArm(t *testing.T, arms []struct {
	name string
	run  func(string) ([]string, error)
}, name, sql, pgAnswer string) {
	t.Helper()
	for _, arm := range arms {
		t.Run(name+"/"+arm.name, func(t *testing.T) {
			got, err := arm.run(sql)
			if err != nil {
				t.Fatalf("REFUSED: %v\n  SQL: %s", err, sql)
			}
			j := strings.Join(got, ";")
			if !strings.Contains(j, "float:") {
				t.Errorf("%s answers %s, no longer a float — PostgreSQL answers %s, so if "+
					"this is now exact the set-operation walk reconciles arms of DIFFERING "+
					"type: delete this pin\n  SQL: %s", arm.name, j, pgAnswer, sql)
			}
		})
	}
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
