package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestCidrColColTwoPath holds both arms — and the two evaluation sites INSIDE
// one process — to PostgreSQL's inet order for a CIDR column compared against
// another CIDR column (#565).
//
// db12e51f's kernel.colColFilterCidr re-keys such a comparison through
// kernel.CidrOrderKey, but only on the VECTORIZED kernel. The row-at-a-time
// evaluator reaches the same predicate through expr.compare()'s both-string
// fast path, which compares the two stored TEXTS byte for byte — and that path
// is not a corner: a projection uses it, and the stage DAG's later stage
// re-parses its filter and ALWAYS compiles to it. So `WHERE c = d` answered
// PostgreSQL's 2 on the single-process arm and 0 on the DAG, and one process
// answered `WHERE c = d` and `SELECT c = d` differently about the same row.
//
// The fixture (cidrTable) spells one address both ways round across its two
// columns, so a byte comparison and an inet comparison disagree on two of its
// three rows in each direction.
func TestCidrColColTwoPath(t *testing.T) {
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
		// pg is what live postgres:17-alpine answers for the same three rows
		// as inet: rows 1 and 3 hold one address spelled two ways (equal), row
		// 2 holds 10.0.0.2/32 against 10.0.0.3/32 (strictly less).
		pg int64
	}{
		// The six operators, in the WHERE clause. Byte order answered
		// 0/3/2/2/1/1 for these before the fix; inet order answers what
		// PostgreSQL does.
		{"eq", "SELECT COUNT(*) AS n FROM %s WHERE c = d", 2},
		{"ne", "SELECT COUNT(*) AS n FROM %s WHERE c <> d", 1},
		{"lt", "SELECT COUNT(*) AS n FROM %s WHERE c < d", 1},
		{"le", "SELECT COUNT(*) AS n FROM %s WHERE c <= d", 3},
		{"gt", "SELECT COUNT(*) AS n FROM %s WHERE c > d", 0},
		{"ge", "SELECT COUNT(*) AS n FROM %s WHERE c >= d", 2},
		// The comparison as a PROJECTED VALUE, filtered above. This is the
		// shape that cannot reach the vectorized kernel on either arm: the
		// boolean is computed row-at-a-time in the SELECT list, so it is the
		// row path's own answer being counted.
		{"projected_eq", "SELECT COUNT(*) AS n FROM (SELECT c = d AS eq FROM %s) x WHERE eq", 2},
		{"projected_lt", "SELECT COUNT(*) AS n FROM (SELECT c < d AS lt FROM %s) x WHERE lt", 1},
		// The same comparison inside a CASE, which is the boxed site the
		// DECIMAL work (#506) already had to bind from the declarations.
		{"case_eq", "SELECT COUNT(*) AS n FROM %s WHERE CASE WHEN c = d THEN 1 ELSE 0 END = 1", 2},
		// A comparison against a quoted literal at a BOXED site: the direct
		// form goes through CmpNetworkLit (#492) and already agreed, but a
		// CASE operand does not reach that binding.
		{"case_literal_eq", "SELECT COUNT(*) AS n FROM %s WHERE CASE c WHEN '10.0.0.1' THEN 1 ELSE 0 END = 1", 2},
		{"is_distinct_from", "SELECT COUNT(*) AS n FROM %s WHERE c IS DISTINCT FROM d", 1},
		{"is_not_distinct_from", "SELECT COUNT(*) AS n FROM %s WHERE c IS NOT DISTINCT FROM d", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.sql, cidrTable)
			var single64, dag64 int64
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
				}
				got := ketInt(t, arm.name, rows[0]["n"])
				if arm.dag {
					dag64 = got
				} else {
					single64 = got
				}
				if got != tc.pg {
					t.Errorf("%s arm: %s\n  got %d, want %d (live PostgreSQL 17, inet order)",
						arm.name, sql, got, tc.pg)
				}
			}
			if single64 != dag64 {
				t.Errorf("the two paths disagree: single-process %d, stage DAG %d\n  SQL: %s",
					single64, dag64, sql)
			}
		})
	}

	// The asymmetry INSIDE one process, which needs no second arm to show: the
	// WHERE clause selects a row through the vectorized kernel and the SELECT
	// list then says the same comparison is false about that same row.
	//
	// This is the property a one-site fix cannot satisfy, and the reason the
	// fix belongs in the comparison rather than in another kernel.
	t.Run("where_and_select_list_agree_in_one_process", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT id, c = d AS eq, c < d AS lt FROM %s WHERE c = d ORDER BY id", cidrTable)
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != 2 {
				t.Errorf("%s arm: %s\n  got %d rows, want 2 (ids 1 and 3, the two equal pairs)",
					arm.name, sql, len(rows))
				continue
			}
			for _, r := range rows {
				if eq, ok := r["eq"].(bool); !ok || !eq {
					t.Errorf("%s arm: the WHERE clause selected id=%v and the SELECT list then "+
						"answered `c = d` = %v — one process, two answers about one row",
						arm.name, r["id"], r["eq"])
				}
				if lt, ok := r["lt"].(bool); !ok || lt {
					t.Errorf("%s arm: id=%v is an EQUAL pair, so `c < d` must be false; got %v",
						arm.name, r["id"], r["lt"])
				}
			}
		}
	})
}

// The IPv6 col-col fixture (#565 ratchet, #580 review). Ten rows: three
// equal pairs (one of them a v4-MAPPED address on both sides), three LT and
// four GT pairs chosen so lexical TEXT order and the ADDRESS's own order
// disagree on at least one pair in each direction ("2001:db8::10" sorts
// BELOW "2001:db8::9" as text and ABOVE it as an address) — the same shape
// TestColColNetworkComparisonAgreesWithTheKernel (expr) already pins at the
// unit level, exercised here across both evaluation sites and both
// distribution arms together.
//
// This is coverage, not a fix: #565's row-at-a-time boxed-pair binding
// (boxIPv6) already gives IPv6 the same treatment CIDR gets, and every
// count below is what live postgres:17-alpine answers on these exact rows
// (verified via a throwaway postgres:17-alpine container, the same
// discipline every other fixture in this file follows) — so a regression
// on this table is a NEW finding, not a known gap being re-recorded.
const ipv6Table = "ipv6pair"

func ipv6Schema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeIPv6},
		{Name: "w", Type: parquet.TypeIPv6},
	}}
}

func ipv6Data() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "v": "2001:db8::9", "w": "2001:db8::9"},
		{"id": int64(2), "v": "2001:db8::10", "w": "2001:db8::9"},
		{"id": int64(3), "v": "2001:db8::9", "w": "2001:db8::10"},
		{"id": int64(4), "v": "::ffff:10.0.0.1", "w": "::ffff:10.0.0.1"},
		{"id": int64(5), "v": "::1", "w": "::2"},
		{"id": int64(6), "v": "::2", "w": "::1"},
		{"id": int64(7), "v": "2001:db8::5", "w": "2001:db8::20"},
		{"id": int64(8), "v": "2001:db8::20", "w": "2001:db8::5"},
		{"id": int64(9), "v": "fe80::1", "w": "fe80::1"},
		{"id": int64(10), "v": "2001:db8::ffff", "w": "2001:db8::1"},
	}
}

// TestIPv6ColColTwoPath is TestCidrColColTwoPath's IPv6 counterpart: the
// same six operators, the same boxed sites, the same projected form, both
// arms — plus two shapes CIDR does not have: the v4-MAPPED address family
// (#580) and case-insensitive hex literal parsing.
func TestIPv6ColColTwoPath(t *testing.T) {
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
		// pg is what live postgres:17-alpine answers for these ten rows.
		pg int64
	}{
		// The six operators, in the WHERE clause.
		{"eq", "SELECT COUNT(*) AS n FROM %s WHERE v = w", 3},
		{"ne", "SELECT COUNT(*) AS n FROM %s WHERE v <> w", 7},
		{"lt", "SELECT COUNT(*) AS n FROM %s WHERE v < w", 3},
		{"le", "SELECT COUNT(*) AS n FROM %s WHERE v <= w", 6},
		{"gt", "SELECT COUNT(*) AS n FROM %s WHERE v > w", 4},
		{"ge", "SELECT COUNT(*) AS n FROM %s WHERE v >= w", 7},
		// The comparison as a PROJECTED VALUE, filtered above — the row
		// path's own answer being counted, the shape that reaches neither
		// arm's vectorized kernel.
		{"projected_eq", "SELECT COUNT(*) AS n FROM (SELECT v = w AS eq FROM %s) x WHERE eq", 3},
		{"projected_lt", "SELECT COUNT(*) AS n FROM (SELECT v < w AS lt FROM %s) x WHERE lt", 3},
		// A searched CASE, the boxed site #506/#565 needed for the direct
		// comparison's own answer to reach it.
		{"case_eq", "SELECT COUNT(*) AS n FROM %s WHERE CASE WHEN v = w THEN 1 ELSE 0 END = 1", 3},
		// A SIMPLE CASE against a quoted literal — Case.arms' own boxedPair
		// binding, not Cmp's. Two rows share v = "2001:db8::9" (ids 1, 3).
		{"case_literal_eq", "SELECT COUNT(*) AS n FROM %s WHERE CASE v WHEN '2001:db8::9' THEN 1 ELSE 0 END = 1", 2},
		{"is_distinct_from", "SELECT COUNT(*) AS n FROM %s WHERE v IS DISTINCT FROM w", 7},
		{"is_not_distinct_from", "SELECT COUNT(*) AS n FROM %s WHERE v IS NOT DISTINCT FROM w", 3},
		// v4-MAPPED family (#580's own repro, verified live): a v4-mapped
		// v6 value equals its own `::ffff:` spelling and NOT the bare v4
		// address it prints as (#580 is a GetValue rendering bug; the
		// comparison itself already gets this right, which is what these
		// two rows ratchet).
		{"v4mapped_equals_own_spelling", "SELECT COUNT(*) AS n FROM %s WHERE v = '::ffff:10.0.0.1'", 1},
		{"v4mapped_not_equal_bare_v4", "SELECT COUNT(*) AS n FROM %s WHERE v = '10.0.0.1'", 0},
		// FAMILY order: PostgreSQL's inet puts every v4 address below every
		// v6 one, v4-mapped included, so every row here — even id=4's
		// v4-mapped value — sorts above the highest possible v4 address.
		{"family_order_v6_above_v4_literal", "SELECT COUNT(*) AS n FROM %s WHERE v > '255.255.255.255'", 10},
		// Uppercase hex in a literal is accepted and compares equal to its
		// lowercase stored spelling — same two rows as case_literal_eq.
		{"uppercase_hex_literal", "SELECT COUNT(*) AS n FROM %s WHERE v = '2001:DB8::9'", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.sql, ipv6Table)
			var single64, dag64 int64
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
				}
				got := ketInt(t, arm.name, rows[0]["n"])
				if arm.dag {
					dag64 = got
				} else {
					single64 = got
				}
				if got != tc.pg {
					t.Errorf("%s arm: %s\n  got %d, want %d (live PostgreSQL 17, inet order)",
						arm.name, sql, got, tc.pg)
				}
			}
			if single64 != dag64 {
				t.Errorf("the two paths disagree: single-process %d, stage DAG %d\n  SQL: %s",
					single64, dag64, sql)
			}
		})
	}

	// The family-order ORDER BY form, literally: every row satisfying
	// `v > '255.255.255.255'` (all ten) must come back sorted by v's
	// ADDRESS order on both arms, not by its rendered text.
	t.Run("order_by_address_not_text", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT id FROM %s WHERE v > '255.255.255.255' ORDER BY v", ipv6Table)
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != 10 {
				t.Errorf("%s arm: %s\n  got %d rows, want 10 (every v6 row, family order)",
					arm.name, sql, len(rows))
			}
		}
	})
}
