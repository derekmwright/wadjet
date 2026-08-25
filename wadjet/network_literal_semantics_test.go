package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The end-to-end half of #492's second pass. The unit tests next door
// (internal/engine/exec/kernel, internal/engine/expr) pin the two comparison
// paths separately; these run real SQL through db.Query, where the SAME
// predicate reaches the scan's kernel from a WHERE clause and the
// row-at-a-time evaluator from a SELECT list. Answering them differently was
// the shipped defect: `WHERE c_cidr = '<a bare address>'` returned nothing
// while `SELECT c_cidr = '<the same address>'` returned true for the row.

// tmIDs runs a query expected to project an "id" column and returns the ids.
func tmIDs(t *testing.T, ctx context.Context, db *DB, sql string) []int64 {
	t.Helper()
	res, err := tmRun(ctx, db, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	var out []int64
	for _, r := range res.Rows {
		n, ok := tmAsInt64(r["id"])
		if !ok {
			t.Fatalf("%s: id came back as %#v", sql, r["id"])
		}
		out = append(out, n)
	}
	return out
}

// TestCidrLiteralSemanticsEndToEnd walks the shapes PostgreSQL's inet decides
// and #492's first pass got wrong, through real SQL.
func TestCidrLiteralSemanticsEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	// Which ids the fixture actually holds for each literal, computed from the
	// fixture itself rather than restated — a wrong expectation copied from a
	// wrong engine is the poisoned-baseline failure mode (ADR-0012).
	rows := typematrix.Data(typematrix.Rows)
	idsWith := func(want string) []int64 {
		var out []int64
		for i, r := range rows {
			if s, _ := r["c_cidr"].(string); s == want {
				out = append(out, int64(i))
			}
		}
		return out
	}

	for _, tc := range []struct {
		name string
		lit  string
		// the stored TEXT the literal names, which may differ from the
		// literal's own spelling (a bare address is a /32 host route).
		stored string
	}{
		{"host-bearing prefix", "192.168.188.190/24", "192.168.188.190/24"},
		{"bare address is a /32", "172.16.2.187", "172.16.2.187/32"},
		{"canonical network", "192.168.188.0/24", "192.168.188.0/24"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := idsWith(tc.stored)
			if len(want) == 0 {
				t.Fatalf("the fixture holds no row with c_cidr = %s", tc.stored)
			}
			got := tmIDs(t, ctx, db, fmt.Sprintf(
				"SELECT id FROM %s WHERE c_cidr = '%s' ORDER BY id", typematrix.Table, tc.lit))
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("WHERE c_cidr = '%s' answered %v, want %v", tc.lit, got, want)
			}

			// The SELECT-list form of the identical comparison. It goes
			// through the row-at-a-time evaluator rather than the scan's
			// kernel, and the two must not disagree.
			res, err := tmRun(ctx, db, fmt.Sprintf(
				"SELECT id, c_cidr = '%s' AS m FROM %s WHERE id = %d",
				tc.lit, typematrix.Table, want[0]))
			if err != nil {
				t.Fatalf("select-list form: %v", err)
			}
			if len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["m"]) != "true" {
				t.Errorf("SELECT c_cidr = '%s' for id %d answered %v; the WHERE clause found it",
					tc.lit, want[0], res.Rows)
			}
		})
	}

	// Host bits are part of the value: the /24 NETWORK row and a row inside it
	// are different values, which the masked-network key made one.
	network := idsWith("192.168.188.0/24")
	inside := idsWith("192.168.188.190/24")
	if len(network) == 0 || len(inside) == 0 {
		t.Fatal("the fixture needs both a /24 network and a host-bearing address inside it")
	}
	got := tmIDs(t, ctx, db, fmt.Sprintf(
		"SELECT id FROM %s WHERE c_cidr = '192.168.188.0/24' ORDER BY id", typematrix.Table))
	for _, id := range got {
		for _, bad := range inside {
			if id == bad {
				t.Errorf("= '192.168.188.0/24' answered id %d, which holds a host-bearing address in that network", id)
			}
		}
	}
}

// TestNonAddressLiteralAgainstACidrColumnIsAQueryError: a literal that names
// no address cannot mean anything against an address column, and PostgreSQL
// refuses it (22P02). It used to answer — differently on each path:
// match-nothing through the scan kernel, a lexical text comparison through
// the row evaluator, so `WHERE c_cidr <> 'garbage'` returned zero rows and
// `SELECT c_cidr <> 'garbage'` returned true for every one.
func TestNonAddressLiteralAgainstACidrColumnIsAQueryError(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	for _, sql := range []string{
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr = 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr <> 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr < 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT id, c_cidr = 'garbage' AS m FROM %s WHERE id = 188", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr IN ('garbage')", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_ipv6 <> 'garbage'", typematrix.Table),
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := tmRun(ctx, db, sql)
			if err == nil {
				t.Fatalf("answered instead of refusing: %s", sql)
			}
			if !strings.Contains(err.Error(), "garbage") {
				t.Errorf("the error must quote the literal, got %q", err.Error())
			}
		})
	}

	// The EMPTY string is a non-address literal too, and it is the one shape
	// that does not reach the refusal the same way: the compiler lowers
	// `col = ''` to an expr.ColEmptyStr zero-length test whose Fallback is the
	// generic Cmp, so the refusal is reached only through that fallback. It
	// also cannot be signalled by an empty `nonAddr` text, which is what made
	// `SELECT c_cidr = ''` answer false while `WHERE c_cidr = ''` raised.
	for _, sql := range []string{
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr = ''", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_cidr <> ''", typematrix.Table),
		fmt.Sprintf("SELECT id, c_cidr = '' AS m FROM %s WHERE id = 188", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_ipv6 = ''", typematrix.Table),
		fmt.Sprintf("SELECT id, c_ipv6 = '' AS m FROM %s WHERE id = 1", typematrix.Table),
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := tmRun(ctx, db, sql)
			if err == nil {
				t.Fatalf("answered instead of refusing: %s", sql)
			}
			if !strings.Contains(err.Error(), `""`) {
				t.Errorf("the error must quote the literal, got %q", err.Error())
			}
		})
	}
}

// TestNonAddressLiteralAgainstIPv4MacOrUUIDColumnIsAQueryError is #519: the
// same defect TestNonAddressLiteralAgainstACidrColumnIsAQueryError closed for
// CIDR/IPv6, one type family over. A literal that names no address used to
// answer a silent SENTINEL instead of erroring, and a different one on each
// path: TypeIPv4/TypeMAC read it as the encoded ZERO, which MATCHED every row
// holding 0.0.0.0 / 00:00:00:00:00:00 through the kernel while the row
// evaluator's lexical fallback answered false; TypeUUID read it as the empty
// string, which matched NOTHING through the kernel (`=`) or EVERYTHING
// (`<>`) while the row evaluator again answered by lexical text.
func TestNonAddressLiteralAgainstIPv4MacOrUUIDColumnIsAQueryError(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	for _, sql := range []string{
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_ipv4 = 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_ipv4 <> 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT id, c_ipv4 = 'garbage' AS m FROM %s WHERE id = 188", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_ipv4 IN ('garbage')", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_mac = 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_mac <> 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT id, c_mac = 'garbage' AS m FROM %s WHERE id = 188", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_mac IN ('garbage')", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_uuid = 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_uuid <> 'garbage'", typematrix.Table),
		fmt.Sprintf("SELECT id, c_uuid = 'garbage' AS m FROM %s WHERE id = 188", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_uuid IN ('garbage')", typematrix.Table),
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := tmRun(ctx, db, sql)
			if err == nil {
				t.Fatalf("answered instead of refusing: %s", sql)
			}
			if !strings.Contains(err.Error(), "garbage") {
				t.Errorf("the error must quote the literal, got %q", err.Error())
			}
		})
	}
}

// TestIPv4LiteralZeroSentinelNoLongerMatchesEndToEnd is the exact repro from
// #501/#519: `WHERE c_ipv4 > 'not-an-ip'` used to be read as `> 0` by the
// kernel, matching every non-zero row, while the SELECT-list form of the
// identical predicate answered false for every row. Both must now refuse.
func TestIPv4LiteralZeroSentinelNoLongerMatchesEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	_, err := tmRun(ctx, db, fmt.Sprintf(
		"SELECT id FROM %s WHERE c_ipv4 > 'not-an-ip' ORDER BY id", typematrix.Table))
	if err == nil {
		t.Fatal("WHERE c_ipv4 > 'not-an-ip' answered instead of refusing")
	}
	_, err = tmRun(ctx, db, fmt.Sprintf(
		"SELECT id, c_ipv4 > 'not-an-ip' AS v FROM %s ORDER BY id", typematrix.Table))
	if err == nil {
		t.Fatal("SELECT c_ipv4 > 'not-an-ip' answered instead of refusing")
	}
}

// TestIPv6ColumnAgainstAV4LiteralEndToEnd: PostgreSQL compares the address
// FAMILY first, so every stored v6 address is above every v4 literal. The
// scan kernel used to read the literal as its v4-MAPPED v6 bytes and the row
// evaluator fell through to a lexical text compare — two paths, two answers.
func TestIPv6ColumnAgainstAV4LiteralEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	rows := typematrix.Data(typematrix.Rows)
	nonNull := 0
	for _, r := range rows {
		if s, _ := r["c_ipv6"].(string); s != "" {
			nonNull++
		}
	}
	res, err := tmRun(ctx, db, fmt.Sprintf(
		"SELECT COUNT(*) AS n FROM %s WHERE c_ipv6 > '10.0.0.2'", typematrix.Table))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got, _ := tmAsInt64(res.Rows[0]["n"])
	if got != int64(nonNull) {
		t.Errorf("c_ipv6 > '10.0.0.2' answered %d rows, want every non-NULL row (%d): "+
			"PostgreSQL puts every v4 address below every v6 one", got, nonNull)
	}
	res, err = tmRun(ctx, db, fmt.Sprintf(
		"SELECT COUNT(*) AS n FROM %s WHERE c_ipv6 < '10.0.0.2'", typematrix.Table))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got, _ := tmAsInt64(res.Rows[0]["n"]); got != 0 {
		t.Errorf("c_ipv6 < '10.0.0.2' answered %d rows, want 0", got)
	}
}
