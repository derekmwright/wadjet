package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestInsertSelectTypesAnUnknownLiteralFromItsTarget is #1088.
//
// `INSERT INTO t (ip) SELECT '10.0.0.1'` was 42804 — "column is of type IPV4
// but expression is of type STRING" — for EVERY target type, because the
// select list's declared output for a bare quoted literal is TypeString and
// the assignment compared that against the target. PostgreSQL 17.11 does not:
// a bare quoted literal is SQL's `unknown`, typed FROM the target and coerced
// with that type's own input function, which is what the VALUES door here has
// always done. A literal the target's grammar cannot read is then a refusal
// naming the column, not a plausible value.
//
// The other direction is the rule's boundary and is asserted too: a STRING
// COLUMN is not unknown-typed, and `INSERT INTO t (ip) SELECT text_col` stays
// 42804 exactly as it is on the server, because text has no assignment cast to
// inet.
func TestInsertSelectTypesAnUnknownLiteralFromItsTarget(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "c_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "c_uuid", Type: parquet.TypeUUID, Nullable: true},
		{Name: "c_port", Type: parquet.TypePort, Nullable: true},
		{Name: "c_proto", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "c_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c_date", Type: parquet.TypeDate, Nullable: true},
		{Name: "c_str", Type: parquet.TypeString, Nullable: true},
		{Name: "c_bool", Type: parquet.TypeBool, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Every target whose TEXT this engine reads takes the literal, and the
	// value it stores is the value the same literal stores through VALUES.
	for _, c := range []struct {
		col, lit, want string
	}{
		{"c_ipv4", "010.1.2.3", "10.1.2.3"},
		{"c_ipv6", "2001:DB8::1", "2001:db8::1"},
		{"c_cidr", "192.168.1.0/24", "192.168.1.0/24"},
		{"c_mac", "AA-BB-CC-DD-EE-FF", "aa:bb:cc:dd:ee:ff"},
		{"c_uuid", "{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{"c_date", "2020-01-01", "2020-01-01"},
		{"c_str", "plain", "plain"},
	} {
		t.Run("select/"+c.col, func(t *testing.T) {
			if _, err := db.Query(ctx,
				"INSERT INTO t ("+c.col+") SELECT '"+c.lit+"'"); err != nil {
				t.Fatalf("INSERT … SELECT '%s' into %s: %v", c.lit, c.col, err)
			}
			res, err := db.Query(ctx,
				"SELECT "+c.col+" AS v FROM t WHERE "+c.col+" IS NOT NULL")
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("%s read back as %#v, want %q", c.col, res.Rows, c.want)
			}
			if _, err := db.Query(ctx, "DELETE FROM t"); err != nil {
				t.Fatal(err)
			}
		})
	}
	// The int4-backed pair answers a number, so it is checked apart from the
	// text ones above.
	for _, c := range []struct {
		col, lit string
		want     any
	}{
		{"c_port", "443", int32(443)},
		{"c_proto", "17", int32(17)},
		{"c_i64", "123", int64(123)},
	} {
		t.Run("select/"+c.col, func(t *testing.T) {
			if _, err := db.Query(ctx,
				"INSERT INTO t ("+c.col+") SELECT '"+c.lit+"'"); err != nil {
				t.Fatalf("INSERT … SELECT '%s' into %s: %v", c.lit, c.col, err)
			}
			res, err := db.Query(ctx,
				"SELECT "+c.col+" AS v FROM t WHERE "+c.col+" IS NOT NULL")
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("%s read back as %#v, want %#v", c.col, res.Rows, c.want)
			}
			if _, err := db.Query(ctx, "DELETE FROM t"); err != nil {
				t.Fatal(err)
			}
		})
	}

	// A literal the target's grammar cannot read is a refusal naming the
	// column — never a stored value nobody wrote.
	t.Run("select/malformed", func(t *testing.T) {
		_, err := db.Query(ctx, "INSERT INTO t (c_ipv4) SELECT 'zzz'")
		if err == nil {
			t.Fatal("INSERT … SELECT 'zzz' into an IPV4 column succeeded")
		}
		if !strings.Contains(err.Error(), "c_ipv4") || !strings.Contains(err.Error(), `"zzz"`) {
			t.Errorf("refusal %q names neither the column nor the literal", err)
		}
	})

	// The boundary: a STRING COLUMN is not unknown-typed and keeps 42804,
	// which is PostgreSQL's answer for `text` into `inet` too.
	for _, sql := range []string{
		"INSERT INTO t (c_ipv4) SELECT c_str FROM t",
		"INSERT INTO t (c_i64) SELECT c_str FROM t",
	} {
		t.Run("column/"+sql, func(t *testing.T) {
			_, err := db.Query(ctx, sql)
			if err == nil {
				t.Fatalf("%s succeeded; a text COLUMN has no assignment cast to the target", sql)
			}
			if st := sqlerr.StateOf(err); st != "42804" {
				t.Errorf("SQLSTATE %q, want 42804 (%v)", st, err)
			}
		})
	}

	// And the declarations whose text no leaf in this writer reads stay
	// 42804 rather than failing at the flush with a box error.
	t.Run("bool/stays_42804", func(t *testing.T) {
		_, err := db.Query(ctx, "INSERT INTO t (c_bool) SELECT 'true'")
		if err == nil || sqlerr.StateOf(err) != "42804" {
			t.Errorf("INSERT … SELECT 'true' into a BOOL column = %v, want 42804", err)
		}
	})
}
