package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// #652's second half: `CAST(x AS <a name no type answers to>)` is SQLSTATE
// 42704, not a STRING column over the operand.
//
// PostgreSQL 17.11, measured live:
//
//	SELECT CAST(1 AS bogustype);
//	ERROR:  42704: type "bogustype" does not exist
//
// wadjet answered the string "1" under DataTypeOID 25, and
// `CAST(<float column> AS bogustype)` answered "0.3333333333333333" the same
// way: expr.Cast.Eval's switch fell to `default: return v` and
// physical.inferCastType's to `default: return TypeString`, so the two layers
// AGREED with each other about a column PostgreSQL says cannot be described at
// all — the #310/#443 shape, a numeric value published as text.
//
// The FLOAT(n) half of #652 closed earlier and is asserted here as the control
// that says which half moved.
func TestUnknownCastDestinationIsUndefinedObject(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table

	for _, c := range []struct{ name, sql, typeName string }{
		{"literal_operand", `SELECT CAST(1 AS bogustype) AS v FROM ` + tbl + ` WHERE id = 1`, "bogustype"},
		{"integer_column_operand",
			`SELECT CAST(c_i64 AS bogustype) AS v FROM ` + tbl + ` WHERE id = 1`, "bogustype"},
		// The float operand is the sharpest cell: the pass-through published a
		// MEASUREMENT as text, which is the class ADR-0012 item 9 calls a
		// different number wearing the right type.
		{"float_column_operand",
			`SELECT CAST(c_f64 AS bogustype) AS v FROM ` + tbl + ` WHERE id = 1`, "bogustype"},
		{"in_a_filter",
			`SELECT COUNT(*) AS n FROM ` + tbl + ` WHERE CAST(c_i64 AS bogustype) = '1'`, "bogustype"},
		{"in_an_aggregate_argument",
			`SELECT MAX(CAST(c_i64 AS bogustype)) AS v FROM ` + tbl, "bogustype"},
		// PostgreSQL type names this engine has no type for. It refuses these
		// where the server answers, and it has refused them on the CREATE
		// TABLE door all along — the point of this pass is that ONE type name
		// gets ONE disposition, and the alternative here is the wrong VALUE
		// above. ADR-0012's divergence list records it.
		{"bytea", `SELECT CAST(c_str AS bytea) AS v FROM ` + tbl + ` WHERE id = 1`, "bytea"},
		{"inet", `SELECT CAST(c_str AS inet) AS v FROM ` + tbl + ` WHERE id = 1`, "inet"},
		{"json", `SELECT CAST(c_str AS json) AS v FROM ` + tbl + ` WHERE id = 1`, "json"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := db.Query(ctx, c.sql)
			if err == nil {
				t.Fatalf("ANSWERED; PostgreSQL 17.11 raises 42704 `type %q does not exist`"+
					"\n  SQL: %s", c.typeName, c.sql)
			}
			if got := sqlerr.StateOf(err); got != "42704" {
				t.Errorf("SQLSTATE %s, want 42704\n  err: %v", got, err)
			}
			want := `type "` + c.typeName + `" does not exist`
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%v does not carry PostgreSQL's message %q", err, want)
			}
		})
	}

	// The BOUNDARY (protocol rule 11), attempted from the outside: every
	// destination this engine DOES have still answers. A pass that refused
	// what it could not IMPLEMENT — rather than what it could not NAME — would
	// fail on the container and network rows, which Cast.Eval passes through
	// unchanged and always has.
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		{"bigint", `SELECT CAST(c_i64 AS BIGINT) AS v FROM ` + tbl + ` WHERE id = 1`, int64(1000003)},
		{"integer", `SELECT CAST(c_i32 AS INTEGER) AS v FROM ` + tbl + ` WHERE id = 1`, int64(3)},
		{"smallint", `SELECT CAST(c_i32 AS SMALLINT) AS v FROM ` + tbl + ` WHERE id = 1`, int64(3)},
		{"int2", `SELECT CAST(c_i32 AS INT2) AS v FROM ` + tbl + ` WHERE id = 1`, int64(3)},
		{"int4", `SELECT CAST(c_i32 AS INT4) AS v FROM ` + tbl + ` WHERE id = 1`, int64(3)},
		{"signed", `SELECT CAST(c_i32 AS SIGNED) AS v FROM ` + tbl + ` WHERE id = 1`, int64(3)},
		{"real", `SELECT CAST(1.0/3 AS REAL) AS v FROM ` + tbl + ` WHERE id = 1`, float32(0.33333334)},
		{"float4", `SELECT CAST(1.0/3 AS FLOAT4) AS v FROM ` + tbl + ` WHERE id = 1`, float32(0.33333334)},
		// The #652 half that closed earlier: FLOAT(n) resolves by width.
		{"float_1", `SELECT CAST(1.0/3 AS FLOAT(1)) AS v FROM ` + tbl + ` WHERE id = 1`, float32(0.33333334)},
		{"float_24", `SELECT CAST(1.0/3 AS FLOAT(24)) AS v FROM ` + tbl + ` WHERE id = 1`, float32(0.33333334)},
		{"float_25", `SELECT CAST(1.0/3 AS FLOAT(25)) AS v FROM ` + tbl + ` WHERE id = 1`, 0.3333333333333333},
		{"float_53", `SELECT CAST(1.0/3 AS FLOAT(53)) AS v FROM ` + tbl + ` WHERE id = 1`, 0.3333333333333333},
		{"bare_float", `SELECT CAST(1.0/3 AS FLOAT) AS v FROM ` + tbl + ` WHERE id = 1`, 0.3333333333333333},
		{"float8", `SELECT CAST(1.0/3 AS FLOAT8) AS v FROM ` + tbl + ` WHERE id = 1`, 0.3333333333333333},
		{"double_precision",
			`SELECT CAST(1.0/3 AS DOUBLE PRECISION) AS v FROM ` + tbl + ` WHERE id = 1`, 0.3333333333333333},
		{"text", `SELECT CAST(1 AS TEXT) AS v FROM ` + tbl + ` WHERE id = 1`, "1"},
		{"varchar", `SELECT CAST(1 AS VARCHAR) AS v FROM ` + tbl + ` WHERE id = 1`, "1"},
		{"varchar_n", `SELECT CAST('abcdef' AS VARCHAR(4)) AS v FROM ` + tbl + ` WHERE id = 1`, "abcd"},
		{"char_n", `SELECT CAST('abcdef' AS CHAR(4)) AS v FROM ` + tbl + ` WHERE id = 1`, "abcd"},
		{"character_varying_n",
			`SELECT CAST('abcdef' AS CHARACTER VARYING(4)) AS v FROM ` + tbl + ` WHERE id = 1`, "abcd"},
		{"boolean", `SELECT CAST(1 AS BOOLEAN) AS v FROM ` + tbl + ` WHERE id = 1`, true},
		{"numeric", `SELECT CAST(1 AS NUMERIC) AS v FROM ` + tbl + ` WHERE id = 1`, "1"},
		{"decimal_p_s", `SELECT CAST(1 AS DECIMAL(9,2)) AS v FROM ` + tbl + ` WHERE id = 1`, "1.00"},
		{"date", `SELECT CAST('2011-02-02' AS DATE) AS v FROM ` + tbl + ` WHERE id = 1`, "2011-02-02"},
		{"uuid", `SELECT CAST('123e4567-e89b-12d3-a456-426614174000' AS UUID) AS v FROM ` + tbl +
			` WHERE id = 1`, "123e4567-e89b-12d3-a456-426614174000"},
	} {
		t.Run("ctl_"+c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("REFUSED a destination this engine has: %v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("= %#v, want %#v", res.Rows, c.want)
			}
		})
	}
	// The destinations Cast.Eval does NOT implement and passes through. They
	// are the boundary in the other direction: this pass refuses names that
	// name nothing, not casts that are unimplemented, so these must still
	// ANSWER exactly as they did before.
	for _, sql := range []string{
		`SELECT CAST(c_str AS IPV4) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT CAST(c_str AS CIDR) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT CAST(c_str AS MAC) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT CAST(c_i64 AS DURATION) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT CAST(c_i64 AS INTERVAL) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT CAST(c_str AS BYTES) AS v FROM ` + tbl + ` WHERE id = 1`,
		`SELECT CAST(c_str AS VECTOR(3)) AS v FROM ` + tbl + ` WHERE id = 1`,
	} {
		t.Run("ctl_unimplemented_but_named", func(t *testing.T) {
			if _, err := db.Query(ctx, sql); err != nil {
				t.Errorf("REFUSED a name this engine HAS: %v\n  SQL: %s", err, sql)
			}
		})
	}
}
