package pgwire

// #849 on the WIRE, which is the half a value oracle cannot see.
//
// `CAST(visits AS BIGINT) * 2` answered "200" — the right number — under
// DataTypeOID 701 (float8) where PostgreSQL 17.11 declares 20 (int8). A test
// that compared only values called that shape correct, and a client that reads
// the declaration (pgJDBC, DataGrip, SQLAlchemy) got a double for a bigint.
//
// The overflowing spelling is the other half: PostgreSQL raises 22003
// `bigint out of range` for the same expression, and the SQLSTATE is a wire
// fact too.

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIntegerDomainDeclaresBigintOnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	for _, c := range []struct {
		name, sql, value string
	}{
		// The bare column is the control that always agreed — it is what the
		// producers below must now match.
		{"bare_column", `SELECT visits * 2 AS v FROM users WHERE id = 1`, "200"},
		{"cast", `SELECT CAST(visits AS BIGINT) * 2 AS v FROM users WHERE id = 1`, "200"},
		{"abs", `SELECT ABS(visits) * 2 AS v FROM users WHERE id = 1`, "200"},
		{"coalesce", `SELECT COALESCE(visits, 1) * 2 AS v FROM users WHERE id = 1`, "200"},
		{"case", `SELECT (CASE WHEN visits > 0 THEN visits ELSE 1 END) * 2 AS v ` +
			`FROM users WHERE id = 1`, "200"},
		{"greatest", `SELECT GREATEST(visits, 1) * 2 AS v FROM users WHERE id = 1`, "200"},
		{"nullif", `SELECT NULLIF(visits, 1) * 2 AS v FROM users WHERE id = 1`, "200"},
		{"cast_plus", `SELECT CAST(visits AS BIGINT) + 2 AS v FROM users WHERE id = 1`, "102"},
	} {
		t.Run(c.name, func(t *testing.T) {
			oid, size, val := wireField(t, srv.Addr(), c.sql)
			if oid != 20 {
				t.Errorf("declared OID %d, want 20 (int8) — PostgreSQL 17.11 types every one "+
					"of these bigint, and 701 (float8) is the declaration this shape carried "+
					"through v0.18.30 (#849)\n  SQL: %s", oid, c.sql)
			}
			if size != 8 {
				t.Errorf("declared size %d, want 8", size)
			}
			if val != c.value {
				t.Errorf("sent %q, want %q (live PostgreSQL 17.11)", val, c.value)
			}
		})
	}
	// The float BOUNDARY on the wire: the same producers over a double
	// precision column stay OID 701, so a fix that declared int8 for
	// everything fails here.
	for _, c := range []struct{ name, sql string }{
		{"ctl_abs_of_a_float", `SELECT ABS(score) * 2 AS v FROM users WHERE id = 1`},
		{"ctl_coalesce_of_a_float", `SELECT COALESCE(score, 1) * 2 AS v FROM users WHERE id = 1`},
		{"ctl_floor_of_an_integer", `SELECT FLOOR(visits) * 2 AS v FROM users WHERE id = 1`},
	} {
		t.Run(c.name, func(t *testing.T) {
			if oid, _, _ := wireField(t, srv.Addr(), c.sql); oid != 701 {
				t.Errorf("declared OID %d, want 701 (float8) — PostgreSQL 17.11 says double "+
					"precision for this shape\n  SQL: %s", oid, c.sql)
			}
		})
	}
}

func TestIntegerDomainOverflowIsSQLSTATE22003OnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	for _, c := range []struct{ name, sql string }{
		{"cast", `SELECT CAST(visits AS BIGINT) * 9223372036854775807 AS v FROM users WHERE id = 1`},
		{"abs", `SELECT ABS(visits) * 9223372036854775807 AS v FROM users WHERE id = 1`},
		{"coalesce", `SELECT COALESCE(visits, 1) * 9223372036854775807 AS v FROM users WHERE id = 1`},
		{"summed", `SELECT SUM(CAST(visits AS BIGINT) * 9223372036854775807) AS v FROM users`},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err == nil {
				t.Fatalf("the wire ANSWERED %v; PostgreSQL 17.11 raises 22003 `bigint out of "+
					"range`\n  SQL: %s", res.Rows, c.sql)
			}
			pge, ok := res.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("error is %T, not a PgError: %v", res.Err, res.Err)
			}
			if pge.Code != "22003" {
				t.Errorf("SQLSTATE %s, want 22003\n  err: %v", pge.Code, pge.Message)
			}
			// Contains, not equality: every wadjet SQLSTATE reaches the wire
			// under an "executing query: " prefix, which is a separate
			// divergence from this one and applies to every error alike
			// (integer_range_sqlstate_test.go asserts the same way).
			if !strings.Contains(pge.Message, "bigint out of range") {
				t.Errorf("message %q does not carry PostgreSQL 17.11's %q",
					pge.Message, "bigint out of range")
			}
		})
	}
}
