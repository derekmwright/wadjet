package pgwire

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// Reconstructed from the 180 cells in r7rev_logs/matrix_wire.log: the saved
// a2r7rev_matrix_test.go.txt actually contains the coordinator probe. All cells
// were remeasured on PostgreSQL 17.11 before promotion. The 24 wide DECIMAL
// value divergences are #1037; a pin that starts agreeing fails.
func TestScalarSubqueryAggregateMatrix(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	m := pgtype.NewMap()
	norm := func(s string) string {
		if r, ok := new(big.Rat).SetString(s); ok {
			return r.RatString()
		}
		return s
	}
	for _, tc := range []struct {
		name, sql      string
		oid, pgOID     uint32
		want, postgres string
	}{
		{"int4/SUM/plain", "SELECT SUM((SELECT CAST(3 AS INT))) AS v FROM users", 20, 20, "9", "9"},
		{"int4/SUM/grouped", "SELECT SUM((SELECT CAST(3 AS INT))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "3;3;3", "3;3;3"},
		{"int4/SUM/window", "SELECT SUM((SELECT CAST(3 AS INT))) OVER () AS v FROM users", 20, 20, "9;9;9", "9;9;9"},
		{"int4/AVG/plain", "SELECT AVG((SELECT CAST(3 AS INT))) AS v FROM users", 1700, 1700, "3", "3"},
		{"int4/AVG/grouped", "SELECT AVG((SELECT CAST(3 AS INT))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "3;3;3", "3;3;3"},
		{"int4/AVG/window", "SELECT AVG((SELECT CAST(3 AS INT))) OVER () AS v FROM users", 1700, 1700, "3;3;3", "3;3;3"},
		{"int4/MIN/plain", "SELECT MIN((SELECT CAST(3 AS INT))) AS v FROM users", 20, 23, "3", "3"},
		{"int4/MIN/grouped", "SELECT MIN((SELECT CAST(3 AS INT))) AS v FROM users GROUP BY id ORDER BY id", 20, 23, "3;3;3", "3;3;3"},
		{"int4/MIN/window", "SELECT MIN((SELECT CAST(3 AS INT))) OVER () AS v FROM users", 20, 23, "3;3;3", "3;3;3"},
		{"int4/MAX/plain", "SELECT MAX((SELECT CAST(3 AS INT))) AS v FROM users", 20, 23, "3", "3"},
		{"int4/MAX/grouped", "SELECT MAX((SELECT CAST(3 AS INT))) AS v FROM users GROUP BY id ORDER BY id", 20, 23, "3;3;3", "3;3;3"},
		{"int4/MAX/window", "SELECT MAX((SELECT CAST(3 AS INT))) OVER () AS v FROM users", 20, 23, "3;3;3", "3;3;3"},
		{"int4/COUNT/plain", "SELECT COUNT((SELECT CAST(3 AS INT))) AS v FROM users", 20, 20, "3", "3"},
		{"int4/COUNT/grouped", "SELECT COUNT((SELECT CAST(3 AS INT))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "1;1;1", "1;1;1"},
		{"int4/COUNT/window", "SELECT COUNT((SELECT CAST(3 AS INT))) OVER () AS v FROM users", 20, 20, "3;3;3", "3;3;3"},
		{"int8/SUM/plain", "SELECT SUM((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users", 1700, 1700, "27021597764222979", "27021597764222979"},
		{"int8/SUM/grouped", "SELECT SUM((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "9007199254740993;9007199254740993;9007199254740993", "9007199254740993;9007199254740993;9007199254740993"},
		{"int8/SUM/window", "SELECT SUM((SELECT CAST(9007199254740993 AS BIGINT))) OVER () AS v FROM users", 1700, 1700, "27021597764222979;27021597764222979;27021597764222979", "27021597764222979;27021597764222979;27021597764222979"},
		{"int8/AVG/plain", "SELECT AVG((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users", 1700, 1700, "9007199254740993", "9007199254740993"},
		{"int8/AVG/grouped", "SELECT AVG((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "9007199254740993;9007199254740993;9007199254740993", "9007199254740993;9007199254740993;9007199254740993"},
		{"int8/AVG/window", "SELECT AVG((SELECT CAST(9007199254740993 AS BIGINT))) OVER () AS v FROM users", 1700, 1700, "9007199254740993;9007199254740993;9007199254740993", "9007199254740993;9007199254740993;9007199254740993"},
		{"int8/MIN/plain", "SELECT MIN((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users", 20, 20, "9007199254740993", "9007199254740993"},
		{"int8/MIN/grouped", "SELECT MIN((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "9007199254740993;9007199254740993;9007199254740993", "9007199254740993;9007199254740993;9007199254740993"},
		{"int8/MIN/window", "SELECT MIN((SELECT CAST(9007199254740993 AS BIGINT))) OVER () AS v FROM users", 20, 20, "9007199254740993;9007199254740993;9007199254740993", "9007199254740993;9007199254740993;9007199254740993"},
		{"int8/MAX/plain", "SELECT MAX((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users", 20, 20, "9007199254740993", "9007199254740993"},
		{"int8/MAX/grouped", "SELECT MAX((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "9007199254740993;9007199254740993;9007199254740993", "9007199254740993;9007199254740993;9007199254740993"},
		{"int8/MAX/window", "SELECT MAX((SELECT CAST(9007199254740993 AS BIGINT))) OVER () AS v FROM users", 20, 20, "9007199254740993;9007199254740993;9007199254740993", "9007199254740993;9007199254740993;9007199254740993"},
		{"int8/COUNT/plain", "SELECT COUNT((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users", 20, 20, "3", "3"},
		{"int8/COUNT/grouped", "SELECT COUNT((SELECT CAST(9007199254740993 AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "1;1;1", "1;1;1"},
		{"int8/COUNT/window", "SELECT COUNT((SELECT CAST(9007199254740993 AS BIGINT))) OVER () AS v FROM users", 20, 20, "3;3;3", "3;3;3"},
		{"numeric/SUM/plain", "SELECT SUM((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users", 1700, 1700, "27021597764222982", "108086391056891919/4"},
		{"numeric/SUM/grouped", "SELECT SUM((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "9007199254740994;9007199254740994;9007199254740994", "36028797018963973/4;36028797018963973/4;36028797018963973/4"},
		{"numeric/SUM/window", "SELECT SUM((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) OVER () AS v FROM users", 1700, 1700, "27021597764222982;27021597764222982;27021597764222982", "108086391056891919/4;108086391056891919/4;108086391056891919/4"},
		{"numeric/AVG/plain", "SELECT AVG((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users", 1700, 1700, "9007199254740994", "36028797018963973/4"},
		{"numeric/AVG/grouped", "SELECT AVG((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "9007199254740994;9007199254740994;9007199254740994", "36028797018963973/4;36028797018963973/4;36028797018963973/4"},
		{"numeric/AVG/window", "SELECT AVG((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) OVER () AS v FROM users", 1700, 1700, "9007199254740994;9007199254740994;9007199254740994", "36028797018963973/4;36028797018963973/4;36028797018963973/4"},
		{"numeric/MIN/plain", "SELECT MIN((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users", 1700, 1700, "9007199254740994", "36028797018963973/4"},
		{"numeric/MIN/grouped", "SELECT MIN((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "9007199254740994;9007199254740994;9007199254740994", "36028797018963973/4;36028797018963973/4;36028797018963973/4"},
		{"numeric/MIN/window", "SELECT MIN((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) OVER () AS v FROM users", 1700, 1700, "9007199254740994;9007199254740994;9007199254740994", "36028797018963973/4;36028797018963973/4;36028797018963973/4"},
		{"numeric/MAX/plain", "SELECT MAX((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users", 1700, 1700, "9007199254740994", "36028797018963973/4"},
		{"numeric/MAX/grouped", "SELECT MAX((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "9007199254740994;9007199254740994;9007199254740994", "36028797018963973/4;36028797018963973/4;36028797018963973/4"},
		{"numeric/MAX/window", "SELECT MAX((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) OVER () AS v FROM users", 1700, 1700, "9007199254740994;9007199254740994;9007199254740994", "36028797018963973/4;36028797018963973/4;36028797018963973/4"},
		{"numeric/COUNT/plain", "SELECT COUNT((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users", 20, 20, "3", "3"},
		{"numeric/COUNT/grouped", "SELECT COUNT((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "1;1;1", "1;1;1"},
		{"numeric/COUNT/window", "SELECT COUNT((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) OVER () AS v FROM users", 20, 20, "3;3;3", "3;3;3"},
		{"count/SUM/plain", "SELECT SUM((SELECT COUNT(*) FROM users)) AS v FROM users", 1700, 1700, "9", "9"},
		{"count/SUM/grouped", "SELECT SUM((SELECT COUNT(*) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "3;3;3", "3;3;3"},
		{"count/SUM/window", "SELECT SUM((SELECT COUNT(*) FROM users)) OVER () AS v FROM users", 1700, 1700, "9;9;9", "9;9;9"},
		{"count/AVG/plain", "SELECT AVG((SELECT COUNT(*) FROM users)) AS v FROM users", 1700, 1700, "3", "3"},
		{"count/AVG/grouped", "SELECT AVG((SELECT COUNT(*) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "3;3;3", "3;3;3"},
		{"count/AVG/window", "SELECT AVG((SELECT COUNT(*) FROM users)) OVER () AS v FROM users", 1700, 1700, "3;3;3", "3;3;3"},
		{"count/MIN/plain", "SELECT MIN((SELECT COUNT(*) FROM users)) AS v FROM users", 20, 20, "3", "3"},
		{"count/MIN/grouped", "SELECT MIN((SELECT COUNT(*) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "3;3;3", "3;3;3"},
		{"count/MIN/window", "SELECT MIN((SELECT COUNT(*) FROM users)) OVER () AS v FROM users", 20, 20, "3;3;3", "3;3;3"},
		{"count/MAX/plain", "SELECT MAX((SELECT COUNT(*) FROM users)) AS v FROM users", 20, 20, "3", "3"},
		{"count/MAX/grouped", "SELECT MAX((SELECT COUNT(*) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "3;3;3", "3;3;3"},
		{"count/MAX/window", "SELECT MAX((SELECT COUNT(*) FROM users)) OVER () AS v FROM users", 20, 20, "3;3;3", "3;3;3"},
		{"count/COUNT/plain", "SELECT COUNT((SELECT COUNT(*) FROM users)) AS v FROM users", 20, 20, "3", "3"},
		{"count/COUNT/grouped", "SELECT COUNT((SELECT COUNT(*) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "1;1;1", "1;1;1"},
		{"count/COUNT/window", "SELECT COUNT((SELECT COUNT(*) FROM users)) OVER () AS v FROM users", 20, 20, "3;3;3", "3;3;3"},
		{"max/SUM/plain", "SELECT SUM((SELECT MAX(id) FROM users)) AS v FROM users", 20, 20, "9", "9"},
		{"max/SUM/grouped", "SELECT SUM((SELECT MAX(id) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "3;3;3", "3;3;3"},
		{"max/SUM/window", "SELECT SUM((SELECT MAX(id) FROM users)) OVER () AS v FROM users", 20, 20, "9;9;9", "9;9;9"},
		{"max/AVG/plain", "SELECT AVG((SELECT MAX(id) FROM users)) AS v FROM users", 1700, 1700, "3", "3"},
		{"max/AVG/grouped", "SELECT AVG((SELECT MAX(id) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "3;3;3", "3;3;3"},
		{"max/AVG/window", "SELECT AVG((SELECT MAX(id) FROM users)) OVER () AS v FROM users", 1700, 1700, "3;3;3", "3;3;3"},
		{"max/MIN/plain", "SELECT MIN((SELECT MAX(id) FROM users)) AS v FROM users", 20, 23, "3", "3"},
		{"max/MIN/grouped", "SELECT MIN((SELECT MAX(id) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 20, 23, "3;3;3", "3;3;3"},
		{"max/MIN/window", "SELECT MIN((SELECT MAX(id) FROM users)) OVER () AS v FROM users", 20, 23, "3;3;3", "3;3;3"},
		{"max/MAX/plain", "SELECT MAX((SELECT MAX(id) FROM users)) AS v FROM users", 20, 23, "3", "3"},
		{"max/MAX/grouped", "SELECT MAX((SELECT MAX(id) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 20, 23, "3;3;3", "3;3;3"},
		{"max/MAX/window", "SELECT MAX((SELECT MAX(id) FROM users)) OVER () AS v FROM users", 20, 23, "3;3;3", "3;3;3"},
		{"max/COUNT/plain", "SELECT COUNT((SELECT MAX(id) FROM users)) AS v FROM users", 20, 20, "3", "3"},
		{"max/COUNT/grouped", "SELECT COUNT((SELECT MAX(id) FROM users)) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "1;1;1", "1;1;1"},
		{"max/COUNT/window", "SELECT COUNT((SELECT MAX(id) FROM users)) OVER () AS v FROM users", 20, 20, "3;3;3", "3;3;3"},
		{"null/SUM/plain", "SELECT SUM((SELECT CAST(NULL AS BIGINT))) AS v FROM users", 1700, 1700, "NULL", "NULL"},
		{"null/SUM/grouped", "SELECT SUM((SELECT CAST(NULL AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "NULL;NULL;NULL", "NULL;NULL;NULL"},
		{"null/SUM/window", "SELECT SUM((SELECT CAST(NULL AS BIGINT))) OVER () AS v FROM users", 1700, 1700, "NULL;NULL;NULL", "NULL;NULL;NULL"},
		{"null/AVG/plain", "SELECT AVG((SELECT CAST(NULL AS BIGINT))) AS v FROM users", 1700, 1700, "NULL", "NULL"},
		{"null/AVG/grouped", "SELECT AVG((SELECT CAST(NULL AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 1700, 1700, "NULL;NULL;NULL", "NULL;NULL;NULL"},
		{"null/AVG/window", "SELECT AVG((SELECT CAST(NULL AS BIGINT))) OVER () AS v FROM users", 1700, 1700, "NULL;NULL;NULL", "NULL;NULL;NULL"},
		{"null/MIN/plain", "SELECT MIN((SELECT CAST(NULL AS BIGINT))) AS v FROM users", 20, 20, "NULL", "NULL"},
		{"null/MIN/grouped", "SELECT MIN((SELECT CAST(NULL AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "NULL;NULL;NULL", "NULL;NULL;NULL"},
		{"null/MIN/window", "SELECT MIN((SELECT CAST(NULL AS BIGINT))) OVER () AS v FROM users", 20, 20, "NULL;NULL;NULL", "NULL;NULL;NULL"},
		{"null/MAX/plain", "SELECT MAX((SELECT CAST(NULL AS BIGINT))) AS v FROM users", 20, 20, "NULL", "NULL"},
		{"null/MAX/grouped", "SELECT MAX((SELECT CAST(NULL AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "NULL;NULL;NULL", "NULL;NULL;NULL"},
		{"null/MAX/window", "SELECT MAX((SELECT CAST(NULL AS BIGINT))) OVER () AS v FROM users", 20, 20, "NULL;NULL;NULL", "NULL;NULL;NULL"},
		{"null/COUNT/plain", "SELECT COUNT((SELECT CAST(NULL AS BIGINT))) AS v FROM users", 20, 20, "0", "0"},
		{"null/COUNT/grouped", "SELECT COUNT((SELECT CAST(NULL AS BIGINT))) AS v FROM users GROUP BY id ORDER BY id", 20, 20, "0;0;0", "0;0;0"},
		{"null/COUNT/window", "SELECT COUNT((SELECT CAST(NULL AS BIGINT))) OVER () AS v FROM users", 20, 20, "0;0;0", "0;0;0"},
	} {
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/f%d", tc.name, format), func(t *testing.T) {
				r := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{format}).Read()
				if r.Err != nil {
					t.Fatal(r.Err)
				}
				if len(r.FieldDescriptions) != 1 {
					t.Fatalf("fields=%v", r.FieldDescriptions)
				}
				field := r.FieldDescriptions[0]
				if field.DataTypeOID != tc.oid || field.Format != format {
					t.Fatalf("OID/format=%d/%d want %d/%d (PG OID %d)", field.DataTypeOID, field.Format, tc.oid, format, tc.pgOID)
				}
				typ, ok := m.TypeForOID(field.DataTypeOID)
				if !ok {
					t.Fatal("unknown OID")
				}
				got := make([]string, 0, len(r.Rows))
				for _, row := range r.Rows {
					v := "NULL"
					if row[0] != nil {
						value, err := typ.Codec.DecodeDatabaseSQLValue(m, field.DataTypeOID, format, row[0])
						if err != nil {
							t.Fatal(err)
						}
						v = norm(fmt.Sprint(value))
					}
					got = append(got, v)
				}
				normalize := func(s string) string {
					v := strings.Split(s, ";")
					for i := range v {
						v[i] = norm(v[i])
					}
					return strings.Join(v, ";")
				}
				actual := strings.Join(got, ";")
				if actual != normalize(tc.want) {
					t.Errorf("got %s want %s (PG %s)", actual, tc.want, tc.postgres)
				}
				if tc.want != tc.postgres && normalize(tc.want) != normalize(tc.postgres) && actual == normalize(tc.postgres) {
					t.Fatal("#1037 pin started agreeing; remove the pin with its fix")
				}
			})
		}
	}
}

// The quoted spelling bypasses #1037's lossy numeric-literal conversion.
func TestQuotedDecimalScalarSubqueryAggregateIsExact(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	m := pgtype.NewMap()
	for _, format := range []int16{0, 1} {
		t.Run(fmt.Sprintf("f%d", format), func(t *testing.T) {
			r := conn.ExecParams(context.Background(), `SELECT SUM((SELECT CAST('9007199254740993.25' AS DECIMAL(30,2)))) AS v FROM users`, nil, nil, nil, []int16{format}).Read()
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			if len(r.Rows) != 1 || len(r.FieldDescriptions) != 1 {
				t.Fatalf("unexpected shape: %+v", r)
			}
			f := r.FieldDescriptions[0]
			if f.DataTypeOID != 1700 || f.Format != format {
				t.Fatalf("field=%+v", f)
			}
			typ, _ := m.TypeForOID(1700)
			value, err := typ.Codec.DecodeDatabaseSQLValue(m, 1700, format, r.Rows[0][0])
			if err != nil {
				t.Fatal(err)
			}
			got, ok := new(big.Rat).SetString(fmt.Sprint(value))
			want, _ := new(big.Rat).SetString("27021597764222979.75")
			if !ok || got.Cmp(want) != 0 {
				t.Fatalf("got %v want %v", value, want)
			}
		})
	}
}
