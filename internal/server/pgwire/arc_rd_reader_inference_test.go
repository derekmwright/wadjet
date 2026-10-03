// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestArcRDReaderPastSampleWire (#1242, arc RD): the readers' position on
// the wire. By default a key first seen past the 100-row sample is 22P04 and
// a nested field first seen past it 22P02, each naming the reader, the file,
// the row and the key; `sample_size = -1` answers the same files with the
// widened column — double precision (OID 701) on the wire, the key a column —
// and a CREATE TABLE AS stores the widened value. At 9420d256 the key and the
// field were dropped from the row and `sample_size=-1` did not parse.
func TestArcRDReaderPastSampleWire(t *testing.T) {
	srv := setupJ1LateralDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	dir := t.TempDir()
	for _, R := range []int{101, 2049, 2200} {
		var key, nested, float strings.Builder
		float.WriteString("id,a\n")
		for i := 1; i <= R; i++ {
			switch i {
			case R:
				fmt.Fprintf(&key, "{\"id\":%d,\"a\":%d,\"k\":7}\n", i, i)
				fmt.Fprintf(&nested, "{\"id\":%d,\"m\":{\"x\":%d,\"y\":2}}\n", i, i)
				fmt.Fprintf(&float, "%d,0.75\n", i)
			default:
				fmt.Fprintf(&key, "{\"id\":%d,\"a\":%d}\n", i, i)
				fmt.Fprintf(&nested, "{\"id\":%d,\"m\":{\"x\":%d}}\n", i, i)
				fmt.Fprintf(&float, "%d,%d\n", i, i)
			}
		}
		write := func(name, body string) string {
			p := filepath.Join(dir, fmt.Sprintf("%d_%s", R, name))
			if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			return p
		}
		kp, np, fp := write("key.json", key.String()), write("nested.json", nested.String()), write("float.csv", float.String())
		t.Run(fmt.Sprint(R), func(t *testing.T) {
			refuse := func(sql, code, want string) {
				t.Helper()
				var n int64
				err := conn.QueryRow(ctx, sql).Scan(&n)
				var pe *pgconn.PgError
				if !errors.As(err, &pe) || pe.Code != code || !strings.Contains(pe.Message, want) {
					t.Errorf("%s: want %s %q, got %v", sql, code, want, err)
				}
			}
			refuse(fmt.Sprintf("SELECT COUNT(*) FROM read_json('%s')", kp), "22P04",
				fmt.Sprintf(`read_json: %s: row %d: key "k" is not a column of the relation (the columns were inferred from the file's first 100 rows); sample_size = -1 infers it from every row`, kp, R))
			refuse(fmt.Sprintf("SELECT COUNT(*) FROM read_json('%s')", np), "22P02",
				fmt.Sprintf(`read_json: %s: row %d column "m" field "y": value 2 is under a field the column's type (record) does not have`, np, R))
			refuse(fmt.Sprintf("SELECT COUNT(*) FROM read_csv('%s')", fp), "22P02",
				fmt.Sprintf(`read_csv: %s: row %d column "a": value "0.75" (double precision) is not of type bigint`, fp, R))
			refuse(fmt.Sprintf("SELECT COUNT(*) FROM read_csv('%s', sample_size = 0)", fp), "22023",
				`read_csv: sample_size must be a number of rows (1 or more) or -1 for every row, not "0"`)

			var k int64
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT k FROM read_json('%s', sample_size = -1) WHERE id = %d", kp, R)).Scan(&k); err != nil || k != 7 {
				t.Errorf("key under -1: %d %v", k, err)
			}
			var y int64
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT (m).y FROM read_json('%s', sample_size = -1) WHERE id = %d", np, R)).Scan(&y); err != nil || y != 2 {
				t.Errorf("nested field under -1: %d %v", y, err)
			}
			rows, err := conn.Query(ctx, fmt.Sprintf("SELECT SUM(a) FROM read_csv('%s', sample_size = -1)", fp))
			if err != nil {
				t.Fatal(err)
			}
			var sum float64
			for rows.Next() {
				if oid := rows.FieldDescriptions()[0].DataTypeOID; oid != 701 {
					t.Errorf("SUM over the widened column: OID %d, want 701 (double precision)", oid)
				}
				if err := rows.Scan(&sum); err != nil {
					t.Fatal(err)
				}
			}
			rows.Close()
			if want := float64((R-1)*R/2) + 0.75; sum != want {
				t.Errorf("SUM under -1: %v, want %v", sum, want)
			}
			table := fmt.Sprintf("rd_wire_%d", R)
			if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_csv('%s', sample_size = -1)", table, fp)); err != nil {
				t.Fatal(err)
			}
			var stored float64
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT a FROM %s WHERE id = %d", table, R)).Scan(&stored); err != nil || stored != 0.75 {
				t.Errorf("CTAS stored %v, %v; want 0.75", stored, err)
			}
		})
	}
}
