// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// WHAT THE WIRE CARRIES FOR A ROW AN ON RESIDUAL PADDED (arc JR, #1153).
//
// A value oracle cannot see a right value under a wrong OID, and the row an
// outer join pads is the one whose declaration has nothing to read it off: its
// build-side columns have no value at all, so the OID can only come from the
// PLAN's declaration of the absent side. That is exactly the declaration this
// arc's shapes reach for the first time — before it, an outer join whose ON
// held a function call, a CAST or LIKE was refused, so no padded row from one
// ever reached a client.
//
// Both FORMAT CODES, because they are two encodings of the same declaration
// and a padded column is where they can disagree: in text a NULL is a -1
// length, in binary it is a -1 length too, but every non-padded value beside it
// is a different byte string, and a field declared bigint must decode as eight
// network-order bytes in binary and as digits in text. A cell asserts the
// RowDescription once and then reads the rows in both.
func TestJRThePaddedRowDeclaresItsBuildSideType(t *testing.T) {
	srv := setupJRResidualDB(t)

	for _, c := range []struct {
		name, sql string
		// want is "field:oid", in order — PostgreSQL 17.11's own for these
		// relations: 20 bigint, 25 text.
		want []string
		// rows is the answer as this gate renders it: one string per row,
		// fields comma-separated, a padded field written NULL. The same
		// strings must come back in TEXT and in BINARY.
		rows []string
	}{
		{
			// The headline shape of #1153: an outer join whose ON holds a
			// CAST. Probe row 2 finds no partner and is padded.
			name: "left_cast_residual",
			sql: `SELECT a.x AS x, b.y AS y, b.z AS z FROM jrw_a a ` +
				`LEFT JOIN jrw_b b ON CAST(a.x AS VARCHAR) = b.y ORDER BY 1`,
			want: []string{"x:20", "y:25", "z:20"},
			rows: []string{"1,1,1", "2,NULL,NULL"},
		},
		{
			// LIKE, and a probe row whose candidates ALL fail the residual —
			// the padding decided by the residual rather than by the key.
			name: "left_like_residual",
			sql: `SELECT a.x AS x, b.y AS y FROM jrw_a a ` +
				`LEFT JOIN jrw_b b ON b.y LIKE '9%' ORDER BY 1`,
			want: []string{"x:20", "y:25"},
			rows: []string{"1,NULL", "2,NULL"},
		},
		{
			// RIGHT: the padded side is the PROBE, so the declaration of the
			// absent columns comes from the other arm.
			name: "right_cast_residual",
			sql: `SELECT a.x AS x, b.y AS y, b.z AS z FROM jrw_a a ` +
				`RIGHT JOIN jrw_b b ON CAST(a.x AS VARCHAR) = b.y ORDER BY 2`,
			want: []string{"x:20", "y:25", "z:20"},
			rows: []string{"1,1,1", "NULL,3,3"},
		},
		{
			// FULL: both sides pad in the same result.
			name: "full_cast_residual",
			sql: `SELECT a.x AS x, b.y AS y, b.z AS z FROM jrw_a a ` +
				`FULL JOIN jrw_b b ON CAST(a.x AS VARCHAR) = b.y ORDER BY 1, 2`,
			want: []string{"x:20", "y:25", "z:20"},
			rows: []string{"1,1,1", "2,NULL,NULL", "NULL,3,3"},
		},
		{
			// #1178's shape: a BETWEEN inside ON. The residual is what
			// decides which rows pad.
			name: "left_between_residual",
			sql: `SELECT a.x AS x, b.z AS z FROM jrw_a a ` +
				`LEFT JOIN jrw_b b ON a.x = b.z AND a.x BETWEEN 2 AND 9 ORDER BY 1`,
			want: []string{"x:20", "z:20"},
			rows: []string{"1,NULL", "2,NULL"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, format := range []struct {
				name string
				code int16
			}{{"text", 0}, {"binary", 1}} {
				conn := connectPgconn(t, srv.Addr())
				res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{format.code}).Read()
				if res.Err != nil {
					t.Fatalf("%s: %v\n  SQL: %s", format.name, res.Err, c.sql)
				}
				got := make([]string, len(res.FieldDescriptions))
				for i, f := range res.FieldDescriptions {
					got[i] = fmt.Sprintf("%s:%d", f.Name, f.DataTypeOID)
					if f.Format != format.code {
						t.Errorf("%s: field %q came back in format %d, not %d",
							format.name, f.Name, f.Format, format.code)
					}
				}
				if strings.Join(got, ",") != strings.Join(c.want, ",") {
					t.Errorf("%s\n  %s RowDescription %v\n  want            %v",
						c.sql, format.name, got, c.want)
				}
				var rows []string
				for _, row := range res.Rows {
					cells := make([]string, len(row))
					for i, v := range row {
						cells[i] = jrWireCell(t, v, res.FieldDescriptions[i].DataTypeOID, format.code)
					}
					rows = append(rows, strings.Join(cells, ","))
				}
				if strings.Join(rows, ";") != strings.Join(c.rows, ";") {
					t.Errorf("%s\n  %s rows %v\n  want      %v", c.sql, format.name, rows, c.rows)
				}
			}
		})
	}
}

// jrWireCell decodes one field the way its DECLARED OID and the result's
// format code say it must be encoded. A padded column is nil in both formats;
// everything else has to decode under the declaration, which is what makes a
// right value under a wrong OID visible here.
func jrWireCell(t *testing.T, v []byte, oid uint32, format int16) string {
	t.Helper()
	if v == nil {
		return "NULL"
	}
	if format == 0 {
		return string(v)
	}
	switch oid {
	case 20: // bigint
		if len(v) != 8 {
			t.Errorf("a field declared bigint (OID 20) carried %d binary bytes, not 8", len(v))
			return "?"
		}
		return fmt.Sprintf("%d", int64(binary.BigEndian.Uint64(v)))
	case 25: // text
		return string(v)
	}
	t.Errorf("unexpected OID %d in this gate's corpus", oid)
	return "?"
}

// setupJRResidualDB is #1153's own two relations: `a(x)` = {1, 2} and
// `b(y, z)` = {('1', 1), ('3', 3)}, the rows the issue was measured over on
// PostgreSQL 17.11.
func setupJRResidualDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	load := func(name string, schema parquet.Schema, rows []map[string]any) {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(name, schema, nil, ingest.Config{MaxBufferRows: 16, RowGroupSize: 16})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	load("jrw_a", parquet.Schema{Columns: []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64},
	}}, []map[string]any{{"x": int64(1)}, {"x": int64(2)}})
	load("jrw_b", parquet.Schema{Columns: []parquet.Column{
		{Name: "y", Type: parquet.TypeString},
		{Name: "z", Type: parquet.TypeInt64},
	}}, []map[string]any{{"y": "1", "z": int64(1)}, {"y": "3", "z": int64(3)}})

	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}
