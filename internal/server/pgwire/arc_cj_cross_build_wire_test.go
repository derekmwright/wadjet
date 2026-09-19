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

// WHAT THE WIRE CARRIES FOR A CROSS JOIN'S PROJECTED BUILD COLUMN (arc CJ,
// #1189).
//
// The arc changes which build rows a cross join publishes, and a build row
// carries its own columns out through the projection. A value oracle cannot
// see a right value under a wrong OID, so the declaration is read here, in
// both format codes, over the shape whose row set moved: a filter over a build
// side that spans three row groups.
//
// The relations are `cjw_p(pid)` = {1, 2} and `cjw_b(bid, f, s)` = nine rows
// written with RowGroupSize 3, `f` true for bid 1,3,4,6,7,9 and `s` one
// distinct string per row. Three batches, six of nine rows accepted — so a
// cell's ROWS are the fix's subject and its OIDs are this gate's.
//
// At 1c2b4d25 every cell's ROWS are wrong in both formats (nine build rows
// where six were accepted) and the OIDs are unchanged, which is the
// distinction a wire gate exists to draw: cj_author/gate_cj_wire_at_base_FAILS.log.
func TestCJACrossJoinsBuildColumnDeclaresItsOwnType(t *testing.T) {
	srv := setupCJCrossDB(t)

	for _, c := range []struct {
		name, sql string
		// want is "field:oid", in order — PostgreSQL 17.11's own for these
		// relations: 20 bigint, 25 text, 16 bool.
		want []string
		rows []string
	}{
		{
			// The build's BOOLEAN is the column the filter reads AND the
			// column the projection publishes. Both readings have to agree,
			// which is what "false,13" in #1189 did not.
			name: "cross_build_bool_and_id",
			sql: `SELECT p.pid AS pid, b.bid AS bid, b.f AS f FROM cjw_p p ` +
				`CROSS JOIN cjw_b b WHERE b.f ORDER BY 1, 2`,
			want: []string{"pid:20", "bid:20", "f:16"},
			rows: []string{
				"1,1,t", "1,3,t", "1,4,t", "1,6,t", "1,7,t", "1,9,t",
				"2,1,t", "2,3,t", "2,4,t", "2,6,t", "2,7,t", "2,9,t",
			},
		},
		{
			// A TEXT build column projected through the same join, under an
			// IN list — the second reproduction #1189 was filed with.
			name: "cross_build_text_in_list",
			sql: `SELECT p.pid AS pid, b.s AS s FROM cjw_p p ` +
				`CROSS JOIN cjw_b b WHERE b.s IN ('alpha', 'zeta') ORDER BY 1, 2`,
			want: []string{"pid:20", "s:25"},
			rows: []string{"1,alpha", "1,zeta", "2,alpha", "2,zeta"},
		},
		{
			// The INNER-join-on-an-expression spelling of the same executor
			// path, with every build column projected.
			name: "on_expression_all_build_columns",
			sql: `SELECT b.bid AS bid, b.f AS f, b.s AS s FROM cjw_p p ` +
				`JOIN cjw_b b ON b.s IN ('alpha', 'zeta') ORDER BY 1`,
			want: []string{"bid:20", "f:16", "s:25"},
			rows: []string{"1,t,alpha", "1,t,alpha", "6,t,zeta", "6,t,zeta"},
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
						cells[i] = cjWireCell(t, v, res.FieldDescriptions[i].DataTypeOID, format.code)
					}
					rows = append(rows, strings.Join(cells, ","))
				}
				if strings.Join(rows, ";") != strings.Join(c.rows, ";") {
					t.Errorf("the cross join's build rows\n  %s\n  %s rows %v\n  want      %v",
						c.sql, format.name, rows, c.rows)
				}
			}
		})
	}
}

// cjWireCell decodes one field the way its DECLARED OID and the result's
// format code say it must be encoded.
func cjWireCell(t *testing.T, v []byte, oid uint32, format int16) string {
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
	case 16: // bool
		if len(v) != 1 {
			t.Errorf("a field declared bool (OID 16) carried %d binary bytes, not 1", len(v))
			return "?"
		}
		// PostgreSQL's TEXT encoding of a boolean is "t"/"f", and the two
		// formats are compared against one expectation here, so the binary
		// decode is rendered the way the text one arrives.
		if v[0] == 0 {
			return "f"
		}
		return "t"
	case 25: // text
		return string(v)
	}
	t.Errorf("unexpected OID %d in this gate's corpus", oid)
	return "?"
}

// setupCJCrossDB writes the build side in THREE row groups, which is the
// condition #1189 needs: a build side arrives as one batch per row group.
func setupCJCrossDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	load := func(name string, schema parquet.Schema, rows []map[string]any, rg int) {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(name, schema, nil, ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: rg})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	load("cjw_p", parquet.Schema{Columns: []parquet.Column{
		{Name: "pid", Type: parquet.TypeInt64},
	}}, []map[string]any{{"pid": int64(1)}, {"pid": int64(2)}}, 16)

	names := []string{"alpha", "beta", "gamma", "delta", "eps", "zeta", "eta", "theta", "iota"}
	build := make([]map[string]any, 0, 9)
	for i := 1; i <= 9; i++ {
		build = append(build, map[string]any{
			"bid": int64(i), "f": i%3 != 2, "s": names[i-1],
		})
	}
	load("cjw_b", parquet.Schema{Columns: []parquet.Column{
		{Name: "bid", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeBool, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}, build, 3)

	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}
