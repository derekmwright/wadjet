package coordinator_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/oracle/rowdecl"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
)

func TestFixedRowScalarInEveryPositionWire(t *testing.T) {
	if testing.Short() {
		t.Skip("five execution arms")
	}
	expr.RegisterFunc("a3b_fixed", func(a []any) any { return rowdecl.Value(a[0].(int64)) }, expr.RetRow(rowdecl.Fields()))
	defer expr.DefaultRegistry.Unregister("a3b_fixed")
	for _, mode := range []string{"single", "spilled", "dag", "dag-shuffled", "dag-morsel"} {
		t.Run(mode, func(t *testing.T) {
			db, c := coordinator.FixedRowWireArm(t, mode)
			srv := pgwire.NewServer(db, pgwire.Config{}, nil)
			srv.SetCoordinator(c)
			if err := srv.Start("127.0.0.1:0"); err != nil {
				t.Fatal(err)
			}
			defer srv.Shutdown()
			ctx := context.Background()
			conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/test?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			for _, producer := range []string{"r", "a3b_fixed(id)"} {
				for _, cell := range rowdecl.Cells(producer) {
					for _, format := range []int16{0, 1} {
						t.Run(fmt.Sprintf("%s/%s/%d", producer, cell.Name, format), func(t *testing.T) {
							if mode == "spilled" {
								defer coordinator.FixedRowSpill(t, cell.Name)()
							}
							checkRoutes := coordinator.FixedRowRouteCheck(t, c, cell.Name)
							r := conn.ExecParams(ctx, cell.SQL, nil, nil, nil, []int16{format}).Read()
							checkRoutes()
							if r.Err != nil {
								t.Fatal(r.Err)
							}
							if len(r.FieldDescriptions) == 0 {
								t.Fatal("missing RowDescription")
							}
							f := r.FieldDescriptions[0]
							wantOID := uint32(20)
							if cell.Row {
								wantOID = 25
							}
							if cell.Name == "derived_field_sum" {
								wantOID = 1700
							}
							if f.DataTypeOID != wantOID {
								t.Errorf("OID %d want %d", f.DataTypeOID, wantOID)
							}
							var got []string
							for _, row := range r.Rows {
								v := string(row[0])
								if row[0] == nil {
									v = "<nil>"
								} else if format == 1 && f.DataTypeOID == 20 {
									v = strconv.FormatInt(int64(binary.BigEndian.Uint64(row[0])), 10)
								} else if format == 1 && f.DataTypeOID == 1700 {
									decoded, e := (pgtype.NumericCodec{}).DecodeDatabaseSQLValue(pgtype.NewMap(), 1700, format, row[0])
									if e != nil {
										t.Fatal(e)
									}
									v = fmt.Sprint(decoded)
								}
								got = append(got, v)
							}
							want := append([]string(nil), cell.Want...)
							if cell.Row {
								for i, w := range want {
									for id := int64(1); id <= 3; id++ {
										if w == fmt.Sprint(rowdecl.Value(id)) {
											want[i] = fmt.Sprintf("(%d,2,3,\"\",\"\")", id)
										}
									}
								}
							}
							if !strings.Contains(cell.SQL, "ORDER BY") {
								sort.Strings(want)
								sort.Strings(got)
							}
							if strings.Join(got, "\n") != strings.Join(want, "\n") {
								t.Errorf("got %q want %q", got, want)
							}
						})
					}
				}
			}
		})
	}
}
