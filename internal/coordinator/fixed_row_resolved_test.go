package coordinator_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestResolvedRowContainersAndNullUnionArms(t *testing.T) {
	if testing.Short() {
		t.Skip("five execution arms")
	}
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
			type cell struct {
				sql      string
				want     []string
				fields   int
				embedded string
			}
			var cells []cell
			for _, parent := range []string{"coalesce(r,r)", "nullif(r,NULL)", "coalesce(CASE WHEN id=1 THEN r ELSE NULL END,r)"} {
				cells = append(cells, cell{"SELECT (" + parent + ").major AS p FROM a3b_rows WHERE id=1", []string{"1"}, 0, "1"})
			}
			for _, producer := range []struct {
				sql, wire, embedded string
				fields              int
			}{
				{"c_row", "(,11)", "map[a:<nil> b:11]", 2},
				{"semver_parse('1.2.3')", "(1,2,3,\"\",\"\")", "map[build: major:1 minor:2 patch:3 prerelease:]", 5},
			} {
				a := "SELECT NULL AS p FROM a3b_nullable_rows WHERE id=2"
				b := "SELECT " + producer.sql + " AS p FROM a3b_nullable_rows WHERE id=1"
				for _, op := range []string{"UNION", "UNION ALL"} {
					for _, arms := range [][]string{{a, b}, {b, a}, {a, b, b}} {
						want := []string{"<nil>", producer.wire}
						if len(arms) == 3 && op == "UNION ALL" {
							want = append(want, producer.wire)
						}
						cells = append(cells, cell{strings.Join(arms, " "+op+" "), want, producer.fields, producer.embedded})
					}
				}
			}
			for _, tc := range cells {
				t.Run(tc.sql, func(t *testing.T) {
					t.Run("embedded", func(t *testing.T) {
						check := coordinator.FixedRowRouteCheck(t, c, "resolved-row")
						defer check()
						var rows []map[string]any
						var fields []parquet.Column
						if c != nil {
							r, e := c.ExecuteSQL(ctx, tc.sql)
							if e != nil {
								t.Fatal(e)
							}
							if r.Error != "" {
								t.Fatal(r.Error)
							}
							rows, e = r.Rows()
							if e != nil {
								t.Fatal(e)
							}
							fields = r.OutputSchema()
						} else {
							r, e := db.Query(ctx, tc.sql)
							if e != nil {
								t.Fatal(e)
							}
							rows = r.Rows
							for _, m := range r.ColumnMetas {
								fields = append(fields, parquet.Column{Name: m.Name, Type: m.TypeID, Fields: m.Fields})
							}
						}
						wantType := parquet.TypeInt64
						if tc.fields > 0 {
							wantType = parquet.TypeRow
						}
						if len(fields) != 1 || fields[0].Type != wantType || len(fields[0].Fields) != tc.fields {
							t.Errorf("declaration %+v want fields %d", fields, tc.fields)
						}
						var got []string
						for _, r := range rows {
							got = append(got, fmt.Sprint(r["p"]))
						}
						want := append([]string(nil), tc.want...)
						for i, w := range want {
							if w != "<nil>" {
								want[i] = tc.embedded
							}
						}
						sort.Strings(got)
						sort.Strings(want)
						if fmt.Sprint(got) != fmt.Sprint(want) {
							t.Errorf("got %q want %q", got, want)
						}
					})
					for _, format := range []int16{0, 1} {
						t.Run(fmt.Sprint(format), func(t *testing.T) {
							check := coordinator.FixedRowRouteCheck(t, c, "resolved-row")
							defer check()
							r := conn.ExecParams(ctx, tc.sql, nil, nil, nil, []int16{format}).Read()
							if r.Err != nil {
								t.Fatal(r.Err)
							}
							wantOID := uint32(20)
							if tc.fields > 0 {
								wantOID = 25
							}
							if len(r.FieldDescriptions) != 1 || r.FieldDescriptions[0].DataTypeOID != wantOID {
								t.Fatalf("declaration %+v want OID %d", r.FieldDescriptions, wantOID)
							}
							var got []string
							for _, row := range r.Rows {
								v := "<nil>"
								if row[0] != nil {
									v = string(row[0])
									if format == 1 && r.FieldDescriptions[0].DataTypeOID == 20 {
										v = fmt.Sprint(int64(binary.BigEndian.Uint64(row[0])))
									}
								}
								got = append(got, v)
							}
							want := append([]string(nil), tc.want...)
							sort.Strings(got)
							sort.Strings(want)
							if fmt.Sprint(got) != fmt.Sprint(want) {
								t.Errorf("got %q want %q", got, want)
							}
						})
					}
				})
			}
		})
	}
}
