package coordinator_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
)

func TestSemverRowWire(t *testing.T) {
	if testing.Short() {
		t.Skip("five execution arms")
	}
	for _, mode := range []string{"single", "spilled", "dag", "dag-shuffled", "dag-morsel"} {
		t.Run(mode, func(t *testing.T) {
			db, c := coordinator.FixedRowWireArm(t, mode)
			srv := pgwire.NewServer(db, pgwire.Config{}, nil)
			srv.SetCoordinator(c)
			if e := srv.Start("127.0.0.1:0"); e != nil {
				t.Fatal(e)
			}
			defer srv.Shutdown()
			ctx := context.Background()
			conn, e := pgconn.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/test?sslmode=disable")
			if e != nil {
				t.Fatal(e)
			}
			defer conn.Close(ctx)
			for _, format := range []int16{0, 1} {
				t.Run(fmt.Sprint(format), func(t *testing.T) {
					q := `SELECT (semver_parse(v)).major AS a,(semver_parse(v)).minor AS b,(semver_parse(v)).patch AS c,(semver_parse(v)).prerelease AS d,(semver_parse(v)).build AS e FROM a3b_rows WHERE id=1`
					check := coordinator.FixedRowRouteCheck(t, c, "components")
					r := conn.ExecParams(ctx, q, nil, nil, nil, []int16{format}).Read()
					check()
					if r.Err != nil {
						t.Fatal(r.Err)
					}
					if len(r.FieldDescriptions) != 5 {
						t.Fatal(r.FieldDescriptions)
					}
					if len(r.Rows) != 1 {
						t.Fatalf("component rows: %q", r.Rows)
					}
					for i, want := range []string{"1", "2", "3", "", ""} {
						raw := r.Rows[0][i]
						if raw == nil {
							t.Errorf("field %d is NULL", i)
							continue
						}
						got := string(raw)
						if format == 1 && i < 3 {
							got = strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10)
						}
						if got != want {
							t.Errorf("field %d value %q want %q", i, got, want)
						}
					}
					for i, oid := range []uint32{20, 20, 20, 25, 25} {
						if r.FieldDescriptions[i].DataTypeOID != oid {
							t.Errorf("field %d OID=%d want %d", i, r.FieldDescriptions[i].DataTypeOID, oid)
						}
					}
					for _, tc := range []struct{ s, want string }{{"1.2.3", `(1,2,3,"","")`}, {"1.2.3-rc.1+b7", `(1,2,3,rc.1,b7)`}} {
						check := coordinator.FixedRowRouteCheck(t, c, "render")
						r := conn.ExecParams(ctx, `SELECT semver_parse('`+tc.s+`') AS p FROM a3b_rows WHERE id=1`, nil, nil, nil, []int16{format}).Read()
						check()
						if r.Err != nil || len(r.Rows) != 1 || string(r.Rows[0][0]) != tc.want {
							t.Errorf("render %s: %q %v", tc.s, r.Rows, r.Err)
						}
					}
					check = coordinator.FixedRowRouteCheck(t, c, "null")
					r = conn.ExecParams(ctx, `SELECT semver_parse(NULL) AS p,semver_parse_strict(NULL) AS s FROM a3b_rows WHERE id=1`, nil, nil, nil, []int16{format}).Read()
					check()
					if r.Err != nil || len(r.Rows) != 1 || r.Rows[0][0] != nil || r.Rows[0][1] != nil {
						t.Errorf("NULL %q %v", r.Rows, r.Err)
					}
					check = coordinator.FixedRowRouteCheck(t, c, "strict")
					r = conn.ExecParams(ctx, `SELECT semver_parse_strict(v) AS p FROM semverpkg WHERE v='not-a-version'`, nil, nil, nil, []int16{format}).Read()
					check()
					pe, ok := r.Err.(*pgconn.PgError)
					if !ok || pe.Code != "22023" || !strings.Contains(pe.Message, "not-a-version") {
						t.Errorf("strict refusal: %v", r.Err)
					}
				})
			}
		})
	}
}
