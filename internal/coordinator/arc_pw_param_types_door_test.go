// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
)

// ARC PW ON THE COORDINATOR DOOR (#1426 #1410): a bound parameter is spliced
// into the statement as a literal of its declared — or its position's — type
// before the statement is routed, so the stage DAG reads `d = $1` bound as a
// TIMESTAMP exactly as it reads the TIMESTAMP literal. The cells run over
// pgwire with the coordinator's router installed (LocalFastPathBytes 0: every
// SELECT plans as a DAG), against `dt_pair`, and every want is PostgreSQL
// 17.11's answer over the same pgconn calls (re-measured with WADJET_PG_DSN).
//
// At c39858f3 the timestamp cells answered the DATE reading — the non-midnight
// 2024-03-04 12:00:00 matched row 2 — the UNION cell was 42883, and the
// undeclared LIMIT was 42601.
func TestArcPWParameterTypesOnTheCoordinatorDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up the DAG arms")
	}
	ctx := context.Background()
	type param struct {
		oid uint32
		bin bool
		v   any
	}
	ts := func(s string) time.Time {
		tm, err := time.Parse("2006-01-02 15:04:05.999999", s)
		if err != nil {
			t.Fatal(err)
		}
		return tm
	}
	text := func(oid uint32, s string) param { return param{oid: oid, v: s} }
	bin := func(oid uint32, v any) param { return param{oid: oid, bin: true, v: v} }
	q := "SELECT id FROM dt_pair WHERE %s ORDER BY id"
	cells := []struct {
		name string
		sql  string
		ps   []param
		want string
	}{
		{"d = $1/1114/text/noon", fmt.Sprintf(q, "d = $1"), []param{text(1114, "2024-03-04 12:00:00")}, "params=[1114] rows=[]"},
		{"d = $1/1114/bin/noon", fmt.Sprintf(q, "d = $1"), []param{bin(1114, ts("2024-03-04 12:00:00"))}, "params=[1114] rows=[]"},
		{"d = $1/1114/text/midnight", fmt.Sprintf(q, "d = $1"), []param{text(1114, "2024-01-02 00:00:00")}, "params=[1114] rows=[1]"},
		{"d < $1/1114/text", fmt.Sprintf(q, "d < $1"), []param{text(1114, "2024-03-04 00:00:00.001")}, "params=[1114] rows=[1 ; 2]"},
		{"d IN ($1, $2)/1114/text", fmt.Sprintf(q, "d IN ($1, $2)"),
			[]param{text(1114, "2024-01-02 00:00:00"), text(1114, "2024-03-04 12:00:00")}, "params=[1114 1114] rows=[1]"},
		{"d NOT IN ($1, $2)/1114/text", fmt.Sprintf(q, "d NOT IN ($1, $2)"),
			[]param{text(1114, "2024-01-02 00:00:00"), text(1114, "2024-03-04 12:00:00")}, "params=[1114 1114] rows=[2 ; 3]"},
		{"d IN (SELECT $1)/1114/text", fmt.Sprintf(q, "d IN (SELECT $1)"), []param{text(1114, "2024-03-04 12:00:00")}, "params=[1114] rows=[]"},
		{"d = ANY (UNION)/1114/text", fmt.Sprintf(q, "d = ANY (SELECT $1 UNION ALL SELECT $2)"),
			[]param{text(1114, "2024-01-02 00:00:00"), text(1114, "2024-03-04 12:00:00")}, "params=[1114 1114] rows=[1]"},
		{"ts = $1/1184/text/offset", fmt.Sprintf(q, "ts = $1"), []param{text(1184, "2024-03-04 14:00:00+02")}, "params=[1184] rows=[2]"},
		{"ts = $1/0/text", fmt.Sprintf(q, "ts = $1"), []param{text(0, "2024-01-02 00:00:00")}, "params=[1114] rows=[1 ; 3]"},
		{"LIMIT $1/0/text", "SELECT id FROM dt_pair ORDER BY id LIMIT $1", []param{text(0, "2")}, "params=[20] rows=[1 ; 2]"},
		{"SELECT $1/20/bin", "SELECT $1 AS v FROM dt_pair WHERE id = 1", []param{bin(20, int64(7))}, "params=[20] fields=20 rows=[7]"},
		{"$1 = id + 0.5/0/text", fmt.Sprintf(q, "$1 = id + 0.5"), []param{text(0, "2.5")}, "params=[1700] rows=[2]"},
	}
	var pg *pgconn.PgConn
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		var err error
		if pg, err = pgconn.Connect(ctx, dsn); err != nil {
			t.Fatal(err)
		}
		defer pg.Close(ctx)
		for _, s := range append([]string{"SET statement_timeout = '30s'", "DROP SCHEMA IF EXISTS pwdoor CASCADE",
			"CREATE SCHEMA pwdoor", "SET search_path = pwdoor"}, strings.Split(dtPGFixture, "\n")...) {
			if _, err := pg.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
	}
	run := func(conn *pgconn.PgConn, sql string, ps []param, k int) string {
		m := pgtype.NewMap()
		oids := make([]uint32, len(ps))
		vals := make([][]byte, len(ps))
		fmts := make([]int16, len(ps))
		for i, p := range ps {
			oids[i] = p.oid
			if s, ok := p.v.(string); ok && !p.bin {
				vals[i] = []byte(s)
				continue
			}
			fmts[i] = 1
			b, err := m.Encode(p.oid, 1, p.v, nil)
			if err != nil {
				return "ENCODE " + err.Error()
			}
			vals[i] = b
		}
		name := fmt.Sprintf("pwdoor%d", k)
		sd, err := conn.Prepare(ctx, name, sql, oids)
		if err != nil {
			return pwDoorErr(err)
		}
		defer func() { conn.Exec(ctx, "DEALLOCATE "+name).ReadAll() }()
		out := fmt.Sprintf("params=%v", sd.ParamOIDs)
		if strings.HasPrefix(sql, "SELECT $1") {
			var fo []string
			for _, f := range sd.Fields {
				fo = append(fo, fmt.Sprint(f.DataTypeOID))
			}
			out += " fields=" + strings.Join(fo, ",")
		}
		rr := conn.ExecPrepared(ctx, name, vals, fmts, nil).Read()
		if rr.Err != nil {
			return pwDoorErr(rr.Err)
		}
		var rows []string
		for _, r := range rr.Rows {
			rows = append(rows, string(r[0]))
		}
		return out + " rows=[" + strings.Join(rows, " ; ") + "]"
	}
	for _, arm := range []string{"dag", "dag-shuffled"} {
		t.Run(arm, func(t *testing.T) {
			infra := tmdInfra(t, ctx)
			tmdWriteTableList(t, ctx, infra, nil, []tmdTable{dtTable()})
			c := tmdCoordinator(t, ctx, infra, func(cfg *Config) {
				if arm == "dag-shuffled" {
					cfg.BroadcastBytesOverride = 1
				}
			})
			srv := pgwire.NewServer(dtStandalone(t, ctx, 0), pgwire.Config{}, nil)
			srv.SetRouter(NewQueryRouter(c))
			if err := srv.Start("127.0.0.1:0"); err != nil {
				t.Fatal(err)
			}
			defer srv.Shutdown()
			conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/test?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			for k, c := range cells {
				t.Run(c.name, func(t *testing.T) {
					if got := run(conn, c.sql, c.ps, k); got != c.want {
						t.Errorf("the coordinator door answered\n  %s\nwant (PostgreSQL 17.11)\n  %s", got, c.want)
					}
					if pg != nil && arm == "dag" {
						if got := run(pg, c.sql, c.ps, k); got != c.want {
							t.Errorf("PostgreSQL answered\n  %s\npinned\n  %s", got, c.want)
						}
					}
				})
			}
		})
	}
}

func pwDoorErr(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}
