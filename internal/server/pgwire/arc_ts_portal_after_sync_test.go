// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// TestArcTSAPortalDoesNotOutliveItsSync: PostgreSQL destroys the portal at
// the Sync that ends an implicit transaction, so an Execute (or a portal
// Describe) after it, with no new Bind, answers 34000 `portal "" does not
// exist` and runs nothing. This connection kept the last Bind's SQL and ran
// it again — after an ordinary Sync, and after a refused Bind (#1266 review
// B7; the cells are the review's tscx_portal probe). A named portal follows
// the same rule, and inside an explicit transaction block the portal
// survives the Sync, as it does in PostgreSQL. With WADJET_PG_DSN set the
// same cells run against PostgreSQL too.
func TestArcTSAPortalDoesNotOutliveItsSync(t *testing.T) {
	_, srv := setupRealDB(t)
	ctx := context.Background()
	servers := []struct{ name, dsn string }{{"wadjet", "postgres://wadjet@" + srv.Addr() + "/wadjet?sslmode=disable"}}
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" && !testing.Short() {
		servers = append(servers, struct{ name, dsn string }{"postgres", dsn})
	}
	for _, sv := range servers {
		t.Run(sv.name, func(t *testing.T) {
			cfg, err := pgx.ParseConfig(sv.dsn)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
				t.Fatal(err)
			}
			pc := conn.PgConn()
			f := pc.Frontend()
			roundTrip := func(msgs ...pgproto3.FrontendMessage) []string {
				t.Helper()
				for _, m := range msgs {
					f.Send(m)
				}
				f.Send(&pgproto3.Sync{})
				if err := f.Flush(); err != nil {
					t.Fatal(err)
				}
				var out []string
				for {
					m, err := f.Receive()
					if err != nil {
						t.Fatal(err)
					}
					switch v := m.(type) {
					case *pgproto3.DataRow:
						out = append(out, fmt.Sprintf("row=%s", v.Values[0]))
					case *pgproto3.ErrorResponse:
						out = append(out, "error="+v.Code)
					case *pgproto3.CommandComplete:
						out = append(out, "complete")
					case *pgproto3.ReadyForQuery:
						return out
					}
				}
			}
			bindExec := func(portal, sql string, params [][]byte, oids []uint32, fmts []int16) []string {
				return roundTrip(
					&pgproto3.Parse{Query: sql, ParameterOIDs: oids},
					&pgproto3.Bind{DestinationPortal: portal, Parameters: params, ParameterFormatCodes: fmts},
					&pgproto3.Execute{Portal: portal})
			}
			next := func() {
				t.Helper()
				r := pc.ExecParams(ctx, "SELECT 99 AS next", nil, nil, nil, nil).Read()
				if r.Err != nil || len(r.Rows) != 1 || string(r.Rows[0][0]) != "99" {
					t.Fatalf("the next statement: rows=%q err=%v", r.Rows, r.Err)
				}
			}
			want := func(what string, got []string, w ...string) {
				t.Helper()
				if strings.Join(got, " ") != strings.Join(w, " ") {
					t.Errorf("%s: got %v, want %v", what, got, w)
				}
			}

			for _, bad := range []bool{false, true} {
				want("old portal", bindExec("", "SELECT 'old portal' AS s", nil, nil, nil), "row=old portal", "complete")
				if bad {
					// PostgreSQL answers 08P01 for the short int4, this
					// engine 22023; either way one error and nothing run.
					got := bindExec("", "SELECT $1::int4 AS s", [][]byte{{0, 2}}, []uint32{23}, []int16{1})
					if len(got) != 1 || !strings.HasPrefix(got[0], "error=") {
						t.Errorf("refused Bind: got %v, want one error", got)
					}
				}
				got := roundTrip(&pgproto3.Execute{})
				want(fmt.Sprintf("Execute after Sync (after a refused Bind: %v)", bad), got, "error=34000")
				want("portal Describe after Sync", roundTrip(&pgproto3.Describe{ObjectType: 'P'}), "error=34000")
				next()
			}

			// A named portal is destroyed by the same Sync.
			want("named portal", bindExec("p1", "SELECT 'named' AS s", nil, nil, nil), "row=named", "complete")
			want("named portal after Sync", roundTrip(&pgproto3.Execute{Portal: "p1"}), "error=34000")
			next()

			// Inside a transaction block Sync ends no transaction, so the
			// portal survives it.
			if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
				t.Fatal(err)
			}
			want("Bind in a block", roundTrip(
				&pgproto3.Parse{Query: "SELECT 'kept' AS s"},
				&pgproto3.Bind{}))
			want("the portal after a Sync inside the block", roundTrip(&pgproto3.Execute{}), "row=kept", "complete")
			if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
				t.Fatal(err)
			}
			next()
		})
	}
}
