// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// THE TimeZone STARTUP PARAMETER IS ACCEPTED AND IGNORED, AS AT THE BASE
// (#1680 line d, the architect's ruling 3): this line refuses a SET TimeZone
// naming another zone, but a client that sends its zone at connect — pgJDBC
// sends the JVM's on every connection (inferred from its startup code, not
// observed on the wire), libpq the PGTZ one, `options=-c TimeZone=…` another
// — still connects, is told TimeZone=UTC by ParameterStatus, and is answered
// at UTC. Whether that should change is the maintainer's decision; this pins
// that it did not.
func TestTimeZoneStartupParameterIsAcceptedAndIgnored(t *testing.T) {
	ctx := context.Background()
	srv := startTestServer(t, setupTestDB(t))
	for _, dsn := range []string{
		"postgres://wadjet@" + srv.Addr() + "/test?sslmode=disable&timezone=America/New_York",
		"postgres://wadjet@" + srv.Addr() + "/test?sslmode=disable&options=-c%20TimeZone%3DAsia/Kolkata",
	} {
		cfg, err := pgconn.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := pgconn.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("%s: connection refused: %v", dsn, err)
		}
		if got := conn.ParameterStatus("TimeZone"); got != "UTC" {
			t.Errorf("%s: ParameterStatus TimeZone = %q, want UTC", dsn, got)
		}
		r := conn.ExecParams(ctx, "SELECT CAST(TIMESTAMPTZ '2024-03-10 02:30:00' AS TEXT)", nil, nil, nil, nil).Read()
		if r.Err != nil || len(r.Rows) != 1 || string(r.Rows[0][0]) != "2024-03-10 02:30:00" {
			t.Errorf("%s: %v %v, want 2024-03-10 02:30:00 (UTC)", dsn, r.Rows, r.Err)
		}
		show := conn.ExecParams(ctx, "SHOW TimeZone", nil, nil, nil, nil).Read()
		if show.Err != nil || len(show.Rows) != 1 || string(show.Rows[0][0]) != "UTC" {
			t.Errorf("%s: SHOW TimeZone = %v %v, want UTC", dsn, show.Rows, show.Err)
		}
		conn.Close(ctx)
	}
}
