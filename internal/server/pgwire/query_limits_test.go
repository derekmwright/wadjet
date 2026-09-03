package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// errorFields parses an ErrorResponse into its field map, so a test can read
// the SQLSTATE ('C') and not just the message ('M').
func errorFields(data []byte) map[byte]string {
	out := map[byte]string{}
	for len(data) > 0 {
		typ := data[0]
		data = data[1:]
		if typ == 0 {
			break
		}
		val := readCString(data)
		data = data[len(val)+1:]
		out[typ] = val
	}
	return out
}

// simpleQueryError runs sql and returns the ErrorResponse fields, or nil when
// the statement succeeded.
func (c *pgClient) simpleQueryError(sql string) map[byte]string {
	c.t.Helper()
	c.writeMsg('Q', append([]byte(sql), 0))
	var fields map[byte]string
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading query response: %v", err)
		}
		switch typ {
		case 'E':
			fields = errorFields(data)
		case 'Z':
			return fields
		}
	}
}

// limitedDB is a DB whose cost guard rejects any scan: max_scan_bytes 1.
func limitedDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		QueryLimits: &config.QueryLimits{MaxScanBytes: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "name", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("events", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int32(1), "name": "a"},
		{"id": int32(2), "name": "b"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestQueryLimitsBoundTheStatementsPgwireDoesNotRoute is #803's pgwire gate.
//
// pgwire hands a statement to the coordinator only when shouldRouteThroughCoord
// says so — the first six non-space bytes are SELECT, or it starts with WITH.
// Everything else answers from the embedded wadjet.DB, which had no cost guard
// at all, so `query_limits:` bounded nothing on the PostgreSQL wire for a
// leading comment (what JDBC, DataGrip and dbt emit routinely) or a TABLE
// statement. Those are the BI clients this project explicitly supports.
//
// Every case below is asserted through the WIRE, with the SQLSTATE the guard
// documents (53400, configuration_limit_exceeded).
func TestQueryLimitsBoundTheStatementsPgwireDoesNotRoute(t *testing.T) {
	db := limitedDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	defer client.terminate()
	client.startup("wadjet", "wadjet")

	cases := []struct {
		name string
		sql  string
	}{
		{"plain-select", "SELECT * FROM events"},
		{"block-comment-prefixed", "/* a leading block comment */ SELECT * FROM events"},
		{"line-comment-prefixed", "-- a leading comment\nSELECT * FROM events"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The routing fact this case is about, asserted so a change to the
			// gate is visible here rather than silently widening the hole.
			// The routing FACT this case turns on. A comment-prefixed SELECT
			// is not routed (the gate reads the first six non-space bytes),
			// so it reaches the embedded planner; the plain one is routed and
			// is here as the control that both planners carry the guard.
			routed := shouldRouteThroughCoord(tc.sql)
			if want := tc.name == "plain-select"; routed != want {
				t.Fatalf("test premise stale: shouldRouteThroughCoord(%q) = %v, want %v", tc.sql, routed, want)
			}

			fields := client.simpleQueryError(tc.sql)
			if fields == nil {
				t.Fatalf("%s: a scan over the configured max_scan_bytes must be rejected; it succeeded", tc.sql)
			}
			if got := fields['C']; got != "53400" {
				t.Errorf("%s: SQLSTATE = %q, want 53400 (%s)", tc.sql, got, fields['M'])
			}
			if !strings.Contains(fields['M'], "exceeding limit") {
				t.Errorf("%s: message %q does not name the limit exceeded", tc.sql, fields['M'])
			}
		})
	}
}

// TestQueryLimitsBindWhenAuthIsPresentButDisabled is the other half of the
// pgwire hole: canBypassDB() is false when a provider exists but reports
// !Enabled(), so EVERY statement — plain SELECT included — takes the embedded
// path. A config carrying both `query_limits:` and `auth: {enabled: false}` is
// an ordinary deployment, and it was entirely unbounded.
//
// The routing predicate is asserted directly because reproducing the state
// end-to-end needs a coordinator (and therefore NATS); the ANSWER for that
// path is what the test above already covers, since it is the same
// wadjet.DB.Query entry.
func TestQueryLimitsBindWhenAuthIsPresentButDisabled(t *testing.T) {
	authn, authz := auth.New(auth.Config{Enabled: false})
	disabled := auth.NewProvider(authn, authz, nil, nil)
	if disabled.Enabled() {
		t.Fatal("test setup: a provider with no mechanism should report !Enabled()")
	}

	c := &pgConn{authProvider: disabled}
	if c.canBypassDB() {
		t.Fatal("test premise stale: a present-but-disabled provider now bypasses to the coordinator")
	}

	// Same entry point, so the guard that binds it is the one the wire test
	// above exercises.
	db := limitedDB(t)
	if _, err := db.Query(context.Background(), "SELECT * FROM events"); err == nil {
		t.Fatal("the embedded path must enforce the configured limit; it did not")
	}
}

// TestUnroutedNonSelectStatementsAreRefusedNotUnbounded records the OTHER two
// shapes the review named as bypasses. They are not bypasses: the parser does
// not accept them at all, so there is no scan to bound and the refusal is
// loud (42601). Recorded here so a later change that starts ACCEPTING them
// cannot quietly make them unbounded — this test fails the moment they parse.
func TestUnroutedNonSelectStatementsAreRefusedNotUnbounded(t *testing.T) {
	db := limitedDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	defer client.terminate()
	client.startup("wadjet", "wadjet")

	for _, sql := range []string{"TABLE events", "VALUES (1),(2)"} {
		fields := client.simpleQueryError(sql)
		if fields == nil {
			t.Errorf("%s: now parses — it must also be bounded by query_limits; add it to the gate above", sql)
			continue
		}
		if got := fields['C']; got != "42601" {
			t.Errorf("%s: SQLSTATE = %q, want 42601 (%s)", sql, got, fields['M'])
		}
	}
}
