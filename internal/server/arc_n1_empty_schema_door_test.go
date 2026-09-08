package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// AN EMPTY COLUMN LIST IS NEVER AN ANSWER, ON THIS DOOR TOO — arc N1.
//
// A statement that produces a result set produces COLUMNS, whether or not it
// produces rows; a result carrying none and no error is the engine failing to
// describe its own output, and at the client it is indistinguishable from a
// query that legitimately found nothing. That is how #1008 and #1010 reached
// one.
//
// The HTTP query door RUNS ITS OWN PIPELINE — it does not call
// `wadjet.DB.Query` or `Coordinator.ExecuteSQL` — so it asks the shared
// decision (`sqlerr.EmptyResultColumns`) itself rather than inheriting it. The
// shape that reaches it is a zero-row `SELECT *` over a BUSHY join, which
// `physical.starJoinDeclaredOutputSchema` declines to declare (its own stated
// bound, #978): with no rows to read a schema off and no declaration, the
// response used to carry `"columns": null` and HTTP 200.
//
// PostgreSQL answers this query with a header and zero rows, so the refusal is
// a wadjet-side bound in ADR-0012's divergence list — and what it replaces was
// not PostgreSQL's answer either.
//
// THE CONTROLS ARE THE BOUNDARY: a zero-row star over one relation and over
// one join still declare their columns and must still answer 200.
func TestN1AnEmptyColumnListIsRefusedOnTheHTTPDoor(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	ord := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "customer", Type: parquet.TypeString},
	}}
	item := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "order_id", Type: parquet.TypeInt64},
	}}
	for _, tbl := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{"n1ord", ord, []map[string]any{{"id": int64(1), "customer": "Alice"}}},
		{"n1item", item, []map[string]any{{"id": int64(1), "order_id": int64(1)}}},
	} {
		if err := cat.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := ingest.New(cat, tbl.name, tbl.schema, nil, ingest.DefaultConfig())
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()
	client := ts.Client()

	t.Run("a zero-row star over a bushy join is refused", func(t *testing.T) {
		status, body := postSQL(t, client, ts.URL,
			"SELECT * FROM n1ord o JOIN n1item i ON i.order_id = o.id "+
				"JOIN n1item j ON j.order_id = o.id WHERE o.id > 99")
		if status == http.StatusOK {
			t.Fatalf("answered HTTP 200 with %s — a result set declares its columns or fails", body)
		}
		var resp map[string]string
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatalf("unparseable body %q: %v", body, err)
		}
		if resp["sqlstate"] != "XX000" {
			t.Errorf("SQLSTATE %q, want XX000 — nothing about the STATEMENT is wrong\n  body: %s",
				resp["sqlstate"], body)
		}
	})

	for _, ctl := range []struct {
		name, sql string
		cols      int
	}{
		{"control: a zero-row star over one relation", "SELECT * FROM n1ord WHERE id > 99", 2},
		{"control: a zero-row star over one join",
			"SELECT * FROM n1ord o JOIN n1item i ON i.order_id = o.id WHERE o.id > 99", 4},
		{"control: a zero-row named select list",
			"SELECT o.id, o.customer FROM n1ord o WHERE o.id > 99", 2},
		{"control: the same query with rows", "SELECT * FROM n1ord", 2},
	} {
		t.Run(ctl.name, func(t *testing.T) {
			status, body := postSQL(t, client, ts.URL, ctl.sql)
			if status != http.StatusOK {
				t.Fatalf("HTTP %d for a query that declares its columns: %s", status, body)
			}
			var resp struct {
				Columns []string `json:"columns"`
			}
			if err := json.Unmarshal([]byte(body), &resp); err != nil {
				t.Fatal(err)
			}
			if len(resp.Columns) != ctl.cols {
				t.Errorf("declared %d columns, want %d: %s", len(resp.Columns), ctl.cols, body)
			}
		})
	}
}
