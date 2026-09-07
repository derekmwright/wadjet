package pgwire

// THE HIDDEN SLOT DOES NOT REACH THE WIRE — arc J1, #956 / #767.
//
// A decorrelated LATERAL materializes its correlation key into `__key_N` so
// the join it manufactures has a column to key on. That column is the
// PLANNER's, not the query's, and `SELECT *` over the join used to publish it:
// five columns in the result and five FieldDescriptions in the
// RowDescription, where PostgreSQL 17 sends four. A client reads the wire, so
// a leak that a row-value gate cannot see is still a leak — this is the door
// that sees it (ADR-0012: the wire arm is the one a value oracle cannot
// provide).
//
// The slot is dropped by the JOIN now, below every star: the outer `*`, a
// qualified `o.*` / `s.*`, a derived table's star and a CTE's star all read
// what the join emits, so one drop covers all of them (ADR-0026 §3c).
//
// Every "PostgreSQL 17" column list below is measured live over the same rows.

import (
	"context"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupJ1LateralDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	ord := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "customer", Type: parquet.TypeString},
		{Name: "total", Type: parquet.TypeFloat64},
	}}
	item := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "order_id", Type: parquet.TypeInt64},
		{Name: "product", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}}
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
	load("j1ord", ord, []map[string]any{
		{"id": int64(1), "customer": "Alice", "total": 150.0},
		{"id": int64(2), "customer": "Bob", "total": 200.0},
		{"id": int64(3), "customer": "Carol", "total": 0.0},
	})
	load("j1item", item, []map[string]any{
		{"id": int64(1), "order_id": int64(1), "product": "Widget", "amount": 50.0},
		{"id": int64(2), "order_id": int64(1), "product": "Gadget", "amount": 100.0},
		{"id": int64(3), "order_id": int64(2), "product": "Widget", "amount": 75.0},
		{"id": int64(4), "order_id": int64(2), "product": "Doohickey", "amount": 125.0},
	})

	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func TestArcJ1AHiddenSlotIsNotInTheRowDescription(t *testing.T) {
	srv := setupJ1LateralDB(t)
	const aggLateral = `FROM j1ord o JOIN LATERAL (SELECT MAX(amount) AS mx FROM j1item ` +
		`WHERE order_id = o.id) s ON true`
	for _, c := range []struct {
		name, sql string
		// want is the RowDescription's field names, in order.
		want []string
		// pgSays is what PostgreSQL 17 sends for the same statement, when it
		// differs — a divergence this arc does NOT close is stated, never
		// left to be discovered.
		pgSays string
	}{
		{"star", `SELECT * ` + aggLateral,
			[]string{"id", "customer", "total", "mx"}, ""},
		{"star_over_an_aliased_key", `SELECT * FROM j1ord o JOIN LATERAL (` +
			`SELECT MAX(amount) AS order_id FROM j1item WHERE order_id = o.id) s ON true`,
			[]string{"id", "customer", "total", "order_id"}, ""},
		{"star_over_a_non_aggregated_lateral", `SELECT * FROM j1ord o JOIN LATERAL (` +
			`SELECT amount FROM j1item WHERE order_id = o.id) li ON true`,
			[]string{"amount", "id", "customer", "total"},
			"the same four columns as (id, customer, total, amount) — PostgreSQL puts " +
				"the LATERAL's columns last, and a join here emits the probe side first"},
		{"outer_qualified_star", `SELECT o.* ` + aggLateral,
			[]string{"id", "customer", "total", "mx"},
			"(id, customer, total) — a QUALIFIED star is not narrowed to its own " +
				"relation here, which is pre-existing and not this arc's (see REPORT)"},
		{"inner_qualified_star", `SELECT s.* ` + aggLateral,
			[]string{"id", "customer", "total", "mx"},
			"(mx) — same pre-existing qualified-star gap"},
		{"derived_star", `SELECT * FROM (SELECT * ` + aggLateral + `) x`,
			[]string{"id", "customer", "total", "mx"}, ""},
		{"cte_star", `WITH c AS (SELECT * ` + aggLateral + `) SELECT * FROM c`,
			[]string{"id", "customer", "total", "mx"}, ""},
		{"ctl_a_plain_join_star_is_untouched",
			`SELECT * FROM j1ord o JOIN j1item li ON li.order_id = o.id`,
			[]string{"id", "order_id", "product", "amount", "o.id", "customer", "total"},
			"(id, customer, total, id, order_id, product, amount) — the join's own " +
				"duplicate-name qualification, pre-existing and unrelated"},
		{"ctl_an_explicit_list_over_the_lateral",
			`SELECT o.customer AS c, s.mx AS m ` + aggLateral,
			[]string{"c", "m"}, ""},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, c.sql)
			}
			got := make([]string, len(res.FieldDescriptions))
			for i, f := range res.FieldDescriptions {
				got[i] = f.Name
			}
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("%s\n  RowDescription %v, want %v\n  PostgreSQL 17: %s",
					c.sql, got, c.want, c.pgSays)
			}
			// The property, independent of the exact list: nothing the
			// planner minted for itself is on the wire. A name a user cannot
			// spell is a name a client cannot use.
			for _, name := range got {
				if fam := plansql.ReservedSlotFamily(name); fam != "" {
					t.Errorf("%s\n  RowDescription carries %q, which is in the reserved "+
						"slot namespace %q* — the planner's own column reached the client",
						c.sql, name, fam)
				}
			}
		})
	}
}
