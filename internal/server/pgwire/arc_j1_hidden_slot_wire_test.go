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
	// A table that already STORES a reserved name, through the CATALOG door —
	// the way a binary older than the reservation wrote one, and the only way
	// to make one now (the DDL, API and ingest doors answer 42939).
	stored := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "__key_0", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.Catalog().CreateTable(ctx, "j1stored", stored, nil); err != nil {
		t.Fatal(err)
	}
	sing := ingest.New(db.Catalog(), "j1stored", stored, nil,
		ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := sing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "__key_0": "mine-1"},
		{"id": int64(2), "__key_0": "mine-2"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

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
		// wantErrLike marks a statement this engine REFUSES rather than
		// answering, with the substring the refusal must carry. PostgreSQL
		// answers it; the refusal is the recorded divergence (ADR-0012) and
		// the alternative was publishing a column set that is not the one the
		// query asked for.
		//
		// The substring is the PLANNER's sentence, and that is the point of
		// asserting it here rather than any error at all: the same statement
		// reached the executor's generic `operator execute: column "s.*" does
		// not exist in the input schema` (42000) until a star-only SELECT list
		// built a projection for the planner's refusal to see (#979), and a
		// client cannot act on that one.
		wantErrLike string
	}{
		{"star", `SELECT * ` + aggLateral,
			[]string{"id", "customer", "total", "mx"}, "", ""},
		{"star_over_an_aliased_key", `SELECT * FROM j1ord o JOIN LATERAL (` +
			`SELECT MAX(amount) AS order_id FROM j1item WHERE order_id = o.id) s ON true`,
			[]string{"id", "customer", "total", "order_id"}, "", ""},
		{"star_over_a_non_aggregated_lateral", `SELECT * FROM j1ord o JOIN LATERAL (` +
			`SELECT amount FROM j1item WHERE order_id = o.id) li ON true`,
			[]string{"amount", "id", "customer", "total"},
			"the same four columns as (id, customer, total, amount) — PostgreSQL puts " +
				"the LATERAL's columns last, and a join here emits the probe side first", ""},
		// A QUALIFIED star names ONE relation on the wire too (arc K1, #979):
		// it published the whole join for as long as a star-only SELECT list
		// built no projection for the expansion to rewrite.
		{"outer_qualified_star", `SELECT o.* ` + aggLateral,
			[]string{"id", "customer", "total"}, "", ""},
		// The LATERAL's OWN star is still refused, which is arc J1's decision
		// and ADR-0012's record: the lateral's output is a projection the
		// expansion does not enumerate, and its scan carries the correlation
		// slot the join is about to drop. It published the whole join before,
		// which is a column set the query did not ask for; a refusal on the
		// wire is the honest form of not knowing.
		{"inner_qualified_star", `SELECT s.* ` + aggLateral, nil,
			"(mx) — the lateral's own star is refused here (ADR-0012)",
			"a `s.*` expands only from a relation whose column list is known"},
		{"derived_star", `SELECT * FROM (SELECT * ` + aggLateral + `) x`,
			[]string{"id", "customer", "total", "mx"}, "", ""},
		{"cte_star", `WITH c AS (SELECT * ` + aggLateral + `) SELECT * FROM c`,
			[]string{"id", "customer", "total", "mx"}, "", ""},
		{"ctl_a_plain_join_star_is_untouched",
			`SELECT * FROM j1ord o JOIN j1item li ON li.order_id = o.id`,
			[]string{"id", "order_id", "product", "amount", "o.id", "customer", "total"},
			"(id, customer, total, id, order_id, product, amount) — the join's own " +
				"duplicate-name qualification, pre-existing and unrelated", ""},
		// A STORED column in the reserved namespace is a USER's column and
		// reaches the wire: the drop is by identity — the slot this join
		// minted, on the side it minted it for — never by a name a table
		// could also own (arc J1 round 2). `j1ord` is loaded through the
		// CATALOG door below, because the DDL door refuses the schema.
		{"a_stored_reserved_name_is_on_the_wire",
			`SELECT * FROM j1stored o JOIN LATERAL (SELECT MAX(amount) AS mx ` +
				`FROM j1item WHERE order_id = o.id) s ON true`,
			[]string{"id", "__key_0", "mx"}, "", ""},
		{"a_stored_reserved_name_by_name",
			`SELECT o.__key_0 AS mine FROM j1stored o JOIN LATERAL (` +
				`SELECT MAX(amount) AS mx FROM j1item WHERE order_id = o.id) s ON true`,
			[]string{"mine"}, "", ""},
		{"ctl_the_stored_table_with_no_lateral", `SELECT * FROM j1stored`,
			[]string{"id", "__key_0"}, "", ""},
		// The shape that defeated the key test: the stored column IS what the
		// query correlates on, so it is a join key of its own side. Only the
		// POSITION says which column the lowering minted (round 3).
		{"a_stored_name_that_is_the_correlation_key",
			`SELECT * FROM j1stored o JOIN LATERAL (SELECT MAX(amount) AS mx ` +
				`FROM j1item WHERE product = o.__key_0) s ON true`,
			[]string{"id", "__key_0", "mx"}, "", ""},
		{"the_same_read_by_name",
			`SELECT o.__key_0 AS mine FROM j1stored o JOIN LATERAL (` +
				`SELECT MAX(amount) AS mx FROM j1item WHERE product = o.__key_0) s ON true`,
			[]string{"mine"}, "", ""},
		{"ctl_an_explicit_list_over_the_lateral",
			`SELECT o.customer AS c, s.mx AS m ` + aggLateral,
			[]string{"c", "m"}, "", ""},
		// TWO INDEPENDENT LATERALS over one table (#988). The leak is a
		// DISTRIBUTED one — `fuseStageChains` absorbed the second join into
		// the first's fragment and its `ChainedJoinSpec` carried neither the
		// pad marker nor the empty-input defaults, so `__key_1` reached the
		// client on both DAG arms. This door is single-process and was right
		// throughout; it is here because the wire is where a leaked column is
		// SEEN, and a shape whose column set moved on one path is asserted on
		// every door this package can drive.
		{"two_independent_laterals",
			`SELECT * FROM j1ord o JOIN LATERAL (SELECT MAX(amount) AS mx FROM j1item ` +
				`WHERE order_id = o.id) s ON true JOIN LATERAL (SELECT MIN(amount) AS mn ` +
				`FROM j1item WHERE order_id = o.id) s2 ON true`,
			[]string{"id", "customer", "total", "mx", "mn"}, "", ""},
		{"three_independent_laterals",
			`SELECT * FROM j1ord o JOIN LATERAL (SELECT MAX(amount) AS mx FROM j1item ` +
				`WHERE order_id = o.id) s ON true JOIN LATERAL (SELECT MIN(amount) AS mn ` +
				`FROM j1item WHERE order_id = o.id) s2 ON true JOIN LATERAL (` +
				`SELECT SUM(amount) AS sm FROM j1item WHERE order_id = o.id) s3 ON true`,
			[]string{"id", "customer", "total", "mx", "mn", "sm"}, "", ""},
		{"derived_star_over_two_laterals",
			`SELECT * FROM (SELECT * FROM j1ord o JOIN LATERAL (SELECT MAX(amount) AS mx ` +
				`FROM j1item WHERE order_id = o.id) s ON true JOIN LATERAL (` +
				`SELECT MIN(amount) AS mn FROM j1item WHERE order_id = o.id) s2 ON true) x`,
			[]string{"id", "customer", "total", "mx", "mn"}, "", ""},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if c.wantErrLike != "" {
				if res.Err == nil {
					t.Fatalf("ANSWERED where the record says it is refused\n  SQL: %s", c.sql)
				}
				if !strings.Contains(res.Err.Error(), c.wantErrLike) {
					t.Fatalf("refused by a DIFFERENT sentence: %v\n  want %q\n  SQL: %s",
						res.Err, c.wantErrLike, c.sql)
				}
				return
			}
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
			// spell is a name a client cannot use — but a name a table
			// STORES is the user's, and the cells above assert it stays.
			for _, name := range got {
				if strings.HasPrefix(c.name, "a_stored_") || strings.HasPrefix(c.name, "ctl_the_stored_") ||
					strings.HasPrefix(c.name, "the_same_read_") {
					continue
				}
				if fam := plansql.ReservedSlotFamily(name); fam != "" {
					t.Errorf("%s\n  RowDescription carries %q, which is in the reserved "+
						"slot namespace %q* — the planner's own column reached the client",
						c.sql, name, fam)
				}
			}
		})
	}
}
