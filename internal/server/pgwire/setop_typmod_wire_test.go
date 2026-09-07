package pgwire

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A DECIMAL set operation whose arms disagree about (p,s) declares typmod -1
// on the WIRE, in every spelling — not only when the set operation IS the
// query's output (#884 round-1 B1).
//
// PostgreSQL 17.11, read through `pg_attribute` on a view (the only way to see
// a typmod; `pg_typeof` cannot):
//
//	CREATE TEMP VIEW v AS SELECT v FROM (SELECT d92 AS v FROM t
//	                                     UNION ALL SELECT d206 FROM t) x;
//	-- atttypid 1700, atttypmod -1, format_type = numeric
//
// The #884 fix gave `emittedColDecimal`'s set-op arm the reconciled (p,s),
// because the ARITHMETIC walk needs a scale to compute exactly on. That map is
// also what `declaredTypmod`'s ColRef arm reads for a bare projection over the
// set operation, so five spellings started sending numeric(20,6) — a modifier
// PostgreSQL drops. The CARRIER and the WIRE want different answers about one
// node; `emittedComputedCols`' set-operation arm is the seam that gives each
// its own.
//
// The value half is asserted beside the modifier on purpose: the whole point of
// the reconciled carrier is that the arithmetic stays exact, and a fix that
// restored the typmod by dropping the reconciliation would pass a
// modifier-only assertion.
func TestASetOperationDeclaresAnUnconstrainedNumericOnTheWire(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "h3st"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d92", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "d206", Type: parquet.TypeDecimal, Precision: 20, Scale: 6, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "h3st", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("h3st", schema, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "d92": "12.75", "d206": "12.750000"},
		{"id": int64(2), "d92": "1.00", "d206": "0.000001"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())

	const mixed = `SELECT d92 AS v FROM h3st UNION ALL SELECT d206 FROM h3st`
	const same = `SELECT d92 AS v FROM h3st UNION ALL SELECT d92 FROM h3st`

	// `want` is what THIS engine renders; `pgRow` is PostgreSQL 17.11's own
	// text for the same row, measured on the same fixture. Where they differ
	// it is only in trailing zeros — the carrier has one scale for the whole
	// column and PostgreSQL's typmod -1 prints each value at its own, which is
	// #764's recorded residual and not what this gate is about.
	for _, c := range []struct {
		name        string
		sql         string
		typmod      int32
		want, pgRow string
	}{
		// The set operation IS the output. This spelling was already right:
		// it goes through setOpWireUnconstrainedDecimal.
		{"root", mixed + ` ORDER BY v LIMIT 1`, -1, "0.000001", "0.000001"},
		// The five spellings B1 measured as numeric(20,6) after #884.
		{"derived_table", `SELECT v FROM (` + mixed + `) x ORDER BY v LIMIT 1`,
			-1, "0.000001", "0.000001"},
		{"derived_table_order_by",
			`SELECT v FROM (` + mixed + `) x ORDER BY v DESC LIMIT 1`,
			-1, "12.750000", "12.75"},
		{"cte", `WITH c AS (` + mixed + `) SELECT v FROM c ORDER BY v LIMIT 1`,
			-1, "0.000001", "0.000001"},
		{"except",
			`SELECT v FROM (SELECT d92 AS v FROM h3st EXCEPT SELECT d206 FROM h3st) x ` +
				`ORDER BY v LIMIT 1`, -1, "1.000000", "1.00"},
		{"null_arm",
			`SELECT v FROM (SELECT d92 AS v FROM h3st UNION ALL SELECT NULL) x ` +
				`ORDER BY v LIMIT 1`, -1, "1.00", "1.00"},
		// The CONTROL, and it is the half that says this is not "every set
		// operation is unconstrained": arms carrying the SAME typmod keep it
		// on the server (measured: atttypmod 589830 = numeric(9,2)).
		{"same_typmod_arms", `SELECT v FROM (` + same + `) x ORDER BY v LIMIT 1`,
			589830, "1.00", "1.00"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("ExecParams: %v\n  SQL: %s", res.Err, c.sql)
			}
			f := res.FieldDescriptions[0]
			if f.DataTypeOID != 1700 {
				t.Fatalf("declared OID %d, want 1700 (numeric)", f.DataTypeOID)
			}
			if f.TypeModifier != c.typmod {
				t.Errorf("typmod %d, want %d — PostgreSQL 17.11 declares a set operation "+
					"whose arms disagree about (p,s) as plain numeric (atttypmod -1), and "+
					"keeps the modifier only when every arm carries the same one"+
					"\n  SQL: %s", f.TypeModifier, c.typmod, c.sql)
			}
			if len(res.Rows) != 1 || string(res.Rows[0][0]) != c.want {
				t.Errorf("rendered %q, want %q (PostgreSQL 17.11: %s) — the CARRIER must "+
					"still take the reconciled scale, so a fix that restores the typmod by "+
					"dropping the reconciliation fails here\n  SQL: %s",
					res.Rows, c.want, c.pgRow, c.sql)
			}
		})
	}

	// The exactness the reconciled carrier exists for, on the same fixture and
	// through the same door: arithmetic over an aggregate across the mixed set
	// operation is a numeric, not a float8. SUM(v*2)+1 over the four values
	// (12.75, 1.00, 12.750000, 0.000001) is 54.000002 on the server, measured.
	t.Run("arithmetic_over_the_aggregate_stays_exact", func(t *testing.T) {
		sql := `SELECT SUM(v * 2) + 1 AS v FROM (` + mixed + `) x`
		res := conn.ExecParams(ctx, sql, nil, nil, nil, []int16{0}).Read()
		if res.Err != nil {
			t.Fatalf("ExecParams: %v", res.Err)
		}
		if got := res.FieldDescriptions[0].DataTypeOID; got != 1700 {
			t.Errorf("declared OID %d, want 1700 (numeric); float8 is 701", got)
		}
		if len(res.Rows) != 1 || string(res.Rows[0][0]) != "54.000002" {
			t.Errorf("= %q, want 54.000002 (PostgreSQL 17.11, measured)", fmt.Sprint(res.Rows))
		}
	})
}
