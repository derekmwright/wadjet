package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Arithmetic OVER an aggregate carries the AGGREGATE'S declared type, whatever
// its argument is (#867, ADR-0024 item 2).
//
// It used to carry it only when the argument was a BARE COLUMN. Give the
// aggregate an expression and the whole term fell to float8 — and at int8
// scale that is not a declaration defect but a VALUE one: 36280278840510000001
// is not representable in a float64, so the outer `+ 1` vanished and the answer
// came back BELOW the sum it was added to. Adding one and getting less is what
// made this a P0 rather than a wire-metadata note.
//
// The mechanism was the walk over the aggregate, never its own output:
// `SELECT SUM(c_i64 * 3000000)` alone was already exact, and the subquery
// spelling of the same arithmetic already answered correctly. Both are still
// asserted below, as the controls that say a regression is in the walk.
// physical.aggSpecOutputType declined a computed argument and returned
// FLOAT64, so `__agg_0` was declared float in emittedColDecls and
// nodeDeclaredType's BinaryOp arm took its float fall-through;
// aggComputedInputDecl now types it from the argument's own declaration
// through aggOutputFromInputDecl — the same function the runtime AggColumn and
// the DAG's AggSpec already read, so the declaration and the value come from
// one source.
//
// Every expectation is live PostgreSQL 17.11 over the same values.
func TestArithmeticOverAComputedAggregateCarriesTheAggregatesType(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table

	for _, c := range []struct {
		name, sql string
		want      any
		decl      parquet.TypeID
	}{
		// The half that was always right, and must stay right.
		{"bare_sum_int8", `SELECT SUM(c_i64) + 1 AS v FROM ` + tbl, "12093426280171", parquet.TypeDecimal},
		{"bare_sum_int4", `SELECT SUM(c_i32) + 1 AS v FROM ` + tbl, int64(36198631), parquet.TypeInt64},
		{"bare_sum_decimal", `SELECT SUM(c_dec) + 1 AS v FROM ` + tbl, "12375062.3824", parquet.TypeDecimal},
		{"bare_count", `SELECT COUNT(c_i64) + 1 AS v FROM ` + tbl, int64(4840), parquet.TypeInt64},
		{"bare_max", `SELECT MAX(c_i64) + 1 AS v FROM ` + tbl, int64(4999014998), parquet.TypeInt64},
		// The controls that localize a regression: the aggregate's own output,
		// and the same arithmetic spelled so the aggregate becomes a column.
		{"computed_sum_alone", `SELECT SUM(c_i64 * 3000000) AS v FROM ` + tbl,
			"36280278840510000000", parquet.TypeDecimal},
		{"computed_sum_through_a_subquery",
			`SELECT s + 1 AS v FROM (SELECT SUM(c_i64 * 3000000) AS s FROM ` + tbl + `) q`,
			"36280278840510000001", parquet.TypeDecimal},
		// The four shapes #867 was filed for. Each was a float64 before, and
		// the first one lost its `+ 1` entirely.
		{"computed_sum_of_a_wide_product", `SELECT SUM(c_i64 * 3000000) + 1 AS v FROM ` + tbl,
			"36280278840510000001", parquet.TypeDecimal},
		{"computed_sum_of_a_sum", `SELECT SUM(c_i64 + 0) + 1 AS v FROM ` + tbl,
			"12093426280171", parquet.TypeDecimal},
		{"computed_sum_of_a_decimal_product", `SELECT SUM(c_dec * 2) + 1 AS v FROM ` + tbl,
			"24750123.7648", parquet.TypeDecimal},
		// MAX over an int8 expression is bigint on the server, and the value
		// is MAX(c_i64) * 3000000 + 1 = 4999014997 * 3000000 + 1.
		{"computed_max_of_a_product", `SELECT MAX(c_i64 * 3000000) + 1 AS v FROM ` + tbl,
			int64(14997044991000001), parquet.TypeInt64},
		{"computed_sum_of_an_int4_product", `SELECT SUM(c_i32 * 2) + 1 AS v FROM ` + tbl,
			int64(72397261), parquet.TypeInt64},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("= %#v, want %#v (live PostgreSQL 17.11)\n  SQL: %s",
					res.Rows, c.want, c.sql)
			}
			// The DECLARATION beside the value: a right number under a float8
			// OID is what a wire client reads as a float, and it is the half a
			// value-only assertion cannot see.
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("%d column metas\n  SQL: %s", len(res.ColumnMetas), c.sql)
			}
			if got := res.ColumnMetas[0].TypeID; got != c.decl {
				t.Errorf("declares %v, want %v\n  SQL: %s", got, c.decl, c.sql)
			}
		})
	}

	// The rest of the family, so the rule is not read as being about SUM. The
	// expectation here is the SUBQUERY spelling of the same arithmetic rather
	// than a transcribed number: that spelling reaches the aggregate through a
	// real output column and was exact before this fix, so it is the control
	// that says the two ways of writing one query answer one thing — and it
	// cannot inherit a wrong value from a wrong engine, because a divergence
	// between the two spellings is itself the failure.
	for _, c := range []struct {
		name, agg string
		decl      parquet.TypeID
	}{
		{"computed_min_of_a_decimal_product", `MIN(c_dec * 2)`, parquet.TypeDecimal},
		{"computed_max_of_a_decimal_product", `MAX(c_dec * 2)`, parquet.TypeDecimal},
		{"computed_avg_of_an_int_product", `AVG(c_i32 * 2)`, parquet.TypeDecimal},
		{"computed_avg_of_a_decimal_product", `AVG(c_dec * 2)`, parquet.TypeDecimal},
		{"computed_sum_of_a_float_product", `SUM(c_f64 * 2)`, parquet.TypeFloat64},
	} {
		t.Run(c.name, func(t *testing.T) {
			direct := `SELECT ` + c.agg + ` + 1 AS v FROM ` + tbl
			nested := `SELECT a + 1 AS v FROM (SELECT ` + c.agg + ` AS a FROM ` + tbl + `) q`
			dres, err := db.Query(ctx, direct)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, direct)
			}
			nres, err := db.Query(ctx, nested)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, nested)
			}
			if len(dres.Rows) != 1 || len(nres.Rows) != 1 || dres.Rows[0]["v"] != nres.Rows[0]["v"] {
				t.Errorf("%s answers %#v and its subquery spelling answers %#v — one query, "+
					"two values", c.agg, dres.Rows, nres.Rows)
			}
			if got := dres.ColumnMetas[0].TypeID; got != c.decl {
				t.Errorf("%s + 1 declares %v, want %v", c.agg, got, c.decl)
			}
		})
	}
}
