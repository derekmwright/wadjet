package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Arithmetic OVER an aggregate carries the AGGREGATE'S declared type when the
// aggregate's argument is computed over columns the SCAN below it carries
// (#867, ADR-0024 item 2).
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
	// FLOAT is deliberately absent from this list and asserted on its
	// DECLARATION alone below: float addition is not associative, so two
	// spellings that aggregate in a different order may differ in the last
	// ulp — ADR-0013's legal nondeterminism, not a divergence. The EXACT
	// types are where "one query, two values" is a defect, and they are what
	// this asserts.
	for _, c := range []struct {
		name, agg string
		decl      parquet.TypeID
	}{
		{"computed_min_of_a_decimal_product", `MIN(c_dec * 2)`, parquet.TypeDecimal},
		{"computed_max_of_a_decimal_product", `MAX(c_dec * 2)`, parquet.TypeDecimal},
		{"computed_avg_of_an_int_product", `AVG(c_i32 * 2)`, parquet.TypeDecimal},
		{"computed_avg_of_a_decimal_product", `AVG(c_dec * 2)`, parquet.TypeDecimal},
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

	// The DERIVED-TABLE boundary, which was #867's own headline shape and is
	// where round 2 of this arc stopped (its pins are deleted here, which is
	// this fix's proof).
	//
	// `aggComputedInputDecl` typed the aggregate's argument through
	// `inputColDecls(node.Children[0])`, a walk that has no Project arm at
	// all: it stops dead at ANY projection list — renamed or not — and
	// answers nil for the whole subtree. `nodeDeclaredType` over that empty
	// map does not report Undecided; its arithmetic arm falls through to
	// `Decl(FLOAT64), Decided`, so the function actively DECLARED float8 and
	// the outer `+ 1` was lost at int8 magnitude.
	//
	// The walk that types a derived table's output from its own projection
	// list already exists — `emittedColTypes`, whose NodeProject arm types
	// each item against the child's emitted types and whose default arm is
	// `inputColTypes` itself, so it is a strict superset. Asking it when the
	// scan walk answered nothing is the whole fix, and it makes all five
	// spellings answer PostgreSQL's exact value under numeric.
	for _, c := range []struct{ name, sql string }{
		{"derived_rename",
			`SELECT SUM(v * 3000000) + 1 AS v FROM (SELECT c_i64 AS v FROM ` + tbl + `) x`},
		{"derived_projection",
			`SELECT SUM(c_i64 * 3000000) + 1 AS v FROM (SELECT c_i64 FROM ` + tbl + `) x`},
		{"derived_two_columns",
			`SELECT SUM(c_i64 * 3000000) + 1 AS v FROM (SELECT c_i64, id FROM ` + tbl + `) x`},
		{"cte_rename",
			`WITH c AS (SELECT c_i64 AS v FROM ` + tbl + `) SELECT SUM(v * 3000000) + 1 AS v FROM c`},
		{"cte_no_rename",
			`WITH c AS (SELECT c_i64 FROM ` + tbl + `) SELECT SUM(c_i64 * 3000000) + 1 AS v FROM c`},
		{"select_star",
			`SELECT SUM(c_i64 * 3000000) + 1 AS v FROM (SELECT * FROM ` + tbl + `) x`},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if got := res.Rows[0]["v"]; got != "36280278840510000001" {
				t.Errorf("= %#v, PostgreSQL 17.11 says 36280278840510000001 — the "+
					"aggregate's declaration is not crossing the derived boundary"+
					"\n  SQL: %s", got, c.sql)
			}
			if d := res.ColumnMetas[0].TypeID; d != parquet.TypeDecimal {
				t.Errorf("declares %v, want DECIMAL (PostgreSQL: numeric)\n  SQL: %s", d, c.sql)
			}
		})
	}

	// The SET-OPERATION face of the same walk, which round 3 left open one
	// node-kind over: `emittedColTypes` and `emittedColDecimal` had no
	// NodeUnion/Intersect/Except arm either, so `SUM(v * 2) + 1` over a
	// `UNION ALL` found no declaration for `v`, fell to the float rule, and
	// went out as OID 701 where PostgreSQL sends 1700 (round-3 review P-B).
	//
	// Both arms answer from `setOpDeclaredOutputSchema`, the SAME function
	// that computes the plan-declared output schema for a query whose output
	// IS a set operation: it skips an arm whose column is an UNKNOWN-typed
	// literal, folds the rest through setOpWiden's ladder, and resolves
	// DECIMAL (p,s) through batch.DecimalCommon (#884).
	//
	// They used to compare the arms for AGREEMENT and leave a disagreeing
	// column untyped — and "left out" is not neutral: nodeDeclaredType over a
	// map without the name falls through to Decl(FLOAT64), Decided. So the
	// four shapes at the end of this table were float8/OID 701 with the outer
	// `+ 1` lost at int8 magnitude, and `UNION ALL SELECT NULL` is the
	// commonest set-op spelling there is.
	//
	// The values are arithmetic over the single-table answers above — two
	// copies of the same table through UNION ALL — so no number here is
	// transcribed from a run.
	for _, c := range []struct{ name, sql, want string }{
		{"setop_int_product",
			`SELECT SUM(v * 3000000) + 1 AS v FROM (SELECT c_i64 AS v FROM ` + tbl +
				` UNION ALL SELECT c_i64 FROM ` + tbl + `) x`, "72560557681020000001"},
		{"setop_split_arms",
			`SELECT SUM(v * 3000000) + 1 AS v FROM (SELECT c_i64 AS v FROM ` + tbl +
				` WHERE id < 10 UNION ALL SELECT c_i64 FROM ` + tbl +
				` WHERE id >= 10) x`, "36280278840510000001"},
		{"setop_decimal_product",
			`SELECT SUM(v * 2) + 1 AS v FROM (SELECT c_dec AS v FROM ` + tbl +
				` UNION ALL SELECT c_dec FROM ` + tbl + `) x`, "49500246.5296"},
		{"setop_bare_argument",
			`SELECT SUM(v) + 1 AS v FROM (SELECT c_i64 AS v FROM ` + tbl +
				` UNION ALL SELECT c_i64 FROM ` + tbl + `) x`, "24186852560341"},
		// #884's three, moved up from the fail-on-agree pin that stood here.
		// An UNKNOWN-typed NULL arm contributes no type — PostgreSQL resolves
		// `c_i64 UNION ALL SELECT NULL` to bigint and `c_i32 UNION ALL SELECT
		// NULL` to integer, measured live on 17.11 through pg_attribute — so
		// the answer is the single-table one; the mixed integer widths widen
		// to bigint, so the answer is the sum of the two single-table sums.
		{"setop_null_arm",
			`SELECT SUM(v * 3000000) + 1 AS v FROM (SELECT c_i64 AS v FROM ` + tbl +
				` UNION ALL SELECT NULL) x`, "36280278840510000001"},
		{"setop_int32_and_int64_arms",
			`SELECT SUM(v * 3000000) + 1 AS v FROM (SELECT c_i32 AS v FROM ` + tbl +
				` UNION ALL SELECT c_i64 FROM ` + tbl + `) x`, "36280387436400000001"},
		{"setop_decimal_and_null_arms",
			`SELECT SUM(v * 2) + 1 AS v FROM (SELECT c_dec AS v FROM ` + tbl +
				` UNION ALL SELECT NULL) x`, "24750123.7648"},
		// The BARE-argument spelling of the same NULL arm, which the filing
		// does not name and which was float8 too: what was missing is the
		// declaration, not the arithmetic inside the aggregate.
		{"setop_bare_argument_null_arm",
			`SELECT SUM(v) + 1 AS v FROM (SELECT c_i64 AS v FROM ` + tbl +
				` UNION ALL SELECT NULL) x`, "12093426280171"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if got := res.Rows[0]["v"]; got != c.want {
				t.Errorf("= %#v, want %q — the aggregate's declaration is not crossing "+
					"the set operation\n  SQL: %s", got, c.want, c.sql)
			}
			if d := res.ColumnMetas[0].TypeID; d != parquet.TypeDecimal {
				t.Errorf("declares %v, want DECIMAL (PostgreSQL: numeric)\n  SQL: %s", d, c.sql)
			}
		})
	}

	// The BOUNDARY of that arm. `emittedColTypes` answers from
	// setOpDeclaredOutputSchema now, and that function declines — returns
	// ok=false — for a set operation whose arms it cannot type at all, so
	// the map is empty and nodeDeclaredType takes its float fall-through
	// exactly as before. What must NOT happen is a type named for a pair
	// the ladder does not reconcile: `c_i64 UNION ALL SELECT c_str` has no
	// common type on PostgreSQL (42804) and is refused at plan time here
	// (ADR-0012 item 12), and this cell says the reconciliation did not
	// quietly start answering it instead.
	if _, err := db.Query(ctx, `SELECT SUM(v) + 1 AS v FROM (SELECT c_i64 AS v FROM `+tbl+
		` UNION ALL SELECT c_str FROM `+tbl+`) x`); err == nil {
		t.Errorf("a set operation over bigint and text answered; PostgreSQL raises 42804 " +
			"and this engine refuses it at plan time")
	}

	// The float row's DECLARATION, which is the half that is a claim: a float
	// aggregate stays float8 and must not be dragged into the exact family by
	// the computed-argument rule.
	fres, err := db.Query(ctx, `SELECT SUM(c_f64 * 2) + 1 AS v FROM `+tbl)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if got := fres.ColumnMetas[0].TypeID; got != parquet.TypeFloat64 {
		t.Errorf("SUM(c_f64 * 2) + 1 declares %v, want FLOAT64", got)
	}
}
