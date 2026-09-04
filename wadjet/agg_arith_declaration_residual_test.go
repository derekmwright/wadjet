package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// Arithmetic OVER an aggregate takes the aggregate's declared output type only
// when the aggregate's ARGUMENT is a bare column. Give it an expression and the
// whole term falls to float8, whatever the aggregate and whatever its input
// type — and at int8 scale the outer operand is then silently LOST.
//
// Measured on the typemx fixture:
//
//	SELECT SUM(c_i64) + 1                          "12093426280171"       DECIMAL
//	SELECT SUM(c_i32) + 1                          36198631               INT64
//	SELECT SUM(c_dec) + 1                          "12375062.3824"        DECIMAL
//	SELECT SUM(c_i64 * 3000000) + 1                3.6280278840509997e+19 FLOAT64
//	SELECT SUM(c_i64 + 0) + 1                      1.2093426280171e+13    FLOAT64
//	SELECT SUM(c_dec * 2) + 1                      2.47501237648e+07      FLOAT64
//	SELECT MAX(c_i64 * 3000000) + 1                1.4997044991e+16       FLOAT64
//
// PostgreSQL answers `numeric` for every SUM cell there and `bigint` for the
// MAX, so the fourth line is 36280278840510000001 on the server. This engine
// can reach that answer — through the SUBQUERY spelling, which is the control
// below — so the aggregate's own OUTPUT declaration is right and only the walk
// over it is missing.
//
// The mechanism, located: `physical.aggOutputFromInputDecl` already types a
// computed aggregate argument (`SELECT SUM(c_i64 * 3000000)` alone IS DECIMAL,
// asserted below). What the projection layer does with it depends on whether
// the aggregate node gets rewritten to a ColRef against the aggregate's output
// schema — which happens for a wrapping scalar FUNCTION (`replaceAggWithColRef`
// in the `format_bytes(SUM(x))` path) and for a bare-column aggregate, and not
// for arithmetic over a computed one. `nodeDeclaredType`'s FuncCallNode arm
// then asks the SCALAR registry, which has no aggregate in it, and the
// BinaryOp arm falls to Float64.
//
// It is DEFERRED rather than fixed here because the fix is that rewrite, in the
// projection-building layer, on a path shared by the DAG's own
// `requoteAggOutputRefs` — a different layer from this arc's declared-type walk
// and one whose blast radius is every aggregate projection. Recorded in the arc
// report with the issue text.
//
// TODO(#849/#850 follow-up): delete this pin when arithmetic over a computed
// aggregate carries the aggregate's declared type. The cells FAIL when they
// start agreeing with PostgreSQL, which is what makes deleting it the proof.
func TestArithmeticOverAComputedAggregateArgumentIsFloat(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table

	// The half that is RIGHT, and must stay right: a bare aggregate argument.
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		{"bare_sum_int8", `SELECT SUM(c_i64) + 1 AS v FROM ` + tbl, "12093426280171"},
		{"bare_sum_int4", `SELECT SUM(c_i32) + 1 AS v FROM ` + tbl, int64(36198631)},
		{"bare_sum_decimal", `SELECT SUM(c_dec) + 1 AS v FROM ` + tbl, "12375062.3824"},
		{"bare_count", `SELECT COUNT(c_i64) + 1 AS v FROM ` + tbl, int64(4840)},
		{"bare_max", `SELECT MAX(c_i64) + 1 AS v FROM ` + tbl, int64(4999014998)},
		// The aggregate over a computed argument is itself EXACT — this is
		// what says the residual is in the walk over it and not in its own
		// declaration.
		{"computed_sum_alone", `SELECT SUM(c_i64 * 3000000) AS v FROM ` + tbl,
			"36280278840510000000"},
		// And the same arithmetic, spelled so the aggregate becomes a real
		// column: the right answer, from this engine, for the query the cell
		// below gets wrong.
		{"computed_sum_through_a_subquery",
			`SELECT s + 1 AS v FROM (SELECT SUM(c_i64 * 3000000) AS s FROM ` + tbl + `) q`,
			"36280278840510000001"},
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
		})
	}

	// The RESIDUAL, pinned fail-on-agree. Each cell records a float64 where
	// PostgreSQL answers exactly, and the first one records a DROPPED `+ 1`:
	// 36280278840510000001 is not representable in a float64, so the outer
	// operand vanishes and the answer comes back BELOW the sum it was added
	// to. Adding one and getting less is the shape that makes this a value
	// defect and not only a declaration one.
	for _, c := range []struct {
		name, sql string
		pin       float64
		pg        string
	}{
		{"residual_sum_of_a_wide_product", `SELECT SUM(c_i64 * 3000000) + 1 AS v FROM ` + tbl,
			3.6280278840509997e+19, "36280278840510000001"},
		{"residual_sum_of_a_sum", `SELECT SUM(c_i64 + 0) + 1 AS v FROM ` + tbl,
			1.2093426280171e+13, "12093426280171"},
		{"residual_sum_of_a_decimal_product", `SELECT SUM(c_dec * 2) + 1 AS v FROM ` + tbl,
			2.47501237648e+07, "24750123.7648"},
		{"residual_max_of_a_product", `SELECT MAX(c_i64 * 3000000) + 1 AS v FROM ` + tbl,
			1.4997044991e+16, "14997044991000000001"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.pin {
				t.Errorf("= %#v, this pin records the float64 %v and PostgreSQL 17.11 says "+
					"%s. If it has moved, arithmetic over a computed aggregate now carries "+
					"the aggregate's declared type: re-measure this family and delete the "+
					"pin\n  SQL: %s", res.Rows, c.pin, c.pg, c.sql)
			}
		})
	}
}
