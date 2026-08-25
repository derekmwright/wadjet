package wadjet

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #452: a numeric literal reached the comparison as a float64, so a DECIMAL
// literal with more significant digits than a double carries was silently
// replaced by the nearest double before it ever met the column.
//
// The fixture is deliberately WIDER than float64 on two of its three columns:
// d_4's unscaled values run to 18 digits and d_wide's to 25, so a literal
// naming one of those values exactly cannot survive a float64 round trip.
// Every expectation below is computed from the fixture with exact rational
// arithmetic — PostgreSQL's rule (ADR-0012), which compares numeric against a
// numeric literal at full precision.

const declitRows = 200

// declitCol is one DECIMAL column of the fixture: its scale and the unscaled
// step between consecutive rows, as a decimal string so no float is involved
// in building the data either.
type declitCol struct {
	name  string
	prec  int
	scale int
	step  string
	// nullStride: row i is NULL when i%nullStride == 0.
	nullStride int
}

var declitCols = []declitCol{
	// 9 digits: every value here IS a float64, so this column is the control
	// — the fix must not change what it already answers.
	{name: "d_2", prec: 9, scale: 2, step: "4321987", nullStride: 17},
	// 18 digits: past float64's 15-16 significant decimal digits.
	{name: "d_4", prec: 18, scale: 4, step: "9876543210987654", nullStride: 23},
	// 25 digits, and past int64 entirely — the arm the oracle found (#452).
	// A wide DECIMAL carries no footer statistics, so nothing is pruned here
	// and the answer is the row-level comparison's alone.
	{name: "d_wide", prec: 38, scale: 10, step: "98765432109876543210987", nullStride: 13},
}

func declitSchema() parquet.Schema {
	cols := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	for _, c := range declitCols {
		cols = append(cols, parquet.Column{
			Name: c.name, Type: parquet.TypeDecimal,
			Precision: c.prec, Scale: c.scale, Nullable: true,
		})
	}
	return parquet.Schema{Columns: cols}
}

// declitUnscaled is row i's unscaled value: monotonic in i and spanning both
// signs, so each row group's bounds are narrow and a literal inside one sits
// outside every other's — the shape a wrong row-group prune is visible
// through.
func (c declitCol) declitUnscaled(i int) *big.Int {
	step, ok := new(big.Int).SetString(c.step, 10)
	if !ok {
		panic("step is not an integer: " + c.step)
	}
	return step.Mul(step, big.NewInt(int64(i-100)))
}

// value returns row i's value as an exact rational, and whether it is NULL.
func (c declitCol) value(i int) (*big.Rat, bool) {
	if i%c.nullStride == 0 {
		return nil, true
	}
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(c.scale)), nil)
	return new(big.Rat).SetFrac(c.declitUnscaled(i), den), false
}

// text renders row i's value as plain decimal text with exactly scale digits
// after the point — the literal a user would type to name it.
func (c declitCol) text(i int) string {
	return declitText(c.declitUnscaled(i), c.scale)
}

func declitText(unscaled *big.Int, scale int) string {
	neg := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()
	for len(digits) <= scale {
		digits = "0" + digits
	}
	out := digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	if neg {
		out = "-" + out
	}
	return out
}

// decimal128FromBigInt renders a signed big.Int as the two 64-bit halves
// parquet.Decimal128 carries, two's complement. The box is the writer's
// verbatim unscaled input (ADR-0018 §4); a float64 box could not hold 25
// significant digits and a numeric string box goes through ParseFloat.
func decimal128FromBigInt(n *big.Int) parquet.Decimal128 {
	m := new(big.Int).Set(n)
	if m.Sign() < 0 {
		m.Add(m, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	var b [16]byte
	m.FillBytes(b[:])
	return parquet.Decimal128{
		Hi: int64(binary.BigEndian.Uint64(b[0:8])),
		Lo: binary.BigEndian.Uint64(b[8:16]),
	}
}

func declitOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := declitSchema()
	if err := db.CreateTable(ctx, "declit", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, declitRows)
	for i := range rows {
		r := map[string]any{"k": int64(i)}
		for _, c := range declitCols {
			if _, null := c.value(i); null {
				continue
			}
			r[c.name] = decimal128FromBigInt(c.declitUnscaled(i))
		}
		rows[i] = r
	}
	ing := db.NewIngester("declit", schema, nil, ingest.Config{
		MaxBufferRows: declitRows + 1, RowGroupSize: 40,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// declitExpect counts the rows of column c that satisfy `value <op> literal`,
// with the literal read as an exact rational. NULL satisfies nothing.
func (c declitCol) declitExpect(t *testing.T, op string, literals ...string) int64 {
	t.Helper()
	lits := make([]*big.Rat, len(literals))
	for i, s := range literals {
		r, ok := new(big.Rat).SetString(s)
		if !ok {
			t.Fatalf("literal %q is not a number", s)
		}
		lits[i] = r
	}
	var n int64
	for i := 0; i < declitRows; i++ {
		v, null := c.value(i)
		if null {
			continue
		}
		keep := false
		switch op {
		case "=":
			keep = v.Cmp(lits[0]) == 0
		case "<>":
			keep = v.Cmp(lits[0]) != 0
		case "<":
			keep = v.Cmp(lits[0]) < 0
		case "<=":
			keep = v.Cmp(lits[0]) <= 0
		case ">":
			keep = v.Cmp(lits[0]) > 0
		case ">=":
			keep = v.Cmp(lits[0]) >= 0
		case "in":
			for _, l := range lits {
				if v.Cmp(l) == 0 {
					keep = true
					break
				}
			}
		case "between":
			keep = v.Cmp(lits[0]) >= 0 && v.Cmp(lits[1]) <= 0
		default:
			t.Fatalf("unknown op %q", op)
		}
		if keep {
			n++
		}
	}
	return n
}

// TestDecimalLiteralIsExactAtTheColumnsScale sweeps the operators against
// literals with fewer, equal and more fractional digits than the column holds,
// at three scales, on the vectorized kernel path and the row-at-a-time
// expression path, with pruning on and off.
func TestDecimalLiteralIsExactAtTheColumnsScale(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, c := range declitCols {
		t.Run(c.name, func(t *testing.T) {
			// Two target rows inside different row groups, one of each sign,
			// neither of them NULL.
			pos, neg := declitPick(c, 131), declitPick(c, 69)
			target, negTarget := c.text(pos), c.text(neg)
			hi := c.text(declitPick(c, 159))
			// One unit of the last place BELOW the target: the literal a
			// float64 cannot tell apart from the target itself.
			ulpDown := declitText(new(big.Int).Sub(c.declitUnscaled(pos), big.NewInt(1)), c.scale)
			// A literal with one more fractional digit than the column can
			// hold, sitting strictly between the target and the next value
			// up. Nothing equals it; it orders strictly above the target.
			finer := target + "5"

			for _, tc := range []struct {
				name string
				sql  string
				want int64
			}{
				{"eq", "= " + target, c.declitExpect(t, "=", target)},
				{"eq_negative", "= " + negTarget, c.declitExpect(t, "=", negTarget)},
				{"eq_zero", "= 0", c.declitExpect(t, "=", "0")},
				{"eq_ulp_down", "= " + ulpDown, c.declitExpect(t, "=", ulpDown)},
				{"eq_trailing_zeros", "= " + target + "000", c.declitExpect(t, "=", target+"000")},
				{"eq_finer_than_scale", "= " + finer, c.declitExpect(t, "=", finer)},
				{"ne", "<> " + target, c.declitExpect(t, "<>", target)},
				{"lt", "< " + target, c.declitExpect(t, "<", target)},
				{"le", "<= " + target, c.declitExpect(t, "<=", target)},
				{"gt", "> " + target, c.declitExpect(t, ">", target)},
				{"ge", ">= " + target, c.declitExpect(t, ">=", target)},
				{"gt_finer_than_scale", "> " + finer, c.declitExpect(t, ">", finer)},
				{"ge_finer_than_scale", ">= " + finer, c.declitExpect(t, ">=", finer)},
				{"lt_finer_than_scale", "< " + finer, c.declitExpect(t, "<", finer)},
				{"le_finer_than_scale", "<= " + finer, c.declitExpect(t, "<=", finer)},
				{"gt_zero", "> 0", c.declitExpect(t, ">", "0")},
				{"in", "IN (" + target + ", " + negTarget + ", 0)",
					c.declitExpect(t, "in", target, negTarget, "0")},
				{"in_finer_than_scale", "IN (" + finer + ", " + negTarget + ")",
					c.declitExpect(t, "in", finer, negTarget)},
				{"between", "BETWEEN " + target + " AND " + hi,
					c.declitExpect(t, "between", target, hi)},
				{"between_finer_than_scale", "BETWEEN " + finer + " AND " + hi,
					c.declitExpect(t, "between", finer, hi)},
				{"not_between", "NOT BETWEEN " + target + " AND " + hi,
					c.declitExpect(t, "<", target) + c.declitExpect(t, ">", hi)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					pred := c.name + " " + tc.sql
					declitCheck(t, ctx, db,
						"SELECT COUNT(*) AS n FROM declit WHERE "+pred, tc.want)
					// The row-at-a-time expression path: a CASE around the
					// same comparison is not vectorizable, so the predicate
					// is evaluated row by row instead of by a kernel. The two
					// must answer alike or the query's meaning depends on
					// which one the planner picked.
					if canCase(tc.sql) {
						declitCheck(t, ctx, db,
							"SELECT COUNT(*) AS n FROM declit WHERE CASE WHEN "+
								pred+" THEN 1 ELSE 0 END = 1", tc.want)
					}
				})
			}
		})
	}
}

// canCase reports whether the predicate can be wrapped in a CASE — NOT
// BETWEEN and IN both can, so this is every case; kept as the one place to
// exclude a shape the row path cannot express if one appears.
func canCase(string) bool { return true }

// declitPick returns i itself, or the next row that is not NULL in c.
func declitPick(c declitCol, i int) int {
	for {
		if _, null := c.value(i); !null {
			return i
		}
		i++
	}
}

func declitCheck(t *testing.T, ctx context.Context, db *DB, sql string, want int64) {
	t.Helper()
	for _, prune := range []bool{true, false} {
		prevStats := scan.StatsPrune.Set(prune)
		prevDict := scan.DictPrune.Set(prune)
		res, err := tmRun(ctx, db, sql)
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
		if err != nil {
			t.Fatalf("prune=%v: %s: %v", prune, sql, err)
		}
		got, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
		if !ok {
			t.Fatalf("prune=%v: %s: COUNT(*) came back as %#v", prune, sql,
				res.Rows[0][res.Columns[0]])
		}
		if got != want {
			t.Errorf("prune=%v: %s\n  got %d rows, want %d", prune, sql, got, want)
		}
	}
}

// TestDecimalLiteralInEveryClause takes the one literal a float64 rounds away
// from and puts it in the clauses that reach a different lowering: a join ON
// conjunct, a HAVING, and a projected CASE.
func TestDecimalLiteralInEveryClause(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)
	c := declitCols[2] // d_wide
	target := c.text(declitPick(c, 131))

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		{"join_on_literal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit a JOIN declit b ON a.k = b.k AND a.%s = %s",
			c.name, target), 1},
		// HAVING over the GROUPED column, not over an aggregate of it: MIN
		// and MAX of a DECIMAL currently answer as a float64 (the aggregate's
		// output type, not the literal's — the PostgreSQL oracle compares
		// those two entries with a float tolerance for the same reason), so
		// an aggregate would test that limitation rather than this fix.
		{"having_grouped_column", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT %s FROM declit GROUP BY %s HAVING %s = %s) x",
			c.name, c.name, c.name, target), 1},
		{"having_grouped_column_gt", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT %s FROM declit GROUP BY %s HAVING %s > %s) x",
			c.name, c.name, c.name, target), 63},
		{"projected_case", fmt.Sprintf(
			"SELECT SUM(CASE WHEN %s = %s THEN 1 ELSE 0 END) AS n FROM declit",
			c.name, target), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tmRun(ctx, db, tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			got, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
			if !ok {
				t.Fatalf("%s: answer came back as %#v", tc.sql, res.Rows[0][res.Columns[0]])
			}
			if got != tc.want {
				t.Errorf("%s\n  got %d, want %d", tc.sql, got, tc.want)
			}
		})
	}
}

// TestDecimalLiteralPastTheCarrierSaturates is #462 end to end. A literal too
// wide for Int128 at the column's scale used to WRAP two's complement and
// reappear inside the ordinary range, so `WHERE d_2 < 1e39` — true of every
// row the column can hold — returned none of them.
//
// Every expectation is PostgreSQL's, whose numeric is unbounded and compares
// these exactly (verified against postgres:17-alpine on the same fixture).
func TestDecimalLiteralPastTheCarrierSaturates(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	// Written out in full as well as in exponent form: the plain spelling is
	// what a wrapped narrowing hit first, and the two must agree.
	const e39 = "1000000000000000000000000000000000000000"
	for _, tc := range []struct {
		pred string
		want int64
	}{
		{"d_2 < 1e39", 188},
		{"d_2 < " + e39, 188},
		{"d_2 <= " + e39, 188},
		{"d_2 > -1e39", 188},
		{"d_2 >= -" + e39, 188},
		{"d_2 <> " + e39, 188},
		{"d_2 > 1e39", 0},
		{"d_2 >= " + e39, 0},
		{"d_2 = " + e39, 0},
		{"d_2 < -" + e39, 0},
		{"d_2 IN (" + e39 + ", -" + e39 + ")", 0},
		{"d_2 BETWEEN -" + e39 + " AND " + e39, 188},
		// The same literal against the WIDE column: 10^29 is inside Int128 as
		// an integer and outside it once the column's scale of 10 is applied.
		{"d_wide < 100000000000000000000000000000", 184},
		{"d_wide > 100000000000000000000000000000", 0},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE "+tc.pred, tc.want)
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE CASE WHEN "+tc.pred+
					" THEN 1 ELSE 0 END = 1", tc.want)
		})
	}
}

// TestDecimalLiteralExponentFormIsExact is #463. An exponent-form literal used
// to be expanded through strconv.ParseFloat before a single digit was scaled,
// and a magnitude past float64's range made ParseFloat report ErrRange: the
// expansion gave up, the untouched "1e400" reached a parser with no exponent
// handling, and THAT returned the value ZERO. `WHERE d_2 = 1e400` matched the
// row holding 0.00.
//
// PostgreSQL's numeric is unbounded and reads all of these as the numbers they
// name; every count below is its answer on the same fixture.
func TestDecimalLiteralExponentFormIsExact(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		// Past float64's range in both directions: not zero, and ordered.
		{"d_2 = 1e400", 0},
		{"d_2 <> 1e400", 188},
		{"d_2 < 1e400", 188},
		{"d_2 > 1e400", 0},
		{"d_2 > -1e400", 188},
		{"d_2 BETWEEN -1e400 AND 1e400", 188},
		{"d_2 IN (1e400, 2160993.50)", 1},
		// Underflow is the same defect mirrored: 1e-400 is a positive number
		// smaller than the column's last place, so it equals nothing and
		// splits the rows exactly at zero.
		{"d_2 = 1e-400", 0},
		{"d_2 < 1e-400", 95},
		{"d_2 > 1e-400", 93},
		{"d_2 >= -1e-400", 94},
		// Inside float64's range but past its 15-16 significant digits: the
		// exponent is folded into the scaling, so the literal keeps all 25.
		{"d_wide = 4.938271605493827160549350e14", 1},
		{"d_wide > 4.938271605493827160549350e14", 45},
		{"d_wide = 4938271605493827160549350e-10", 1},
		{"d_wide = 49382716054938271605493.50E-8", 1},
		// A quoted numeric constant is a numeric comparison against a DECIMAL
		// column, as it is in PostgreSQL — not a string one.
		{"d_2 = '2160993.50'", 1},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE "+tc.pred, tc.want)
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE CASE WHEN "+tc.pred+
					" THEN 1 ELSE 0 END = 1", tc.want)
		})
	}
}

// TestDecimalNonNumericLiteralIsRefused is the other half of #463. A constant
// that is not a number used to resolve to the value ZERO and match every row
// holding zero — no error, and rows the user never asked for. PostgreSQL
// refuses the statement ("invalid input syntax for type numeric"), and
// ADR-0012 makes PostgreSQL the authority on error-versus-not.
//
// Both paths are asserted: the vectorized kernel raises it from the filter and
// the row-at-a-time evaluator from the bound comparison, so the two cannot
// disagree about whether the query is legal.
func TestDecimalNonNumericLiteralIsRefused(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, pred := range []string{
		"d_2 = 'abc'",
		"d_2 <> 'abc'",
		"d_2 > 'abc'",
		"d_2 IN ('abc', 1.0)",
		"d_2 BETWEEN 'abc' AND 'def'",
		"'abc' = d_2",
		"d_wide < 'not a number'",
	} {
		t.Run(pred, func(t *testing.T) {
			for _, sql := range []string{
				"SELECT COUNT(*) AS n FROM declit WHERE " + pred,
				"SELECT COUNT(*) AS n FROM declit WHERE CASE WHEN " + pred + " THEN 1 ELSE 0 END = 1",
			} {
				_, err := tmRun(ctx, db, sql)
				if err == nil {
					t.Fatalf("%s answered instead of refusing a non-numeric constant", sql)
				}
				if !strings.Contains(err.Error(), "invalid input syntax for type numeric") {
					t.Errorf("%s\n  error = %v, want PostgreSQL's numeric input-syntax error", sql, err)
				}
			}
		})
	}
}

// TestDecimalNonNumericLiteralAtBoxedSitesIsRefused is #505: the #463
// refusal reached the direct comparison shapes above but not the three sites
// #465 taught to compare a DECIMAL column against a literal's exact text —
// CASE's simple form, IS DISTINCT FROM, and GREATEST/LEAST — nor a unary
// minus over a string literal, which read an unparseable operand as the
// float64 zero and matched the row holding 0.00, #463's exact failure mode
// on a path #463 never touched.
func TestDecimalNonNumericLiteralAtBoxedSitesIsRefused(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, pred := range []string{
		"CASE d_2 WHEN 'abc' THEN 1 ELSE 0 END = 1",
		"d_2 IS DISTINCT FROM 'abc'",
		"GREATEST(d_2, 'abc') = 'abc'",
		"LEAST(d_2, 'abc') = 'abc'",
		"d_2 = -'abc'",
		"d_2 = -'1e400x'", // not a number even in exponent form
	} {
		t.Run(pred, func(t *testing.T) {
			sql := "SELECT COUNT(*) AS n FROM declit WHERE " + pred
			_, err := tmRun(ctx, db, sql)
			if err == nil {
				t.Fatalf("%s answered instead of refusing a non-numeric constant", sql)
			}
			if !strings.Contains(err.Error(), "invalid input syntax for type numeric") {
				t.Errorf("%s\n  error = %v, want PostgreSQL's numeric input-syntax error", sql, err)
			}
		})
	}

	// The refusal must not depend on which row happens to reach it: k=100
	// holds exactly 0.00, the value an unparseable constant used to read as.
	t.Run("does_not_depend_on_the_zero_row_surviving", func(t *testing.T) {
		sql := "SELECT COUNT(*) AS n FROM declit WHERE k <> 100 AND d_2 IS DISTINCT FROM 'abc'"
		_, err := tmRun(ctx, db, sql)
		if err == nil || !strings.Contains(err.Error(), "invalid input syntax for type numeric") {
			t.Errorf("error = %v, want PostgreSQL's numeric input-syntax error", err)
		}
	})
}

// TestDecimalUnaryMinusOverQuotedStringLiteral is #505's other half: a
// QUOTED string literal under unary minus must fold the same way an
// unquoted numeric literal already does when its content is a number
// (`d = -'43219.87'` works exactly like `d = -43219.87`), and must saturate
// rather than match the wrong row when the number is past the DECIMAL
// carrier's range (ADR-0012 item 6) — `d = -'1e400'` used to match the row
// holding 0.00; it must now match nothing, the same as unquoted `-1e400`.
func TestDecimalUnaryMinusOverQuotedStringLiteral(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	// Row 99 holds d_2 = -43219.87 (declitCol.declitUnscaled(99) = step*-1).
	const negRowKey = 99
	const negRowText = "43219.87" // the POSITIVE text; the query negates it
	declitCheck(t, ctx, db,
		fmt.Sprintf("SELECT COUNT(*) AS n FROM declit WHERE k = %d AND d_2 = -'%s'", negRowKey, negRowText),
		1)
	declitCheck(t, ctx, db,
		"SELECT COUNT(*) AS n FROM declit WHERE d_2 = -'1e400'", 0)
	declitCheck(t, ctx, db,
		"SELECT COUNT(*) AS n FROM declit WHERE d_2 <> -'1e400'", 188)
}

// TestRefusedLiteralReachesTheClientAsItsOwnError is the #505 review finding:
// the compiler DID refuse `-'abc'` before any row existed, and the physical
// planner then swallowed the refusal.
//
// Six sites there compile an AST and quietly keep going when it will not
// compile, because a failed compile usually means "this expression is really a
// reference to an aggregate's output column" and the right recovery is to copy
// that column. Only `expr.IsUnknownFunc` was exempt from the fallback, so a
// refused literal fell into it and came back as `column "-'abc'" does not
// exist in the input schema` — a name-resolution message for a diagnosed data
// error, with no SQLSTATE at all (the blanket 42000 on the wire). The refusal
// is now its own error TYPE and `expr.IsCompileRefusal` names both classes.
//
// PostgreSQL refuses every `-'…'` form with 42725 ("operator is not unique:
// - unknown") — verified live — rather than 22P02; wadjet has one generic
// unary-minus operator and no overload ambiguity to report, so it reports what
// it actually found. ADR-0012 item 5 records that difference.
func TestRefusedLiteralReachesTheClientAsItsOwnError(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, sql := range []string{
		"SELECT -'abc' AS v FROM declit",
		"SELECT k FROM declit WHERE -'abc' > 0",
		"SELECT k FROM declit ORDER BY -'abc'",
		"SELECT k, COUNT(*) AS n FROM declit GROUP BY k, -'abc'",
		"SELECT MAX(-'abc') AS v FROM declit",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := db.Query(ctx, sql)
			if err == nil {
				t.Fatalf("%s answered instead of refusing", sql)
			}
			if !strings.Contains(err.Error(), `invalid input syntax for type numeric: "abc"`) {
				t.Errorf("%s\n  error = %v\n  want PostgreSQL's numeric input-syntax error, quoting the literal", sql, err)
			}
			if got := sqlerr.StateOf(err); got != "22P02" {
				t.Errorf("%s\n  SQLSTATE = %q, want 22P02 — a client branches on this", sql, got)
			}
		})
	}

	// A numeric-looking quoted literal is not refused: it folds into the
	// literal path and answers, which is the other half of #505's fold.
	if _, err := db.Query(ctx, "SELECT -'5' AS v FROM declit"); err != nil {
		t.Errorf("-'5' must still compile: %v", err)
	}
}

// TestDecimalLiteralRefusalIsPlanTime is #517: the refusal of a constant that
// names no number against a DECIMAL column is a TYPE rule, so it must depend
// on the column's DECLARATION and on nothing else — not on a row reaching the
// comparison, and not on which operand won one.
//
// It used to depend on both, because it lived inside the comparison:
//
//   - PER ROW. `k > 100000 AND d_2 IS DISTINCT FROM 'abc'` answered zero rows,
//     and so did the same predicate over an empty table. Nothing evaluated the
//     comparison, so nothing refused it.
//   - PAIRWISE, so the DATA decided. GREATEST/LEAST compare (best-so-far,
//     candidate) pairs and only a pair with a DECIMAL column on one side and
//     the bad literal on the other refuses — so with GREATEST, 'abc' beat the
//     integer and met d_2 (refused), while with LEAST the integer stayed best
//     and met d_2 directly (answered). The SAME three arguments.
//
// The check is now the binder's (`physical.checkLiteralTypes`), against the
// column's catalog type, before a plan exists. The runtime refusals stay for
// the shapes the binder cannot prove, and share its numeric test
// (`expr.IsNumericLiteralText`) so the two cannot disagree.
func TestDecimalLiteralRefusalIsPlanTime(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	refused := func(t *testing.T, sql string) {
		t.Helper()
		_, err := tmRun(ctx, db, sql)
		if err == nil {
			t.Fatalf("%s answered instead of refusing a non-numeric constant", sql)
		}
		if !strings.Contains(err.Error(), `invalid input syntax for type numeric: "abc"`) {
			t.Errorf("%s\n  error = %v\n  want PostgreSQL's numeric input-syntax error, quoting the literal", sql, err)
		}
		if got := sqlerr.StateOf(err); got != "22P02" {
			t.Errorf("%s\n  SQLSTATE = %q, want 22P02 — a client branches on this", sql, got)
		}
	}

	t.Run("no row survives the conjunct", func(t *testing.T) {
		for _, sql := range []string{
			"SELECT COUNT(*) AS n FROM declit WHERE k > 100000 AND d_2 IS DISTINCT FROM 'abc'",
			"SELECT COUNT(*) AS n FROM declit WHERE k > 100000 AND d_2 = 'abc'",
			"SELECT COUNT(*) AS n FROM declit WHERE k > 100000 AND CASE d_2 WHEN 'abc' THEN 1 ELSE 0 END = 1",
			// The conjunct short-circuits before the comparison is ever
			// COMPILED into a row loop, which is the other way a per-row
			// refusal was skipped.
			"SELECT COUNT(*) AS n FROM declit WHERE 1 = 0 AND d_2 = 'abc'",
		} {
			refused(t, sql)
		}
	})

	t.Run("empty table", func(t *testing.T) {
		empty := declitEmptyOpen(t)
		for _, pred := range []string{
			"d_2 = 'abc'",
			"d_2 IS DISTINCT FROM 'abc'",
			"CASE d_2 WHEN 'abc' THEN 1 ELSE 0 END = 1",
			"GREATEST(d_2, 'abc') = 'abc'",
		} {
			sql := "SELECT COUNT(*) AS n FROM declit WHERE " + pred
			_, err := tmRun(ctx, empty, sql)
			if err == nil {
				t.Errorf("%s answered over an EMPTY table instead of refusing", sql)
				continue
			}
			if !strings.Contains(err.Error(), "invalid input syntax for type numeric") {
				t.Errorf("%s\n  error = %v, want PostgreSQL's numeric input-syntax error", sql, err)
			}
		}
	})

	t.Run("GREATEST and LEAST agree on the same arguments", func(t *testing.T) {
		// The pair that used to split: with GREATEST the bad literal met the
		// DECIMAL column and refused; with LEAST it never did and the query
		// answered a row.
		refused(t, "SELECT COUNT(*) AS n FROM declit WHERE GREATEST(k, 'abc', d_2) = 'abc'")
		refused(t, "SELECT COUNT(*) AS n FROM declit WHERE LEAST(k, 'abc', d_2) = 'abc'")
		// Argument ORDER cannot matter either.
		refused(t, "SELECT COUNT(*) AS n FROM declit WHERE LEAST(d_2, 'abc', k) = 'abc'")
		refused(t, "SELECT COUNT(*) AS n FROM declit WHERE GREATEST('abc', d_2, k) = 'abc'")
	})

	// What must NOT be refused, so the check has not widened into a type
	// error of its own.
	t.Run("still answers", func(t *testing.T) {
		for _, sql := range []string{
			// A quoted literal that IS a number, against a DECIMAL column:
			// PostgreSQL types it from the column and compares numerically.
			"SELECT COUNT(*) AS n FROM declit WHERE d_2 = '0.00'",
			"SELECT COUNT(*) AS n FROM declit WHERE d_2 IS DISTINCT FROM '0.00'",
			"SELECT COUNT(*) AS n FROM declit WHERE GREATEST(d_2, '0.00') = '0.00'",
			// Exponent form is a number too (ADR-0012 item 6).
			"SELECT COUNT(*) AS n FROM declit WHERE d_2 = '1e400'",
			// A non-numeric literal against a NON-DECIMAL column is an
			// ordinary comparison, not a refusal.
			"SELECT COUNT(*) AS n FROM declit WHERE k = 'abc'",
			// And a literal on its own, with no DECIMAL column in sight.
			"SELECT COUNT(*) AS n FROM declit WHERE 'abc' = 'def'",
		} {
			if _, err := tmRun(ctx, db, sql); err != nil {
				t.Errorf("%s was refused: %v", sql, err)
			}
		}
	})
}

// declitEmptyOpen is declitOpen's table with no rows in it: the fixture that
// tells a plan-time refusal from a per-row one, because a per-row check has
// nothing to run on.
func declitEmptyOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(ctx, "declit", declitSchema(), nil); err != nil {
		t.Fatal(err)
	}
	return db
}
