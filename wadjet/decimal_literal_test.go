package wadjet

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
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
