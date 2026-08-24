package wadjet

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #455: MIN, MAX, SUM and AVG over a DECIMAL column answered as float64, so a
// DECIMAL(38,10) holding 977777777887777.7577887713 came back as
// 9.777777778877776e+14 — every digit past the 16th gone before any consumer
// saw it, and no literal naming the value the column holds could match the
// aggregate's own output.
//
// The reference here is big.Int arithmetic over the fixture generator, not
// another engine path: MIN/MAX/SUM of exact integers is a computation Go can
// do independently, so an aggregate and a comparator that were wrong together
// still fail. The rendering is spelled out too (decText), so the test pins the
// TEXT a client reads and not just the number.

const (
	daTable  = "decagg"
	daRows   = 400
	daGroups = 4
)

// daSchema carries one DECIMAL of each physical encoding ADR-0018 §4 defines:
// precision 9 is an INT32 leaf, 18 an INT64 leaf, 38 a FIXED_LEN_BYTE_ARRAY.
// The wide column is the one a float64 cannot hold at all.
func daSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "d2", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "d4", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "dw", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
}

// daWideStep is 23 digits, so every non-zero dw value needs more than 64 bits
// and the sum of a group needs more than 80.
const daWideStep = "97777777788777775778877"

// daUnscaled returns row i's unscaled value for one column, or nil for the
// rows that are NULL. Integer arithmetic throughout: the fixture's exact
// answer is computable without the engine.
func daUnscaled(col string, i int) *big.Int {
	switch col {
	case "d2":
		if i%5 == 4 {
			return nil
		}
		return big.NewInt(int64(i)*37 - 4321) // -43.21 .. 145.34
	case "d4":
		if i%7 == 6 {
			return nil
		}
		return big.NewInt(int64(i+1) * 1234567890123) // up to ~4.9e14 → 49392060.4938 at scale 4
	case "dw":
		if i%11 == 10 {
			return nil
		}
		step, _ := new(big.Int).SetString(daWideStep, 10)
		return step.Mul(step, big.NewInt(int64(i-137))) // spans both signs
	}
	panic("unknown column " + col)
}

func daScale(col string) int {
	switch col {
	case "d2":
		return 2
	case "d4":
		return 4
	}
	return 10
}

func daRowsData() []map[string]any {
	rows := make([]map[string]any, daRows)
	for i := range rows {
		r := map[string]any{"k": int64(i % daGroups)}
		for _, col := range []string{"d2", "d4", "dw"} {
			if u := daUnscaled(col, i); u != nil {
				// An INTEGER box IS the unscaled value at the column's
				// declared scale (ADR-0018's writer corollary). A float64
				// box would be scaled on the way in and could not carry 23
				// digits at all.
				r[col] = daDecimal128(u)
			} else {
				r[col] = nil
			}
		}
		rows[i] = r
	}
	return rows
}

// daDecimal128 renders a signed big.Int as the two halves parquet.Decimal128
// carries, two's complement.
func daDecimal128(n *big.Int) parquet.Decimal128 {
	m := new(big.Int).Set(n)
	if m.Sign() < 0 {
		m.Add(m, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	var b [16]byte
	m.FillBytes(b[:])
	hi := new(big.Int).Rsh(new(big.Int).SetBytes(b[:]), 64).Uint64()
	lo := new(big.Int).And(new(big.Int).SetBytes(b[:]), new(big.Int).SetUint64(^uint64(0))).Uint64()
	return parquet.Decimal128{Hi: int64(hi), Lo: lo}
}

// decText renders an unscaled integer at a scale exactly as a DECIMAL cell
// reaches a client: the digits split at the scale, the fraction EXACTLY scale
// digits wide and never trimmed (#453 — a numeric(p,s) renders at its declared
// scale, PostgreSQL's rule). Written out here rather than called from the
// engine so the test is a statement about the contract.
func decText(unscaled *big.Int, scale int) string {
	neg := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()
	if scale <= 0 {
		if neg {
			return "-" + digits
		}
		return digits
	}
	intPart, frac := "0", digits
	if len(digits) > scale {
		intPart, frac = digits[:len(digits)-scale], digits[len(digits)-scale:]
	} else {
		frac = strings.Repeat("0", scale-len(digits)) + digits
	}
	out := intPart + "." + frac
	if neg {
		out = "-" + out
	}
	return out
}

// daExpect is the exact MIN/MAX/SUM/AVG of one column over the rows a
// predicate selects, as unscaled integers.
type daExpect struct {
	min, max, sum *big.Int
	count         int64
}

func daCompute(col string, keep func(i int) bool) daExpect {
	var e daExpect
	e.sum = big.NewInt(0)
	for i := 0; i < daRows; i++ {
		if !keep(i) {
			continue
		}
		u := daUnscaled(col, i)
		if u == nil {
			continue
		}
		if e.count == 0 {
			e.min, e.max = new(big.Int).Set(u), new(big.Int).Set(u)
		} else {
			if u.Cmp(e.min) < 0 {
				e.min = new(big.Int).Set(u)
			}
			if u.Cmp(e.max) > 0 {
				e.max = new(big.Int).Set(u)
			}
		}
		e.sum.Add(e.sum, u)
		e.count++
	}
	return e
}

// avgText is the contract for AVG over a DECIMAL: exact division at
// scale+4, rounded half away from zero (batch.AvgScale). Computed here with
// big.Rat so the expectation is independent of the engine's implementation.
func avgText(sum *big.Int, count int64, scale int) string {
	outScale := scale + 4
	if outScale > 38 {
		outScale = 38
	}
	shift := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(outScale-scale)), nil)
	num := new(big.Int).Mul(sum, shift)
	den := big.NewInt(count)
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	rem.Abs(rem)
	if rem.Lsh(rem, 1).Cmp(den) >= 0 {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return decText(q, outScale)
}

func daOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := daSchema()
	if err := db.CreateTable(ctx, daTable, schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	rows := daRowsData()
	ing := db.NewIngester(daTable, schema, nil, ingest.Config{
		MaxBufferRows: len(rows) + 1, RowGroupSize: 137,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}

// TestDecimalScalarAggregatesAreExact is the issue's own shape: the whole-input
// (ungrouped) aggregate over each DECIMAL encoding.
func TestDecimalScalarAggregatesAreExact(t *testing.T) {
	ctx := context.Background()
	db := daOpen(t)
	for _, col := range []string{"d2", "d4", "dw"} {
		t.Run(col, func(t *testing.T) {
			scale := daScale(col)
			want := daCompute(col, func(int) bool { return true })
			res, err := db.Query(ctx, fmt.Sprintf(
				"SELECT MIN(%s) AS lo, MAX(%s) AS hi, SUM(%s) AS s, AVG(%s) AS a FROM %s",
				col, col, col, col, daTable))
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(res.Rows))
			}
			r := res.Rows[0]
			daAssertCell(t, "MIN", r["lo"], decText(want.min, scale))
			daAssertCell(t, "MAX", r["hi"], decText(want.max, scale))
			daAssertCell(t, "SUM", r["s"], decText(want.sum, scale))
			daAssertCell(t, "AVG", r["a"], avgText(want.sum, want.count, scale))

			// The declared TYPE, which is what a client reads the value
			// through: a NUMERIC column, not a double. The SCALE is pinned
			// by the rendered text above — ColumnMeta does not carry a type
			// modifier yet (#454).
			mbAssertTypes(t, res.ColumnMetas, parquet.TypeDecimal, "lo", "hi", "s", "a")
		})
	}
}

// TestDecimalGroupedAggregatesAreExact is the same over GROUP BY, which is a
// different accumulator path: the flat SoA arrays (agg_scatter.go) rather than
// the whole-input batch kernels.
func TestDecimalGroupedAggregatesAreExact(t *testing.T) {
	ctx := context.Background()
	db := daOpen(t)
	for _, col := range []string{"d2", "d4", "dw"} {
		t.Run(col, func(t *testing.T) {
			scale := daScale(col)
			res, err := db.Query(ctx, fmt.Sprintf(
				"SELECT k, MIN(%s) AS lo, MAX(%s) AS hi, SUM(%s) AS s, AVG(%s) AS a "+
					"FROM %s GROUP BY k ORDER BY k", col, col, col, col, daTable))
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if len(res.Rows) != daGroups {
				t.Fatalf("got %d groups, want %d", len(res.Rows), daGroups)
			}
			for g, r := range res.Rows {
				want := daCompute(col, func(i int) bool { return i%daGroups == g })
				if want.count == 0 {
					t.Fatalf("group %d has no non-NULL rows — the fixture proves nothing", g)
				}
				daAssertCell(t, fmt.Sprintf("group %d MIN", g), r["lo"], decText(want.min, scale))
				daAssertCell(t, fmt.Sprintf("group %d MAX", g), r["hi"], decText(want.max, scale))
				daAssertCell(t, fmt.Sprintf("group %d SUM", g), r["s"], decText(want.sum, scale))
				daAssertCell(t, fmt.Sprintf("group %d AVG", g), r["a"], avgText(want.sum, want.count, scale))
			}
			mbAssertTypes(t, res.ColumnMetas, parquet.TypeDecimal, "lo", "hi", "s", "a")
		})
	}
}

// TestDecimalAggregateNullAndEmptyInput: SQL says MIN/MAX/SUM/AVG over no rows
// are NULL, and a DECIMAL answer must be NULL rather than a zero of the right
// type.
func TestDecimalAggregateNullAndEmptyInput(t *testing.T) {
	ctx := context.Background()
	db := daOpen(t)
	res, err := db.Query(ctx, "SELECT MIN(dw) AS lo, MAX(dw) AS hi, SUM(dw) AS s, AVG(dw) AS a, "+
		"COUNT(dw) AS n FROM "+daTable+" WHERE k = 9999")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 — an ungrouped aggregate owes SQL one row over the empty set", len(res.Rows))
	}
	r := res.Rows[0]
	for _, c := range []string{"lo", "hi", "s", "a"} {
		if r[c] != nil {
			t.Errorf("%s over the empty set = %#v, want NULL", c, r[c])
		}
	}
	if n, ok := r["n"].(int64); !ok || n != 0 {
		t.Errorf("COUNT over the empty set = %#v, want 0", r["n"])
	}
}

// TestDecimalAggregateOrderByIsExact: the aggregate's own output feeding a
// sort. A float64 MAX would order two groups whose extremes differ past the
// 16th digit arbitrarily; the exact one does not.
func TestDecimalAggregateOrderByIsExact(t *testing.T) {
	ctx := context.Background()
	db := daOpen(t)
	res, err := db.Query(ctx, "SELECT k, MAX(dw) AS hi FROM "+daTable+" GROUP BY k ORDER BY hi DESC")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != daGroups {
		t.Fatalf("got %d groups, want %d", len(res.Rows), daGroups)
	}
	var prev *big.Int
	for i, r := range res.Rows {
		g, ok := r["k"].(int64)
		if !ok {
			t.Fatalf("row %d: group key is %#v", i, r["k"])
		}
		want := daCompute("dw", func(j int) bool { return int64(j%daGroups) == g })
		daAssertCell(t, fmt.Sprintf("row %d MAX", i), r["hi"], decText(want.max, 10))
		if prev != nil && want.max.Cmp(prev) > 0 {
			t.Fatalf("row %d breaks the DESC order: %s follows %s", i, want.max, prev)
		}
		prev = want.max
	}
}

// TestDecimalSumOverflowIsAnError pins the contract's other half: wadjet's
// exact carrier is 128 bits, so a SUM that leaves it is reported, never
// answered with the wrapped total.
func TestDecimalSumOverflowIsAnError(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "big", Type: parquet.TypeDecimal, Precision: 38, Scale: 0},
	}}
	if err := db.CreateTable(ctx, "decovf", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// 9 x 10^37 is the widest a DECIMAL(38,0) holds and still fits the
	// Int128 carrier (ceiling 1.70 x 10^38). One per group, so every group
	// sum is representable and only the whole-table sum — 2.7 x 10^38 — is
	// not.
	nine37, _ := new(big.Int).SetString("90000000000000000000000000000000000000", 10)
	rows := make([]map[string]any, 3)
	for i := range rows {
		rows[i] = map[string]any{"k": int64(i), "big": daDecimal128(nine37)}
	}
	ing := db.NewIngester("decovf", schema, nil, ingest.Config{MaxBufferRows: 16})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Grouped: one row per group, inside the range.
	res, err := db.Query(ctx, "SELECT k, SUM(big) AS s FROM decovf GROUP BY k ORDER BY k")
	if err != nil {
		t.Fatalf("the grouped sum fits an Int128 and must answer: %v", err)
	}
	want := decText(nine37, 0)
	if len(res.Rows) != 3 {
		t.Fatalf("got %d groups, want 3", len(res.Rows))
	}
	for _, r := range res.Rows {
		daAssertCell(t, "grouped SUM", r["s"], want)
	}

	// Ungrouped: all three, 2.7 x 10^38 — outside it.
	if _, err := db.Query(ctx, "SELECT SUM(big) AS s FROM decovf"); err == nil {
		t.Fatal("SUM over three values of 9x10^37 answered; the exact total has no Int128 and the " +
			"wrapped one is a different number")
	} else if !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("the error does not name the overflow: %v", err)
	}
}

// TestDecimalHavingEqualsTheAggregateValue is the consequence #455 opens with:
// `GROUP BY k HAVING MAX(dw) = <the value the column holds>` found nothing,
// because the aggregate's own output no longer equaled any literal naming it.
//
// It takes both halves: the aggregate's exact DECIMAL output (#455, here) and
// the literal's text carrier (#452, already on main), so the 25-digit literal
// the HAVING names is the number the column holds rather than the nearest
// double.
func TestDecimalHavingEqualsTheAggregateValue(t *testing.T) {
	ctx := context.Background()
	db := daOpen(t)
	want := daCompute("dw", func(i int) bool { return i%daGroups == 1 })
	lit := decText(want.max, 10)

	res, err := db.Query(ctx, fmt.Sprintf(
		"SELECT k, MAX(dw) AS hi FROM %s GROUP BY k HAVING MAX(dw) = %s", daTable, lit))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("HAVING MAX(dw) = %s matched %d groups, want exactly the one whose MAX it is",
			lit, len(res.Rows))
	}
	if k, _ := res.Rows[0]["k"].(int64); k != 1 {
		t.Errorf("matched group %v, want 1", res.Rows[0]["k"])
	}
	daAssertCell(t, "HAVING MAX", res.Rows[0]["hi"], lit)
}

func daAssertCell(t *testing.T, what string, got any, want string) {
	t.Helper()
	s, ok := got.(string)
	if !ok {
		t.Errorf("%s = %#v (%T), want the DECIMAL text %q — a non-string box is the "+
			"float64 answer #455 is about", what, got, got, want)
		return
	}
	if s != want {
		t.Errorf("%s = %q, want %q", what, s, want)
	}
}

// TestDecimalAggregateOverflowDirectionsAndShapes is the overflow contract in
// the shapes TestDecimalSumOverflowIsAnError leaves out: the NEGATIVE side of
// the range, the GROUPED accumulator (flat SoA arrays, a different add site
// from the whole-input kernel), and AVG — whose scaling step overflows on a
// sum that fits, so SUM's check does not cover it.
//
// AVG is the one that mattered: it used to answer NULL here while the DAG's
// avg-fold raised, so the same query returned "no value" in one deployment
// and an error in another, and a NULL is indistinguishable from an empty
// group.
func TestDecimalAggregateOverflowDirectionsAndShapes(t *testing.T) {
	ctx := context.Background()

	// nine37 is the widest DECIMAL(38,0) the Int128 carrier still holds
	// (ceiling 1.70 x 10^38); three of them, either sign, do not fit.
	nine37, _ := new(big.Int).SetString("90000000000000000000000000000000000000", 10)
	// wide is 5 x 10^24 at scale 10: three of them SUM fine, but AVG scales
	// the sum by 10^4 before dividing and that product has no Int128.
	wide, _ := new(big.Int).SetString("50000000000000000000000000000000000", 10)

	for _, tc := range []struct {
		name, col, sql, want string
		unscaled             *big.Int
		scale                int
	}{
		{name: "sum_positive_grouped", col: "big", unscaled: nine37, scale: 0,
			sql: "SELECT 1 AS g, SUM(big) AS s FROM %s GROUP BY g", want: "overflow"},
		{name: "sum_negative_scalar", col: "big", unscaled: new(big.Int).Neg(nine37), scale: 0,
			sql: "SELECT SUM(big) AS s FROM %s", want: "overflow"},
		{name: "sum_negative_grouped", col: "big", unscaled: new(big.Int).Neg(nine37), scale: 0,
			sql: "SELECT 1 AS g, SUM(big) AS s FROM %s GROUP BY g", want: "overflow"},
		{name: "avg_scalar", col: "wide", unscaled: wide, scale: 10,
			sql: "SELECT AVG(wide) AS a FROM %s", want: "no exact 128-bit value"},
		{name: "avg_grouped", col: "wide", unscaled: wide, scale: 10,
			sql: "SELECT 1 AS g, AVG(wide) AS a FROM %s GROUP BY g", want: "no exact 128-bit value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := "ovf_" + tc.name
			schema := parquet.Schema{Columns: []parquet.Column{
				{Name: tc.col, Type: parquet.TypeDecimal, Precision: 38, Scale: tc.scale, Nullable: true},
			}}
			if err := db.CreateTable(ctx, table, schema, nil); err != nil {
				t.Fatalf("create table: %v", err)
			}
			rows := make([]map[string]any, 3)
			for i := range rows {
				rows[i] = map[string]any{tc.col: daDecimal128(tc.unscaled)}
			}
			ing := db.NewIngester(table, schema, nil, ingest.Config{MaxBufferRows: 16})
			if err := ing.Ingest(ctx, rows); err != nil {
				t.Fatalf("ingest: %v", err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatalf("flush: %v", err)
			}

			res, err := db.Query(ctx, fmt.Sprintf(tc.sql, table))
			if err == nil {
				t.Fatalf("answered %v; the exact value has no Int128, and neither a wrapped "+
					"total nor a NULL is the answer", res.Rows)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the error does not name the condition (%q): %v", tc.want, err)
			}
		})
	}
}
