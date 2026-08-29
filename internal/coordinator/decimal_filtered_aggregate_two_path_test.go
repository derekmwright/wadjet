package coordinator

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #685: an UNGROUPED DECIMAL aggregate under a SELECTIVE filter answered
// 10^scale too large on the stage DAG — SUM(a) WHERE id < 5 came back 3824.00
// where the answer is 38.24, and AVG/MIN/MAX carried the same factor.
//
// The mechanism, and why it needs a filter that empties a WHOLE TASK:
//
//   - On the DAG each partial aggregate is its own task over its own files. A
//     filter that matches nothing in one task's files leaves that task's
//     ungrouped aggregate with no input at all, and it still owes one row —
//     SQL's identity row (SUM/MIN/MAX/AVG -> NULL, COUNT -> 0).
//   - That row has no input VECTOR to take a DECIMAL scale from, so it shipped
//     a scale-0 column, and the .wshf header it wrote said DECIMAL(0,0) for a
//     column the stage declares DECIMAL(38,2). A header is half of every
//     DECIMAL value in its file (ADR-0010).
//   - The final aggregate merged that all-NULL scale-0 batch alongside the two
//     that carried real values, and kernel's DECIMAL batch arms adopted
//     `acc.DecScale = vec.DecimalData.Scale` OUTSIDE the null guard — so a
//     batch that contributed nothing overwrote the scale the contributing ones
//     had established. FinalSum then rendered a right Int128 as text at scale
//     0 ("3824") and the emit re-parsed it into the scale-2 output vector
//     (382400). One rescale of a value already at that scale, which is why the
//     factor is exactly 10^(input scale) and why SUM, AVG, MIN and MAX carried
//     it identically while COUNT did not.
//
// GROUP BY was correct throughout for the reason the shapes below re-assert: a
// grouped partial with no rows emits no rows, so no scale-0 batch is ever
// written. An all-match filter was correct for the same reason.
//
// This gate is deliberately a MATRIX rather than the one failing query: the
// defect was invisible to every existing DECIMAL gate because none of them had
// a predicate that could empty a whole task (TestDecimalAggregatesTwoPath's
// "sparse_predicate" filters on k = i%4, which matches rows in every chunk).
// The three filter classes below are exactly "some tasks empty", "all but one
// task empty" and "no task empty", and the expectation is computed here in
// big.Int from the fixture rather than read off either engine — two paths
// through one wrong accumulator would agree with each other and still fail.
//
// The expectations were checked against live postgres:17-alpine over the same
// nine rows before the fix was written (the correctness-fix protocol's step 1).
// PostgreSQL 17.11, `numeric(9,2) a` / `numeric(18,4) b`:
//
//	                          SUM       MIN       MAX       AVG                    COUNT
//	a WHERE id < 5            38.24     -0.01     12.75     9.5600000000000000     4
//	a WHERE id < 4            38.25     12.75     12.75     12.7500000000000000    3
//	a WHERE id < 100          52.99     -0.01     12.75     7.5700000000000000     7
//	a WHERE id < 0            NULL      NULL      NULL      NULL                   0
//	b WHERE id < 5            38.2400   -0.0100   12.7501   9.5600000000000000     4
//	b WHERE id < 100          49.2400   -0.0100   12.7501   7.0342857142857143     7
//	SUM(a)*2 WHERE id < 5     76.48     MIN(a)*2 = -0.02     MAX(a)*2 = 25.50
//	SUM(b)*2 WHERE id < 5     76.4800   MIN(b)*2 = -0.0200   MAX(b)*2 = 25.5002
//	SUM(a) OVER () id < 5     38.24 on every row; MIN -0.01, MAX 12.75
//	GROUP BY a, id < 5        -0.01 -> 1 row, 12.75 -> 3 rows
//
// SUM, MIN, MAX, COUNT, the wrapped forms, the group keys and the window agree
// digit for digit. AVG is the one documented divergence: PostgreSQL picks a
// scale giving at least 16 significant digits and wadjet widens the input's
// scale by 4 (batch.AvgScaleIncrement, ADR-0012 item 9), so 9.560000 here and
// 9.5600000000000000 there — the same number to min(scale), which is why the
// AVG assertions below are written against dtpAvgText rather than PG's text.
//
// Refs #685, #533, ADR-0010, ADR-0024.

// dfaScale is the declared scale of the decpair fixture's two DECIMAL columns.
func dfaScale(col string) int {
	if col == "a" {
		return 2
	}
	return 4
}

// dfaUnscaled reads row i's unscaled value for col out of the fixture itself,
// so the expectation cannot drift from the data. The fixture's magnitudes fit
// int64, so the Hi half is pure sign extension.
func dfaUnscaled(row map[string]any, col string) (*big.Int, bool) {
	v, present := row[col]
	if !present || v == nil {
		return nil, false
	}
	d, ok := v.(parquet.Decimal128)
	if !ok {
		return nil, false
	}
	return big.NewInt(int64(d.Lo)), true
}

type dfaExpect struct {
	min, max, sum *big.Int
	count         int64
}

// dfaCompute is the independent oracle: it walks dbpData() applying the same
// predicate the SQL states, in Go, with no engine involved.
func dfaCompute(col string, idBelow int64) dfaExpect {
	var e dfaExpect
	e.sum = big.NewInt(0)
	for _, r := range dbpData() {
		id, _ := r["id"].(int64)
		if id >= idBelow {
			continue
		}
		u, ok := dfaUnscaled(r, col)
		if !ok {
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

// dfaFilters are the three task-population classes the defect turns on, plus
// the empty one. The decpair fixture is written as three chunks of three rows
// (tmdWriteTables), ids 1..3, 4..6 and 7..9, and the cluster has three
// workers — so the id bound directly selects how many TASKS see a row.
var dfaFilters = []struct {
	name string
	// idBelow is the WHERE bound; nonEmptyTasks records what the fixture's
	// chunking makes of it, and is asserted by dfaAssertChunking so a change
	// to tmdWriteTables cannot quietly turn this matrix vacuous.
	idBelow        int64
	nonEmptyChunks int
}{
	// The #685 shape: chunk 0 matches 3 rows, chunk 1 matches 1, chunk 2
	// matches NONE — so one partial task writes the identity row.
	{"some_tasks_empty", 5, 2},
	// Only the first chunk matches; TWO partials write identity rows.
	{"one_task_only", 4, 1},
	// Every chunk matches. This was always correct, and is the control that
	// says the matrix is testing selectivity and not the filter itself.
	{"all_match", 100, 3},
	// Nothing matches anywhere: the whole answer is the identity row.
	{"no_task_matches", 0, 0},
}

// dfaAssertChunking checks the premise the filter table above rests on. If the
// fixture ever stops splitting into three chunks of three, "some_tasks_empty"
// stops emptying a task and this gate would pass for the wrong reason.
func dfaAssertChunking(t *testing.T) {
	t.Helper()
	const chunks = 4 // tmdWriteTables' constant
	rows := dbpData()
	per := (len(rows) + chunks - 1) / chunks
	for _, f := range dfaFilters {
		got := 0
		for c := 0; c*per < len(rows); c++ {
			lo, hi := c*per, min(c*per+per, len(rows))
			for _, r := range rows[lo:hi] {
				if id, _ := r["id"].(int64); id < f.idBelow {
					got++
					break
				}
			}
		}
		if got != f.nonEmptyChunks {
			t.Fatalf("filter %q matches rows in %d of the fixture's chunks, not %d — the decpair "+
				"fixture's chunking changed and this matrix no longer covers the shape it exists for",
				f.name, got, f.nonEmptyChunks)
		}
	}
}

func TestFilteredDecimalAggregateTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	dfaAssertChunking(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, col := range []string{"a", "b"} {
		scale := dfaScale(col)
		for _, f := range dfaFilters {
			want := dfaCompute(col, f.idBelow)
			where := fmt.Sprintf("WHERE id < %d", f.idBelow)

			// --- Ungrouped, bare aggregate: the #685 shape itself. ---
			t.Run(fmt.Sprintf("ungrouped_bare_%s_%s", col, f.name), func(t *testing.T) {
				sql := fmt.Sprintf(
					"SELECT SUM(%[1]s) AS s, MIN(%[1]s) AS lo, MAX(%[1]s) AS hi, AVG(%[1]s) AS av, "+
						"COUNT(%[1]s) AS n FROM %[2]s %[3]s", col, dbpTable, where)
				for _, arm := range []struct {
					name string
					dag  bool
				}{{"single", false}, {"dag", true}} {
					rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
					if len(rows) != 1 {
						t.Fatalf("%s: %d rows, want exactly 1 (an ungrouped aggregate always answers one row)",
							arm.name, len(rows))
					}
					r := rows[0]
					pre := arm.name + " "
					if want.count == 0 {
						for _, c := range []string{"s", "lo", "hi", "av"} {
							if r[c] != nil {
								t.Errorf("%s%s = %#v, want NULL (no row matched)", pre, c, r[c])
							}
						}
					} else {
						dtpCell(t, pre+"SUM", r["s"], dtpText(want.sum, scale))
						dtpCell(t, pre+"MIN", r["lo"], dtpText(want.min, scale))
						dtpCell(t, pre+"MAX", r["hi"], dtpText(want.max, scale))
						dtpCell(t, pre+"AVG", r["av"], dtpAvgText(want.sum, want.count, scale))
					}
					if n := toInt64(r["n"]); n != want.count {
						t.Errorf("%sCOUNT = %v, want %d", pre, r["n"], want.count)
					}
				}
			})

			// --- Ungrouped, WRAPPED aggregate. ---
			//
			// The aggregate's value reaches an arithmetic operator instead of
			// the client directly, which is a second consumer of the same
			// carrier (#685's table records SUM(b)*2 answering 764800.0000 for
			// 76.4800).
			//
			// Asserted NUMERICALLY here, plus a byte-for-byte comparison of
			// the two arms' rendering below. The exact-DECIMAL arithmetic of
			// ADR-0024 item 3 has since landed, so the digits are now pinned
			// by name for these shapes in
			// TestFilteredDecimalAggregateAgreesOnBothPaths ("wrapped sum,
			// selective filter" -> 76.4800); what this arm owns is the
			// FACTOR, which is 100x at the narrowest and survives any
			// rendering either path picks.
			t.Run(fmt.Sprintf("ungrouped_wrapped_%s_%s", col, f.name), func(t *testing.T) {
				sql := fmt.Sprintf(
					"SELECT SUM(%[1]s) * 2 AS s, MIN(%[1]s) * 2 AS lo, MAX(%[1]s) * 2 AS hi "+
						"FROM %[2]s %[3]s", col, dbpTable, where)
				var wantS, wantLo, wantHi *big.Rat
				if want.count > 0 {
					wantS = dfaScaled(want.sum, scale, 2)
					wantLo = dfaScaled(want.min, scale, 2)
					wantHi = dfaScaled(want.max, scale, 2)
				}
				var armRows []map[string]any
				for _, arm := range []struct {
					name string
					dag  bool
				}{{"single", false}, {"dag", true}} {
					rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
					if len(rows) != 1 {
						t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
					}
					armRows = append(armRows, rows[0])
					for _, c := range []struct {
						key  string
						want *big.Rat
					}{{"s", wantS}, {"lo", wantLo}, {"hi", wantHi}} {
						dfaNumeric(t, arm.name+" "+c.key, rows[0][c.key], c.want)
					}
				}
				// And the two paths must render it the same way, not merely
				// mean the same number: a difference in the declared type is a
				// two-path divergence a client can see.
				for _, k := range []string{"s", "lo", "hi"} {
					if fmt.Sprintf("%#v", armRows[0][k]) != fmt.Sprintf("%#v", armRows[1][k]) {
						t.Errorf("%s: single %#v vs DAG %#v — the arms render the wrapped aggregate differently",
							k, armRows[0][k], armRows[1][k])
					}
				}
			})

			// --- GROUP BY an INT key. Correct before the fix (a grouped
			// partial with no rows emits no rows), asserted so it stays so. ---
			t.Run(fmt.Sprintf("grouped_%s_%s", col, f.name), func(t *testing.T) {
				sql := fmt.Sprintf(
					"SELECT id, SUM(%[1]s) AS s, MIN(%[1]s) AS lo, MAX(%[1]s) AS hi, AVG(%[1]s) AS av, "+
						"COUNT(%[1]s) AS n FROM %[2]s %[3]s GROUP BY id ORDER BY id",
					col, dbpTable, where)
				wantByID := map[int64]dfaExpect{}
				for _, r := range dbpData() {
					id, _ := r["id"].(int64)
					if id >= f.idBelow {
						continue
					}
					e := dfaExpect{sum: big.NewInt(0)}
					if u, ok := dfaUnscaled(r, col); ok {
						e.min, e.max, e.count = u, u, 1
						e.sum.Add(e.sum, u)
					}
					wantByID[id] = e
				}
				for _, arm := range []struct {
					name string
					dag  bool
				}{{"single", false}, {"dag", true}} {
					rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
					if len(rows) != len(wantByID) {
						t.Fatalf("%s: %d groups, want %d", arm.name, len(rows), len(wantByID))
					}
					for _, r := range rows {
						id := toInt64(r["id"])
						w, ok := wantByID[id]
						if !ok {
							t.Errorf("%s: unexpected group id=%d", arm.name, id)
							continue
						}
						pre := fmt.Sprintf("%s group id=%d ", arm.name, id)
						if w.count == 0 {
							for _, c := range []string{"s", "lo", "hi", "av"} {
								if r[c] != nil {
									t.Errorf("%s%s = %#v, want NULL", pre, c, r[c])
								}
							}
							continue
						}
						dtpCell(t, pre+"SUM", r["s"], dtpText(w.sum, scale))
						dtpCell(t, pre+"MIN", r["lo"], dtpText(w.min, scale))
						dtpCell(t, pre+"MAX", r["hi"], dtpText(w.max, scale))
						dtpCell(t, pre+"AVG", r["av"], dtpAvgText(w.sum, w.count, scale))
					}
				}
			})
		}

		// --- GROUP BY the DECIMAL COLUMN ITSELF, under the emptying filter.
		//
		// ADR-0024's other exact path through the same carrier: the key is the
		// value, and a key column that lost its (p,s) truncates rather than
		// rescaling (#144/#379). Distinct from the aggregate arms above
		// because the scale rides the GROUP-BY key encoding, not an
		// accumulator.
		t.Run("decimal_group_key_"+col, func(t *testing.T) {
			sql := fmt.Sprintf(
				"SELECT %[1]s AS k, COUNT(*) AS n FROM %[2]s WHERE id < 5 GROUP BY %[1]s ORDER BY %[1]s",
				col, dbpTable)
			wantKeys := map[string]int64{}
			for _, r := range dbpData() {
				if id, _ := r["id"].(int64); id >= 5 {
					continue
				}
				key := "NULL"
				if u, ok := dfaUnscaled(r, col); ok {
					key = dtpText(u, scale)
				}
				wantKeys[key]++
			}
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				got := map[string]int64{}
				for _, r := range rows {
					key := "NULL"
					if r["k"] != nil {
						s, ok := r["k"].(string)
						if !ok {
							t.Errorf("%s: DECIMAL group key came back %#v (%T), not text", arm.name, r["k"], r["k"])
							continue
						}
						key = s
					}
					got[key] += toInt64(r["n"])
				}
				dfaCompareCounts(t, arm.name, got, wantKeys)
			}
		})

		// --- A WINDOW aggregate under the same filter.
		//
		// The third exact path (ADR-0024): a windowed SUM/MIN/MAX answers what
		// the grouped one answers, and it reaches the value through a separate
		// accumulator (exec/window_decimal_agg.go) on a stage the DAG plans
		// separately.
		t.Run("window_"+col, func(t *testing.T) {
			// The output names avoid "s": decpair carries a TEXT column of
			// that name, and an alias that shadows a base column reads back
			// as the base column's value on both arms — a test that agrees
			// with itself while measuring nothing.
			sql := fmt.Sprintf(
				"SELECT id, SUM(%[1]s) OVER () AS wsum, MIN(%[1]s) OVER () AS wlo, MAX(%[1]s) OVER () AS whi "+
					"FROM %[2]s WHERE id < 5 ORDER BY id", col, dbpTable)
			want := dfaCompute(col, 5)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) == 0 {
					t.Fatalf("%s: no rows", arm.name)
				}
				for _, r := range rows {
					pre := fmt.Sprintf("%s row id=%v ", arm.name, r["id"])
					dtpCell(t, pre+"SUM OVER ()", r["wsum"], dtpText(want.sum, scale))
					dtpCell(t, pre+"MIN OVER ()", r["wlo"], dtpText(want.min, scale))
					dtpCell(t, pre+"MAX OVER ()", r["whi"], dtpText(want.max, scale))
				}
			}
		})
	}
}

// dfaScaled turns an unscaled integer at a scale into the exact rational it
// denotes, times mult.
func dfaScaled(unscaled *big.Int, scale int, mult int64) *big.Rat {
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	r := new(big.Rat).SetFrac(new(big.Int).Set(unscaled), den)
	return r.Mul(r, new(big.Rat).SetInt64(mult))
}

// dfaNumeric compares a cell to an exact rational, whatever box the engine
// rendered it in. want == nil means the cell must be NULL.
//
// The comparison is relative and generous (1e-12) because a wrapped DECIMAL
// may still arrive as a float64 until ADR-0024 item 3's arithmetic lands. That
// is ample here: the defect this file exists for moves a value by 10^scale,
// which is 100x at the narrowest.
func dfaNumeric(t *testing.T, what string, got any, want *big.Rat) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Errorf("%s = %#v, want NULL", what, got)
		}
		return
	}
	if got == nil {
		w, _ := want.Float64()
		t.Errorf("%s = NULL, want %v", what, w)
		return
	}
	var f float64
	switch v := got.(type) {
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			t.Errorf("%s = %q, which is not a number", what, v)
			return
		}
		f = parsed
	case float64:
		f = v
	case float32:
		f = float64(v)
	case int64:
		f = float64(v)
	default:
		t.Errorf("%s = %#v (%T), which is not a numeric box", what, got, got)
		return
	}
	w, _ := want.Float64()
	den := w
	if den < 0 {
		den = -den
	}
	if den == 0 {
		den = 1
	}
	if diff := f - w; diff/den > 1e-12 || diff/den < -1e-12 {
		t.Errorf("%s = %v, want %v (ratio %v — a 10^scale factor is #685)", what, f, w, f/w)
	}
}

func dfaCompareCounts(t *testing.T, arm string, got, want map[string]int64) {
	t.Helper()
	keys := map[string]bool{}
	for k := range got {
		keys[k] = true
	}
	for k := range want {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		if got[k] != want[k] {
			t.Errorf("%s: group key %q has %d rows, want %d", arm, k, got[k], want[k])
		}
	}
}
