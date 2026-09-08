package exec

import (
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE MERGE LAW IS ASSOCIATIVE AND COMMUTATIVE (#965, ADR-0035).
//
// That is not a nicety: it is the whole claim the bar makes. A state whose
// merge depends on the ORDER the pieces arrive in answers one thing on the
// single-process path, another when a clone merges, and a third when four DAG
// tasks finish in a different order — and every one of them looks like a
// plausible bar.
//
// The property is checked over random partitions of one row set rather than
// over a fixed pair, because the shape that breaks it is a tie: two rows
// sharing an instant landing in different partitions. The generator plants
// those deliberately.
func TestTheBarsMergeIsAssociativeAndCommutative(t *testing.T) {
	for _, dom := range []struct {
		name string
		d    ohlcvDomain
	}{
		{"exact", ohlcvDomain{exact: true, priceScale: 2, volScale: 0, pvScale: 2}},
		{"float", ohlcvDomain{}},
	} {
		t.Run(dom.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(1))
			rows := ohlcvTestRows(60)
			ref := ohlcvFoldAll(dom.d, rows)
			for trial := 0; trial < 200; trial++ {
				parts := ohlcvSplit(rng, rows)
				// Fold each partition into its own state, then merge them
				// together in a random order — which is what a clone merge,
				// a spilled k-way merge and a DAG fan-in all do.
				states := make([]*ohlcvState, 0, len(parts))
				for _, p := range parts {
					states = append(states, ohlcvFoldAll(dom.d, p))
				}
				rng.Shuffle(len(states), func(i, j int) { states[i], states[j] = states[j], states[i] })
				acc := &ohlcvState{dom: dom.d}
				for _, s := range states {
					acc.merge(s)
				}
				if !ohlcvSame(acc, ref) {
					t.Fatalf("trial %d over %d partitions:\n merged %s\n whole   %s",
						trial, len(parts), ohlcvShow(acc), ohlcvShow(ref))
				}
			}
		})
	}
}

// A partial state ROUND-TRIPS through its encoding unchanged, and a merge of
// two ENCODED states equals a merge of the two states themselves.
//
// The encoding is what crosses parquet, the .wshf shuffle format and the NATS
// gather (ADR-0010), so a lossy field there is a bar that changes when the
// query is distributed and not otherwise — the class ADR-0027 exists for.
func TestTheBarsEncodedStateRoundTrips(t *testing.T) {
	for _, dom := range []ohlcvDomain{
		{exact: true, priceScale: 4, volScale: 2, pvScale: 6},
		{exact: true},
		{},
	} {
		rows := ohlcvTestRows(37)
		a := ohlcvFoldAll(dom, rows[:20])
		b := ohlcvFoldAll(dom, rows[20:])

		for _, s := range []*ohlcvState{a, b} {
			back, ok := decodeOhlcvState(s.encode())
			if !ok {
				t.Fatalf("decode refused its own encoding (%d chars)", len(s.encode()))
			}
			if !ohlcvSame(&back, s) {
				t.Errorf("round trip changed the state:\n got  %s\n want %s", ohlcvShow(&back), ohlcvShow(s))
			}
			if len(s.encode()) != ohlcvStateWidth {
				t.Errorf("encoded width %d, want %d — the width is fixed so a "+
					"truncated column is a decode refusal rather than a plausible bar",
					len(s.encode()), ohlcvStateWidth)
			}
		}

		want := ohlcvFoldAll(dom, rows)
		gotEnc := MergeOhlcvStates(a.encode(), b.encode())
		got, ok := decodeOhlcvState(gotEnc)
		if !ok {
			t.Fatal("MergeOhlcvStates produced something decodeOhlcvState refuses")
		}
		if !ohlcvSame(&got, want) {
			t.Errorf("merging two ENCODED states differs from folding every row:\n got  %s\n want %s",
				ohlcvShow(&got), ohlcvShow(want))
		}
		// And it is commutative through the encoding too.
		other, _ := decodeOhlcvState(MergeOhlcvStates(b.encode(), a.encode()))
		if !ohlcvSame(&other, &got) {
			t.Errorf("MergeOhlcvStates is not commutative:\n a,b %s\n b,a %s",
				ohlcvShow(&got), ohlcvShow(&other))
		}
	}
	// Anything that is not this encoding is "no rows to merge", never a
	// plausible empty bar — varianceState's rule.
	for _, bad := range []string{"", "zz", "00", string(make([]byte, ohlcvStateWidth))} {
		if _, ok := decodeOhlcvState(bad); ok {
			t.Errorf("decodeOhlcvState accepted %q", bad)
		}
	}
}

// THE TIEBREAK IS A VALUE. Two rows sharing an instant must give the same
// open and close whichever order they are folded in, and the answer must be
// PostgreSQL's `ORDER BY ts, price` / `ORDER BY ts DESC, price DESC`.
func TestTheBarsTiebreakIsAValueNotAnArrivalOrder(t *testing.T) {
	dom := ohlcvDomain{exact: true}
	one := func(order []int) *ohlcvState {
		s := &ohlcvState{dom: dom}
		// Three instants; the first and the last are each shared by two rows.
		rows := [][3]int64{{10, 20, 1}, {10, 18, 1}, {20, 5, 1}, {30, 19, 1}, {30, 21, 1}}
		for _, i := range order {
			s.observeExact(rows[i][0], batch.Int128From(rows[i][1]), batch.Int128From(rows[i][2]))
		}
		return s
	}
	base := one([]int{0, 1, 2, 3, 4})
	if got := base.firstPx.ToInt64(); got != 18 {
		t.Errorf("open = %d, want 18 — the SMALLER price at the earliest instant "+
			"(PostgreSQL's (array_agg(px ORDER BY ts, px))[1])", got)
	}
	if got := base.lastPx.ToInt64(); got != 21 {
		t.Errorf("close = %d, want 21 — the LARGER price at the latest instant "+
			"(PostgreSQL's (array_agg(px ORDER BY ts DESC, px DESC))[1])", got)
	}
	for _, order := range [][]int{
		{4, 3, 2, 1, 0}, {1, 0, 4, 2, 3}, {2, 4, 0, 3, 1}, {3, 1, 4, 0, 2},
	} {
		s := one(order)
		if !ohlcvSame(s, base) {
			t.Errorf("fold order %v changed the bar:\n got  %s\n want %s",
				order, ohlcvShow(s), ohlcvShow(base))
		}
	}
}

// The bar's DECLARED fields follow the inputs, and each field declares what
// its own spelled-out aggregate declares. Measured against PostgreSQL 17.11's
// declarations for min/max/sum/avg over the same column types.
func TestTheBarsDeclaredFieldsFollowItsInputs(t *testing.T) {
	dec := func(p, s int) parquet.Column {
		return parquet.Column{Name: "px", Type: parquet.TypeDecimal, Precision: p, Scale: s}
	}
	flat := func(t parquet.TypeID) parquet.Column { return parquet.Column{Name: "c", Type: t} }

	for _, tc := range []struct {
		name              string
		price, vol        parquet.Column
		wantPrice         parquet.TypeID
		wantPricePS       [2]int
		wantVol           parquet.TypeID
		wantVolPS         [2]int
		wantVwap          parquet.TypeID
		wantVwapPS        [2]int
		pgDeclarationSays string
	}{
		{"int32_price_int32_volume", flat(parquet.TypeInt32), flat(parquet.TypeInt32),
			parquet.TypeInt32, [2]int{0, 0}, parquet.TypeInt64, [2]int{0, 0},
			parquet.TypeDecimal, [2]int{38, 4},
			"min(int4)=integer, sum(int4)=bigint, avg(int4)=numeric"},
		{"int64_price_int64_volume", flat(parquet.TypeInt64), flat(parquet.TypeInt64),
			parquet.TypeInt64, [2]int{0, 0}, parquet.TypeDecimal, [2]int{38, 0},
			parquet.TypeDecimal, [2]int{38, 4},
			"min(int8)=bigint, sum(int8)=numeric, avg(int8)=numeric"},
		{"real_price", flat(parquet.TypeFloat32), flat(parquet.TypeInt32),
			parquet.TypeFloat32, [2]int{0, 0}, parquet.TypeInt64, [2]int{0, 0},
			parquet.TypeFloat64, [2]int{0, 0},
			"min(real)=real; one approximate operand makes the quotient double"},
		{"double_price", flat(parquet.TypeFloat64), flat(parquet.TypeFloat64),
			parquet.TypeFloat64, [2]int{0, 0}, parquet.TypeFloat64, [2]int{0, 0},
			parquet.TypeFloat64, [2]int{0, 0},
			"min/sum/avg of double precision are all double precision"},
		{"decimal_price_int_volume", dec(9, 2), flat(parquet.TypeInt32),
			parquet.TypeDecimal, [2]int{9, 2}, parquet.TypeInt64, [2]int{0, 0},
			parquet.TypeDecimal, [2]int{38, 6},
			"min(numeric(9,2)) keeps (9,2); avg adds ADR-0024's +4 to the input scale"},
		{"decimal_price_decimal_volume", dec(18, 4), dec(9, 2),
			parquet.TypeDecimal, [2]int{18, 4}, parquet.TypeDecimal, [2]int{38, 2},
			parquet.TypeDecimal, [2]int{38, 8},
			"sum(numeric(9,2)) keeps scale 2; avg of numeric(18,4) is scale 8"},
		{"port_price", flat(parquet.TypePort), flat(parquet.TypeProtocol),
			parquet.TypePort, [2]int{0, 0}, parquet.TypeInt64, [2]int{0, 0},
			parquet.TypeDecimal, [2]int{38, 4},
			"PORT and PROTOCOL are int4-domain values (#834, #953)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields, ok := OhlcvOutputFields(tc.price, tc.vol)
			if !ok {
				t.Fatalf("no bar over price %v and volume %v", tc.price.Type, tc.vol.Type)
			}
			if len(fields) != len(OhlcvFieldNames) {
				t.Fatalf("%d fields, want %d", len(fields), len(OhlcvFieldNames))
			}
			for i, n := range OhlcvFieldNames {
				if fields[i].Name != n {
					t.Errorf("field %d is %q, want %q — the ORDER is the composite's "+
						"wire rendering and is fixed", i, fields[i].Name, n)
				}
			}
			check := func(idx int, want parquet.TypeID, ps [2]int) {
				f := fields[idx]
				if f.Type != want || f.Precision != ps[0] || f.Scale != ps[1] {
					t.Errorf("%s declares %v(%d,%d), want %v(%d,%d)\n  PostgreSQL: %s",
						f.Name, f.Type, f.Precision, f.Scale, want, ps[0], ps[1],
						tc.pgDeclarationSays)
				}
			}
			for i := 0; i < 4; i++ {
				check(i, tc.wantPrice, tc.wantPricePS)
			}
			check(4, tc.wantVol, tc.wantVolPS)
			check(5, tc.wantVwap, tc.wantVwapPS)
		})
	}

	// A type that has no bar declines rather than declaring a narrower one.
	for _, bad := range []parquet.TypeID{
		parquet.TypeString, parquet.TypeBytes, parquet.TypeBool, parquet.TypeIPv4,
		parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID, parquet.TypeArray,
		parquet.TypeRow, parquet.TypeMap, parquet.TypeVector, parquet.TypeDate,
		parquet.TypeTimestamp, parquet.TypeDuration, parquet.TypeIPv6,
	} {
		if _, ok := OhlcvOutputFields(flat(bad), flat(parquet.TypeInt64)); ok {
			t.Errorf("declared a bar over a %v price", bad)
		}
		if _, ok := OhlcvOutputFields(flat(parquet.TypeInt64), flat(bad)); ok {
			t.Errorf("declared a bar over a %v volume", bad)
		}
	}
	// And a product scale with no exact carrier declines rather than
	// rounding to a scale nobody asked for.
	if _, ok := OhlcvOutputFields(dec(38, 30), dec(38, 30)); ok {
		t.Error("declared a bar whose price*volume has no exact 128-bit carrier")
	}
}

// --- helpers ----------------------------------------------------------------

type ohlcvTestRow struct {
	ts  int64
	px  int64
	vol int64
}

// ohlcvTestRows plants deliberate ties: every fourth row repeats the previous
// instant, which is the only shape a value-decided tiebreak and an
// arrival-ordered one disagree on.
func ohlcvTestRows(n int) []ohlcvTestRow {
	rows := make([]ohlcvTestRow, 0, n)
	ts := int64(1_600_000_000_000)
	for i := 0; i < n; i++ {
		if i%4 != 0 {
			ts += int64(1000 + i*7)
		}
		rows = append(rows, ohlcvTestRow{
			ts:  ts,
			px:  int64((i*37)%211) - 100,
			vol: int64(i%9) + 1,
		})
	}
	return rows
}

func ohlcvFoldAll(dom ohlcvDomain, rows []ohlcvTestRow) *ohlcvState {
	s := &ohlcvState{dom: dom}
	for _, r := range rows {
		if dom.exact {
			s.observeExact(r.ts, batch.Int128From(r.px), batch.Int128From(r.vol))
		} else {
			s.observeFloat(r.ts, float64(r.px), float64(r.vol))
		}
	}
	return s
}

func ohlcvSplit(rng *rand.Rand, rows []ohlcvTestRow) [][]ohlcvTestRow {
	k := 1 + rng.Intn(5)
	parts := make([][]ohlcvTestRow, k)
	for _, r := range rows {
		i := rng.Intn(k)
		parts[i] = append(parts[i], r)
	}
	return parts
}

// ohlcvSame compares two states field for field. reflect.DeepEqual over the
// struct would compare the unused carrier too — an exact state's float fields
// are zero on one side and zero on the other, but a float state's Int128
// fields are equally untouched — so it is spelled out per domain.
func ohlcvSame(a, b *ohlcvState) bool {
	if a.n != b.n || a.firstTS != b.firstTS || a.lastTS != b.lastTS ||
		a.overflow != b.overflow || !reflect.DeepEqual(a.dom, b.dom) {
		return false
	}
	if a.dom.exact {
		return a.firstPx == b.firstPx && a.lastPx == b.lastPx &&
			a.high == b.high && a.low == b.low &&
			a.sumVol == b.sumVol && a.sumPV == b.sumPV
	}
	return a.firstPxF == b.firstPxF && a.lastPxF == b.lastPxF &&
		a.highF == b.highF && a.lowF == b.lowF &&
		a.sumVolF == b.sumVolF && a.sumPVF == b.sumPVF
}

func ohlcvShow(s *ohlcvState) string {
	if s.dom.exact {
		return "n=" + itoa(s.n) + " first=(" + itoa(s.firstTS) + "," + s.firstPx.String() +
			") last=(" + itoa(s.lastTS) + "," + s.lastPx.String() + ") hi=" + s.high.String() +
			" lo=" + s.low.String() + " vol=" + s.sumVol.String() + " pv=" + s.sumPV.String()
	}
	return "n=" + itoa(s.n) + " first=(" + itoa(s.firstTS) + "," + ftoa(s.firstPxF) +
		") last=(" + itoa(s.lastTS) + "," + ftoa(s.lastPxF) + ") hi=" + ftoa(s.highF) +
		" lo=" + ftoa(s.lowF) + " vol=" + ftoa(s.sumVolF) + " pv=" + ftoa(s.sumPVF)
}

func itoa(v int64) string   { return strconv.FormatInt(v, 10) }
func ftoa(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
