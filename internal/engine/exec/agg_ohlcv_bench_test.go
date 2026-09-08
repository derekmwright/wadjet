package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The bar's per-row cost, against the aggregate it is closest to.
//
// MIN_BY is the baseline the brief names and the right one: it is the other
// aggregate that carries a PAIRED state (a value plus an ordering key) through
// `extraState` rather than through a kernel.Accumulator, so the two differ in
// what the state DOES per row and in nothing else about how they are reached.
//
// A bar does strictly more than a MIN_BY — four extremes, two sums, and in the
// exact domain those sums are Int128 with an overflow check — so it is
// expected to cost more. What this benchmark is for is knowing HOW much, and
// noticing if that ever changes by an order rather than by a factor.
func BenchmarkOhlcvObserveExact(b *testing.B) {
	rows := ohlcvBenchRows(2048)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := &ohlcvState{dom: ohlcvDomain{exact: true, priceScale: 2, volScale: 0, pvScale: 2}}
		for _, r := range rows {
			s.observeExact(r.ts, batch.Int128From(r.px), batch.Int128From(r.vol))
		}
	}
	b.SetBytes(int64(len(rows)))
}

func BenchmarkOhlcvObserveFloat(b *testing.B) {
	rows := ohlcvBenchRows(2048)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := &ohlcvState{}
		for _, r := range rows {
			s.observeFloat(r.ts, float64(r.px), float64(r.vol))
		}
	}
	b.SetBytes(int64(len(rows)))
}

// The baseline: MIN_BY's per-row state update over the same rows, spelled the
// way updateGroup spells it.
func BenchmarkMinByObserveBaseline(b *testing.B) {
	rows := ohlcvBenchRows(2048)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := &minMaxByState{isMin: true}
		for _, r := range rows {
			cmp := float64(r.ts)
			if !st.hasValue || cmp < st.bestCmp {
				st.hasValue = true
				st.bestCmp = cmp
				st.bestVal = r.px
			}
		}
	}
	b.SetBytes(int64(len(rows)))
}

// The merge, which is what a clone, a spilled run and a DAG fan-in each pay
// once per group rather than once per row.
func BenchmarkOhlcvMergeExact(b *testing.B) {
	rows := ohlcvBenchRows(2048)
	dom := ohlcvDomain{exact: true, priceScale: 2, volScale: 0, pvScale: 2}
	left := &ohlcvState{dom: dom}
	right := &ohlcvState{dom: dom}
	for i, r := range rows {
		if i%2 == 0 {
			left.observeExact(r.ts, batch.Int128From(r.px), batch.Int128From(r.vol))
		} else {
			right.observeExact(r.ts, batch.Int128From(r.px), batch.Int128From(r.vol))
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc := *left
		acc.merge(right)
	}
}

// The encoding, which every partial pays once per group on the way out and
// every merge stage once per group on the way in.
func BenchmarkOhlcvEncodeDecode(b *testing.B) {
	rows := ohlcvBenchRows(256)
	s := &ohlcvState{dom: ohlcvDomain{exact: true, priceScale: 2, volScale: 0, pvScale: 2}}
	for _, r := range rows {
		s.observeExact(r.ts, batch.Int128From(r.px), batch.Int128From(r.vol))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := decodeOhlcvState(s.encode()); !ok {
			b.Fatal("decode refused its own encoding")
		}
	}
}

// And the finish, once per group.
func BenchmarkOhlcvFinalize(b *testing.B) {
	rows := ohlcvBenchRows(256)
	s := &ohlcvState{dom: ohlcvDomain{exact: true, priceScale: 2, volScale: 0, pvScale: 2}}
	for _, r := range rows {
		s.observeExact(r.ts, batch.Int128From(r.px), batch.Int128From(r.vol))
	}
	fields, ok := OhlcvOutputFields(
		parquet.Column{Name: "px", Type: parquet.TypeDecimal, Precision: 9, Scale: 2},
		parquet.Column{Name: "vol", Type: parquet.TypeInt32})
	if !ok {
		b.Fatal("no bar over decimal price and int4 volume")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.value(fields); err != nil {
			b.Fatal(err)
		}
	}
}

func ohlcvBenchRows(n int) []ohlcvTestRow {
	return ohlcvTestRows(n)
}
