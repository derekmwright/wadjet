package batch

import (
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestDecimalRescaleAgreesWithBatchRescale keeps ONE rescaling rule in the
// engine.
//
// There are two functions that move an unscaled DECIMAL carrier between
// scales, in two packages, because they are reached from different places:
// batch.Rescale is the engine's, on the arithmetic and comparison paths, and
// parquet.DecimalRescale is storage's, reconciling a file's declaration to the
// catalog's on the way in (#707). They are written differently — the engine
// divides 128-bit words, storage routes through the exact text grammar
// ADR-0024 already gates — and if they ever disagree, a number read out of a
// file stops meaning what the same number means once it is in a vector.
//
// This is the same guard, and the same reason, as
// TestDecimalGrammarMatchesBatch: the lower package owns the rule and the
// upper one is held to it.
//
// The comparison is over the SCALING only. parquet.DecimalRescale additionally
// holds its result to a declared precision, which batch.Rescale has no notion
// of, so this passes MaxDecimalPrecision — the widest band the carrier can
// enforce — and every disagreement left is a disagreement about scaling.
func TestDecimalRescaleAgreesWithBatchRescale(t *testing.T) {
	rng := rand.New(rand.NewSource(20260903))
	corpus := []Int128{
		{}, Int128From(1), Int128From(-1), Int128From(5), Int128From(-5),
		Int128From(4), Int128From(-4), Int128From(49), Int128From(-49),
		Int128From(50), Int128From(-50), Int128From(51), Int128From(-51),
		Int128From(99), Int128From(-99), Int128From(100), Int128From(-100),
		Int128From(1 << 62), Int128From(-(1 << 62)),
		{Hi: 1<<63 - 1, Lo: ^uint64(0)}, // 2^127-1
		{Hi: -1 << 63, Lo: 0},           // -2^127
	}
	for i := 0; i < 500; i++ {
		corpus = append(corpus, Int128{
			Hi: int64(rng.Uint64()) >> uint(rng.Intn(64)),
			Lo: rng.Uint64(),
		})
	}
	for _, v := range corpus {
		for from := 0; from <= MaxDecimalPrecision; from++ {
			for to := 0; to <= MaxDecimalPrecision; to++ {
				want, wantOK := Rescale(v, from, to)
				got, err := parquet.DecimalRescale(
					parquet.Decimal128{Hi: v.Hi, Lo: v.Lo}, from, to, MaxDecimalPrecision)
				if !wantOK {
					if err == nil && !decimalInBand(got) {
						t.Fatalf("rescale({%d,%d}, %d->%d): batch refuses, parquet answers %s",
							v.Hi, v.Lo, from, to, got.String())
					}
					continue
				}
				// batch.Rescale has an answer. parquet may still refuse it for
				// the BAND — |v| >= 10^38 — which batch does not model.
				if err != nil {
					if !decimalInBand(parquet.Decimal128{Hi: want.Hi, Lo: want.Lo}) {
						continue
					}
					t.Fatalf("rescale({%d,%d}, %d->%d): batch answers {%d,%d}, parquet refuses: %v",
						v.Hi, v.Lo, from, to, want.Hi, want.Lo, err)
				}
				if got.Hi != want.Hi || got.Lo != want.Lo {
					t.Fatalf("rescale({%d,%d}, %d->%d): parquet %s, batch %s — the two "+
						"rescaling rules have drifted apart",
						v.Hi, v.Lo, from, to, got.String(),
						Int128{Hi: want.Hi, Lo: want.Lo}.FormatDecimal(0))
				}
			}
		}
	}
}

// decimalInBand reports whether a carrier's magnitude is below 10^38 — the
// bound parquet.DecimalRescale enforces and batch.Rescale does not.
func decimalInBand(d parquet.Decimal128) bool {
	return DecimalFitsPrecision(Int128{Hi: d.Hi, Lo: d.Lo}, MaxDecimalPrecision)
}
