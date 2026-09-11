package expr

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/semvergen"
)

// WHAT semver_cmp AND semver_sort_key COST, AGAINST THE THING THEY REPLACE.
//
// The base is not "no work": it is the operation a user writes TODAY over the
// same column and which gives the wrong answer — a byte comparison of the
// version strings, and passing them through unchanged. Those two are the floor,
// and the point of the numbers is to say how far above it the right answer is,
// not to chase a target. No A/B: nothing on a hot path changed, and there is no
// before.
//
// Measured on the corpus these gates already use, so a run is reproducible.

func benchSemverCorpus() []string { return semvergen.Corpus(967, 2048) }

// The floor: a byte comparison, which is what ORDER BY over the raw column is.
func BenchmarkSemverBaseStringCompare(b *testing.B) {
	corpus := benchSemverCorpus()
	b.ReportAllocs()
	b.ResetTimer()
	sink := 0
	for i := 0; i < b.N; i++ {
		for j := 1; j < len(corpus); j++ {
			sink += strings.Compare(corpus[j-1], corpus[j])
		}
	}
	benchSemverSink = sink
}

// The three-way comparison, parsing both sides every call — which is what a
// per-row `semver_cmp(a, b)` over two COLUMNS does.
func BenchmarkSemverCmp(b *testing.B) {
	corpus := benchSemverCorpus()
	b.ReportAllocs()
	b.ResetTimer()
	sink := 0
	for i := 0; i < b.N; i++ {
		for j := 1; j < len(corpus); j++ {
			a, aok := parseSemver(corpus[j-1])
			c, cok := parseSemver(corpus[j])
			if aok && cok {
				sink += compareSemver(a, c)
			}
		}
	}
	benchSemverSink = sink
}

// The floor for the key: handing the string back unchanged, which is what
// ORDER BY over the raw column materializes.
func BenchmarkSemverBaseIdentityKey(b *testing.B) {
	corpus := benchSemverCorpus()
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		for _, v := range corpus {
			n += len(v)
		}
	}
	benchSemverSink = n
}

// The key: one parse and one 58-plus-byte build per row. This is the cost a
// query pays ONCE per row for an ordering every downstream consumer — the
// sort, the merge, MIN/MAX, the spill — then gets for free.
func BenchmarkSemverSortKey(b *testing.B) {
	corpus := benchSemverCorpus()
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		for _, s := range corpus {
			v, ok := parseSemver(s)
			if ok {
				n += len(semverSortKey(v))
			}
		}
	}
	benchSemverSink = n
}

// And the range predicate, whose range is compiled ONCE and memoized — the
// shape a literal range in a WHERE actually has.
func BenchmarkSemverSatisfies(b *testing.B) {
	corpus := benchSemverCorpus()
	semverRangeCache.reset()
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		for _, s := range corpus {
			if fnSemverSatisfies([]any{s, "^1.2.3 || >=2.0.0 <3.0.0"}) == true {
				n++
			}
		}
	}
	benchSemverSink = n
}

// THE RANGE MEMO'S PER-ROW COST, on its own.
//
// `BenchmarkSemverSatisfies` above cannot separate the memo from the parse or
// from the registry's `[]any` argument boxing, and the round-1 review read its
// one-allocation-per-row as the memo's sync.Map key. This is the memo alone:
// the same text asked once per corpus row, which is what a literal range in a
// WHERE does. It allocates nothing with or without the one-entry fast path in
// front of it — the allocation in the predicate benchmark is the argument
// slice — and the fast path is what makes it about five times cheaper.
func BenchmarkSemverRangeMemoLookup(b *testing.B) {
	corpus := benchSemverCorpus()
	semverRangeCache.reset()
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		for range corpus {
			r, err := parseSemverRangeCached("semver_satisfies", "^1.2.3 || >=2.0.0 <3.0.0")
			if err == nil {
				n += len(r)
			}
		}
	}
	benchSemverSink = n
}

var benchSemverSink int
