package tpch

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
	"golang.org/x/sys/unix"
)

// The decimal performance baseline (ADR-0024).
//
// ADR-0024 predicted the cost of exact decimals would be "low, and
// unmeasurable on the current benchmark suite". Nothing in the tree could
// check that, because no benchmark exercised DECIMAL at all. This is the
// measurement.
//
//	TPCH_SCALE=1 go test -v -run TestTPCHDecimalPerformanceBaseline \
//	    -timeout 60m ./benchmarks/tpch/
//
// Protocol (ADR-0011, and the standing four-metric rule — never lean on one):
//
//   - Both fixtures are built in ONE process, from the same generator draws,
//     so the two hold the same numbers and differ only in carrier.
//   - Queries are INTERLEAVED — float, decimal, float, decimal — inside one
//     repetition, so a thermal or scheduler drift over the run lands on both
//     arms alike. A/B across windows is not a comparison (ADR-0011).
//   - The pair's ORDER SWAPS on odd repetitions, so neither arm always pays
//     the first-run cost of a pair. With an even repetition count each arm
//     leads exactly half the time.
//   - Four metrics per query: WALL (mean and max over the repetitions), CPU
//     (user+system, getrusage delta), ALLOC (bytes and object count), and
//     BYTES, which at this scale is what each fixture WROTE — a single
//     in-process run has no wire, so bytes-at-rest is the honest byte metric
//     and it is reported once per fixture rather than per query.
//   - The first repetition is a warm-up and is discarded.
//
// The result is a per-query table and the geomean of decimal/float. It is
// recorded in docs/benchmarks/tpch-decimal-baseline-2026-08-29.md. This test
// MEASURES; it does not gate — there is no threshold to fail, because the
// number it produces is the first one that exists.

const (
	decimalPerfRepsEnv = "TPCH_DECIMAL_REPS"
	// Four, not three: the arms swap order on odd repetitions, so an even
	// count gives each arm the lead exactly half the time.
	decimalPerfReps = 4 // plus one discarded warm-up
)

type perfSample struct {
	wall   time.Duration
	cpu    time.Duration
	alloc  uint64
	allocs uint64
	rows   int
}

type perfStat struct {
	wallMean, wallMax time.Duration
	cpu               time.Duration
	alloc, allocs     uint64
	rows              int
}

func summarize(ss []perfSample) perfStat {
	var st perfStat
	if len(ss) == 0 {
		return st
	}
	var total time.Duration
	for _, s := range ss {
		total += s.wall
		if s.wall > st.wallMax {
			st.wallMax = s.wall
		}
		st.cpu += s.cpu
		st.alloc += s.alloc
		st.allocs += s.allocs
		st.rows = s.rows
	}
	n := time.Duration(len(ss))
	st.wallMean = total / n
	st.cpu /= n
	st.alloc /= uint64(len(ss))
	st.allocs /= uint64(len(ss))
	return st
}

func TestTPCHDecimalPerformanceBaseline(t *testing.T) {
	sf := scaleFromEnv(t)
	reps := decimalPerfReps
	if v := os.Getenv(decimalPerfRepsEnv); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("%s=%q: want a positive integer", decimalPerfRepsEnv, v)
		}
		reps = n
	}
	ctx := context.Background()

	t.Logf("building both fixtures at SF=%v in one process", float64(sf))
	fdb, fstore := setupTPCHStreamingFixture(t, sf, FloatFixture)
	ddb, dstore := setupTPCHStreamingFixture(t, sf, DecimalFixture)

	fBytes, fObjs := storedBytes(t, ctx, fstore)
	dBytes, dObjs := storedBytes(t, ctx, dstore)

	nums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)

	fSamples := map[int][]perfSample{}
	dSamples := map[int][]perfSample{}
	failed := map[int]string{}

	for rep := 0; rep <= reps; rep++ { // rep 0 is the discarded warm-up
		for _, n := range nums {
			// TPCHQueries[n].SQL, not GetQuery(n, sf): every decimal suite
			// runs the corpus spelling, and the two differ for Q11 (whose
			// FRACTION GetQuery rewrites per scale factor, changing the
			// answer from 1 row to 359 at SF0.01). One spelling everywhere
			// keeps the correctness gates and this measurement on the same
			// query. At SF1 the two coincide.
			sql := TPCHQueries[n].SQL
			// ORDER SWAP (ADR-0011): whichever arm runs FIRST in a pair
			// pays for whatever the other one leaves warm — the page cache,
			// the allocator's spans, the branch predictors. Running float
			// first every time would fold that asymmetry into the ratio as
			// a constant. Odd repetitions run the decimal arm first, so
			// across the repetitions each arm leads half the time.
			var fs, ds perfSample
			var ferr, derr error
			if rep%2 == 1 {
				ds, derr = timeQuery(ddb, ctx, sql)
				fs, ferr = timeQuery(fdb, ctx, sql)
			} else {
				fs, ferr = timeQuery(fdb, ctx, sql)
				ds, derr = timeQuery(ddb, ctx, sql)
			}
			if ferr != nil {
				t.Fatalf("Q%02d on the FLOAT64 fixture: %v", n, ferr)
			}
			if derr != nil {
				// A query the DECIMAL carrier cannot run yet is a
				// correctness finding, pinned in decimal_variant_test.go.
				// It has no timing, and saying so beats a missing row.
				failed[n] = derr.Error()
				continue
			}
			if rep == 0 {
				continue
			}
			fSamples[n] = append(fSamples[n], fs)
			dSamples[n] = append(dSamples[n], ds)
		}
	}

	t.Logf("")
	t.Logf("TPC-H SF%v — DECIMAL(15,2) vs FLOAT64, %d interleaved repetitions (warm-up discarded)",
		float64(sf), reps)
	t.Logf("")
	t.Logf("| query | rows | float wall (mean/max) | dec wall (mean/max) | wall D/F | float CPU | dec CPU | CPU D/F | float alloc | dec alloc | alloc D/F | objects D/F |")
	t.Logf("|---|---|---|---|---|---|---|---|---|---|---|---|")

	var logSum float64
	var counted int
	for _, n := range nums {
		if why, ok := failed[n]; ok {
			t.Logf("| Q%02d | — | — | — | — | — | — | — | — | — | — | not runnable on the DECIMAL fixture: %s |",
				n, firstLine(why))
			continue
		}
		f, d := summarize(fSamples[n]), summarize(dSamples[n])
		if f.wallMean == 0 {
			continue
		}
		ratio := float64(d.wallMean) / float64(f.wallMean)
		logSum += math.Log(ratio)
		counted++
		t.Logf("| Q%02d | %d | %v / %v | %v / %v | %.3f | %v | %v | %.3f | %s | %s | %.3f | %.3f |",
			n, d.rows,
			f.wallMean.Round(time.Millisecond), f.wallMax.Round(time.Millisecond),
			d.wallMean.Round(time.Millisecond), d.wallMax.Round(time.Millisecond), ratio,
			f.cpu.Round(time.Millisecond), d.cpu.Round(time.Millisecond), ratioOf(d.cpu, f.cpu),
			mib(f.alloc), mib(d.alloc), ratioU(d.alloc, f.alloc), ratioU(d.allocs, f.allocs))
	}

	geo := math.Exp(logSum / float64(max(1, counted)))
	t.Logf("")
	t.Logf("GEOMEAN wall DECIMAL/FLOAT over %d queries: %.4f", counted, geo)
	t.Logf("BYTES AT REST: float %s in %d objects, decimal %s in %d objects, ratio %.4f",
		mib(uint64(fBytes)), fObjs, mib(uint64(dBytes)), dObjs, float64(dBytes)/float64(fBytes))
	if len(failed) > 0 {
		t.Logf("NOT MEASURED (pinned correctness defects, see decimal_variant_test.go): %v", sortedFailedQueries(failed))
	}
}

func timeQuery(db *wadjet.DB, ctx context.Context, sql string) (perfSample, error) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpu0 := processCPU()
	start := time.Now()
	res, err := db.Query(ctx, sql)
	wall := time.Since(start)
	cpu := processCPU() - cpu0
	if err != nil {
		return perfSample{}, err
	}
	runtime.ReadMemStats(&after)
	return perfSample{
		wall:   wall,
		cpu:    cpu,
		alloc:  after.TotalAlloc - before.TotalAlloc,
		allocs: after.Mallocs - before.Mallocs,
		rows:   len(res.Rows),
	}, nil
}

// processCPU is user+system time for the whole process. Every query in this
// test runs alone, so the delta across one is that query's CPU.
func processCPU() time.Duration {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano()) + time.Duration(ru.Stime.Nano())
}

func storedBytes(t *testing.T, ctx context.Context, store objstore.Store) (int64, int) {
	t.Helper()
	objs, err := store.List(ctx, "tpch", objstore.ListOptions{Prefix: "tables/"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var total int64
	for _, o := range objs {
		total += o.Size
	}
	return total, len(objs)
}

func mib(b uint64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }

func ratioOf(a, b time.Duration) float64 {
	if b == 0 {
		return math.NaN()
	}
	return float64(a) / float64(b)
}

func ratioU(a, b uint64) float64 {
	if b == 0 {
		return math.NaN()
	}
	return float64(a) / float64(b)
}

func firstLine(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			return s[:i]
		}
	}
	return s
}

func sortedFailedQueries(m map[int]string) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}
