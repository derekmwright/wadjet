package wadjet

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/shapegen"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The generated-query arm of the type-coverage gates.
//
// The shape fuzzer generates over shapegen.TPCH(), whose universe is Int32,
// Float64 and String — so nineteen of the engine's twenty-two types were
// unreachable by generated SQL even in principle. shapegen.TypeMatrix() closes
// that; these two tests are the arms that consume it and need no external
// oracle:
//
//	TestTypeMatrixFuzzBatchReuse             — poisoned pool vs clean pool
//	TestTypeMatrixFuzzOptimizationInvariance — each kill switch off vs on
//
// Both also run oracle.CheckOrder, the absolute check that the rows came back
// in the order the query asked for — that one needs no reference at all, so it
// catches an ORDER BY both arms drop the same way.
//
// Bounded by default so the suite stays cheap; WADJET_TM_FUZZ_SEED_COUNT
// extends it for local hunting and WADJET_TM_FUZZ_SEED_START moves the window.
// A failing seed IS the repro: generation is fully determined by it.

func tmFuzzSeeds(def int) []int64 {
	start := int64(tmFuzzEnvInt("WADJET_TM_FUZZ_SEED_START", 1))
	count := tmFuzzEnvInt("WADJET_TM_FUZZ_SEED_COUNT", def)
	out := make([]int64, count)
	for i := range out {
		out[i] = start + int64(i)
	}
	return out
}

func tmFuzzEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// tmFuzzKnownDivergence names the filed defect a generated query is guaranteed
// to hit, or "" when it is fair game. A harness that keeps re-finding a filed
// defect drowns the new ones — but a matcher that outlives its bug hides them,
// so each one says exactly what to delete.
//
// Deliberately EMPTY today: the type-matrix corpus pins its known defects per
// entry, and no generated shape has yet been found that hits one of them so
// reliably that it needs suppressing here. Add a matcher only with an issue
// number and a note saying what deleting it proves.
func tmFuzzKnownDivergence(q *shapegen.Query) string { return "" }

// tmFuzzPins are the seeds whose generated query hits a filed defect, keyed by
// seed because generation is fully determined by it.
//
// Same two-way ratchet as the corpus pins, restricted to the seeds the current
// window actually runs: a pinned seed inside the window that stops diverging
// FAILS, and an unpinned seed that diverges FAILS. A pin for a seed outside the
// window is simply not checked — the window is the corpus.
// #398's three pins (seeds 31, 124, 214) are deleted: the defect was never a
// retention bug at all but an unsynchronized once-cache on the uncorrelated
// subquery expressions, read by every parallel pipeline worker. Poison only
// perturbed the timing that decided which worker lost the race. All three
// seeds now agree on both arms, which IS the fix's proof (ADR-0013 §Pins).
var tmFuzzPins = map[int64]typematrix.Pin{
	48: {Issue: "#399", Reason: "DECIMAL GROUP BY keys read after their batch was released: " +
		"3157 rows both ways, different keys."},
}

// TestTypeMatrixFuzzBatchReuse: generated SQL over all 22 types, answered with
// the batch pool poisoned on release and again without, results required to
// match. Same contract as TestTypeMatrixBatchReuse, over generated shapes
// instead of the fixed corpus.
func TestTypeMatrixFuzzBatchReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping type-matrix gate under -short — the dedicated CI step runs it without -short")
	}
	ctx := context.Background()
	db := tmOpen(t)
	schema := shapegen.TypeMatrix()
	seeds := tmFuzzSeeds(40)

	var compared, reused, rejected int
	seen := map[int64]bool{}
	divergedSeeds := map[int64]bool{}
	for _, seed := range seeds {
		seen[seed] = true
		q := shapegen.New(seed, schema).Query()
		q.Seed = seed
		if bug := tmFuzzKnownDivergence(q); bug != "" {
			continue
		}
		sql := q.SQL()
		clean, cerr := tmRun(ctx, db, sql)
		if cerr != nil {
			rejected++
			continue // an unsupported shape is not this arm's business
		}
		before := batch.PoisonedBatches()
		prev := batch.SetPoisonOnRelease(true)
		dirty, derr := tmRun(ctx, db, sql)
		batch.SetPoisonOnRelease(prev)
		poisoned := batch.PoisonedBatches() - before

		pin, pinned := tmFuzzPins[seed]
		report := func(format string, args ...any) {
			if pinned {
				divergedSeeds[seed] = true
				t.Logf("known divergence, tracked in %s — NOT gated:\n  %s\n  "+format,
					append([]any{pin.Issue, pin.Reason}, args...)...)
				return
			}
			t.Errorf(format, args...)
		}
		if derr != nil {
			report("seed %d: the poisoned run FAILED on a query the clean run answered "+
				"(%d rows): %v\n  SQL: %s", seed, len(clean.Rows), derr, sql)
			continue
		}
		if poisoned == 0 {
			continue // nothing recycled; this seed says nothing about reuse
		}
		reused++
		compared++
		if diff := oracle.Compare(clean, dirty, q.CompareSpec()); diff != "" {
			report("seed %d (shape %s): BATCH-REUSE DIVERGENCE (%d batches poisoned)\n"+
				"  SQL: %s\n  %s\n  clean:    %s\n  poisoned: %s",
				seed, q.Shape, poisoned, sql, diff, tmRender(clean, 3), tmRender(dirty, 3))
			continue
		}
		if bad := oracle.CheckOrder(clean, q.OrderKeys()); bad != "" {
			t.Errorf("seed %d: the result is not ordered as the query asked:\n  SQL: %s\n%s",
				seed, sql, bad)
		}
	}
	// The ratchet, over the seeds this window actually ran: a pinned seed that
	// no longer diverges is a fixed bug still claiming an exemption.
	pinnedSeeds := make([]int64, 0, len(tmFuzzPins))
	for seed := range tmFuzzPins {
		pinnedSeeds = append(pinnedSeeds, seed)
	}
	sort.Slice(pinnedSeeds, func(i, j int) bool { return pinnedSeeds[i] < pinnedSeeds[j] })
	for _, seed := range pinnedSeeds {
		p := tmFuzzPins[seed]
		if !seen[seed] || divergedSeeds[seed] || p.GatedBy != "" {
			continue
		}
		t.Errorf("fuzz pin for seed %d now AGREES, so %s is FIXED:\n  %s\n"+
			"Delete the pin so the seed is gated again.", seed, p.Issue, p.Reason)
	}
	t.Logf("type-matrix fuzz (batch reuse): %d seeds, %d compared, %d recycled a batch, %d shapes rejected",
		len(seeds), compared, reused, rejected)
	if compared == 0 {
		t.Fatal("no generated query was compared — the arm proves nothing")
	}
}

// tmFuzzOptPins are the known optimization-invariance divergences of the
// generated arm, keyed "<seed>/<toggle>".
var tmFuzzOptPins = map[string]typematrix.Pin{
	// #401's two pins (18/scan-filter, 18/all-off) are deleted. The failure was
	// never about name resolution: ResolveFilterKernel had no DECIMAL arm, and
	// KernelFilter reported a nil kernel as `filter column %q does not exist in
	// the input schema`. With the arm added the seed answers the same under
	// every switch.
	//
	// #402's entry ("11/partitioned-agg") is also gone — fixed on main before
	// this rebase (shared-count/merge-bound fix); see tmFuzzIntermittentOptRetries
	// below.
}

// tmFuzzIntermittentOptRetries names optimization-invariance pins whose
// divergence has no known forcing trigger (contrast #391, which
// TestTypeMatrixBatchReuse's poison-on-release forces on every invocation),
// and how many times to retry the comparison before the ratchet concludes "no
// divergence this run". Every attempt is a real comparison against the
// baseline; the loop stops at the first divergence, which is logged exactly
// as a single-attempt divergence would be. This only shrinks the chance that
// an intermittent defect happens to sit out an entire test run — it does not
// change what counts as a divergence, and a pin not listed here still gets
// exactly one attempt.
//
// Empty since #402 — its only entry — was fixed. The mechanism stays: it is
// the policy ADR-0013 settled for a pin with no forcing gate, and the next
// such pin needs a map entry, not a rediscovery.
var tmFuzzIntermittentOptRetries = map[string]int{}

// TestTypeMatrixFuzzOptimizationInvariance: generated SQL over all 22 types,
// answered with every optimization on and then once per kill switch with that
// one off.
func TestTypeMatrixFuzzOptimizationInvariance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping type-matrix gate under -short — the dedicated CI step runs it without -short")
	}
	ctx := context.Background()
	db := tmOpen(t)
	schema := shapegen.TypeMatrix()
	seeds := tmFuzzSeeds(20)

	toggles := optswitch.All()
	if len(toggles) == 0 {
		t.Fatal("no toggles registered — optswitch packages not linked?")
	}
	prev := make(map[string]bool, len(toggles))
	for _, tg := range toggles {
		prev[tg.Name] = tg.Set(true)
	}
	t.Cleanup(func() {
		for _, tg := range toggles {
			tg.Set(prev[tg.Name])
		}
	})

	type entry struct {
		q    *shapegen.Query
		seed int64
		base *oracle.Result
	}
	var corpus []entry
	var baseErrors int
	for _, seed := range seeds {
		q := shapegen.New(seed, schema).Query()
		q.Seed = seed
		if bug := tmFuzzKnownDivergence(q); bug != "" {
			continue
		}
		res, err := tmRun(ctx, db, q.SQL())
		if err != nil {
			baseErrors++ // unsupported shapes are not this arm's business
			continue
		}
		corpus = append(corpus, entry{q: q, seed: seed, base: res})
	}
	if len(corpus) == 0 {
		t.Fatal("no generated query produced a baseline result")
	}
	// A fixed absolute floor doesn't fit here the way tmOptEngagementFloor
	// does: WADJET_TM_FUZZ_SEED_COUNT/START deliberately narrow or move the
	// window for local hunting, so the floor has to scale with the window
	// actually requested rather than a corpus size fixed at compile time.
	// Same 90% rationale as tmOptEngagementFloor: catches a regression that
	// makes most of the window newly unsupported without tripping on the
	// ordinary one-seed drift a plan-shape change can cause.
	if fuzzOptFloor := len(seeds) * 9 / 10; len(corpus) < fuzzOptFloor {
		t.Errorf("only %d of %d generated queries produced a baseline result (%d errored), below "+
			"the floor of %d (90%% of the window). A baseline error for a genuinely unsupported "+
			"shape is not this arm's business, but a regression that makes most of the window "+
			"error would shrink this gate silently instead of failing it — find out what stopped "+
			"answering before lowering the floor.", len(corpus), len(seeds), baseErrors, fuzzOptFloor)
	}
	t.Logf("type-matrix fuzz (optimization invariance): %d generated queries × %d configurations "+
		"(%d baseline errors)", len(corpus), len(toggles)+1, baseErrors)

	optRatchet := newTMRatchet()
	run := func(label string, off []*optswitch.Toggle) {
		t.Run(label, func(t *testing.T) {
			for _, tg := range off {
				tg.Set(false)
			}
			defer func() {
				for _, tg := range off {
					tg.Set(true)
				}
			}()
			for _, e := range corpus {
				sql := e.q.SQL()
				key := strconv.FormatInt(e.seed, 10) + "/" + label
				pin, pinned := tmFuzzOptPins[key]
				report := func(format string, args ...any) {
					if pinned {
						optRatchet.observe(key, true)
						t.Logf("known divergence, tracked in %s — NOT gated:\n  %s\n  "+format,
							append([]any{pin.Issue, pin.Reason}, args...)...)
						return
					}
					t.Errorf(format, args...)
				}

				attempts := 1
				if n, ok := tmFuzzIntermittentOptRetries[key]; ok {
					attempts = n
				}
				diverged := false
				for attempt := 1; attempt <= attempts && !diverged; attempt++ {
					got, err := tmRun(ctx, db, sql)
					if err != nil {
						report("seed %d FAILED with %s disabled (attempt %d/%d): %v\n  SQL: %s",
							e.seed, label, attempt, attempts, err, sql)
						diverged = true
						continue
					}
					if diff := oracle.Compare(e.base, got, e.q.CompareSpec()); diff != "" {
						report("seed %d (shape %s) diverges with %s disabled (attempt %d/%d):\n"+
							"  SQL: %s\n  %s\n  baseline: %s\n  got: %s",
							e.seed, e.q.Shape, label, attempt, attempts, sql, diff,
							tmRender(e.base, 3), tmRender(got, 3))
						diverged = true
					}
				}
				if !diverged && pinned {
					optRatchet.observe(key, false)
				}
			}
		})
	}
	for _, tg := range toggles {
		run(tg.Name, []*optswitch.Toggle{tg})
	}
	run("all-off", toggles)
	// The seeds a narrowed window does not run cannot be judged; drop their
	// pins from the ratchet rather than reporting them as stale.
	inWindow := map[string]typematrix.Pin{}
	for _, e := range corpus {
		prefix := strconv.FormatInt(e.seed, 10) + "/"
		for k, p := range tmFuzzOptPins {
			if strings.HasPrefix(k, prefix) {
				inWindow[k] = p
			}
		}
	}
	optRatchet.finish(t, inWindow, "type-matrix fuzz optimization-invariance")
}
