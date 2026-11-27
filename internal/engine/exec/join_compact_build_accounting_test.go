// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The compact builds' accounting gates (#1706, line 1706p): a key-only
// semi/anti build (parallel and serial) and the distinct-pair NE build
// reserve every allocation before making it, the reservation equals what they
// hold at every synchronized point, a refusal is an honest budget error that
// leaves nothing behind, and an NE build that would not fit takes the spilling
// generic build and answers the same rows.

// cbRoute is one compact route under test.
type cbRoute struct {
	name    string
	workers int  // key-only workers; 1 = the serial route
	ne      bool // the distinct-pair NE build
	want    string
}

func cbRoutes() []cbRoute {
	return []cbRoute{
		{"parallel", 4, false, "parallel key-only"},
		{"serial", 1, false, "serial key-only"},
		{"ne", 0, true, "ne"},
	}
}

// withKeyOnlyWorkers selects the key-only route for one test.
func withKeyOnlyWorkers(t *testing.T, n int) {
	t.Helper()
	if n <= 0 {
		return
	}
	prev := keyOnlyBuildWorkers
	keyOnlyBuildWorkers = func() int { return n }
	t.Cleanup(func() { keyOnlyBuildWorkers = prev })
}

// cbJoin is a compact join of type jt over src's schema. A NE join is given
// no spill manager unless spill is set, so it cannot decline.
func cbJoin(t *testing.T, jt JoinType, src *cbGenSource, tr *memory.Tracker, spill bool) *HashJoin {
	t.Helper()
	hj := NewHashJoin(jt, []string{"k"}, []string{"k"})
	if src.ne {
		hj.SemiAntiFilter = BuildNETestFilter("pv", "v")
		hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
		hj.ProbeSchemaHint = cbProbeSchema(src)
		hj.BuildSchemaHint = src.schema
		hj.BuildStoreCols = []string{"k", "v"}
	} else {
		hj.SemiAntiKeyOnly = true
	}
	hj.BuildRowHint = int64(src.n)
	hj.MemTracker = tr
	if spill {
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sm.Cleanup() })
		hj.Spill = sm
	}
	return hj
}

func cbProbeSchema(src *cbGenSource) []parquet.Column {
	kt := parquet.TypeInt64
	if src.str {
		kt = parquet.TypeString
	}
	cols := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}, {Name: "k", Type: kt}}
	if src.ne {
		cols = append(cols, parquet.Column{Name: "pv", Type: parquet.TypeInt64})
	}
	return cols
}

// cbProbeRows probes every key the build can hold plus some it cannot, a
// NULL key, and for NE every value 0..3 and a NULL value.
func cbProbeRows(src *cbGenSource) []map[string]any {
	maxKey := int64(src.n/max(src.dup, 1)) + 20
	var rows []map[string]any
	id := int64(0)
	add := func(k any, pv any) {
		r := map[string]any{"id": id, "k": k}
		if src.ne {
			r["pv"] = pv
		}
		rows = append(rows, r)
		id++
	}
	step := max(maxKey/3000, 1)
	for k := int64(0); k < maxKey; k += step {
		var key any = k
		if src.str {
			key = cbStrKey(k)
		}
		if src.ne {
			for _, pv := range []any{int64(0), int64(1), int64(2), int64(3), nil} {
				add(key, pv)
			}
		} else {
			add(key, nil)
		}
	}
	add(nil, int64(1))
	return rows
}

// cbExpected is the answer computed from the generated rows themselves:
// EXISTS a live build row with the probe's key (and, for NE, a non-NULL value
// different from the probe's non-NULL value). Semi emits the rows where it
// holds, anti the others.
func cbExpected(src *cbGenSource, jt JoinType, probe []map[string]any) []string {
	vals := map[string]map[int64]bool{}
	for i := 0; i < src.n; i++ {
		if !src.live(i) {
			continue
		}
		r := src.row(i)
		if r.keyNull {
			continue
		}
		k := fmt.Sprint(r.key)
		if src.str {
			k = cbStrKey(r.key)
		}
		if vals[k] == nil {
			vals[k] = map[int64]bool{}
		}
		if src.ne && !r.valNull {
			vals[k][r.val] = true
		}
	}
	var out []string
	for _, p := range probe {
		exists := false
		if p["k"] != nil {
			set, ok := vals[fmt.Sprint(p["k"])]
			switch {
			case !ok:
			case !src.ne:
				exists = true
			case p["pv"] != nil:
				for v := range set {
					if v != p["pv"].(int64) {
						exists = true
					}
				}
			}
		}
		if exists == (jt == SemiJoin) {
			out = append(out, fmt.Sprint(p["id"]))
		}
	}
	sort.Strings(out)
	return out
}

// cbProbe runs the probe and returns the emitted ids, sorted.
func cbProbe(t *testing.T, hj *HashJoin, src *cbGenSource) []string {
	t.Helper()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(cbProbeSchema(src), cbProbeRows(src)),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	ids := make([]string, 0, len(sink.Rows))
	for _, r := range sink.Rows {
		ids = append(ids, fmt.Sprint(r["id"]))
	}
	sort.Strings(ids)
	return ids
}

func cbSameIDs(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s: %d rows, want %d (first got %v, first want %v)", what, len(got), len(want),
			got[:min(5, len(got))], want[:min(5, len(want))])
	}
}

// cbForced is every forced purpose's outstanding bytes.
func cbForced(tr *memory.Tracker) []int64 {
	var out []int64
	for p := memory.ForcePurpose(0); p < memory.NumForcePurposes; p++ {
		out = append(out, tr.ForcedFor(p))
	}
	return out
}

// cbPeer is a live peer on the same tracker: ordinary and forced charge that
// must survive anything the join does.
type cbPeer struct {
	tr     *memory.Tracker
	used   int64
	forced []int64
}

func newCBPeer(t *testing.T, tr *memory.Tracker) *cbPeer {
	t.Helper()
	if err := tr.Reserve(48 << 10); err != nil {
		t.Fatal(err)
	}
	tr.ForceReserveFor(16<<10, memory.ForceJoinIndex)
	return &cbPeer{tr: tr, used: tr.Used(), forced: cbForced(tr)}
}

// requireOnlyPeer asserts the tracker holds exactly the peer's charge under
// every purpose: the join holds nothing.
func (p *cbPeer) requireOnlyPeer(t *testing.T, when string) {
	t.Helper()
	if got := p.tr.Used(); got != p.used {
		t.Fatalf("%s: tracker holds %d, the live peer %d (%d stranded)", when, got, p.used, got-p.used)
	}
	if got := cbForced(p.tr); fmt.Sprint(got) != fmt.Sprint(p.forced) {
		t.Fatalf("%s: forced purposes %v, the live peer's %v", when, got, p.forced)
	}
}

// requireDropped asserts a failed compact build kept no state.
func requireDropped(t *testing.T, hj *HashJoin) {
	t.Helper()
	if n := hj.compactCharged.Load(); n != 0 {
		t.Fatalf("compactCharged = %d after the failure", n)
	}
	for i := range hj.parts {
		if hj.parts[i].ints != nil || hj.parts[i].strs != nil {
			t.Fatalf("part %d still holds a key table after the failure", i)
		}
	}
	if hj.bloom != nil || hj.nePairs != nil {
		t.Fatal("bloom or NE pairs survived the failure")
	}
}

type cbShape struct {
	name string
	mk   func(ne bool) *cbGenSource
}

func cbShapes() []cbShape {
	gen := func(n int, str bool, f func(*cbGenSource)) func(bool) *cbGenSource {
		return func(ne bool) *cbGenSource {
			s := newCBGen(n, str && !ne, ne)
			if f != nil {
				f(s)
			}
			return s
		}
	}
	return []cbShape{
		{"int-distinct", gen(60_000, false, nil)},
		{"int-dup-nulls", gen(60_000, false, func(s *cbGenSource) { s.dup, s.nulls = 3, 97 })},
		{"int-selected", gen(60_000, false, func(s *cbGenSource) { s.sel, s.dup = true, 2 })},
		{"str", gen(30_000, true, func(s *cbGenSource) { s.dup, s.nulls = 2, 101 })},
		{"one-row", gen(1, false, nil)},
		{"empty", gen(0, false, nil)},
		{"null-only", gen(5000, false, func(s *cbGenSource) { s.nulls = 1 })},
	}
}

// TestCompactBuildHeldEqualsCharged: at every synchronized point of every
// compact route the join's charge equals the bytes an independent enumeration
// of its structures finds, the build answers the generated rows' answer, and
// Close leaves the tracker at the live peer's balance under every purpose.
func TestCompactBuildHeldEqualsCharged(t *testing.T) {
	for _, r := range cbRoutes() {
		for _, sh := range cbShapes() {
			if r.ne && sh.name == "str" {
				continue // the NE build needs an integer key
			}
			for _, budget := range []int64{0, 64 << 20} {
				for _, jt := range []JoinType{SemiJoin, AntiJoin} {
					name := fmt.Sprintf("%s/%s/budget=%d/%s", r.name, sh.name, budget, map[JoinType]string{SemiJoin: "semi", AntiJoin: "anti"}[jt])
					t.Run(name, func(t *testing.T) {
						withKeyOnlyWorkers(t, r.workers)
						src := sh.mk(r.ne)
						tr := memory.NewTracker("cb", budget)
						peer := newCBPeer(t, tr)
						hj := cbJoin(t, jt, src, tr, true)
						points, routes := 0, map[string]bool{}
						restore := SetPhaseHook(func(site PhaseSite, op any) {
							pt, ok := op.(*CompactBuildPoint)
							if !ok {
								return
							}
							points++
							routes[pt.Route] = true
							held, charged := cbModel(pt), pt.Join.compactCharged.Load()
							if held != charged {
								t.Errorf("%s: held %d, charged %d", site, held, charged)
							}
							if pt.Join.trackedMem != 0 {
								t.Errorf("%s: trackedMem %d: a compact build must not use the forced reconciliation", site, pt.Join.trackedMem)
							}
							if got := tr.Used() - peer.used; got != charged {
								t.Errorf("%s: tracker grew by %d, the join's charge is %d", site, got, charged)
							}
						})
						err := hj.Build(context.Background(), src)
						restore()
						if err != nil {
							t.Fatalf("build: %v", err)
						}
						if src.n > 0 {
							if points == 0 || !routes[r.want] || len(routes) != 1 {
								t.Fatalf("routes seen %v at %d points, want only %q", routes, points, r.want)
							}
						}
						if r.ne && src.n > 0 && !hj.neActive {
							t.Fatal("the NE build did not engage")
						}
						cbSameIDs(t, "answer", cbProbe(t, hj, src), cbExpected(src, jt, cbProbeRows(src)))
						if err := hj.Close(); err != nil {
							t.Fatal(err)
						}
						peer.requireOnlyPeer(t, "after Close")
					})
				}
			}
		}
	}
}

// TestCompactBuildRefusalIsHonestAndReleases: a compact build that does not
// fit fails with the budget error naming the join and that it cannot spill,
// and on the failure itself — before any Close — it holds no state and the
// tracker is back at the live peer's balance under every purpose.
func TestCompactBuildRefusalIsHonestAndReleases(t *testing.T) {
	for _, r := range cbRoutes() {
		for _, str := range []bool{false, true} {
			if r.ne && str {
				continue
			}
			t.Run(fmt.Sprintf("%s/str=%v", r.name, str), func(t *testing.T) {
				withKeyOnlyWorkers(t, r.workers)
				src := newCBGen(200_000, str, r.ne)
				tr := memory.NewTracker("cb", 1<<20)
				peer := newCBPeer(t, tr)
				hj := cbJoin(t, AntiJoin, src, tr, false) // no spill manager: NE cannot decline
				err := hj.Build(context.Background(), src)
				if err == nil {
					t.Fatal("a 200,000-key build answered under a 1 MiB budget")
				}
				if !errors.Is(err, memory.ErrMemoryExceeded) || !strings.Contains(err.Error(), "cannot spill") ||
					!strings.Contains(err.Error(), "anti join") {
					t.Fatalf("refusal does not name the budget, the join and that it cannot spill: %v", err)
				}
				requireDropped(t, hj)
				peer.requireOnlyPeer(t, "on the failure, before Close")
				if err := hj.Close(); err != nil {
					t.Fatal(err)
				}
				peer.requireOnlyPeer(t, "after Close")
			})
		}
	}
}

// TestCompactBuildFailureReleasesEverything: an error, a cancel or a panic in
// the build source, early or late, in any worker, leaves no state and no
// charge, on the failure itself.
func TestCompactBuildFailureReleasesEverything(t *testing.T) {
	type fault struct {
		name string
		arm  func(s *cbGenSource, cancel context.CancelFunc, at int)
	}
	faults := []fault{
		{"error", func(s *cbGenSource, _ context.CancelFunc, at int) { s.failAt = at }},
		{"cancel", func(s *cbGenSource, cancel context.CancelFunc, at int) {
			s.before = func(call int) {
				if call == at {
					cancel()
				}
			}
		}},
		{"panic", func(s *cbGenSource, _ context.CancelFunc, at int) {
			s.before = func(call int) {
				if call == at {
					panic("injected compact build source panic")
				}
			}
		}},
	}
	for _, r := range cbRoutes() {
		for _, f := range faults {
			for _, at := range []int{2, 20} {
				for _, str := range []bool{false, true} {
					if r.ne && str {
						continue
					}
					t.Run(fmt.Sprintf("%s/%s/at=%d/str=%v", r.name, f.name, at, str), func(t *testing.T) {
						withKeyOnlyWorkers(t, r.workers)
						src := newCBGen(100_000, str, r.ne)
						tr := memory.NewTracker("cb", 0)
						peer := newCBPeer(t, tr)
						hj := cbJoin(t, SemiJoin, src, tr, true)
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						f.arm(src, cancel, at)
						var err error
						func() {
							defer func() {
								if p := recover(); p != nil {
									err = fmt.Errorf("panic: %v", p)
								}
							}()
							err = hj.Build(ctx, src)
						}()
						if err == nil {
							t.Fatal("the build succeeded through the fault")
						}
						requireDropped(t, hj)
						peer.requireOnlyPeer(t, "on the failure, before Close")
						if err := hj.Close(); err != nil {
							t.Fatal(err)
						}
						peer.requireOnlyPeer(t, "after Close")
					})
				}
			}
		}
	}
}

// TestKeyOnlyMergeRefusalAndBloomOmission: the parallel merge's growth is
// refusable (a refusal mid-merge drops every table and every charge), and a
// refused bloom is omitted, not allocated uncharged: the build answers
// without it.
func TestKeyOnlyMergeRefusalAndBloomOmission(t *testing.T) {
	for _, str := range []bool{false, true} {
		t.Run(fmt.Sprintf("merge/str=%v", str), func(t *testing.T) {
			withKeyOnlyWorkers(t, 4)
			src := newCBGen(120_000, str, false)
			tr := memory.NewTracker("cb", 256<<20)
			peer := newCBPeer(t, tr)
			hj := cbJoin(t, SemiJoin, src, tr, false)
			// Hold the source a moment on every pull so the waiting workers
			// take turns (a mutex waiting past 1 ms hands off in order): on one
			// CPU one worker would otherwise drain the whole source, and a
			// merge with only empty donors allocates nothing to refuse.
			src.before = func(int) { time.Sleep(2 * time.Millisecond) }
			var squeeze int64
			var donors int
			merged := false
			restore := SetPhaseHook(func(site PhaseSite, op any) {
				switch site {
				case PhaseKeyOnlyLocals:
					for _, lb := range op.(*CompactBuildPoint).Locals {
						if (lb.intIndex != nil && lb.intIndex.Len() > 0) || (lb.strIndex != nil && lb.strIndex.Len() > 0) {
							donors++
						}
					}
					// Leave the merge almost no room.
					squeeze = tr.Budget() - tr.Used() - 1024
					if err := tr.Reserve(squeeze); err != nil {
						t.Error(err)
					}
				case PhaseKeyOnlyMerged:
					merged = true
				}
			})
			err := hj.Build(context.Background(), src)
			restore()
			if donors < 2 {
				t.Fatalf("only %d worker tables hold keys: the merge has nothing to grow by", donors)
			}
			// The refusal comes from the merge — after the locals, before the
			// merged point — whether its table growth or an arena chunk.
			if err == nil || !errors.Is(err, memory.ErrMemoryExceeded) || merged ||
				!(strings.Contains(err.Error(), "merged key table") || strings.Contains(err.Error(), "key-arena chunk")) {
				t.Fatalf("want the merge's refusal, got %v (merged point reached: %v)", err, merged)
			}
			tr.Release(squeeze)
			requireDropped(t, hj)
			peer.requireOnlyPeer(t, "on the merge refusal")
		})
		t.Run(fmt.Sprintf("bloom/str=%v", str), func(t *testing.T) {
			withKeyOnlyWorkers(t, 4)
			src := newCBGen(120_000, str, false)
			tr := memory.NewTracker("cb", 256<<20)
			peer := newCBPeer(t, tr)
			hj := cbJoin(t, SemiJoin, src, tr, false)
			var squeeze int64
			restore := SetPhaseHook(func(site PhaseSite, op any) {
				switch site {
				case PhaseKeyOnlyMerged:
					squeeze = tr.Budget() - tr.Used() - 1024
					if err := tr.Reserve(squeeze); err != nil {
						t.Error(err)
					}
				case PhaseCompactDone:
					pt := op.(*CompactBuildPoint)
					if held, ch := cbModel(pt), pt.Join.compactCharged.Load(); held != ch {
						t.Errorf("done: held %d, charged %d", held, ch)
					}
				}
			})
			before := CompactBloomsOmitted.Load()
			err := hj.Build(context.Background(), src)
			restore()
			tr.Release(squeeze)
			if err != nil {
				t.Fatalf("a refused bloom failed the build: %v", err)
			}
			if hj.bloom != nil || CompactBloomsOmitted.Load() != before+1 {
				t.Fatalf("bloom=%v omitted=%d: the refused bloom was not omitted", hj.bloom != nil, CompactBloomsOmitted.Load()-before)
			}
			cbSameIDs(t, "answer without a bloom", cbProbe(t, hj, src), cbExpected(src, SemiJoin, cbProbeRows(src)))
			hj.Close()
			peer.requireOnlyPeer(t, "after Close")
		})
	}
}

// TestNEBuildDeclinesToTheSpillingBuild: an NE-eligible join whose
// distinct-pair state is estimated not to fit takes the generic filtered,
// grace-partitioned build before reading a row, spills, and answers the rows
// the distinct-pair build answers unbudgeted — NULL keys, NULL values,
// one/two/three distinct values per key, an empty and a NULL-only build. A
// join with no estimate, no spill manager or room keeps the distinct-pair
// build. Either way the choice is already made when the source's first Next
// runs: it is a pre-consumption choice, never a switch after rows were folded
// into the distinct-pair state.
func TestNEBuildDeclinesToTheSpillingBuild(t *testing.T) {
	shapes := []struct {
		name string
		mk   func() *cbGenSource
	}{
		{"dup-nulls", func() *cbGenSource { s := newCBGen(200_000, false, true); s.dup, s.nulls = 3, 89; return s }},
		{"distinct", func() *cbGenSource { return newCBGen(150_000, false, true) }},
		{"selected", func() *cbGenSource { s := newCBGen(150_000, false, true); s.sel, s.dup = true, 4; return s }},
		{"empty", func() *cbGenSource { return newCBGen(0, false, true) }},
		{"null-only", func() *cbGenSource { s := newCBGen(5000, false, true); s.nulls = 1; return s }},
	}
	for _, sh := range shapes {
		for _, jt := range []JoinType{SemiJoin, AntiJoin} {
			t.Run(fmt.Sprintf("%s/%d", sh.name, jt), func(t *testing.T) {
				// Reference: the distinct-pair build, unbudgeted.
				ref := sh.mk()
				refJoin := cbJoin(t, jt, ref, memory.NewTracker("ref", 0), true)
				if err := refJoin.Build(context.Background(), ref); err != nil {
					t.Fatal(err)
				}
				if ref.n > 0 && !refJoin.neActive {
					t.Fatal("reference: the NE build did not engage")
				}
				want := cbProbe(t, refJoin, ref)
				refJoin.Close()
				cbSameIDs(t, "reference against the generated rows", want, cbExpected(ref, jt, cbProbeRows(ref)))

				src := sh.mk()
				src.n = max(src.n, 0)
				tr := memory.NewTracker("cb", 1<<20)
				hj := cbJoin(t, jt, src, tr, true)
				hj.BuildRowHint = 200_000 // the plan's estimate, whatever the rows
				declinedAtFirstRead := false
				src.before = func(call int) {
					if call == 1 {
						declinedAtFirstRead = hj.neDeclined
					}
				}
				before := CompactNEDeclined.Load()
				if err := hj.Build(context.Background(), src); err != nil {
					t.Fatalf("declined build: %v", err)
				}
				if !declinedAtFirstRead {
					t.Fatal("the decline was not made before the source's first Next: it must be a pre-consumption choice")
				}
				if CompactNEDeclined.Load() != before+1 || hj.neActive || !hj.neDeclined {
					t.Fatalf("not declined: declined=%d neActive=%v", CompactNEDeclined.Load()-before, hj.neActive)
				}
				if src.n >= 100_000 && (hj.spillState == nil || len(hj.spillState.spilledParts) == 0) {
					t.Fatal("the declined build did not spill under 1 MiB: the spilled route is untested")
				}
				cbSameIDs(t, "declined (spilling) build", cbProbe(t, hj, src), want)
				hj.Close()
				if tr.Used() != 0 {
					t.Fatalf("tracker holds %d after Close", tr.Used())
				}
			})
		}
	}
	keep := []struct {
		name  string
		setup func(hj *HashJoin)
	}{
		{"no-estimate", func(hj *HashJoin) { hj.BuildRowHint = 0 }},
		{"no-spill-manager", func(hj *HashJoin) { hj.Spill = nil }},
		{"room", func(hj *HashJoin) { hj.MemTracker = memory.NewTracker("roomy", 1<<30) }},
	}
	for _, k := range keep {
		t.Run("keeps/"+k.name, func(t *testing.T) {
			src := newCBGen(5_000, false, true)
			hj := cbJoin(t, SemiJoin, src, memory.NewTracker("cb", 1<<20), true)
			hj.BuildRowHint = 200_000
			k.setup(hj)
			firstRead := false
			src.before = func(call int) {
				if call == 1 {
					firstRead = true
					if hj.neDeclined {
						t.Errorf("declined at the first read")
					}
				}
			}
			before := CompactNEDeclined.Load()
			if err := hj.Build(context.Background(), src); err != nil {
				t.Fatal(err)
			}
			if !firstRead {
				t.Fatal("the build never read its source")
			}
			if CompactNEDeclined.Load() != before || !hj.neActive {
				t.Fatalf("declined=%d neActive=%v: the distinct-pair build should have run", CompactNEDeclined.Load()-before, hj.neActive)
			}
			cbSameIDs(t, "answer", cbProbe(t, hj, src), cbExpected(src, SemiJoin, cbProbeRows(src)))
			hj.Close()
		})
	}
}
