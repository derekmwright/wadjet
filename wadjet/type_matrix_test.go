package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The type-coverage gates over the embeddable engine.
//
// TestTypeMatrixBatchReuse is the BATCH-REUSE ADVERSARIAL gate: every corpus
// query is answered twice over the same data, once with the batch pool
// behaving normally and once with poison-on-release armed (see
// internal/engine/batch/poison.go), and the two answers must be identical.
//
// A pooled batch's storage is undefined the moment Release() hands it back;
// anything that keeps a value past that call has to own it. Nothing enforced
// that, and whether a violation produced a wrong answer depended on what the
// next batch happened to write over the same bytes — data-dependent, invisible
// at small scale, invisible to every corpus gate. Poison mode makes the
// undefined behaviour defined and loud, and this gate is the comparison.
//
// It reproduces the defect it was written for on the first entry it reaches:
// MIN_BY over a BYTES column comes back as poison bytes, because
// (*Vector).GetValue's TypeBytes arm returns a slice aliasing the column
// arena. Pinned below as #391 until that fix lands.
//
// TestTypeMatrixOptimizationInvariance extends the #287 kill-switch
// differential to the 19 types the TPC-H and ClickBench corpora do not have.

// tmPins are the known divergences of the poison arm, per corpus entry.
//
// The comparison STILL RUNS for a pinned entry: a divergence is logged instead
// of failed, and an entry that starts AGREEING fails, so deleting the pin is
// the proof the fix landed (ADR-0013 §Pins). A pin naming an entry the corpus
// does not contain also fails — a renamed entry must not silently take its
// pin's exemption with it.
var tmPins = map[string]typematrix.Pin{}

// tmPinPrefixes pins a whole family of entries by name prefix, for a defect
// that is a property of the TYPE rather than of one query shape. Same ratchet:
// every entry a prefix covers is still compared, and if EVERY entry under a
// prefix agrees, the prefix fails.
//
// #391 ((*Vector).GetValue's TypeBytes arm aliasing the column arena) is fixed
// on main (fa22f72); its three prefixes (minby_c_bytes, maxby_c_bytes,
// minby_scalar_c_bytes) are deleted rather than left in place, per the ratchet
// in tmRatchet.finish — the entries they covered now agree, which IS the
// fix's proof.
var tmPinPrefixes = map[string]typematrix.Pin{}

// tmOpen loads the type matrix into an embedded DB.
func tmOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := typematrix.Schema()
	if err := db.CreateTable(ctx, typematrix.Table, schema, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.Table, err)
	}
	rows := typematrix.Data(typematrix.Rows)
	ing := db.NewIngester(typematrix.Table, schema, nil, ingest.Config{
		MaxBufferRows: typematrix.Rows + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest %s: %v", typematrix.Table, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", typematrix.Table, err)
	}

	nested := typematrix.NestedSchema()
	if err := db.CreateTable(ctx, typematrix.Nested, nested, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.Nested, err)
	}
	ning := db.NewIngester(typematrix.Nested, nested, nil, ingest.Config{
		MaxBufferRows: typematrix.Rows + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ning.Ingest(ctx, typematrix.NestedData(typematrix.Rows)); err != nil {
		t.Fatalf("ingest %s: %v", typematrix.Nested, err)
	}
	if err := ning.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", typematrix.Nested, err)
	}

	dim := typematrix.DimSchema()
	if err := db.CreateTable(ctx, typematrix.Dim, dim, nil); err != nil {
		t.Fatalf("create %s: %v", typematrix.Dim, err)
	}
	ding := db.NewIngester(typematrix.Dim, dim, nil, ingest.Config{MaxBufferRows: 64, RowGroupSize: 4})
	if err := ding.Ingest(ctx, typematrix.DimData()); err != nil {
		t.Fatalf("ingest %s: %v", typematrix.Dim, err)
	}
	if err := ding.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", typematrix.Dim, err)
	}
	return db
}

// tmRun answers one query, converting a panic into a reportable failure rather
// than taking the test process down.
func tmRun(ctx context.Context, db *DB, sql string) (res *oracle.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := db.Query(ctx, sql)
	if qerr != nil {
		return nil, qerr
	}
	return &oracle.Result{Columns: out.Columns, Rows: out.Rows}, nil
}

// tmPinFor returns the pin covering an entry, if any.
func tmPinFor(name string) (typematrix.Pin, string, bool) {
	if p, ok := tmPins[name]; ok {
		return p, name, true
	}
	for prefix, p := range tmPinPrefixes {
		if strings.HasPrefix(name, prefix) {
			return p, prefix, true
		}
	}
	return typematrix.Pin{}, "", false
}

// tmRatchet accumulates which pins were exercised and which of them agreed, so
// a pin that has outlived its bug fails.
type tmRatchet struct {
	covered  map[string]bool
	diverged map[string]bool
}

func newTMRatchet() *tmRatchet {
	return &tmRatchet{covered: map[string]bool{}, diverged: map[string]bool{}}
}

func (r *tmRatchet) observe(key string, diverged bool) {
	r.covered[key] = true
	if diverged {
		r.diverged[key] = true
	}
}

// finish is the two-way ratchet.
//
// Per PIN: a pin nothing exercised is a pin whose entry no longer exists, or
// was renamed out from under it, so it exempts nothing and hides nothing.
//
// Per ISSUE, not per pin: the "this bug is fixed, delete the pin" half is
// checked once per issue number, satisfied by ANY of that issue's pins having
// diverged. Some of the defects here are NONDETERMINISTIC by nature — #391
// answers with whatever the allocator wrote over a freed arena, so a given
// entry may agree by luck on a given run. Requiring every pin to diverge every
// run would make the gate flap; requiring the issue to show itself somewhere
// keeps the ratchet exact, because a real fix silences every one of its pins
// at once.
//
// The trade this accepts (ADR-0013 amendment 2026-08-23): an issue with many
// pins can have most of them silently start agreeing — e.g. #396's 37 pins —
// while the ratchet stays green on the strength of one. That is not logged as
// a pass/fail signal (only the whole issue is), so finish() also logs a
// "k of m pins still diverge" summary per issue on every run: a real fix
// narrowing from 37/37 to 1/37 is visible in the test log long before the
// last pin agrees and the ratchet actually fires.
func (r *tmRatchet) finish(t *testing.T, pins map[string]typematrix.Pin, kind string) {
	t.Helper()
	keys := make([]string, 0, len(pins))
	for k := range pins {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	issueDiverged := map[string]bool{}
	issueReason := map[string]string{}
	issueTotal := map[string]int{}
	issueDivergedCount := map[string]int{}
	var issues []string
	for _, k := range keys {
		p := pins[k]
		if !r.covered[k] {
			t.Errorf("%s pin %q (%s) matches no corpus entry — the entry was renamed or "+
				"removed, so the pin now exempts nothing and hides nothing. Delete it or fix the name.",
				kind, k, p.Issue)
			continue
		}
		if _, seen := issueTotal[p.Issue]; !seen {
			issues = append(issues, p.Issue)
		}
		issueTotal[p.Issue]++
		if r.diverged[k] {
			issueDivergedCount[p.Issue]++
			issueDiverged[p.Issue] = true
		}
		if p.GatedBy != "" {
			// Another arm owns the must-still-diverge half for this issue —
			// this pin does not have to show up here for the issue to count
			// as still broken.
			issueDiverged[p.Issue] = true
		} else if issueReason[p.Issue] == "" {
			issueReason[p.Issue] = p.Reason
		}
	}
	sort.Strings(issues)
	for _, issue := range issues {
		t.Logf("%s issue %s: %d of %d pins still diverge this run", kind, issue,
			issueDivergedCount[issue], issueTotal[issue])
		// An issue whose every pin names GatedBy has no Reason recorded here
		// — this arm was never the one enforcing its must-still-diverge half.
		if reason := issueReason[issue]; reason != "" && !issueDiverged[issue] {
			t.Errorf("every %s pin for %s now AGREES, so it is FIXED:\n  %s\n"+
				"Delete those pins so the entries are gated again.", kind, issue, reason)
		}
	}
}

// TestTypeMatrixBatchReuse compares each corpus query answered with the batch
// pool poisoned on release against the same query answered normally.
func TestTypeMatrixBatchReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping type-matrix gate under -short — the dedicated CI step runs it without -short")
	}
	ctx := context.Background()
	db := tmOpen(t)
	corpus := typematrix.Corpus()
	t.Logf("batch-reuse gate: %d queries over %d types × 2 arms (clean pool, poisoned pool)",
		len(corpus), len(typematrix.Columns()))

	ratchet := newTMRatchet()
	var diverged, unsupported, compared, noReuse int
	for _, q := range corpus {
		q := q
		t.Run(q.Name, func(t *testing.T) {
			if p, ok := tmCrashPins[q.Name]; ok {
				// A dead process compares nothing. TestTypeMatrixNoProcessKillers
				// owns these entries and fails when one stops crashing, so the
				// skip here cannot outlive the fix.
				t.Skipf("process killer, tracked in %s — gated by TestTypeMatrixNoProcessKillers:\n  %s",
					p.Issue, p.Reason)
			}
			clean, cerr := tmRun(ctx, db, q.SQL)

			before := batch.PoisonedBatches()
			prev := batch.SetPoisonOnRelease(true)
			dirty, derr := tmRun(ctx, db, q.SQL)
			batch.SetPoisonOnRelease(prev)
			poisoned := batch.PoisonedBatches() - before

			pin, pinKey, pinned := tmPinFor(q.Name)
			fail := func(format string, args ...any) {
				if pinned {
					ratchet.observe(pinKey, true)
					t.Logf("known divergence, tracked in %s — NOT gated:\n  %s\n  "+format,
						append([]any{pin.Issue, pin.Reason}, args...)...)
					return
				}
				t.Errorf(format, args...)
			}

			if cerr != nil {
				// A query the engine does not support is not this gate's
				// business — but the two arms must fail the same way, or the
				// poison changed the CONTROL flow, not just the data.
				unsupported++
				if derr == nil {
					fail("poisoned run answered a query the clean run rejected: %v\n  SQL: %s", cerr, q.SQL)
				}
				t.Skipf("engine rejects this shape on both arms: %v", cerr)
			}
			if derr != nil {
				fail("poisoned run FAILED on a query the clean run answered (%d rows): %v\n  SQL: %s",
					len(clean.Rows), derr, q.SQL)
				return
			}
			if poisoned == 0 {
				// Not a failure: a plan that never returns a batch to a pool
				// cannot exhibit a reuse defect, and several shapes Detach
				// every batch on the way through. It IS counted, because a
				// suite where nothing recycles proves nothing — see the
				// engagement floor at the end.
				noReuse++
				return
			}
			compared++
			if diff := oracle.Compare(clean, dirty, oracle.CompareSpec{Mode: q.Mode}); diff != "" {
				diverged++
				fail("BATCH-REUSE DIVERGENCE (%d batches poisoned): a value survived past the "+
					"Release() that made it undefined\n  SQL: %s\n  %s\n  clean:    %s\n  poisoned: %s",
					poisoned, q.SQL, diff, tmRender(clean, 3), tmRender(dirty, 3))
				return
			}
			if pinned {
				ratchet.observe(pinKey, false)
			}
		})
	}
	ratchet.finish(t, tmPinPrefixes, "batch-reuse prefix")
	ratchet.finish(t, tmPins, "batch-reuse")
	t.Logf("batch-reuse gate: %d entries actually recycled a batch, %d diverged, "+
		"%d recycled nothing, %d shapes unsupported", compared, diverged, noReuse, unsupported)
	if compared < tmReuseEngagementFloor {
		t.Errorf("only %d corpus entries returned a batch to a pool, below the floor of %d. "+
			"The poisoned arm compares nothing on the rest, so this gate has quietly stopped "+
			"testing what it exists to test — find out which plans stopped recycling before "+
			"lowering the floor.", compared, tmReuseEngagementFloor)
	}
}

// tmReuseEngagementFloor is how many corpus entries must actually return a
// batch to a pool for this gate to be doing its job. It is a RATCHET, not a
// target: raise it when a change makes more plans recycle, and treat a drop as
// the finding it is.
//
// It is well below the corpus size on purpose. Most plans in this corpus put a
// projection above the scan, and the projection's augment path
// (internal/planner/physical/plan.go:8020) calls in.Detach() on its input — so
// the scan's pooled batch never goes back to its pool, and there is no reuse
// to poison. That is a real property of the engine, not a harness limitation.
//
// Set to 90% of the 205 entries actually measured recycling a batch
// (2026-08-23, deterministic across runs — this corpus has no randomized
// planning). 90%, not the measured count itself, so the floor still catches a
// regression that stops a chunk of the corpus from recycling without tripping
// on the ordinary one-or-two-entry drift a plan-shape change can cause.
const tmReuseEngagementFloor = 184

// tmOptEngagementFloor is the equivalent floor for
// TestTypeMatrixOptimizationInvariance's baseline pass: how many corpus
// entries must produce a baseline result before the per-toggle comparisons
// even start. Same rationale as tmReuseEngagementFloor — 90% of the 239
// entries measured producing a baseline (2026-08-23) — so a regression that
// makes most of the corpus newly unsupported shrinks the comparison set
// loudly instead of silently.
const tmOptEngagementFloor = 215

// tmOptPins are the known divergences of the optimization-invariance arm,
// keyed "<corpus entry>/<toggle>". Same two-way ratchet as tmPins: the
// comparison runs, a pinned divergence is logged, and a pin that starts
// agreeing (or names an entry/toggle pair that no longer occurs) FAILS.
var tmOptPins = map[string]typematrix.Pin{
	"groupby_c_ipv6/partitioned-agg": {
		Issue: "#395",
		Reason: "GROUP BY an IPV6 or UUID column loses the KEY VALUE with partitioned " +
			"aggregation disabled: every output key is the empty string while the counts stay " +
			"right. The row count matches, so only a value-level compare sees it.",
	},
	"distinct_c_ipv6/partitioned-agg": {
		Issue:  "#395",
		Reason: "Same lost IPV6 key value, reached through DISTINCT.",
	},
	"groupby_c_uuid/partitioned-agg": {
		Issue:  "#395",
		Reason: "Same lost key value for UUID.",
	},
	"distinct_c_uuid/partitioned-agg": {
		Issue:  "#395",
		Reason: "Same lost key value for UUID, reached through DISTINCT.",
	},
}

// tmOptPinPrefixes pins every toggle of one corpus entry. Used when the entry's
// answer is not a function of the query at all, so naming toggles one by one
// would be recording noise rather than knowledge.
//
// #391 is fixed on main (fa22f72); its three prefixes (minby_scalar_c_bytes/,
// minby_c_bytes/, maxby_c_bytes/) are deleted along with tmPinPrefixes above.
var tmOptPinPrefixes = map[string]typematrix.Pin{}

// TestTypeMatrixOptimizationInvariance is the #287 kill-switch differential
// over the type matrix: every corpus query answered with all optimizations on,
// then once per registered switch with that one off. The TPC-H arm of the same
// contract can only see Int32/Float64/String; this one sees all 22 types.
func TestTypeMatrixOptimizationInvariance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping type-matrix gate under -short — the dedicated CI step runs it without -short")
	}
	ctx := context.Background()
	db := tmOpen(t)
	corpus := typematrix.Corpus()

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
		q    typematrix.Query
		base *oracle.Result
	}
	var base []entry
	var baseErrors int
	for _, q := range corpus {
		if _, ok := tmCrashPins[q.Name]; ok {
			continue // see TestTypeMatrixNoProcessKillers
		}
		res, err := tmRun(ctx, db, q.SQL)
		if err != nil {
			baseErrors++ // unsupported shapes are not this arm's business
			continue
		}
		base = append(base, entry{q: q, base: res})
	}
	if len(base) == 0 {
		t.Fatal("no corpus query produced a baseline result")
	}
	t.Logf("type-matrix optimization invariance: %d queries × %d configurations (%d baseline errors)",
		len(base), len(toggles)+1, baseErrors)
	if len(base) < tmOptEngagementFloor {
		t.Errorf("only %d corpus entries produced a baseline result (%d errored), below the floor "+
			"of %d. A baseline error for a genuinely unsupported shape is not this arm's business, "+
			"but a regression that makes most of the corpus error would shrink this gate silently "+
			"instead of failing it — find out what stopped answering before lowering the floor.",
			len(base), baseErrors, tmOptEngagementFloor)
	}

	ratchet := newTMRatchet()
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
			for _, e := range base {
				key := e.q.Name + "/" + label
				pin, pinned := tmOptPins[key]
				if !pinned {
					for prefix, p := range tmOptPinPrefixes {
						if strings.HasPrefix(key, prefix) {
							pin, key, pinned = p, prefix, true
							break
						}
					}
				}
				report := func(format string, args ...any) {
					if pinned {
						ratchet.observe(key, true)
						t.Logf("known divergence, tracked in %s — NOT gated:\n  %s\n  "+format,
							append([]any{pin.Issue, pin.Reason}, args...)...)
						return
					}
					t.Errorf(format, args...)
				}
				got, err := tmRun(ctx, db, e.q.SQL)
				if err != nil {
					report("%s FAILED with %s disabled: %v\n  SQL: %s", e.q.Name, label, err, e.q.SQL)
					continue
				}
				if diff := oracle.Compare(e.base, got, oracle.CompareSpec{Mode: e.q.Mode}); diff != "" {
					report("%s diverges with %s disabled:\n  SQL: %s\n  %s\n  baseline: %s\n  got: %s",
						e.q.Name, label, e.q.SQL, diff, tmRender(e.base, 3), tmRender(got, 3))
					continue
				}
				if pinned {
					ratchet.observe(key, false)
				}
			}
		})
	}
	for _, tg := range toggles {
		run(tg.Name, []*optswitch.Toggle{tg})
	}
	run("all-off", toggles)
	ratchet.finish(t, tmOptPinPrefixes, "optimization-invariance prefix")
	ratchet.finish(t, tmOptPins, "optimization-invariance")
}

// tmRender renders up to n rows for a failure message.
func tmRender(res *oracle.Result, n int) string {
	if res == nil {
		return "<no result>"
	}
	if len(res.Rows) == 0 {
		return "<0 rows>"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d rows %v", len(res.Rows), res.Columns)
	for i, row := range res.Rows {
		if i >= n {
			fmt.Fprintf(&sb, "\n      ... %d more", len(res.Rows)-n)
			break
		}
		sb.WriteString("\n      ")
		for j, c := range res.Columns {
			if j > 0 {
				sb.WriteString(" | ")
			}
			fmt.Fprintf(&sb, "%s=%v", c, tmCell(row[c]))
		}
	}
	return sb.String()
}

// tmCell renders one cell, quoting bytes so poison (0xA5 filler) is legible in
// a failure message rather than arriving as replacement characters.
func tmCell(v any) string {
	if b, ok := v.([]byte); ok {
		return fmt.Sprintf("%q", b)
	}
	return fmt.Sprint(v)
}
