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
var tmPinPrefixes = map[string]typematrix.Pin{
	"minby_c_bytes": {
		Issue: "#391",
		Reason: "(*Vector).GetValue's TypeBytes arm (internal/engine/batch/vector.go:678) " +
			"returns v.BytesData.Value(i), a slice ALIASING the column arena. MIN_BY/MAX_BY " +
			"retain that box in minMaxByState.bestVal across every later batch, so once the " +
			"pool recycles the batch the aggregate answers with whatever was written over " +
			"those bytes. Being fixed on a separate branch.",
	},
	"maxby_c_bytes": {
		Issue:  "#391",
		Reason: "Same aliasing arm as minby_c_bytes; MAX_BY retains the same box.",
	},
	"minby_scalar_c_bytes": {
		Issue: "#391",
		Reason: "Same aliasing arm, on the whole-input (no GROUP BY) aggregate path — so the " +
			"defect is not specific to grouped aggregation.",
	},
}

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
func (r *tmRatchet) finish(t *testing.T, pins map[string]typematrix.Pin, kind string) {
	t.Helper()
	keys := make([]string, 0, len(pins))
	for k := range pins {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	issueDiverged := map[string]bool{}
	issueReason := map[string]string{}
	var issues []string
	for _, k := range keys {
		p := pins[k]
		if !r.covered[k] {
			t.Errorf("%s pin %q (%s) matches no corpus entry — the entry was renamed or "+
				"removed, so the pin now exempts nothing and hides nothing. Delete it or fix the name.",
				kind, k, p.Issue)
			continue
		}
		if p.GatedBy != "" {
			// Another arm owns the must-still-diverge half for this issue.
			issueDiverged[p.Issue] = true
			continue
		}
		if _, seen := issueReason[p.Issue]; !seen {
			issues = append(issues, p.Issue)
			issueReason[p.Issue] = p.Reason
		}
		if r.diverged[k] {
			issueDiverged[p.Issue] = true
		}
	}
	for _, issue := range issues {
		if !issueDiverged[issue] {
			t.Errorf("every %s pin for %s now AGREES, so it is FIXED:\n  %s\n"+
				"Delete those pins so the entries are gated again.", kind, issue, issueReason[issue])
		}
	}
}

// TestTypeMatrixBatchReuse compares each corpus query answered with the batch
// pool poisoned on release against the same query answered normally.
func TestTypeMatrixBatchReuse(t *testing.T) {
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
const tmReuseEngagementFloor = 200

// tmOptPins are the known divergences of the optimization-invariance arm,
// keyed "<corpus entry>/<toggle>". Same two-way ratchet as tmPins: the
// comparison runs, a pinned divergence is logged, and a pin that starts
// agreeing (or names an entry/toggle pair that no longer occurs) FAILS.
var tmOptPins = map[string]typematrix.Pin{
	"groupby_c_dec/agg-fast-paths": {
		Issue: "#394",
		Reason: "ORDER BY over a DECIMAL column sorts LEXICOGRAPHICALLY on one path and " +
			"NUMERICALLY on the other, so `10.001` sorts before `2.0002` with the fast paths on " +
			"and after with them off. kernel/sort.go's ResolveSortCompare has no DECIMAL arm and " +
			"its default returns a comparator that reports every row equal (kernel/sort.go:21), " +
			"while the other path falls through to compareAny on the FORMATTED string " +
			"(exec/sort.go:903). Same rows, different sequence: only the ordered compare sees it.",
	},
	"distinct_c_dec/agg-fast-paths": {
		Issue:  "#394",
		Reason: "Same DECIMAL ordering divergence, reached through DISTINCT.",
	},
	"groupby_c_dec/all-off": {
		Issue:  "#394",
		Reason: "Same DECIMAL ordering divergence, with every switch off.",
	},
	"distinct_c_dec/all-off": {
		Issue:  "#394",
		Reason: "Same DECIMAL ordering divergence, with every switch off.",
	},
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
var tmOptPinPrefixes = map[string]typematrix.Pin{
	"minby_scalar_c_bytes/": {
		Issue: "#391",
		Reason: "The BYTES aliasing defect, visible WITHOUT poison and under nearly every " +
			"toggle: MIN_BY(c_bytes, id) answers bytes-000000 in the baseline and a different " +
			"row's bytes whenever a switch perturbs allocation order, because the retained " +
			"alias reads whatever the pool wrote into those offsets by finalize time. WHICH " +
			"value comes back is a function of allocation order, not of the query — so this is " +
			"pinned for the whole entry rather than per toggle. Being fixed on a separate branch.",
		GatedBy: "TestTypeMatrixBatchReuse",
	},
	"minby_c_bytes/": {
		Issue:   "#391",
		Reason:  "Same aliasing defect, grouped form.",
		GatedBy: "TestTypeMatrixBatchReuse",
	},
	"maxby_c_bytes/": {
		Issue:   "#391",
		Reason:  "Same aliasing defect, grouped MAX_BY form.",
		GatedBy: "TestTypeMatrixBatchReuse",
	},
}

// TestTypeMatrixOptimizationInvariance is the #287 kill-switch differential
// over the type matrix: every corpus query answered with all optimizations on,
// then once per registered switch with that one off. The TPC-H arm of the same
// contract can only see Int32/Float64/String; this one sees all 22 types.
func TestTypeMatrixOptimizationInvariance(t *testing.T) {
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
	for _, q := range corpus {
		if _, ok := tmCrashPins[q.Name]; ok {
			continue // see TestTypeMatrixNoProcessKillers
		}
		res, err := tmRun(ctx, db, q.SQL)
		if err != nil {
			continue // unsupported shapes are not this arm's business
		}
		base = append(base, entry{q: q, base: res})
	}
	if len(base) == 0 {
		t.Fatal("no corpus query produced a baseline result")
	}
	t.Logf("type-matrix optimization invariance: %d queries × %d configurations",
		len(base), len(toggles)+1)

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
