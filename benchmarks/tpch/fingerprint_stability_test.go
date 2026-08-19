package tpch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/worker"
)

// Float stability of the SF100 fingerprints.
//
// A SUM at SF100 accumulates over ~600M rows, and the ORDER of accumulation
// varies with partitioning, worker count and run. A digest is brittle to
// exactly that: quantizing to N significant digits only helps until a value
// lands on a rounding boundary, where one ULP of noise flips the rendered
// digit. oracle.Fingerprint's mitigation is dual precision — 6 and 4
// significant digits, a match at EITHER counts — because a boundary hit at two
// independent quanta simultaneously is vanishingly rare. Whether that holds at
// SF100 is not something to assume, so this test measures it.
//
// It runs the correctness variants repeatedly under configurations that
// genuinely change accumulation order, and reports, per query, how many
// distinct fine and coarse digests appeared and whether the GATE's own
// comparison (FPEntry.Match against the first sample) held for every later
// sample. A query whose fine digest moves but whose coarse digest holds is
// working as designed; a query where Match fails is a query the SF100 gate
// would flake on.
//
//	# default: SF0.01, both arms, ~1 min
//	go test -run TestFingerprintStability ./benchmarks/tpch/
//
//	# the real measurement: SF1 single-process arm
//	WADJET_FP_STABILITY_SCALE=1 WADJET_FP_STABILITY_REPEATS=2 \
//	  go test -run TestFingerprintStability -timeout 90m -v ./benchmarks/tpch/
//
//	# add the distributed arm (worker counts 1/2/3 over the stage DAG)
//	WADJET_FP_STABILITY_DIST=1 go test -run TestFingerprintStability -v ./benchmarks/tpch/
func TestFingerprintStability(t *testing.T) {
	if testing.Short() {
		t.Skip("stability measurement runs the whole corpus many times")
	}
	sf := stabilityScale(t)
	repeats := envInt("WADJET_FP_STABILITY_REPEATS", 2)
	corpus := CorrectnessQueries(sf)

	samples := newStabilityLedger()
	ctx := context.Background()

	// Arm A: one process, accumulation order changed underneath the same
	// query. The toggles named here are the ones that decide HOW a sum is
	// accumulated — partitioned aggregation sums per hash partition and
	// combines, parallel emit walks partitions concurrently, the two-level
	// table changes which bucket a group lands in — so flipping them
	// reorders the additions without changing the answer.
	db := setupTPCHStreaming(t, sf)
	procs := runtime.GOMAXPROCS(0)
	armA := []struct {
		name string
		off  []string
		gomp int
	}{
		{name: "default", gomp: procs},
		{name: "no-partitioned-agg", off: []string{"partitioned-agg"}, gomp: procs},
		{name: "no-parallel-emit", off: []string{"parallel-emit"}, gomp: procs},
		{name: "no-two-level-ht", off: []string{"two-level-ht"}, gomp: procs},
		{name: "no-agg-fast-paths", off: []string{"agg-fast-paths"}, gomp: procs},
		{name: "gomaxprocs-1", gomp: 1},
		{name: "gomaxprocs-4", gomp: 4},
	}
	for _, cfg := range armA {
		restore := setToggles(t, cfg.off, false)
		prevProcs := runtime.GOMAXPROCS(cfg.gomp)
		for r := 0; r < repeats; r++ {
			label := fmt.Sprintf("single/%s/run%d", cfg.name, r+1)
			for _, q := range corpus {
				start := time.Now()
				res, err := db.Query(ctx, q.SQL)
				if err != nil {
					t.Errorf("%s %s: %v", label, q.Name, err)
					continue
				}
				samples.add(q, label, &oracle.Result{Columns: res.Columns, Rows: res.Rows})
				if r == 0 && cfg.name == "default" {
					t.Logf("%s %s: %d rows in %v", label, q.Name, len(res.Rows), time.Since(start).Round(time.Millisecond))
				}
			}
		}
		runtime.GOMAXPROCS(prevProcs)
		restore()
	}

	// Arm B: the stage DAG with different worker counts — the SF100 shape,
	// where each worker aggregates its own partition of the input and the
	// coordinator merges the partials. Different worker counts mean
	// different partial sums combined in a different order.
	if os.Getenv("WADJET_FP_STABILITY_DIST") == "1" {
		if sf > SF01 {
			t.Fatalf("the distributed arm materializes the whole fixture in memory; keep it at SF0.1 or below (got %v)", sf)
		}
		data := Generate(sf)
		for _, workers := range []int{1, 2, 3} {
			coord := setupStabilityCluster(t, ctx, data, workers)
			for r := 0; r < repeats; r++ {
				label := fmt.Sprintf("dag/%dworker/run%d", workers, r+1)
				for _, q := range corpus {
					res, err := coord.ExecuteSQL(ctx, q.SQL)
					if err != nil {
						t.Errorf("%s %s: %v", label, q.Name, err)
						continue
					}
					rows, err := res.Rows()
					res.Close()
					if err != nil {
						t.Errorf("%s %s: reading rows: %v", label, q.Name, err)
						continue
					}
					samples.add(q, label, &oracle.Result{Columns: res.Columns, Rows: rows})
				}
			}
		}
	}

	samples.report(t, sf)
}

// stabilityLedger collects one signature per (query, configuration) sample.
type stabilityLedger struct {
	order []string
	byQ   map[string][]stabilitySample
	q     map[string]FPQuery
}

type stabilitySample struct {
	label string
	sig   Signature
	// exact is a digest of the SAME rows rendered at full float precision.
	// It is the control: if it never moves, nothing in the configuration
	// actually reordered an accumulation and a stable fine digest proves
	// nothing about float stability. When it moves while fine/coarse hold,
	// the quantization is doing exactly the job it was added for.
	exact string
	// nums are every numeric cell in row order, so the report can state the
	// SIZE of the accumulation noise rather than only whether a digest
	// moved. That number is what extrapolates to SF100: a rendered digit
	// flips only when a value sits within the noise of a rounding boundary,
	// so noise/quantum is the per-cell flip probability.
	nums []float64
	// fine holds the rows rendered exactly as the digest hashes them, so a
	// failure can name the differing CELL instead of two opaque digests.
	fine []string
}

// renderRows mirrors internal/oracle's fingerprintCell at prec significant
// digits, including its exact-integer branch, so a diff over these strings is
// a diff of what the digest actually hashed.
func renderRows(res *oracle.Result, prec int) []string {
	out := make([]string, len(res.Rows))
	var sb strings.Builder
	for i, row := range res.Rows {
		sb.Reset()
		for j, col := range res.Columns {
			if j > 0 {
				sb.WriteByte('|')
			}
			switch v := row[col].(type) {
			case nil:
				sb.WriteString("<null>")
			case float64:
				sb.WriteString(renderFloat(v, prec))
			case float32:
				sb.WriteString(renderFloat(float64(v), prec))
			default:
				fmt.Fprintf(&sb, "%v", v)
			}
		}
		out[i] = sb.String()
	}
	return out
}

func renderFloat(f float64, prec int) string {
	if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
		return strconv.FormatInt(int64(f), 10)
	}
	if f == 0 {
		f = 0
	}
	return strconv.FormatFloat(f, 'g', prec, 64)
}

// numericCells extracts every numeric cell in row order.
func numericCells(res *oracle.Result) []float64 {
	out := make([]float64, 0, len(res.Rows)*len(res.Columns))
	for _, row := range res.Rows {
		for _, col := range res.Columns {
			switch v := row[col].(type) {
			case float64:
				out = append(out, v)
			case float32:
				out = append(out, float64(v))
			case int64:
				out = append(out, float64(v))
			case int32:
				out = append(out, float64(v))
			case int:
				out = append(out, float64(v))
			}
		}
	}
	return out
}

// orderMargin is the smallest relative gap between two ADJACENT rows'
// values in any column that runs monotonically down the result — i.e. a sort
// key. It is the other half of the risk model for a positional digest: rows
// swap when the accumulation noise exceeds the gap between their sort keys, so
// margin/noise is the safety factor. cols is the number of numeric cells per
// row.
//
// It is a heuristic on purpose: it needs no per-query key declaration, and a
// non-key column that happens to be monotone only makes the reported margin
// more conservative.
func orderMargin(nums []float64, cols int) float64 {
	if cols <= 0 || len(nums) < 2*cols {
		return math.Inf(1)
	}
	rows := len(nums) / cols
	best := math.Inf(1)
	for c := 0; c < cols; c++ {
		up, down, minGap := true, true, math.Inf(1)
		for r := 1; r < rows; r++ {
			a, b := nums[(r-1)*cols+c], nums[r*cols+c]
			switch {
			case b > a:
				down = false
			case b < a:
				up = false
			}
			scale := math.Max(math.Abs(a), math.Abs(b))
			if a != b && scale != 0 {
				if g := math.Abs(a-b) / scale; g < minGap {
					minGap = g
				}
			}
		}
		if (up || down) && minGap < best {
			best = minGap
		}
	}
	return best
}

// maxRelDiff is the largest relative difference between two same-shaped cell
// vectors, and the index where it occurred. -1 when the shapes differ.
func maxRelDiff(a, b []float64) (float64, int) {
	if len(a) != len(b) {
		return 0, -1
	}
	worst, at := 0.0, 0
	for i := range a {
		if a[i] == b[i] {
			continue
		}
		scale := math.Max(math.Abs(a[i]), math.Abs(b[i]))
		if scale == 0 {
			continue
		}
		if d := math.Abs(a[i]-b[i]) / scale; d > worst {
			worst, at = d, i
		}
	}
	return worst, at
}

// exactDigest renders res at full float precision (shortest round-trip form)
// and digests it, ordered the way q is compared.
func exactDigest(res *oracle.Result, q FPQuery) string {
	rows := make([]string, len(res.Rows))
	var sb strings.Builder
	for i, row := range res.Rows {
		sb.Reset()
		for j, col := range res.Columns {
			if j > 0 {
				sb.WriteByte('|')
			}
			switch v := row[col].(type) {
			case nil:
				sb.WriteString("<null>")
			case float64:
				sb.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
			case float32:
				sb.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
			default:
				fmt.Fprintf(&sb, "%v", v)
			}
		}
		rows[i] = sb.String()
	}
	if q.Mode == ModeMultisetKeySeq {
		sort.Strings(rows)
	}
	h := sha256.New()
	for _, r := range rows {
		h.Write([]byte(r))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func newStabilityLedger() *stabilityLedger {
	return &stabilityLedger{byQ: map[string][]stabilitySample{}, q: map[string]FPQuery{}}
}

func (l *stabilityLedger) add(q FPQuery, label string, res *oracle.Result) {
	if _, ok := l.byQ[q.Name]; !ok {
		l.order = append(l.order, q.Name)
		l.q[q.Name] = q
	}
	l.byQ[q.Name] = append(l.byQ[q.Name], stabilitySample{
		label: label,
		sig:   SignatureOf(res, q),
		exact: exactDigest(res, q),
		nums:  numericCells(res),
		fine:  renderRows(res, 6),
	})
}

// report prints the measurement and fails on the condition that would make the
// SF100 gate flake: a later sample the gate's own comparison rejects.
func (l *stabilityLedger) report(t *testing.T, sf ScaleFactor) {
	t.Helper()
	sort.Strings(l.order)
	t.Logf("float stability at SF%g: %d queries", float64(sf), len(l.order))
	t.Logf("%-6s %-7s %-6s %-6s %-7s %-6s %-10s %-10s %s",
		"query", "samples", "exact", "fine", "coarse", "rows", "max-rel", "margin", "verdict")

	var reordered, wobbled, broke []string
	worstRelAll, worstRelQuery := 0.0, "none"
	tightestMargin, tightestQuery := math.Inf(1), "none"
	for _, name := range l.order {
		got := l.byQ[name]
		if len(got) < 2 {
			continue
		}
		q := l.q[name]
		exact, fine, coarse, rows := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[int]bool{}
		for _, s := range got {
			exact[s.exact] = true
			fine[s.sig.Rows.Fine+"|"+s.sig.Keys.Fine] = true
			coarse[s.sig.Rows.Coarse+"|"+s.sig.Keys.Coarse] = true
			rows[s.sig.Rows.Rows] = true
		}
		// The gate compares every run against ONE stored entry, so that is
		// what stability has to mean here.
		ref := NewEntry("duckdb", "test", "test", "test", q, got[0].sig)
		var failures []string
		worstRel := 0.0
		for _, s := range got[1:] {
			if ok, detail := ref.Match(s.sig); !ok {
				failures = append(failures, fmt.Sprintf("%s: %s%s", s.label, detail, firstRowDiff(got[0], s)))
			}
			if d, at := maxRelDiff(got[0].nums, s.nums); at >= 0 && d > worstRel {
				worstRel = d
			}
		}
		if worstRel > worstRelAll {
			worstRelAll, worstRelQuery = worstRel, name
		}
		verdict := "stable"
		switch {
		case len(failures) > 0:
			verdict = fmt.Sprintf("UNSTABLE — the gate would flake (%d/%d samples)", len(failures), len(got)-1)
			broke = append(broke, name)
		case len(fine) > 1:
			verdict = "fine digest moved, coarse held (dual precision absorbed it)"
			wobbled = append(wobbled, name)
		case len(exact) > 1:
			verdict = "accumulation order moved, both digests held"
			reordered = append(reordered, name)
		}
		margin := math.Inf(1)
		if n := len(got[0].nums); n > 0 && got[0].sig.Rows.Rows > 0 {
			margin = orderMargin(got[0].nums, n/got[0].sig.Rows.Rows)
		}
		if margin < tightestMargin {
			tightestMargin, tightestQuery = margin, name
		}
		t.Logf("%-6s %-7d %-6d %-6d %-7d %-6d %-10.2e %-10.2e %s",
			name, len(got), len(exact), len(fine), len(coarse), len(rows), worstRel, margin, verdict)
		for _, f := range failures {
			t.Logf("       %s", f)
		}
	}

	t.Logf("summary: %d queries whose full-precision answer moved between samples (%v) — these are the ones "+
		"this run actually tested for float stability; %d also moved their fine digest (%v); %d the gate would "+
		"reject (%v). A query with one exact digest was never reordered, so it says nothing either way.",
		len(reordered)+len(wobbled)+len(broke), append(append(append([]string{}, reordered...), wobbled...), broke...),
		len(wobbled), wobbled, len(broke), broke)
	// The headline number. The fine digest renders 6 significant digits, so
	// a cell's rendered value only moves when it sits within the observed
	// noise of a rounding boundary: noise/1e-6 is the per-cell probability,
	// and the coarse digest is a second, independent 1e-4 lottery behind it.
	t.Logf("worst relative deviation between two samples of the same query: %.2e (%s). "+
		"The fine digest quantum is 1e-6 and the coarse is 1e-4, so a cell flips its fine rendering with "+
		"probability ~%.1e and both renderings together with ~%.1e.",
		worstRelAll, worstRelQuery, worstRelAll/1e-6, (worstRelAll/1e-6)*(worstRelAll/1e-4))
	// And the ordering risk, which no precision tier absorbs: two rows swap
	// when the noise exceeds the gap between their sort keys.
	t.Logf("tightest gap between adjacent sort-key values: %.2e (%s) against %.2e of noise — safety factor %.1e. "+
		"Below ~1 the positional order is a coin flip and the query needs the key-sequence mode.",
		tightestMargin, tightestQuery, worstRelAll, tightestMargin/math.Max(worstRelAll, 1e-300))
	if len(broke) > 0 {
		t.Errorf("%d queries are not stable enough for a stored digest at SF%g: %v — "+
			"each needs a remedy (a coarser tier, a key-sequence comparison, or exclusion with a stated reason) "+
			"before its entry can gate SF100", len(broke), float64(sf), broke)
	}
}

// firstRowDiff names the first row whose digest input differs, which is the
// difference the digests are reporting.
func firstRowDiff(a, b stabilitySample) string {
	if len(a.fine) != len(b.fine) {
		return fmt.Sprintf("\n         row counts differ: %d vs %d", len(a.fine), len(b.fine))
	}
	for i := range a.fine {
		if a.fine[i] != b.fine[i] {
			return fmt.Sprintf("\n         first differing rendered row %d:\n           %s: %s\n           %s: %s",
				i, a.label, a.fine[i], b.label, b.fine[i])
		}
	}
	return "\n         (no rendered row differs at 6 digits — the divergence is in the row SEQUENCE)"
}

func stabilityScale(t *testing.T) ScaleFactor {
	t.Helper()
	v := os.Getenv("WADJET_FP_STABILITY_SCALE")
	if v == "" {
		return SF001
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		t.Fatalf("WADJET_FP_STABILITY_SCALE=%q: %v", v, err)
	}
	return ScaleFactor(f)
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// setToggles flips the named optimization switches and returns a restore func.
// An unknown name fails the test rather than silently measuring nothing.
func setToggles(t *testing.T, names []string, on bool) func() {
	t.Helper()
	if len(names) == 0 {
		return func() {}
	}
	byName := map[string]*optswitch.Toggle{}
	for _, tg := range optswitch.All() {
		byName[tg.Name] = tg
	}
	prev := map[*optswitch.Toggle]bool{}
	for _, n := range names {
		tg, ok := byName[n]
		if !ok {
			t.Fatalf("no optimization toggle named %q (registered: %v)", n, toggleNames())
		}
		prev[tg] = tg.Set(on)
	}
	return func() {
		for tg, v := range prev {
			tg.Set(v)
		}
	}
}

func toggleNames() []string {
	var out []string
	for _, tg := range optswitch.All() {
		out = append(out, tg.Name)
	}
	return out
}

// setupStabilityCluster stands up an embedded cluster with a configurable
// worker count and returns a coordinator pinned to the stage DAG. It is
// deliberately its own copy rather than a parameter on setupTwoPathCluster:
// the worker count is the variable under study here, and this file must not
// perturb the two-path gate's fixture.
func setupStabilityCluster(t *testing.T, ctx context.Context, data map[string][]map[string]any, workers int) *coordinator.Coordinator {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("embedded nats: %v", err)
	}
	t.Cleanup(embedded.Shutdown)
	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("catalog kv: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	loadTPCHIntoCatalog(t, ctx, cat, store, data)

	ids := make([]string, workers)
	for i := range ids {
		ids[i] = fmt.Sprintf("stability-worker-%d", i)
		w := worker.New(worker.Config{
			WorkerID:      ids[i],
			NATSUrl:       embedded.ClientURL(),
			MaxConcurrent: 4,
			CacheBytes:    64 << 20,
		}, store, nc, js, logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatalf("worker start: %v", err)
		}
		t.Cleanup(w.Stop)
	}

	// LocalFastPathBytes=0 forces every query onto the stage DAG, which is
	// the path whose partitioning changes with the worker count.
	coord := coordinator.New(coordinator.Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test",
		LocalFastPathBytes: 0,
	}, cat, nc, js, logger)

	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, id := range ids {
			hb, err := distributed.Marshal(distributed.WorkerHeartbeat{
				WorkerID: id, MaxConcurrent: 4, Timestamp: time.Now(),
			})
			if err != nil {
				t.Fatalf("marshal heartbeat: %v", err)
			}
			if err := nc.Publish(distributed.SubjectHeartbeat, hb); err != nil {
				t.Fatalf("publish heartbeat: %v", err)
			}
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("nats flush: %v", err)
		}
		if coord.Workers().Count() >= workers {
			return coord
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers never registered: %d, want %d", coord.Workers().Count(), workers)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
