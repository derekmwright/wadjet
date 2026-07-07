package exec

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// chunkBatches splits rows into batches of chunk rows each.
func chunkBatches(tb testing.TB, schema []parquet.Column, rows []map[string]any, chunk int) []*batch.RecordBatch {
	tb.Helper()
	var out []*batch.RecordBatch
	for i := 0; i < len(rows); i += chunk {
		end := i + chunk
		if end > len(rows) {
			end = len(rows)
		}
		out = append(out, batch.FromRows(schema, rows[i:end]))
	}
	return out
}

// runSMJ drives the operator through its full lifecycle — Build (right),
// Consume (left), Finalize, drain — and returns the joined rows.
func runSMJ(tb testing.TB, j *SortMergeJoin, rightBatches, leftBatches []*batch.RecordBatch) []map[string]any {
	tb.Helper()
	ctx := context.Background()
	if err := j.Init(ctx); err != nil {
		tb.Fatalf("Init: %v", err)
	}
	if err := j.Build(ctx, NewBatchSource(rightBatches)); err != nil {
		tb.Fatalf("Build: %v", err)
	}
	for i, b := range leftBatches {
		if err := j.Consume(ctx, b); err != nil {
			tb.Fatalf("Consume batch %d: %v", i, err)
		}
	}
	if err := j.Finalize(ctx); err != nil {
		tb.Fatalf("Finalize: %v", err)
	}
	var rows []map[string]any
	for {
		b, err := j.Next(ctx)
		if err != nil {
			tb.Fatalf("Next: %v", err)
		}
		if b == nil {
			return rows
		}
		rows = append(rows, b.ToRows()...)
	}
}

// bruteInnerJoin computes the expected inner-join multiset with a plain
// rendered-key map: an oracle independent of both operators' typed hash /
// merge machinery. Rows with a NULL in any join key match nothing. Output
// rows merge left columns with the right columns not shadowed by left names
// (tests use distinct names unless exercising qualification, which uses the
// HashJoin reference instead).
func bruteInnerJoin(leftRows, rightRows []map[string]any, leftKeys, rightKeys []string) []map[string]any {
	renderKey := func(row map[string]any, keys []string) (string, bool) {
		var sb strings.Builder
		for _, k := range keys {
			v := row[k]
			if v == nil {
				return "", false
			}
			fmt.Fprintf(&sb, "%T:%v|", v, v)
		}
		return sb.String(), true
	}
	rightByKey := make(map[string][]map[string]any)
	for _, r := range rightRows {
		if key, ok := renderKey(r, rightKeys); ok {
			rightByKey[key] = append(rightByKey[key], r)
		}
	}
	var out []map[string]any
	for _, l := range leftRows {
		key, ok := renderKey(l, leftKeys)
		if !ok {
			continue
		}
		for _, r := range rightByKey[key] {
			row := make(map[string]any, len(l)+len(r))
			for k, v := range r {
				row[k] = v
			}
			for k, v := range l {
				row[k] = v
			}
			out = append(out, row)
		}
	}
	return out
}

// renderRows canonicalizes a row multiset for order-independent comparison.
func renderRows(rows []map[string]any, cols []string) []string {
	out := make([]string, len(rows))
	var sb strings.Builder
	for i, r := range rows {
		sb.Reset()
		for _, c := range cols {
			fmt.Fprintf(&sb, "%s=%v|", c, r[c])
		}
		out[i] = sb.String()
	}
	sort.Strings(out)
	return out
}

func assertSameRows(tb testing.TB, got, want []map[string]any, cols []string) {
	tb.Helper()
	g, w := renderRows(got, cols), renderRows(want, cols)
	if len(g) != len(w) {
		tb.Fatalf("row count: got %d want %d", len(g), len(w))
	}
	for i := range w {
		if g[i] != w[i] {
			tb.Fatalf("row %d (canonical order):\n  got  %s\n  want %s", i, g[i], w[i])
		}
	}
}

// newSMJSpillHarness attaches a SpillManager with the given budget and
// lowers the run floor so unit-scale data exercises the spill path.
func newSMJSpillHarness(tb testing.TB, j *SortMergeJoin, budget int64) *memory.Tracker {
	tb.Helper()
	forceTinyRuns(tb)
	tracker := memory.NewTracker("smj-test", budget)
	sm, err := memory.NewSpillManager(tb.TempDir(), tracker)
	if err != nil {
		tb.Fatal(err)
	}
	j.Spill = sm
	return tracker
}

// TestSortMergeJoin_InnerDupGroups: duplicate keys on BOTH sides within one
// batch — every (left dup × right dup) pair must appear exactly once.
func TestSortMergeJoin_InnerDupGroups(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "lv", Type: parquet.TypeString},
	}
	rightSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeString},
	}
	leftRows := []map[string]any{
		{"k": int64(1), "lv": "l1a"},
		{"k": int64(2), "lv": "l2a"},
		{"k": int64(2), "lv": "l2b"},
		{"k": int64(3), "lv": "l3a"},
		{"k": int64(5), "lv": "l5a"}, // no right match
	}
	rightRows := []map[string]any{
		{"rk": int64(1), "rv": "r1a"},
		{"rk": int64(2), "rv": "r2a"},
		{"rk": int64(2), "rv": "r2b"},
		{"rk": int64(2), "rv": "r2c"},
		{"rk": int64(4), "rv": "r4a"}, // no left match
	}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	got := runSMJ(t, j,
		chunkBatches(t, rightSchema, rightRows, 100),
		chunkBatches(t, leftSchema, leftRows, 100))
	want := bruteInnerJoin(leftRows, rightRows, []string{"k"}, []string{"rk"})
	if len(got) != 7 { // k=1: 1×1, k=2: 2×3, unmatched keys contribute nothing
		t.Fatalf("row count: got %d want 7", len(got))
	}
	assertSameRows(t, got, want, []string{"k", "lv", "rk", "rv"})
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSortMergeJoin_MatchesHashJoin runs identical random data through
// SortMergeJoin and the HashJoin build+probe pipeline and requires identical
// row multisets — the operator-level equivalence the planner will rely on.
func TestSortMergeJoin_MatchesHashJoin(t *testing.T) {
	for _, tc := range []struct {
		name    string
		keyType parquet.TypeID
	}{
		{"int64_keys", parquet.TypeInt64},
		{"string_keys", parquet.TypeString},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leftSchema := []parquet.Column{
				{Name: "k", Type: tc.keyType},
				{Name: "amount", Type: parquet.TypeFloat64},
			}
			rightSchema := []parquet.Column{
				{Name: "rk", Type: tc.keyType},
				{Name: "label", Type: parquet.TypeString},
			}
			rng := rand.New(rand.NewSource(99))
			mkKey := func(n int) any {
				if tc.keyType == parquet.TypeString {
					return fmt.Sprintf("key-%03d", n)
				}
				return int64(n)
			}
			var leftRows, rightRows []map[string]any
			for i := 0; i < 3000; i++ {
				leftRows = append(leftRows, map[string]any{"k": mkKey(rng.Intn(400)), "amount": float64(i)})
			}
			for i := 0; i < 2000; i++ {
				rightRows = append(rightRows, map[string]any{"rk": mkKey(rng.Intn(400)), "label": fmt.Sprintf("r%d", i)})
			}

			j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
			got := runSMJ(t, j,
				chunkBatches(t, rightSchema, rightRows, 700),
				chunkBatches(t, leftSchema, leftRows, 700))
			defer j.Close()

			hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"rk"})
			hj.BuildFromRows(rightSchema, rightRows)
			sink := &CollectSink{}
			pipe := &Pipeline{Source: NewSliceSource(leftSchema, leftRows), Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
			if err := pipe.Run(context.Background()); err != nil {
				t.Fatal(err)
			}

			assertSameRows(t, got, sink.Rows, []string{"k", "amount", "rk", "label"})
		})
	}
}

// TestSortMergeJoin_CrossBatchDupGroups: hot keys whose duplicate groups span
// input batch boundaries AND merge-output batch boundaries (> DefaultBatchSize
// rows), exercising group batch pinning and the mid-group pending-flush
// resume path.
func TestSortMergeJoin_CrossBatchDupGroups(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "lv", Type: parquet.TypeInt64},
	}
	rightSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	var leftRows, rightRows []map[string]any
	// Key 42: build-side group of 2500 (> DefaultBatchSize → spans merge
	// batches), 3 probe rows. Key 7: probe-side run of 2200, 3 build rows.
	// Key 9: 1:1. Keys 100/200: unmatched on either side.
	for i := 0; i < 2500; i++ {
		rightRows = append(rightRows, map[string]any{"rk": int64(42), "rv": int64(i)})
	}
	for i := 0; i < 3; i++ {
		rightRows = append(rightRows, map[string]any{"rk": int64(7), "rv": int64(10000 + i)})
		leftRows = append(leftRows, map[string]any{"k": int64(42), "lv": int64(20000 + i)})
	}
	for i := 0; i < 2200; i++ {
		leftRows = append(leftRows, map[string]any{"k": int64(7), "lv": int64(i)})
	}
	leftRows = append(leftRows, map[string]any{"k": int64(9), "lv": int64(1)}, map[string]any{"k": int64(100), "lv": int64(2)})
	rightRows = append(rightRows, map[string]any{"rk": int64(9), "rv": int64(1)}, map[string]any{"rk": int64(200), "rv": int64(2)})

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	got := runSMJ(t, j,
		chunkBatches(t, rightSchema, rightRows, 512),
		chunkBatches(t, leftSchema, leftRows, 512))
	defer j.Close()

	wantRows := 3*2500 + 2200*3 + 1
	if len(got) != wantRows {
		t.Fatalf("row count: got %d want %d", len(got), wantRows)
	}
	want := bruteInnerJoin(leftRows, rightRows, []string{"k"}, []string{"rk"})
	assertSameRows(t, got, want, []string{"k", "lv", "rk", "rv"})
}

// TestSortMergeJoin_NullKeys: NULL join keys on either side match nothing
// (SQL equi-join semantics) — including NULL-NULL pairs.
func TestSortMergeJoin_NullKeys(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "lv", Type: parquet.TypeString},
	}
	rightSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64, Nullable: true},
		{Name: "rv", Type: parquet.TypeString},
	}
	leftRows := []map[string]any{
		{"k": int64(1), "lv": "l1"},
		{"k": nil, "lv": "lnull-a"},
		{"k": int64(2), "lv": "l2"},
		{"k": nil, "lv": "lnull-b"},
	}
	rightRows := []map[string]any{
		{"rk": nil, "rv": "rnull"},
		{"rk": int64(1), "rv": "r1"},
		{"rk": int64(3), "rv": "r3"},
	}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	got := runSMJ(t, j,
		chunkBatches(t, rightSchema, rightRows, 10),
		chunkBatches(t, leftSchema, leftRows, 10))
	defer j.Close()

	if len(got) != 1 {
		t.Fatalf("expected exactly the k=1 match, got %d rows: %v", len(got), got)
	}
	if got[0]["lv"] != "l1" || got[0]["rv"] != "r1" {
		t.Fatalf("wrong match: %v", got[0])
	}
}

// TestSortMergeJoin_EmptySides: inner join with any empty side is empty and
// must tear down cleanly (no stranded run files or reservations).
func TestSortMergeJoin_EmptySides(t *testing.T) {
	schema := func(k, v string) []parquet.Column {
		return []parquet.Column{
			{Name: k, Type: parquet.TypeInt64},
			{Name: v, Type: parquet.TypeString},
		}
	}
	someRows := func(k, v string) []map[string]any {
		return []map[string]any{
			{k: int64(1), v: "a"},
			{k: int64(2), v: "b"},
		}
	}
	for _, tc := range []struct {
		name        string
		left, right []map[string]any
	}{
		{"both_empty", nil, nil},
		{"probe_empty", nil, someRows("rk", "rv")},
		{"build_empty", someRows("k", "lv"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
			tracker := newSMJSpillHarness(t, j, 1<<30)
			got := runSMJ(t, j,
				chunkBatches(t, schema("rk", "rv"), tc.right, 10),
				chunkBatches(t, schema("k", "lv"), tc.left, 10))
			if len(got) != 0 {
				t.Fatalf("expected empty join, got %d rows", len(got))
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if used := tracker.Used(); used != 0 {
				t.Fatalf("tracker should be fully released, still holds %d bytes", used)
			}
		})
	}
}

// TestSortMergeJoin_ForcedSpillBothSides: a budget small enough that BOTH
// sides self-spill sorted runs, with a hot key thrown in so group pinning
// runs under a real tracker. Output must match the oracle; run scratch must
// be deleted after the drain; the tracker must end at zero.
func TestSortMergeJoin_ForcedSpillBothSides(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "lv", Type: parquet.TypeInt64},
	}
	rightSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	rng := rand.New(rand.NewSource(5))
	var leftRows, rightRows []map[string]any
	for i := 0; i < 12000; i++ {
		leftRows = append(leftRows, map[string]any{"k": int64(rng.Intn(4000)), "lv": int64(i)})
	}
	for i := 0; i < 12000; i++ {
		rightRows = append(rightRows, map[string]any{"rk": int64(rng.Intn(4000)), "rv": int64(i)})
	}
	// Hot key: build group of 3000 spans merge batches → pinned batches are
	// Reserved against the same tight budget.
	for i := 0; i < 3000; i++ {
		rightRows = append(rightRows, map[string]any{"rk": int64(9999), "rv": int64(i)})
	}
	leftRows = append(leftRows, map[string]any{"k": int64(9999), "lv": int64(-1)})

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	tracker := newSMJSpillHarness(t, j, 256<<10)

	ctx := context.Background()
	if err := j.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Build(ctx, NewBatchSource(chunkBatches(t, rightSchema, rightRows, 2048))); err != nil {
		t.Fatal(err)
	}
	if len(j.build.runFiles) == 0 {
		t.Fatal("build side never spilled; budget/floor setup is wrong")
	}
	for _, b := range chunkBatches(t, leftSchema, leftRows, 2048) {
		if err := j.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if len(j.probe.runFiles) == 0 {
		t.Fatal("probe side never spilled; budget/floor setup is wrong")
	}
	probeRuns := append([]string(nil), j.probe.runFiles...)
	buildRuns := append([]string(nil), j.build.runFiles...)

	if err := j.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	var got []map[string]any
	for {
		b, err := j.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		got = append(got, b.ToRows()...)
	}

	want := bruteInnerJoin(leftRows, rightRows, []string{"k"}, []string{"rk"})
	assertSameRows(t, got, want, []string{"k", "lv", "rk", "rv"})

	for _, p := range append(probeRuns, buildRuns...) {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("run file %s not cleaned up after drain", p)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("tracker should be fully released after drain+Close, still holds %d bytes", used)
	}
}

// TestSortMergeJoin_NestedPayload: ARRAY and ROW payload columns on both
// sides ride the sorted-run spill format and the output gather, coming back
// value-identical to the oracle.
func TestSortMergeJoin_NestedPayload(t *testing.T) {
	elem := parquet.Column{Name: "element", Type: parquet.TypeInt64}
	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "tags", Type: parquet.TypeArray, ElementType: &elem, Nullable: true},
	}
	rightSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rec", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64},
			{Name: "b", Type: parquet.TypeString, Nullable: true},
		}},
	}
	rng := rand.New(rand.NewSource(13))
	var leftRows, rightRows []map[string]any
	for i := 0; i < 1500; i++ {
		var tags any
		switch i % 4 {
		case 0:
			tags = nil
		case 1:
			tags = []any{}
		default:
			tags = []any{int64(i), int64(i % 9)}
		}
		leftRows = append(leftRows, map[string]any{"k": int64(rng.Intn(150)), "tags": tags})
	}
	for i := 0; i < 1500; i++ {
		var b any
		if i%5 == 0 {
			b = nil
		} else {
			b = fmt.Sprintf("b%d", i)
		}
		rightRows = append(rightRows, map[string]any{
			"rk":  int64(rng.Intn(150)),
			"rec": map[string]any{"a": int64(i * 3), "b": b},
		})
	}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	// Tight enough that both sides spill sorted runs, with headroom for the
	// merge-phase group pins (~one merge batch of nested build rows).
	tracker := newSMJSpillHarness(t, j, 64<<10)

	ctx := context.Background()
	if err := j.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Build(ctx, NewBatchSource(chunkBatches(t, rightSchema, rightRows, 64))); err != nil {
		t.Fatal(err)
	}
	for _, b := range chunkBatches(t, leftSchema, leftRows, 64) {
		if err := j.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if len(j.build.runFiles) == 0 || len(j.probe.runFiles) == 0 {
		t.Fatalf("nested payloads must ride the run format on both sides (probe %d runs, build %d runs)",
			len(j.probe.runFiles), len(j.build.runFiles))
	}
	if err := j.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	var got []map[string]any
	for {
		b, err := j.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		got = append(got, b.ToRows()...)
	}

	want := bruteInnerJoin(leftRows, rightRows, []string{"k"}, []string{"rk"})
	assertSameRows(t, got, want, []string{"k", "tags", "rk", "rec"})
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("tracker should be fully released, still holds %d bytes", used)
	}
}

// TestSortMergeJoin_QualificationAndOutputFilter: duplicate build column
// names qualify under BuildTableAlias and OutputFilter prunes columns —
// byte-for-byte the HashJoinProbe semantics (shared helper), verified against
// the hash pipeline on identical data.
func TestSortMergeJoin_QualificationAndOutputFilter(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64}, // collides with probe "id"
		{Name: "region", Type: parquet.TypeString},
	}
	var leftRows, rightRows []map[string]any
	for i := 0; i < 200; i++ {
		leftRows = append(leftRows, map[string]any{"id": int64(i % 50), "name": fmt.Sprintf("n%d", i)})
	}
	for i := 0; i < 80; i++ {
		rightRows = append(rightRows, map[string]any{"id": int64(i % 50), "region": fmt.Sprintf("g%d", i)})
	}
	filter := map[string]bool{"name": true, "region": true}

	j := NewSortMergeJoin([]string{"id"}, []string{"id"})
	j.BuildTableAlias = "r"
	j.OutputFilter = filter
	got := runSMJ(t, j,
		chunkBatches(t, rightSchema, rightRows, 30),
		chunkBatches(t, leftSchema, leftRows, 30))
	defer j.Close()

	for i, col := range j.outSchema {
		if col.Name == "id" || strings.HasPrefix(col.Name, "r.") {
			t.Fatalf("output column %d = %q; OutputFilter should have pruned ids", i, col.Name)
		}
	}

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildTableAlias = "r"
	hj.BuildFromRows(rightSchema, rightRows)
	probe := hj.Probe()
	probe.OutputFilter = filter
	sink := &CollectSink{}
	pipe := &Pipeline{Source: NewSliceSource(leftSchema, leftRows), Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertSameRows(t, got, sink.Rows, []string{"name", "region"})
}

// TestSortMergeJoin_QualifiedAndSwappedKeyNames: the planner emits
// SQL-qualified key names ("l.id") while batch columns are bare, and SQL may
// put the build column on the left of "=" so the pair arrives side-swapped.
// Consume must normalize both via columnIndexFallback + counterpart adoption
// before anything sorts by them — an unresolved sort key is silently SKIPPED
// by resolveSortKeysForBatches and the merge would run over UNSORTED runs
// (wrong results, not an error).
func TestSortMergeJoin_QualifiedAndSwappedKeyNames(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "lv", Type: parquet.TypeString},
	}
	rightSchema := []parquet.Column{
		{Name: "rid", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeString},
	}
	var leftRows, rightRows []map[string]any
	// Reverse-ordered keys so unsorted runs would visibly break the merge.
	for i := 500; i > 0; i-- {
		leftRows = append(leftRows, map[string]any{"id": int64(i), "lv": fmt.Sprintf("l%d", i)})
		rightRows = append(rightRows, map[string]any{"rid": int64(i), "rv": fmt.Sprintf("r%d", i)})
	}

	for _, tc := range []struct {
		name                string
		leftKeys, rightKeys []string
	}{
		{"qualified", []string{"smj_l.id"}, []string{"smj_r.rid"}},
		{"swapped_pair", []string{"rid"}, []string{"id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := NewSortMergeJoin(tc.leftKeys, tc.rightKeys)
			got := runSMJ(t, j,
				chunkBatches(t, rightSchema, rightRows, 64),
				chunkBatches(t, leftSchema, leftRows, 64))
			defer j.Close()

			if len(got) != 500 {
				t.Fatalf("expected 500 joined rows, got %d", len(got))
			}
			for _, r := range got {
				id := r["id"].(int64)
				if r["lv"] != fmt.Sprintf("l%d", id) || r["rv"] != fmt.Sprintf("r%d", id) {
					t.Fatalf("mismatched pair: %v", r)
				}
			}
		})
	}
}

// TestSortMergeJoin_FinalizeBeforeBuild: Finalize without a completed build
// phase must fail loudly, mirroring HashJoinProbe.Init's guard.
func TestSortMergeJoin_FinalizeBeforeBuild(t *testing.T) {
	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	if err := j.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := j.Finalize(context.Background()); err == nil {
		t.Fatal("Finalize before Build should error")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---- Accounting truth-tests (mirroring Sort's contract) ----

// TestSortMergeJoinAccounting_ReleaseOnDrainAndClose: bytes tracked during
// consume are visible via Inspect/PublishOwned and fully released once the
// merge drains — no phantom reservation survives the operator.
func TestSortMergeJoinAccounting_ReleaseOnDrainAndClose(t *testing.T) {
	leftSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "lv", Type: parquet.TypeInt64}}
	rightSchema := []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}, {Name: "rv", Type: parquet.TypeInt64}}
	var leftRows, rightRows []map[string]any
	for i := 0; i < 500; i++ {
		leftRows = append(leftRows, map[string]any{"k": int64(i), "lv": int64(i)})
		rightRows = append(rightRows, map[string]any{"rk": int64(i), "rv": int64(i)})
	}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	// Large budget: nothing spills; both sides stay buffered and tracked.
	tracker := newSMJSpillHarness(t, j, 1<<30)

	ctx := context.Background()
	if err := j.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Build(ctx, NewBatchSource(chunkBatches(t, rightSchema, rightRows, 100))); err != nil {
		t.Fatal(err)
	}
	for _, b := range chunkBatches(t, leftSchema, leftRows, 100) {
		if err := j.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	fp := j.Inspect()
	if fp.OwnedBytes == 0 || fp.OwnedBytes != j.probe.trackedMem+j.build.trackedMem {
		t.Fatalf("Inspect.OwnedBytes = %d, want %d (both sides)", fp.OwnedBytes, j.probe.trackedMem+j.build.trackedMem)
	}
	if fp.SpillableBytes != fp.OwnedBytes {
		t.Fatalf("pre-finalize SpillableBytes = %d, want %d (both sides spillable)", fp.SpillableBytes, fp.OwnedBytes)
	}
	if got := tracker.OwnedFor(j.accInstanceID); got != fp.OwnedBytes {
		t.Fatalf("PublishOwned reported %d, Inspect says %d", got, fp.OwnedBytes)
	}
	if tracker.Used() != fp.OwnedBytes {
		t.Fatalf("tracker.Used = %d, want %d", tracker.Used(), fp.OwnedBytes)
	}

	if err := j.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	rows := 0
	for {
		b, err := j.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		rows += b.ActiveLen()
	}
	if rows != 500 {
		t.Fatalf("expected 500 joined rows, got %d", rows)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("tracker should be fully released after drain, still holds %d bytes", used)
	}
	if fp := j.Inspect(); fp.State != memory.OpClosed || fp.OwnedBytes != 0 {
		t.Fatalf("post-drain Inspect = %+v, want OpClosed with zero bytes", fp)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("Close must not double-release or re-reserve, tracker at %d", used)
	}
}

// TestSortMergeJoinAccounting_CloseWithoutFinalize: an early cancel (Close
// with buffered state and spilled runs, no Finalize) releases every tracked
// byte and deletes run scratch — Sort.Close's guarantee.
func TestSortMergeJoinAccounting_CloseWithoutFinalize(t *testing.T) {
	schemaL := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "lv", Type: parquet.TypeInt64}}
	schemaR := []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}, {Name: "rv", Type: parquet.TypeInt64}}
	var rowsL, rowsR []map[string]any
	for i := 0; i < 4000; i++ {
		rowsL = append(rowsL, map[string]any{"k": int64(i), "lv": int64(i)})
		rowsR = append(rowsR, map[string]any{"rk": int64(i), "rv": int64(i)})
	}
	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	tracker := newSMJSpillHarness(t, j, 64<<10) // tight: some batches spill, some stay buffered

	ctx := context.Background()
	if err := j.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Build(ctx, NewBatchSource(chunkBatches(t, schemaR, rowsR, 512))); err != nil {
		t.Fatal(err)
	}
	for _, b := range chunkBatches(t, schemaL, rowsL, 512) {
		if err := j.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	runs := append(append([]string(nil), j.probe.runFiles...), j.build.runFiles...)
	if len(runs) == 0 {
		t.Fatal("expected spilled runs under the tight budget")
	}

	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("tracker should be fully released after Close, still holds %d bytes", used)
	}
	for _, p := range runs {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("run file %s not cleaned up by Close", p)
		}
	}
	if fp := j.Inspect(); fp.State != memory.OpClosed || fp.OwnedBytes != 0 || fp.SpillableBytes != 0 {
		t.Fatalf("closed Inspect = %+v, want OpClosed with zero bytes", fp)
	}
}

// TestSortMergeJoinSpillSome_ReliefContract: EstimateRelief's claim is
// delivered by SpillSome; the larger buffered side sheds first; a target the
// larger side covers leaves the smaller side buffered; and the join still
// produces correct output afterward.
func TestSortMergeJoinSpillSome_ReliefContract(t *testing.T) {
	schemaL := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "lv", Type: parquet.TypeInt64}}
	schemaR := []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}, {Name: "rv", Type: parquet.TypeInt64}}
	var rowsL, rowsR []map[string]any
	for i := 0; i < 3000; i++ { // probe = the larger side
		rowsL = append(rowsL, map[string]any{"k": int64(i % 500), "lv": int64(i)})
	}
	for i := 0; i < 500; i++ {
		rowsR = append(rowsR, map[string]any{"rk": int64(i), "rv": int64(i)})
	}
	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	tracker := newSMJSpillHarness(t, j, 1<<30) // no self-spill; SpillSome is the only trigger

	ctx := context.Background()
	if err := j.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Build(ctx, NewBatchSource(chunkBatches(t, schemaR, rowsR, 250))); err != nil {
		t.Fatal(err)
	}
	for _, b := range chunkBatches(t, schemaL, rowsL, 250) {
		if err := j.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if j.probe.trackedMem <= j.build.trackedMem {
		t.Fatal("test setup: probe side must be the larger buffer")
	}
	largerBytes := j.probe.trackedMem

	// A target the larger side covers: claim and delivery are larger-side-only.
	claimed := j.EstimateRelief(1)
	if claimed != largerBytes {
		t.Fatalf("EstimateRelief(1) = %d, want larger side %d", claimed, largerBytes)
	}
	freed, err := j.SpillSome(1)
	if err != nil {
		t.Fatal(err)
	}
	if freed != claimed {
		t.Fatalf("SpillSome freed %d, EstimateRelief claimed %d", freed, claimed)
	}
	if len(j.probe.runFiles) == 0 || j.probe.trackedMem != 0 {
		t.Fatal("larger (probe) side should have spilled to a run")
	}
	if len(j.build.runFiles) != 0 || j.build.trackedMem == 0 {
		t.Fatal("smaller (build) side should still be buffered")
	}

	// A target beyond the remaining side: claim and delivery cover it too.
	claimed = j.EstimateRelief(1 << 30)
	if claimed != j.build.trackedMem {
		t.Fatalf("EstimateRelief(big) = %d, want remaining build side %d", claimed, j.build.trackedMem)
	}
	freed, err = j.SpillSome(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	if freed != claimed {
		t.Fatalf("second SpillSome freed %d, claimed %d", freed, claimed)
	}
	if tracker.Used() != 0 {
		t.Fatalf("all buffers spilled, tracker should be 0, at %d", tracker.Used())
	}

	if err := j.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	rows := 0
	for {
		b, err := j.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		rows += b.ActiveLen()
	}
	if rows != 3000 { // every probe row matches exactly one build row
		t.Fatalf("post-relief join produced %d rows, want 3000", rows)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if tracker.Used() != 0 {
		t.Fatalf("tracker should end at 0, at %d", tracker.Used())
	}
}

// TestSortMergeJoin_HotKeyGroupReserveFailsLoud: the documented v1 bound — a
// single key whose build-side duplicate group outgrows the memory budget
// fails the query with a clear error instead of OOMing.
func TestSortMergeJoin_HotKeyGroupReserveFailsLoud(t *testing.T) {
	schemaL := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "lv", Type: parquet.TypeInt64}}
	schemaR := []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}, {Name: "rv", Type: parquet.TypeInt64}}
	var rowsR []map[string]any
	for i := 0; i < 30000; i++ { // one pathologically hot key
		rowsR = append(rowsR, map[string]any{"rk": int64(1), "rv": int64(i)})
	}
	rowsL := []map[string]any{{"k": int64(1), "lv": int64(0)}}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	newSMJSpillHarness(t, j, 64<<10)

	ctx := context.Background()
	if err := j.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Build(ctx, NewBatchSource(chunkBatches(t, schemaR, rowsR, 2048))); err != nil {
		t.Fatal(err)
	}
	for _, b := range chunkBatches(t, schemaL, rowsL, 2048) {
		if err := j.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	var lastErr error
	for {
		b, err := j.Next(ctx)
		if err != nil {
			lastErr = err
			break
		}
		if b == nil {
			break
		}
	}
	if lastErr == nil {
		t.Fatal("expected the hot-key duplicate group to fail Reserve loudly")
	}
	if !strings.Contains(lastErr.Error(), "duplicate group") {
		t.Fatalf("error should name the duplicate-group bound, got: %v", lastErr)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}
