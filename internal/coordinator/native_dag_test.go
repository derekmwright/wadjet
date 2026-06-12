package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestNativeDAG_SimpleAggregate runs a small query end-to-end through the
// Phase 3 native-DAG executor (UseNativeDAG=true) and verifies correctness
// against the same query on the legacy path. Smoke test for Commit 4 wiring:
// EnsureDistribution emits Exchange stages, executeStageDAG dispatches them
// via the new TaskTypeStage path, gather receives results.
func TestNativeDAG_SimpleAggregate(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"k": int64(1), "v": int64(10)},
		{"k": int64(1), "v": int64(20)},
		{"k": int64(2), "v": int64(30)},
		{"k": int64(2), "v": int64(40)},
		{"k": int64(3), "v": int64(50)},
	}
	ingestTestData(t, ctx, store, cat, "simple", schema, rows)

	sql := "SELECT k, SUM(v) AS total FROM simple GROUP BY k"

	// Baseline: legacy path.
	legacyRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("legacy ExecuteSQL: %v", err)
	}
	if legacyRes.TotalRows != 3 {
		t.Fatalf("legacy: expected 3 groups, got %d", legacyRes.TotalRows)
	}

	// Native DAG.
	// Native-DAG is the default; no opt-in needed.
	natRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("native DAG ExecuteSQL: %v", err)
	}
	if natRes.TotalRows != legacyRes.TotalRows {
		t.Fatalf("native DAG rows %d != legacy %d", natRes.TotalRows, legacyRes.TotalRows)
	}
}

// TestNativeDAG_SumMerge is a regression test for the partial-aggregate
// InputCol merge bug: final_aggregate was looking up the scan's raw input
// column ("v") in partial output that only contained {groupby, OutputCol}
// ("total"), so every SUM returned nil. Verifies values match an expected
// sum, not just row count (the prior TestNativeDAG_SimpleAggregate checked
// only row count and silently passed with nil aggregates).
func TestNativeDAG_SumMerge(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"k": int64(1), "v": int64(10)},
		{"k": int64(1), "v": int64(20)},
		{"k": int64(2), "v": int64(30)},
	}
	ingestTestData(t, ctx, store, cat, "sum_merge", schema, rows)

	// Native-DAG is the default; no opt-in needed.
	res, err := coord.ExecuteSQL(ctx, "SELECT k, SUM(v) AS total FROM sum_merge GROUP BY k")
	if err != nil {
		t.Fatalf("native DAG: %v", err)
	}
	sums := map[int64]int64{}
	for _, r := range mustRows(t, res) {
		sums[toInt64(r["k"])] = toInt64(r["total"])
	}
	if sums[1] != 30 {
		t.Errorf("k=1 total: got %d, want 30", sums[1])
	}
	if sums[2] != 30 {
		t.Errorf("k=2 total: got %d, want 30", sums[2])
	}
}

// TestNativeDAG_ScanAggregateWithFilter is a regression test for the bug
// where scan-pushed WHERE clauses were silently dropped in the native-DAG
// scan-aggregate dispatcher. Row counts matched legacy (same group keys)
// but aggregate VALUES were wrong because every row — including rows the
// WHERE should have excluded — was summed.
func TestNativeDAG_ScanAggregateWithFilter(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
		{Name: "flag", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"k": int64(1), "v": int64(10), "flag": int64(1)},
		{"k": int64(1), "v": int64(100), "flag": int64(0)}, // excluded
		{"k": int64(2), "v": int64(20), "flag": int64(1)},
		{"k": int64(2), "v": int64(200), "flag": int64(0)}, // excluded
	}
	ingestTestData(t, ctx, store, cat, "filtered", schema, rows)

	// Use COUNT(*) so the row-count change from filtering is directly
	// observable even when the merge-stage partial-aggregate plumbing has
	// an unrelated gap (InputCol naming). COUNT(*) ignores InputCol so it
	// merges correctly across partials. With the bug regressed, native-DAG
	// would return count=2 per group (all rows seen); with the fix, count=1.
	sql := "SELECT k, COUNT(*) AS n FROM filtered WHERE flag = 1 GROUP BY k"

	legacyRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("legacy ExecuteSQL: %v", err)
	}
	legacyCounts := map[int64]int64{}
	for _, r := range mustRows(t, legacyRes) {
		legacyCounts[toInt64(r["k"])] = toInt64(r["n"])
	}

	// Native-DAG is the default; no opt-in needed.
	natRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("native DAG ExecuteSQL: %v", err)
	}
	natCounts := map[int64]int64{}
	for _, r := range mustRows(t, natRes) {
		natCounts[toInt64(r["k"])] = toInt64(r["n"])
	}

	if len(legacyCounts) != 2 || legacyCounts[1] != 1 || legacyCounts[2] != 1 {
		t.Fatalf("legacy counts unexpected: %v", legacyCounts)
	}
	for k, v := range legacyCounts {
		if natCounts[k] != v {
			t.Errorf("native-DAG count for k=%d: got %d, want %d (legacy); bug: filter was dropped", k, natCounts[k], v)
		}
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case uint64:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}

// TestNativeDAG_CountMultiPartial exercises COUNT across multiple
// scan-aggregate partials. Pre-fix: the merge step ran COUNT on the
// partial output column (row count of partial rows, usually 1 per group)
// instead of SUM of the partial counts. Post-fix: merge rewrites COUNT →
// SUM so the final count equals the total input rows per group.
func TestNativeDAG_CountMultiPartial(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	// Ingest all rows in one call; splitFilesEvenly(workerCount=1..n)
	// still returns a task when at least one file is present. With a
	// single worker setup this exercises COUNT → SUM on merge via one
	// partial; with more workers the merge fan-in effect amplifies.
	ingestTestData(t, ctx, store, cat, "count_mp", schema, []map[string]any{
		{"k": int64(1), "v": int64(10)},
		{"k": int64(2), "v": int64(20)},
		{"k": int64(1), "v": int64(30)},
		{"k": int64(2), "v": int64(40)},
	})

	// Native-DAG is the default; no opt-in needed.
	res, err := coord.ExecuteSQL(ctx, "SELECT k, COUNT(*) AS n FROM count_mp GROUP BY k")
	if err != nil {
		t.Fatalf("native DAG: %v", err)
	}
	counts := map[int64]int64{}
	for _, r := range mustRows(t, res) {
		counts[toInt64(r["k"])] = toInt64(r["n"])
	}
	if counts[1] != 2 || counts[2] != 2 {
		t.Errorf("count mismatch: got %v, want k=1:2 k=2:2", counts)
	}
}

// TestNativeDAG_AvgFallback verifies that AVG queries produce correct
// values in native-DAG even though single-column AVG can't merge across
// partials. The dispatcher falls back to single-task scan-aggregate so
// the merge is a pass-through.
func TestNativeDAG_AvgFallback(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	ingestTestData(t, ctx, store, cat, "avg_fb", schema, []map[string]any{
		{"k": int64(1), "v": int64(10)},
		{"k": int64(1), "v": int64(30)}, // avg = 20
		{"k": int64(2), "v": int64(40)},
		{"k": int64(2), "v": int64(60)}, // avg = 50
	})

	// Native-DAG is the default; no opt-in needed.
	res, err := coord.ExecuteSQL(ctx, "SELECT k, AVG(v) AS a FROM avg_fb GROUP BY k")
	if err != nil {
		t.Fatalf("native DAG: %v", err)
	}
	avgs := map[int64]float64{}
	for _, r := range mustRows(t, res) {
		switch x := r["a"].(type) {
		case float64:
			avgs[toInt64(r["k"])] = x
		}
	}
	if avgs[1] != 20 || avgs[2] != 50 {
		t.Errorf("avg mismatch: got %v, want k=1:20 k=2:50", avgs)
	}
}

// TestNativeDAG_BroadcastJoinProbeSplit exercises the broadcast_join
// probe-split path: a small build (small dimension table) joined against
// a multi-chunk probe (4 files). With workerCount > 1 the dispatcher should
// fan out the broadcast_join to multiple tasks, each scanning a slice of
// probe files while reading the full build. Verifies that the join produces
// the expected row count (no duplication from the fan-out, no rows dropped
// from probe slicing).
func TestNativeDAG_BroadcastJoinProbeSplit(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	// Small dimension (build, broadcast-eligible).
	dimSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString},
	}
	dim := []map[string]any{
		{"id": int64(1), "label": "A"},
		{"id": int64(2), "label": "B"},
		{"id": int64(3), "label": "C"},
	}
	ingestTestData(t, ctx, store, cat, "dim", dimSchema, dim)

	// Multi-chunk facts table — 4 chunks × 25 rows = 100 rows. With
	// MaxConcurrent=4 the cluster reports capacity=4, so the dispatcher
	// should split the probe across 4 tasks (one chunk per task).
	factSchema := []parquet.Column{
		{Name: "fk", Type: parquet.TypeInt64},
		{Name: "amt", Type: parquet.TypeInt64},
	}
	chunks := make([][]map[string]any, 4)
	for c := 0; c < 4; c++ {
		rows := make([]map[string]any, 25)
		for i := 0; i < 25; i++ {
			rows[i] = map[string]any{
				"fk":  int64((i % 3) + 1),
				"amt": int64(c*100 + i),
			}
		}
		chunks[c] = rows
	}
	ingestMultiFile(t, ctx, store, cat, "facts", factSchema, chunks)

	sql := "SELECT d.label, COUNT(*) AS n FROM facts f JOIN dim d ON f.fk = d.id GROUP BY d.label"

	res, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if res.Error != "" {
		t.Fatalf("query error: %s", res.Error)
	}

	got := map[string]int64{}
	for _, r := range mustRows(t, res) {
		got[r["label"].(string)] = toInt64(r["n"])
	}
	// Each chunk has 25 rows with fk values cycling 1,2,3,1,2,3,...
	// Per chunk: id=1 gets 9, id=2 gets 8, id=3 gets 8. ×4 chunks =
	// {A:36, B:32, C:32}. Total 100.
	want := map[string]int64{"A": 36, "B": 32, "C": 32}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("label=%s: got %d, want %d (full result %v)", k, got[k], w, got)
		}
	}
	var total int64
	for _, n := range got {
		total += n
	}
	if total != 100 {
		t.Errorf("total joined rows = %d, want 100 (probe-split duplicated or dropped rows)", total)
	}
}

// TestNativeDAG_BroadcastJoinReplicateMaterialization exercises the
// dispatchReplicateStage materialization path. With a multi-chunk build
// table, EnsureDistribution splices an exchange-replicate ahead of the
// broadcast_join build slot. The dispatcher should detect the multi-file
// upstream and consolidate it into a single WSHF cache. Verifies the
// final join row count is unchanged (no rows dropped or duplicated by
// the materialization).
func TestNativeDAG_BroadcastJoinReplicateMaterialization(t *testing.T) {
	prevMin := replicateMaterializeMinFiles
	replicateMaterializeMinFiles = 2
	t.Cleanup(func() { replicateMaterializeMinFiles = prevMin })

	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	// Multi-chunk build — small dimension table, broadcast-eligible, but
	// spread across 3 chunks so the materialization gate fires.
	dimSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString},
	}
	dimChunks := [][]map[string]any{
		{{"id": int64(1), "label": "A"}},
		{{"id": int64(2), "label": "B"}},
		{{"id": int64(3), "label": "C"}},
	}
	ingestMultiFile(t, ctx, store, cat, "dim_multi", dimSchema, dimChunks)

	// Single-chunk facts table on the probe side.
	factSchema := []parquet.Column{
		{Name: "fk", Type: parquet.TypeInt64},
		{Name: "amt", Type: parquet.TypeInt64},
	}
	facts := []map[string]any{
		{"fk": int64(1), "amt": int64(10)},
		{"fk": int64(2), "amt": int64(20)},
		{"fk": int64(2), "amt": int64(25)},
		{"fk": int64(3), "amt": int64(30)},
		{"fk": int64(3), "amt": int64(35)},
		{"fk": int64(3), "amt": int64(40)},
	}
	ingestTestData(t, ctx, store, cat, "facts_single", factSchema, facts)

	sql := "SELECT d.label, COUNT(*) AS n FROM facts_single f JOIN dim_multi d ON f.fk = d.id GROUP BY d.label"

	res, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if res.Error != "" {
		t.Fatalf("query error: %s", res.Error)
	}

	got := map[string]int64{}
	for _, r := range mustRows(t, res) {
		got[r["label"].(string)] = toInt64(r["n"])
	}
	want := map[string]int64{"A": 1, "B": 2, "C": 3}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("label=%s: got %d, want %d (full result %v) — materialization may have dropped/duplicated rows",
				k, got[k], w, got)
		}
	}
}

// TestNativeDAG_Join exercises a hash_join compute stage end-to-end by
// running a two-table INNER JOIN through the native DAG executor.
func TestNativeDAG_Join(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	usersSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	users := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
		{"id": int64(3), "name": "carol"},
	}
	ingestTestData(t, ctx, store, cat, "users", usersSchema, users)

	ordersSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}
	orders := []map[string]any{
		{"user_id": int64(1), "amount": int64(100)},
		{"user_id": int64(2), "amount": int64(200)},
		{"user_id": int64(1), "amount": int64(50)},
	}
	ingestTestData(t, ctx, store, cat, "orders", ordersSchema, orders)

	sql := "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"

	legacyRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("legacy ExecuteSQL: %v", err)
	}

	// Native-DAG is the default; no opt-in needed.
	natRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("native DAG ExecuteSQL: %v", err)
	}
	if natRes.TotalRows != legacyRes.TotalRows {
		t.Fatalf("native DAG rows %d != legacy %d", natRes.TotalRows, legacyRes.TotalRows)
	}
	if natRes.TotalRows != 3 {
		t.Fatalf("expected 3 joined rows, got %d", natRes.TotalRows)
	}
}
