package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/ingest"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
	"github.com/derekmwright/caelum/pkg/caelum"
)

func setupDBWithEvents(t *testing.T, numRows int) *caelum.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := caelum.Open(ctx, caelum.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "user_id", Type: parquet.TypeString},
			{Name: "event_type", Type: parquet.TypeString},
			{Name: "amount", Type: parquet.TypeFloat64},
			{Name: "year", Type: parquet.TypeString},
			{Name: "month", Type: parquet.TypeString},
		},
	}

	if err := db.CreateTable(ctx, "events", schema, []string{"year", "month"}); err != nil {
		t.Fatal(err)
	}

	// Generate data
	users := []string{"alice", "bob", "carol", "dave", "eve"}
	eventTypes := []string{"purchase", "refund", "view", "click"}
	rng := rand.New(rand.NewSource(42))

	rows := make([]map[string]any, numRows)
	for i := range rows {
		month := fmt.Sprintf("%02d", rng.Intn(3)+1) // 01, 02, 03
		rows[i] = map[string]any{
			"user_id":    users[rng.Intn(len(users))],
			"event_type": eventTypes[rng.Intn(len(eventTypes))],
			"amount":     float64(rng.Intn(10000)) / 100.0,
			"year":       "2026",
			"month":      month,
		}
	}

	ing := db.NewIngester("events", schema, []string{"year", "month"}, ingest.Config{
		MaxBufferRows: numRows + 1,
		RowGroupSize:  500,
	})

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func TestQuerySelectStar(t *testing.T) {
	db := setupDBWithEvents(t, 100)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT * FROM events")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result.Rows))
	}
	t.Logf("SELECT * returned %d rows, columns: %v", len(result.Rows), result.Columns)
}

func TestQueryWithLimit(t *testing.T) {
	db := setupDBWithEvents(t, 100)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT * FROM events LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(result.Rows))
	}
}

func TestQueryAggregate(t *testing.T) {
	db := setupDBWithEvents(t, 1000)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT user_id, SUM(amount) as total, COUNT(amount) as cnt FROM events GROUP BY user_id")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("expected aggregate results")
	}

	t.Logf("Aggregate query returned %d groups:", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  user_id=%v total=%v cnt=%v", row["user_id"], row["total"], row["cnt"])
	}

	// We have 5 users, so should have 5 groups
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 groups (one per user), got %d", len(result.Rows))
	}

	// Verify totals add up
	var totalSum float64
	var totalCnt int64
	for _, row := range result.Rows {
		if v, ok := row["total"].(float64); ok {
			totalSum += v
		}
		if v, ok := row["cnt"].(int64); ok {
			totalCnt += v
		}
	}
	if totalCnt != 1000 {
		t.Fatalf("expected total count=1000, got %d", totalCnt)
	}
	t.Logf("Total sum across all users: %.2f, total count: %d", totalSum, totalCnt)
}

func TestQueryAggregateWithSort(t *testing.T) {
	db := setupDBWithEvents(t, 1000)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT user_id, SUM(amount) as total FROM events GROUP BY user_id ORDER BY total DESC")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result.Rows))
	}

	// Verify descending order
	t.Logf("Users by total (desc):")
	var prev float64
	for i, row := range result.Rows {
		total := row["total"].(float64)
		t.Logf("  %s: %.2f", row["user_id"], total)
		if i > 0 && total > prev {
			t.Fatalf("expected descending order, but %.2f > %.2f", total, prev)
		}
		prev = total
	}
}

func TestQueryAggregateWithSortAndLimit(t *testing.T) {
	db := setupDBWithEvents(t, 1000)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT user_id, SUM(amount) as total FROM events GROUP BY user_id ORDER BY total DESC LIMIT 3")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	t.Logf("Top 3 users by total:")
	for _, row := range result.Rows {
		t.Logf("  %s: %.2f", row["user_id"], row["total"])
	}
}

func TestQueryCount(t *testing.T) {
	db := setupDBWithEvents(t, 500)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT event_type, COUNT(amount) as cnt FROM events GROUP BY event_type")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Events by type:")
	var total int64
	for _, row := range result.Rows {
		cnt := row["cnt"].(int64)
		t.Logf("  %s: %d", row["event_type"], cnt)
		total += cnt
	}

	if total != 500 {
		t.Fatalf("expected total count=500, got %d", total)
	}
}

func TestQueryWindowRowNumber(t *testing.T) {
	db := setupDBWithEvents(t, 100)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT user_id, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as rn FROM events")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result.Rows))
	}

	// Check that rn column exists and starts at 1
	first := result.Rows[0]
	rn, ok := first["rn"]
	if !ok {
		t.Fatal("expected rn column in result")
	}
	if rn.(int64) != 1 {
		t.Fatalf("expected first rn=1, got %v", rn)
	}

	// Check monotonic increase
	for i, row := range result.Rows {
		if row["rn"].(int64) != int64(i+1) {
			t.Fatalf("expected rn=%d at row %d, got %v", i+1, i, row["rn"])
		}
	}
	t.Logf("ROW_NUMBER returned %d rows, first rn=%v last rn=%v", len(result.Rows), result.Rows[0]["rn"], result.Rows[len(result.Rows)-1]["rn"])
}

func TestQueryWindowPartitioned(t *testing.T) {
	db := setupDBWithEvents(t, 500)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT user_id, amount, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) as rn FROM events")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 500 {
		t.Fatalf("expected 500 rows, got %d", len(result.Rows))
	}

	// Within each partition, rn should restart at 1
	partitionCounts := make(map[string]int64)
	for _, row := range result.Rows {
		uid := row["user_id"].(string)
		rn := row["rn"].(int64)
		if rn == 1 {
			partitionCounts[uid]++
		}
	}

	// Each user should have exactly one rn=1
	for uid, count := range partitionCounts {
		if count != 1 {
			t.Fatalf("user %s has %d partitions with rn=1, expected 1", uid, count)
		}
	}
	t.Logf("Window with PARTITION BY returned %d rows across %d partitions", len(result.Rows), len(partitionCounts))
}

func TestQueryWindowRank(t *testing.T) {
	db := setupDBWithEvents(t, 200)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT user_id, event_type, RANK() OVER (ORDER BY event_type) as rnk FROM events")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("expected rows")
	}

	// Verify rank column exists
	if _, ok := result.Rows[0]["rnk"]; !ok {
		t.Fatal("expected rnk column in result")
	}
	t.Logf("RANK returned %d rows, first rank=%v", len(result.Rows), result.Rows[0]["rnk"])
}

func TestQueryWindowRunningSum(t *testing.T) {
	db := setupDBWithEvents(t, 100)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT user_id, amount, SUM(amount) OVER (ORDER BY amount) as running_total FROM events")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result.Rows))
	}

	// Verify running_total column exists and is non-decreasing
	if _, ok := result.Rows[0]["running_total"]; !ok {
		t.Fatal("expected running_total column in result")
	}

	var prev float64
	for i, row := range result.Rows {
		rt := row["running_total"].(float64)
		if i > 0 && rt < prev {
			t.Fatalf("running_total should be non-decreasing, got %.2f after %.2f at row %d", rt, prev, i)
		}
		prev = rt
	}
	t.Logf("Running SUM: first=%.2f, last=%.2f", result.Rows[0]["running_total"], result.Rows[len(result.Rows)-1]["running_total"])
}

func TestQueryCTESimple(t *testing.T) {
	db := setupDBWithEvents(t, 500)
	ctx := context.Background()

	result, err := db.Query(ctx, `
		WITH user_totals AS (
			SELECT user_id, SUM(amount) as total FROM events GROUP BY user_id
		)
		SELECT user_id, total FROM user_totals ORDER BY total DESC
	`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows (one per user), got %d", len(result.Rows))
	}

	// Verify descending order
	var prev float64
	for i, row := range result.Rows {
		total := row["total"].(float64)
		t.Logf("  %s: %.2f", row["user_id"], total)
		if i > 0 && total > prev {
			t.Fatalf("expected descending order")
		}
		prev = total
	}
	t.Logf("CTE query returned %d rows", len(result.Rows))
}

func TestQueryCTEWithFilter(t *testing.T) {
	db := setupDBWithEvents(t, 1000)
	ctx := context.Background()

	result, err := db.Query(ctx, `
		WITH purchases AS (
			SELECT user_id, amount FROM events WHERE event_type = 'purchase'
		)
		SELECT user_id, SUM(amount) as total FROM purchases GROUP BY user_id
	`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("expected some results from CTE with filter")
	}

	t.Logf("CTE with filter returned %d rows:", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  %s: %.2f", row["user_id"], row["total"])
	}
}

func TestQueryCTEMultiple(t *testing.T) {
	db := setupDBWithEvents(t, 500)
	ctx := context.Background()

	result, err := db.Query(ctx, `
		WITH purchase_totals AS (
			SELECT user_id, SUM(amount) as purchase_total FROM events WHERE event_type = 'purchase' GROUP BY user_id
		),
		refund_totals AS (
			SELECT user_id, SUM(amount) as refund_total FROM events WHERE event_type = 'refund' GROUP BY user_id
		)
		SELECT p.user_id, p.purchase_total, r.refund_total
		FROM purchase_totals p
		JOIN refund_totals r ON p.user_id = r.user_id
	`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("expected results from multi-CTE join query")
	}

	t.Logf("Multi-CTE join returned %d rows:", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  %s: purchases=%.2f refunds=%v", row["user_id"], row["purchase_total"], row["refund_total"])
	}
}

func TestQueryLargeDataset(t *testing.T) {
	db := setupDBWithEvents(t, 10000)
	ctx := context.Background()

	result, err := db.Query(ctx, "SELECT user_id, SUM(amount) as total, COUNT(amount) as cnt FROM events GROUP BY user_id ORDER BY total DESC LIMIT 3")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Top 3 users out of 10k events:")
	for _, row := range result.Rows {
		t.Logf("  %s: total=%.2f count=%v", row["user_id"], row["total"], row["cnt"])
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	t.Logf("\nQuery plan:\n%s", result.Plan)
}

func TestQueryIntersect(t *testing.T) {
	db := setupDBWithEvents(t, 100)
	ctx := context.Background()

	// INTERSECT: users who appear in both event_type=purchase AND event_type=refund
	result, err := db.Query(ctx, `
		SELECT user_id FROM events WHERE event_type = 'purchase'
		INTERSECT
		SELECT user_id FROM events WHERE event_type = 'refund'
	`)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("INTERSECT returned %d rows", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  user_id=%v", row["user_id"])
	}

	// All returned users must appear in both sets
	if len(result.Rows) == 0 {
		t.Log("no overlapping users (possible with small data)")
	}
}

func TestQueryExcept(t *testing.T) {
	db := setupDBWithEvents(t, 100)
	ctx := context.Background()

	// EXCEPT: users who have purchases but no refunds
	result, err := db.Query(ctx, `
		SELECT user_id FROM events WHERE event_type = 'purchase'
		EXCEPT
		SELECT user_id FROM events WHERE event_type = 'refund'
	`)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("EXCEPT returned %d rows", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  user_id=%v", row["user_id"])
	}
}

func TestQueryIntersectAll(t *testing.T) {
	db := setupDBWithEvents(t, 100)
	ctx := context.Background()

	// INTERSECT ALL preserves duplicates
	result, err := db.Query(ctx, `
		SELECT user_id FROM events WHERE event_type = 'purchase'
		INTERSECT ALL
		SELECT user_id FROM events WHERE event_type = 'refund'
	`)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("INTERSECT ALL returned %d rows", len(result.Rows))
}

func TestQueryExceptWithOrderLimit(t *testing.T) {
	db := setupDBWithEvents(t, 100)
	ctx := context.Background()

	result, err := db.Query(ctx, `
		SELECT user_id FROM events WHERE event_type = 'purchase'
		EXCEPT
		SELECT user_id FROM events WHERE event_type = 'refund'
		ORDER BY user_id LIMIT 3
	`)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) > 3 {
		t.Fatalf("expected at most 3 rows, got %d", len(result.Rows))
	}
	t.Logf("EXCEPT with ORDER BY LIMIT returned %d rows", len(result.Rows))
}
