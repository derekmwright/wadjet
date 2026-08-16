package alerts_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestCreateAlertEndToEnd boots an embedded Wadjet with alerts enabled,
// creates an alert that fires every second, and asserts webhook fires and
// alert_history rows land.
func TestCreateAlertEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end alerts test skipped under -short")
	}

	// Lower the parser's CREATE ALERT interval floor to 1 second for this test.
	restore := plansql.SetAlertIntervalFloorForTest(1 * time.Second)
	defer restore()

	// Fake webhook server: count how many times it is called.
	var hits int32
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer fake.Close()

	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:        objstore.NewMemStore(),
		Bucket:       "test",
		EnableAlerts: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Create a table with a few rows for the alert query to match.
	if _, err := db.Query(ctx, `CREATE TABLE t (x BIGINT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Query(ctx, `INSERT INTO t (x) VALUES (1), (2), (3)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Create alert: fires every 1 second, posts webhook, inserts into alert_history.
	createSQL := fmt.Sprintf(
		`CREATE ALERT t_rows AS SELECT * FROM t EVERY 1 SECONDS WEBHOOK '%s' INSERT INTO alert_history`,
		fake.URL,
	)
	if _, err := db.Query(ctx, createSQL); err != nil {
		t.Fatalf("CREATE ALERT: %v", err)
	}

	// Wait up to 8 seconds for at least 2 webhook fires.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&hits) >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := atomic.LoadInt32(&hits); n < 2 {
		t.Errorf("want >=2 webhook hits within 8s, got %d", n)
	} else {
		t.Logf("webhook hit count: %d", n)
	}

	// Assert alert_history has at least one row.
	res, err := db.Query(ctx, `SELECT COUNT(*) FROM alert_history WHERE alert_name = 't_rows'`)
	if err != nil {
		t.Fatalf("SELECT COUNT(*) from alert_history: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("no row returned from COUNT query")
	}
	// Count value may be under any column key; accept any positive numeric value.
	var count int64
	for _, v := range res.Rows[0] {
		switch cv := v.(type) {
		case int64:
			count = cv
		case int32:
			count = int64(cv)
		case float64:
			count = int64(cv)
		}
		break
	}
	if count < 1 {
		t.Errorf("alert_history row count: want >=1, got %d", count)
	} else {
		t.Logf("alert_history rows: %d", count)
	}

	// DROP ALERT: fires should stop.
	if _, err := db.Query(ctx, `DROP ALERT t_rows`); err != nil {
		t.Fatalf("DROP ALERT: %v", err)
	}
	hitsBefore := atomic.LoadInt32(&hits)
	time.Sleep(2 * time.Second)
	hitsAfter := atomic.LoadInt32(&hits)
	// Allow at most 1 in-flight fire that was already scheduled before the drop.
	if delta := hitsAfter - hitsBefore; delta > 1 {
		t.Errorf("fires continued after DROP ALERT: %d new hits in 2s", delta)
	} else {
		t.Logf("hits after DROP: %d (delta %d)", hitsAfter, delta)
	}
}
