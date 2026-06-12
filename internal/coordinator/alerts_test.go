package coordinator

import (
	"context"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func newTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	kv := catalog.NewMemKV()
	store := objstore.NewMemStore()
	cat := catalog.NewWithCluster(kv, store, "b", "c")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{catalog: cat}
	c.SetAlertsEnabled(true)
	return c
}

func TestCreateAlertHandler(t *testing.T) {
	c := newTestCoordinator(t)
	sqlText := `CREATE ALERT a1 AS SELECT 1 FROM t EVERY 30 SECONDS WEBHOOK 'https://x'`
	if err := c.handleCreateAlertSQL(context.Background(), sqlText); err != nil {
		t.Fatalf("handleCreateAlertSQL: %v", err)
	}
	m, err := c.catalog.GetAlert(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "a1" || m.IntervalSeconds != 30 {
		t.Errorf("unexpected meta: %+v", m)
	}
}

func TestDropAlertIfExistsMissing(t *testing.T) {
	c := newTestCoordinator(t)
	err := c.handleDropAlertSQL(context.Background(), `DROP ALERT IF EXISTS nope`)
	if err != nil {
		t.Errorf("IF EXISTS should swallow missing: %v", err)
	}
	err = c.handleDropAlertSQL(context.Background(), `DROP ALERT nope`)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("want not-found error, got %v", err)
	}
}

func TestAlterAlertToggles(t *testing.T) {
	c := newTestCoordinator(t)
	_ = c.handleCreateAlertSQL(context.Background(), `CREATE ALERT a AS SELECT 1 FROM t EVERY 10 SECONDS WEBHOOK 'https://x'`)
	if err := c.handleAlterAlertSQL(context.Background(), `ALTER ALERT a DISABLE`); err != nil {
		t.Fatal(err)
	}
	m, _ := c.catalog.GetAlert(context.Background(), "a")
	if m.Enabled {
		t.Error("want disabled")
	}
}

func TestCreateAlertRejectsUnknownInsertTarget(t *testing.T) {
	c := newTestCoordinator(t)
	err := c.handleCreateAlertSQL(context.Background(),
		`CREATE ALERT a AS SELECT 1 FROM t EVERY 10 SECONDS INSERT INTO does_not_exist`)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("want not-found error, got %v", err)
	}
}

func TestCreateAlertRejectedWhenDisabled(t *testing.T) {
	c := newTestCoordinator(t)
	c.SetAlertsEnabled(false)
	err := c.handleCreateAlertSQL(context.Background(), `CREATE ALERT a AS SELECT 1 FROM t EVERY 10 SECONDS WEBHOOK 'http://x'`)
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("want disabled error, got %v", err)
	}
}

// Regression test for sweep finding #22: the alert executor boxed the full
// result via rs.Rows() and only then truncated to limit — every scheduled
// tick paid for the query's whole cardinality. boxRowsUpTo must stop boxing
// at the limit and report truncation without materializing the excess.
func TestBoxRowsUpTo(t *testing.T) {
	schema := []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}
	mk := func(vals ...int64) *batch.RecordBatch {
		rows := make([]map[string]any, len(vals))
		for i, v := range vals {
			rows[i] = map[string]any{"n": v}
		}
		return batch.FromRows(schema, rows)
	}

	tests := []struct {
		name      string
		batches   []*batch.RecordBatch
		limit     int
		wantRows  int
		wantTrunc bool
	}{
		{"under limit", []*batch.RecordBatch{mk(1, 2)}, 5, 2, false},
		{"exact limit", []*batch.RecordBatch{mk(1, 2, 3)}, 3, 3, false},
		{"split mid-batch", []*batch.RecordBatch{mk(1, 2, 3, 4)}, 2, 2, true},
		{"stops at later batch", []*batch.RecordBatch{mk(1, 2), mk(3, 4)}, 2, 2, true},
		{"trailing empty batch is not truncation", []*batch.RecordBatch{mk(1, 2), mk()}, 2, 2, false},
		{"no limit", []*batch.RecordBatch{mk(1), mk(2)}, 0, 2, false},
		{"empty result", nil, 5, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, trunc := boxRowsUpTo(tc.batches, tc.limit)
			if len(rows) != tc.wantRows {
				t.Errorf("got %d rows, want %d", len(rows), tc.wantRows)
			}
			if trunc != tc.wantTrunc {
				t.Errorf("truncated = %v, want %v", trunc, tc.wantTrunc)
			}
		})
	}
}
