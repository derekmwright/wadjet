package coordinator

import (
	"context"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
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

func TestCreateAlertRejectedWhenDisabled(t *testing.T) {
	c := newTestCoordinator(t)
	c.SetAlertsEnabled(false)
	err := c.handleCreateAlertSQL(context.Background(), `CREATE ALERT a AS SELECT 1 FROM t EVERY 10 SECONDS WEBHOOK 'http://x'`)
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("want disabled error, got %v", err)
	}
}
