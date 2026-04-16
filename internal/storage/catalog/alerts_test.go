package catalog

import (
	"context"
	"testing"
	"time"
)

func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	kv := NewMemKV()
	cat := &Catalog{kv: kv, clusterID: "test"}
	return cat
}

func TestCreateAndGetAlert(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{
		Name:            "a1",
		QueryText:       "SELECT 1",
		IntervalSeconds: 60,
		WebhookURL:      "https://x",
		Enabled:         true,
		CreatedAt:       time.Unix(1000, 0).UTC(),
	}
	if err := cat.CreateAlert(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	got, err := cat.GetAlert(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "a1" || got.QueryText != "SELECT 1" || got.IntervalSeconds != 60 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	if got.Version != 1 {
		t.Errorf("version: want 1, got %d", got.Version)
	}
}

func TestCreateAlertDuplicate(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 60}
	if err := cat.CreateAlert(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	err := cat.CreateAlert(context.Background(), m)
	if err == nil {
		t.Fatal("want duplicate error, got nil")
	}
}

func TestDropAlert(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{Name: "a1", IntervalSeconds: 60}
	_ = cat.CreateAlert(context.Background(), m)
	if err := cat.DropAlert(context.Background(), "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.GetAlert(context.Background(), "a1"); err == nil {
		t.Error("alert still exists after drop")
	}
}

func TestSetAlertEnabled(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{Name: "a1", IntervalSeconds: 60, Enabled: true}
	_ = cat.CreateAlert(context.Background(), m)
	if err := cat.SetAlertEnabled(context.Background(), "a1", false); err != nil {
		t.Fatal(err)
	}
	got, _ := cat.GetAlert(context.Background(), "a1")
	if got.Enabled {
		t.Error("want disabled, got enabled")
	}
	if got.Version != 2 {
		t.Errorf("version: want 2, got %d", got.Version)
	}
}

func TestTouchAlertEvaluated(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{Name: "a1", IntervalSeconds: 60}
	_ = cat.CreateAlert(context.Background(), m)
	when := time.Unix(5000, 0).UTC()
	if err := cat.TouchAlertEvaluated(context.Background(), "a1", when); err != nil {
		t.Fatal(err)
	}
	got, _ := cat.GetAlert(context.Background(), "a1")
	if !got.LastEvaluatedAt.Equal(when) {
		t.Errorf("last evaluated: want %v, got %v", when, got.LastEvaluatedAt)
	}
}

func TestListAlerts(t *testing.T) {
	cat := newTestCatalog(t)
	for _, name := range []string{"b", "a", "c"} {
		_ = cat.CreateAlert(context.Background(), AlertMeta{Name: name, IntervalSeconds: 60})
	}
	alerts, err := cat.ListAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 3 {
		t.Fatalf("want 3 alerts, got %d", len(alerts))
	}
}
