package skew

import (
	"fmt"
	"testing"
)

func testConfig() Config {
	return Config{
		EventsRows: 10_000,
		DimsRows:   1_000,
		HotKey:     7,
		HotPct:     90,
		KeySpace:   1_400,
		NameCard:   48,
		PadBytes:   32,
		ChunkRows:  3_000,
		Seed:       42,
	}
}

// collect runs GenerateChunked and returns rows per table plus chunk sizes.
func collect(t *testing.T, cfg Config) (map[string][]map[string]any, map[string][]int) {
	t.Helper()
	rows := make(map[string][]map[string]any)
	chunks := make(map[string][]int)
	err := GenerateChunked(cfg, func(table string, r []map[string]any) error {
		rows[table] = append(rows[table], r...)
		chunks[table] = append(chunks[table], len(r))
		return nil
	})
	if err != nil {
		t.Fatalf("GenerateChunked: %v", err)
	}
	return rows, chunks
}

func TestGenerateChunked_ShapeAndDeterminism(t *testing.T) {
	cfg := testConfig()
	rows1, chunks1 := collect(t, cfg)
	rows2, _ := collect(t, cfg)

	if got := len(rows1["skew_events"]); got != cfg.EventsRows {
		t.Errorf("events rows = %d, want %d", got, cfg.EventsRows)
	}
	if got := len(rows1["skew_dims"]); got != cfg.DimsRows {
		t.Errorf("dims rows = %d, want %d", got, cfg.DimsRows)
	}
	for table, sizes := range chunks1 {
		for i, n := range sizes {
			if n > cfg.ChunkRows {
				t.Errorf("%s chunk %d has %d rows > ChunkRows %d", table, i, n, cfg.ChunkRows)
			}
		}
	}
	// Same config → identical rows (the A/B arms regenerate independently).
	for _, table := range []string{"skew_events", "skew_dims"} {
		for i := range rows1[table] {
			if fmt.Sprintf("%v", rows1[table][i]) != fmt.Sprintf("%v", rows2[table][i]) {
				t.Fatalf("%s row %d differs between identical-config runs", table, i)
			}
		}
	}
}

func TestGenerateChunked_HotFractionAndKeyRanges(t *testing.T) {
	cfg := testConfig()
	rows, _ := collect(t, cfg)

	hot := 0
	unique := make(map[string]bool, cfg.EventsRows)
	for i, r := range rows["skew_events"] {
		k := r["k"].(int64)
		if k == cfg.HotKey {
			hot++
		}
		if k < 0 || k >= cfg.KeySpace {
			t.Fatalf("events row %d key %d outside [0, %d)", i, k, cfg.KeySpace)
		}
		unique[r["v"].(string)] = true
	}
	// row%100 < HotPct rows are pinned to HotKey; uniform draws can add a few.
	minHot := cfg.EventsRows * cfg.HotPct / 100
	if hot < minHot {
		t.Errorf("hot rows = %d, want >= %d", hot, minHot)
	}
	if hot > minHot+cfg.EventsRows/50 {
		t.Errorf("hot rows = %d, way over pinned %d — hot fraction wrong", hot, minHot)
	}
	if len(unique) != cfg.EventsRows {
		t.Errorf("distinct v = %d, want %d (v must be unique for parity invariants)", len(unique), cfg.EventsRows)
	}

	names := make(map[string]bool)
	for i, r := range rows["skew_dims"] {
		if k := r["k"].(int64); k != int64(i) {
			t.Fatalf("dims row %d has key %d, want sequential", i, k)
		}
		names[r["name"].(string)] = true
	}
	if len(names) != cfg.NameCard {
		t.Errorf("distinct dims.name = %d, want %d", len(names), cfg.NameCard)
	}
}

func TestConfigValidate(t *testing.T) {
	if err := DefaultLocalConfig().Validate(); err != nil {
		t.Errorf("DefaultLocalConfig invalid: %v", err)
	}
	if err := DefaultDeployConfig().Validate(); err != nil {
		t.Errorf("DefaultDeployConfig invalid: %v", err)
	}
	bad := testConfig()
	bad.HotKey = int64(bad.DimsRows) // hot key would never match a dim
	if err := bad.Validate(); err == nil {
		t.Error("HotKey >= DimsRows not rejected")
	}
	bad = testConfig()
	bad.KeySpace = int64(bad.DimsRows) // no unmatched rows for LEFT JOIN
	if err := bad.Validate(); err == nil {
		t.Error("KeySpace <= DimsRows not rejected")
	}
}

func TestQueriesCoverOrder(t *testing.T) {
	if len(QueryOrder) != len(Queries) {
		t.Fatalf("QueryOrder has %d entries, Queries has %d", len(QueryOrder), len(Queries))
	}
	for _, name := range QueryOrder {
		if _, ok := Queries[name]; !ok {
			t.Errorf("QueryOrder entry %q missing from Queries", name)
		}
	}
}
