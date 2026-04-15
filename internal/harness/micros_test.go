package harness

import "testing"

func TestGenerateMicroData(t *testing.T) {
	data := generateMicroData()

	cases := []struct {
		table string
		rows  int
		cols  int
	}{
		{"micro_lineitem", 200_000, 3},
		{"micro_orders", 20_000, 2},
		{"micro_build", 500_000, 3},
		{"micro_probe", 50_000, 2},
		{"micro_agg", 200_000, 2},
	}
	for _, tc := range cases {
		mt, ok := data[tc.table]
		if !ok {
			t.Errorf("missing table %s", tc.table)
			continue
		}
		if len(mt.rows) != tc.rows {
			t.Errorf("%s: want %d rows, got %d", tc.table, tc.rows, len(mt.rows))
		}
		if len(mt.schema.Columns) != tc.cols {
			t.Errorf("%s: want %d cols, got %d", tc.table, tc.cols, len(mt.schema.Columns))
		}
	}
}

func TestGenerateMicroDataDeterministic(t *testing.T) {
	d1 := generateMicroData()
	d2 := generateMicroData()
	// Check a sample value from micro_agg is identical across runs.
	r1 := d1["micro_agg"].rows[0]["group_key"]
	r2 := d2["micro_agg"].rows[0]["group_key"]
	if r1 != r2 {
		t.Errorf("non-deterministic: %v != %v", r1, r2)
	}
}
