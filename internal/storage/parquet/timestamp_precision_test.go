package parquet

import (
	"os"
	"testing"
)

// Timestamp precision: parquet TIMESTAMP columns carry a unit (MILLIS,
// MICROS or NANOS); the engine has exactly one, epoch MILLIS. Our own writer
// only ever emits MILLIS, so a missing conversion is invisible in every test
// and benchmark that round-trips through our writer — it bites only on files
// produced elsewhere, which is the whole point of pointing the engine at a
// data lake. The error is silent and off by 1000x or 1000000x.
//
// The fixtures below are written by PyArrow (the Apache reference
// implementation), not by us — see testdata/gen_timestamp_precision.py. Each
// carries an expect_ms int64 column holding the epoch-millisecond value the
// engine must decode, written by the same reference writer from the same
// source integers, so the assertion compares our reader against the reference
// encoding rather than against a number this test recomputed.

// openFixture opens a testdata parquet file as a Reader.
func openFixture(tb testing.TB, name string) *Reader {
	tb.Helper()
	f, err := os.Open("testdata/" + name)
	if err != nil {
		tb.Fatalf("open fixture (regen with testdata/gen_timestamp_precision.py): %v", err)
	}
	tb.Cleanup(func() { f.Close() })
	st, err := f.Stat()
	if err != nil {
		tb.Fatal(err)
	}
	r, err := NewReader(f, st.Size())
	if err != nil {
		tb.Fatalf("open reader: %v", err)
	}
	return r
}

// leafByName returns the schema leaf for a flat column.
func leafByName(tb testing.TB, r *Reader, name string) *SchemaNode {
	tb.Helper()
	for _, leaf := range r.FileReader().Leaves() {
		if leaf.Name == name {
			return leaf
		}
	}
	tb.Fatalf("column %q not in fixture schema", name)
	return nil
}

// TestTimestampPrecisionFixtureShape is the guard that keeps the two tests
// below honest: if a regenerated fixture lost its NANOS column (the parquet
// 1.0 writer silently coerces nanoseconds to micros) or stopped declaring
// logical types, the value assertions would pass while testing nothing.
func TestTimestampPrecisionFixtureShape(t *testing.T) {
	r := openFixture(t, "timestamp_precision.parquet")

	want := map[string]struct {
		lt  LogicalTypeID
		div int64
	}{
		"ts_millis":  {LogicalTimestampMillis, 1},
		"ts_micros":  {LogicalTimestampMicros, 1_000},
		"ts_nanos":   {LogicalTimestampNanos, 1_000_000},
		"ts_us_utc":  {LogicalTimestampMicros, 1_000},
		"ts_ms_null": {LogicalTimestampMillis, 1},
		"ts_us_null": {LogicalTimestampMicros, 1_000},
	}
	for name, w := range want {
		leaf := leafByName(t, r, name)
		if leaf.LogicalType == nil {
			t.Fatalf("%s: no logical type — fixture must declare TIMESTAMP", name)
		}
		if leaf.LogicalType.Type != w.lt {
			t.Errorf("%s: logical type %v, want %v", name, leaf.LogicalType.Type, w.lt)
		}
		if got := TimestampDivisorFromSchemaNode(leaf); got != w.div {
			t.Errorf("%s: divisor %d, want %d", name, got, w.div)
		}
		if got := TypeIDFromSchemaNode(leaf); got != TypeTimestamp {
			t.Errorf("%s: TypeID %v, want TypeTimestamp", name, got)
		}
	}

	// The tz-aware column must be the isAdjustedToUTC=true variant: the
	// conversion keys off the UNIT, and this pins that a producer's zone
	// choice does not change which divisor applies.
	if leaf := leafByName(t, r, "ts_us_utc"); !leaf.LogicalType.IsAdjustedToUTC {
		t.Error("ts_us_utc: want isAdjustedToUTC=true — regenerate the fixture")
	}
	if leaf := leafByName(t, r, "ts_micros"); leaf.LogicalType.IsAdjustedToUTC {
		t.Error("ts_micros: want isAdjustedToUTC=false — regenerate the fixture")
	}
}

// TestTimestampPrecisionDecodesToEngineMillis is the regression for issue
// #321 defect 2: the same instants written at MILLIS, MICROS and NANOS must
// all decode to the identical epoch-millisecond value.
func TestTimestampPrecisionDecodesToEngineMillis(t *testing.T) {
	r := openFixture(t, "timestamp_precision.parquet")

	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("fixture decoded to zero rows")
	}

	// Columns holding every instant (no nulls).
	dense := []string{"ts_millis", "ts_micros", "ts_nanos", "ts_us_utc"}

	for i, row := range rows {
		label, _ := row["label"].(string)
		want, ok := row["expect_ms"].(int64)
		if !ok {
			t.Fatalf("row %d (%s): expect_ms is %T, want int64", i, label, row["expect_ms"])
		}
		for _, col := range dense {
			got, ok := row[col].(int64)
			if !ok {
				t.Fatalf("row %d (%s): %s is %T, want int64", i, label, col, row[col])
			}
			if got != want {
				t.Errorf("row %d (%s): %s decoded %d, want %d (off by %dx)",
					i, label, col, got, want, ratio(got, want))
			}
		}

		// Nullable pair: NULL stays NULL in both units, and the values
		// around a null are not shifted by the rescale.
		msNull, usNull := row["ts_ms_null"], row["ts_us_null"]
		if (msNull == nil) != (usNull == nil) {
			t.Errorf("row %d (%s): nullability diverged, ts_ms_null=%v ts_us_null=%v",
				i, label, msNull, usNull)
		}
		if msNull != nil {
			if msNull != usNull {
				t.Errorf("row %d (%s): ts_ms_null=%v but ts_us_null=%v", i, label, msNull, usNull)
			}
			if msNull != want {
				t.Errorf("row %d (%s): ts_ms_null=%v, want %d", i, label, msNull, want)
			}
		}
	}
}

// TestTimestampSubMillisecondTruncation pins the rule for precision the
// engine unit cannot hold: an instant is reported as the millisecond that
// CONTAINS it, so truncation goes toward the past on both sides of the
// epoch. Go's `/` truncates toward zero, which would move pre-1970 instants
// forward and make the conversion sign-dependent.
func TestTimestampSubMillisecondTruncation(t *testing.T) {
	r := openFixture(t, "timestamp_subms.parquet")

	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("fixture decoded to zero rows")
	}

	for i, row := range rows {
		label, _ := row["label"].(string)
		want, ok := row["expect_ms"].(int64)
		if !ok {
			t.Fatalf("row %d (%s): expect_ms is %T, want int64", i, label, row["expect_ms"])
		}
		for _, col := range []string{"ts_micros", "ts_nanos"} {
			got, ok := row[col].(int64)
			if !ok {
				t.Fatalf("row %d (%s): %s is %T, want int64", i, label, col, row[col])
			}
			if got != want {
				t.Errorf("row %d (%s): %s decoded %d, want %d", i, label, col, got, want)
			}
		}
	}
}

// TestTimestampStatsScaledToEngineMillis covers the other half of the unit
// crossing: footer statistics are raw file values, and every consumer of them
// (zone-map and dynamic-range row-group pruning, bloom pruning, the
// footer-answered MIN/MAX, the catalog's persisted per-file stats) compares
// against engine values without ever seeing the schema. Unscaled micros
// bounds do not merely mis-answer MIN/MAX — they prune away row groups that
// do contain matching rows.
func TestTimestampStatsScaledToEngineMillis(t *testing.T) {
	r := openFixture(t, "timestamp_precision.parquet")
	fr := r.FileReader()

	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	// True bounds, from the reference writer's own expect_ms column.
	wantMin, wantMax := rows[0]["expect_ms"].(int64), rows[0]["expect_ms"].(int64)
	for _, row := range rows {
		v := row["expect_ms"].(int64)
		if v < wantMin {
			wantMin = v
		}
		if v > wantMax {
			wantMax = v
		}
	}

	checked := 0
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		stats := fr.RowGroupStats(rg)
		for _, col := range []string{"ts_millis", "ts_micros", "ts_nanos", "ts_us_utc"} {
			cs, ok := stats.Columns[col]
			if !ok || !cs.HasStats || cs.MinValue == nil || cs.MaxValue == nil {
				continue
			}
			mn, ok1 := cs.MinValue.(int64)
			mx, ok2 := cs.MaxValue.(int64)
			if !ok1 || !ok2 {
				t.Fatalf("rg%d %s: stats are %T/%T, want int64", rg, col, cs.MinValue, cs.MaxValue)
			}
			if mn != wantMin || mx != wantMax {
				t.Errorf("rg%d %s: stats [%d,%d], want [%d,%d] (engine millis)",
					rg, col, mn, mx, wantMin, wantMax)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("fixture carries no timestamp statistics — nothing was verified")
	}
}

// TestTimestampToEngineMillis covers the scalar conversion directly,
// including the edges the fixtures cannot reach.
func TestTimestampToEngineMillis(t *testing.T) {
	cases := []struct {
		name string
		v    int64
		div  int64
		want int64
	}{
		{"millis passthrough", 826727136000, 1, 826727136000},
		{"millis passthrough negative", -826727136000, 1, -826727136000},
		{"zero", 0, 1_000, 0},
		{"micros exact", 826727136000000, 1_000, 826727136000},
		{"nanos exact", 826727136000000000, 1_000_000, 826727136000},
		// Truncation toward the past, both signs.
		{"micros +0.5ms", 500, 1_000, 0},
		{"micros -0.5ms", -500, 1_000, -1},
		{"micros +1.5ms", 1500, 1_000, 1},
		{"micros -1.5ms", -1500, 1_000, -2},
		{"micros -1us", -1, 1_000, -1},
		{"nanos -1ns", -1, 1_000_000, -1},
		{"nanos +999999ns", 999_999, 1_000_000, 0},
		{"nanos -999999ns", -999_999, 1_000_000, -1},
		// Extremes: nanoseconds saturate int64 around 1677..2262, and the
		// micros divisor must survive values far outside that.
		{"nanos min representable", -9_223_372_036_854_775_808, 1_000_000, -9_223_372_036_855},
		{"nanos max representable", 9_223_372_036_854_775_807, 1_000_000, 9_223_372_036_854},
		{"micros min representable", -9_223_372_036_854_775_808, 1_000, -9_223_372_036_854_776},
		{"micros max representable", 9_223_372_036_854_775_807, 1_000, 9_223_372_036_854_775},
	}
	for _, tc := range cases {
		if got := TimestampToEngineMillis(tc.v, tc.div); got != tc.want {
			t.Errorf("%s: TimestampToEngineMillis(%d, %d) = %d, want %d",
				tc.name, tc.v, tc.div, got, tc.want)
		}
	}

	// Monotonicity is what makes scaled min/max bounds sound for pruning:
	// a <= b must imply scale(a) <= scale(b) for every divisor.
	vals := []int64{-1_000_001, -1_000_000, -999_999, -1_500, -1_000, -1, 0, 1, 999, 1_000, 1_500, 1_000_000}
	for _, div := range []int64{1, 1_000, 1_000_000} {
		for i := 1; i < len(vals); i++ {
			lo := TimestampToEngineMillis(vals[i-1], div)
			hi := TimestampToEngineMillis(vals[i], div)
			if lo > hi {
				t.Errorf("div=%d: scale(%d)=%d > scale(%d)=%d — bounds would be unsound",
					div, vals[i-1], lo, vals[i], hi)
			}
		}
	}
}

// TestScaleTimestampsToEngine covers the in-place slice form used by the
// columnar decode path, including the no-op fast path our own MILLIS files
// take on every scan.
func TestScaleTimestampsToEngine(t *testing.T) {
	in := []int64{0, 1_500, -1_500, 826727136000000}
	millis := append([]int64(nil), in...)
	ScaleTimestampsToEngine(millis, 1)
	for i := range in {
		if millis[i] != in[i] {
			t.Errorf("div=1 must not touch values: [%d] = %d, want %d", i, millis[i], in[i])
		}
	}

	micros := append([]int64(nil), in...)
	ScaleTimestampsToEngine(micros, 1_000)
	want := []int64{0, 1, -2, 826727136000}
	for i := range want {
		if micros[i] != want[i] {
			t.Errorf("div=1000: [%d] = %d, want %d", i, micros[i], want[i])
		}
	}

	// Empty and nil slices must not panic.
	ScaleTimestampsToEngine(nil, 1_000)
	ScaleTimestampsToEngine([]int64{}, 1_000_000)
}

// TestTimestampDivisorNonTimestampNodes: the divisor is applied
// unconditionally by callers, so every non-timestamp shape must answer 1.
func TestTimestampDivisorNonTimestampNodes(t *testing.T) {
	i64 := PhysicalInt64
	ct := ConvertedTimestampMillis
	ctMicros := ConvertedTimestampMicros
	ctDate := ConvertedDate

	cases := []struct {
		name string
		node *SchemaNode
		want int64
	}{
		{"nil node", nil, 1},
		{"bare int64", &SchemaNode{Name: "n", Type: &i64}, 1},
		{"date", &SchemaNode{Name: "d", Type: &i64, LogicalType: &LogicalType{Type: LogicalDate}}, 1},
		{"string", &SchemaNode{Name: "s", LogicalType: &LogicalType{Type: LogicalString}}, 1},
		{"decimal", &SchemaNode{Name: "dec", LogicalType: &LogicalType{Type: LogicalDecimal, Precision: 18, Scale: 2}}, 1},
		// TIME carries a unit too, but maps to a plain integer in the file's
		// own unit (there is no engine TIME type), so it must NOT be scaled.
		{"time millis", &SchemaNode{Name: "t", LogicalType: &LogicalType{Type: LogicalTimeMillis}}, 1},
		{"time micros", &SchemaNode{Name: "t", LogicalType: &LogicalType{Type: LogicalTimeMicros}}, 1},
		// Old-style ConvertedType-only files.
		{"converted millis", &SchemaNode{Name: "c", Type: &i64, ConvertedType: &ct}, 1},
		{"converted micros", &SchemaNode{Name: "c", Type: &i64, ConvertedType: &ctMicros}, 1_000},
		{"converted date", &SchemaNode{Name: "c", ConvertedType: &ctDate}, 1},
		// A LogicalType wins over a ConvertedType that disagrees: writers
		// emit both, and the logical union is the authoritative one.
		{
			"logical millis beats converted micros",
			&SchemaNode{Name: "c", Type: &i64, ConvertedType: &ctMicros, LogicalType: &LogicalType{Type: LogicalTimestampMillis}},
			1,
		},
	}
	for _, tc := range cases {
		if got := TimestampDivisorFromSchemaNode(tc.node); got != tc.want {
			t.Errorf("%s: divisor %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestTimeLogicalTypeMapping pins the deliberate choice for TIME: the engine
// has no time-of-day type, so a TIME column stays the file's own integer in
// the file's own unit rather than being coerced into an instant.
func TestTimeLogicalTypeMapping(t *testing.T) {
	i32, i64 := PhysicalInt32, PhysicalInt64
	if got := TypeIDFromSchemaNode(&SchemaNode{Name: "t", Type: &i32, LogicalType: &LogicalType{Type: LogicalTimeMillis}}); got != TypeInt32 {
		t.Errorf("TIME_MILLIS: got %v, want TypeInt32", got)
	}
	if got := TypeIDFromSchemaNode(&SchemaNode{Name: "t", Type: &i64, LogicalType: &LogicalType{Type: LogicalTimeMicros}}); got != TypeInt64 {
		t.Errorf("TIME_MICROS: got %v, want TypeInt64", got)
	}
}

// ratio reports the order-of-magnitude error in a failure message, which is
// what identifies a missing unit conversion at a glance (1000 or 1000000).
func ratio(got, want int64) int64 {
	if want == 0 {
		return 0
	}
	return got / want
}
