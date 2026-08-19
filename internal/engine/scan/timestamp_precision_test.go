package scan

import (
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Foreign parquet files carry TIMESTAMP columns in MICROS or NANOS (the
// PyArrow, Spark and Iceberg defaults); the engine unit is MILLIS. These
// tests drive the two consumer paths that a missing conversion corrupts
// silently — the native columnar decode, and zone-map row-group pruning.
//
// Fixtures are PyArrow-written; see
// internal/storage/parquet/testdata/gen_timestamp_precision.py.

const (
	tsPrecisionFixture = "../../storage/parquet/testdata/timestamp_precision.parquet"
	tsPruneFixture     = "../../storage/parquet/testdata/timestamp_prune.parquet"
)

func openTSFixture(tb testing.TB, path string) *pqt.Reader {
	tb.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("read fixture (regen with gen_timestamp_precision.py): %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		tb.Fatalf("open reader: %v", err)
	}
	return r
}

// TestNativeReadTimestampPrecision: the native columnar decode path must
// land all three precisions on the same engine millisecond. Before the fix
// the micros column read 1000x high and the nanos column 1000000x high, so
// every instant landed in the far future while ts_millis stayed correct —
// the columns disagree with each other inside one file.
func TestNativeReadTimestampPrecision(t *testing.T) {
	r := openTSFixture(t, tsPrecisionFixture)
	fr := r.FileReader()
	schema := r.Schema().Columns

	batches, err := ReadFileBatchesNative(fr, schema, nil)
	if err != nil {
		t.Fatalf("ReadFileBatchesNative: %v", err)
	}

	colIdx := make(map[string]int, len(schema))
	for i, c := range schema {
		colIdx[c.Name] = i
		if c.Name == "ts_micros" && c.Type != pqt.TypeTimestamp {
			t.Fatalf("ts_micros advertised as %v, want TypeTimestamp", c.Type)
		}
	}

	rows := 0
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			want, ok := b.Columns[colIdx["expect_ms"]].GetInt64(i)
			if !ok {
				t.Fatalf("row %d: expect_ms is null", rows)
			}
			label, _ := b.Columns[colIdx["label"]].GetString(i)
			for _, col := range []string{"ts_millis", "ts_micros", "ts_nanos", "ts_us_utc"} {
				got, ok := b.Columns[colIdx[col]].GetInt64(i)
				if !ok {
					t.Fatalf("row %d (%s): %s is null", rows, label, col)
				}
				if got != want {
					t.Errorf("row %d (%s): %s decoded %d, want %d", rows, label, col, got, want)
				}
			}
			// Null slots must survive the in-place rescale untouched, and
			// the non-null values around them must not shift.
			msNull, msOK := b.Columns[colIdx["ts_ms_null"]].GetInt64(i)
			usNull, usOK := b.Columns[colIdx["ts_us_null"]].GetInt64(i)
			if msOK != usOK {
				t.Errorf("row %d (%s): nullability diverged (ms present=%v, us present=%v)",
					rows, label, msOK, usOK)
			}
			if msOK && (msNull != want || usNull != want) {
				t.Errorf("row %d (%s): nullable columns decoded %d/%d, want %d",
					rows, label, msNull, usNull, want)
			}
			rows++
		}
	}
	if rows == 0 {
		t.Fatal("fixture decoded to zero rows")
	}
}

// TestTimestampStatsDoNotPruneMatchingRowGroup is the row-loss regression.
//
// The fixture's ts_micros column stores ~1.755e15 (microseconds); a query
// predicate on that column is in engine milliseconds, ~1.755e12. Row-group
// statistics decoded straight from the footer are raw microseconds, and
// CanPruneRowGroup compares int64 against int64 perfectly happily — the
// mismatch never declines, it just answers in the wrong domain. Every row
// group looks entirely above the predicate's range, so all three are pruned
// and the query returns zero rows with no error and no warning.
//
// The assertion is the one that matters operationally: the rows come back.
func TestTimestampStatsDoNotPruneMatchingRowGroup(t *testing.T) {
	r := openTSFixture(t, tsPruneFixture)
	fr := r.FileReader()
	schema := r.Schema().Columns

	if fr.NumRowGroups() < 3 {
		t.Fatalf("fixture has %d row groups, want >= 3 — regenerate it", fr.NumRowGroups())
	}

	colIdx := make(map[string]int, len(schema))
	for i, c := range schema {
		colIdx[c.Name] = i
	}

	// Row group 1 holds rows 100..199, i.e. base+100 .. base+199 millis.
	const base = 1755000000000
	const lo, hi = base + 100, base + 199

	for _, col := range []string{"ts_micros", "ts_nanos", "ts_millis"} {
		preds := []StatsPredicate{
			{col, exec.OpGe, int64(lo)},
			{col, exec.OpLe, int64(hi)},
		}

		matched := 0
		var pool *batch.BatchPool
		for rg := 0; rg < fr.NumRowGroups(); rg++ {
			stats := fr.RowGroupStats(rg)

			// Fixture sanity: without per-row-group statistics this test
			// would pass by accident, having pruned nothing.
			cs, ok := stats.Columns[col]
			if !ok || !cs.HasStats || cs.MinValue == nil {
				t.Fatalf("%s rg%d: fixture carries no statistics — regenerate it", col, rg)
			}

			pruned := false
			for _, p := range preds {
				if CanPruneRowGroup(p, stats) {
					pruned = true
					break
				}
			}
			if pruned {
				continue
			}

			b, err := ReadRowGroupNative(fr, rg, schema, pool)
			if err != nil {
				t.Fatalf("%s rg%d: ReadRowGroupNative: %v", col, rg, err)
			}
			for i := 0; i < b.Len; i++ {
				v, ok := b.Columns[colIdx[col]].GetInt64(i)
				if !ok {
					continue
				}
				if v < lo || v > hi {
					continue
				}
				want, _ := b.Columns[colIdx["expect_ms"]].GetInt64(i)
				if v != want {
					t.Errorf("%s: row decoded %d, want %d", col, v, want)
				}
				matched++
			}
		}

		if matched != 100 {
			t.Errorf("%s: predicate [%d,%d] returned %d rows, want 100 — "+
				"row groups holding matching rows were pruned by statistics in the wrong unit",
				col, lo, hi, matched)
		}
	}
}

// TestTimestampStatsBoundDecodedValues: pruning is only sound if the scaled
// [min,max] still contains every decoded value in the row group. Flooring is
// monotonic so it does; this checks it against the fixture rather than
// trusting the argument.
func TestTimestampStatsBoundDecodedValues(t *testing.T) {
	r := openTSFixture(t, tsPruneFixture)
	fr := r.FileReader()
	schema := r.Schema().Columns

	colIdx := make(map[string]int, len(schema))
	for i, c := range schema {
		colIdx[c.Name] = i
	}

	var pool *batch.BatchPool
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		stats := fr.RowGroupStats(rg)
		b, err := ReadRowGroupNative(fr, rg, schema, pool)
		if err != nil {
			t.Fatalf("rg%d: %v", rg, err)
		}
		for _, col := range []string{"ts_micros", "ts_nanos", "ts_millis"} {
			cs := stats.Columns[col]
			mn, ok1 := cs.MinValue.(int64)
			mx, ok2 := cs.MaxValue.(int64)
			if !ok1 || !ok2 {
				t.Fatalf("rg%d %s: stats %T/%T, want int64", rg, col, cs.MinValue, cs.MaxValue)
			}
			for i := 0; i < b.Len; i++ {
				v, ok := b.Columns[colIdx[col]].GetInt64(i)
				if !ok {
					continue
				}
				if v < mn || v > mx {
					t.Errorf("rg%d %s: decoded %d outside stats [%d,%d] — pruning would be unsound",
						rg, col, v, mn, mx)
				}
			}
		}
	}
}

// TestDictPruneDeclinesForeignTimestampUnits: dictionary probing compares an
// engine-unit literal against RAW dictionary entries. For a micro/nano
// column those never match, and an "absent" verdict prunes the whole row
// group — dropping rows that do match. Unlike min/max bounds this cannot be
// rescaled into agreement (one engine millisecond covers a 1000-wide band of
// stored micros), so the probe must decline.
func TestDictPruneDeclinesForeignTimestampUnits(t *testing.T) {
	r := openTSFixture(t, tsPruneFixture)
	fr := r.FileReader()

	// Fixture sanity: both columns must really be pure-dictionary chunks,
	// or this test proves nothing — a chunk with no dictionary page
	// declines for an unrelated reason.
	dictCol := -1
	dictColMs := -1
	for i, leaf := range fr.Leaves() {
		switch leaf.Name {
		case "ts_dict":
			dictCol = i
		case "ts_dict_ms":
			dictColMs = i
		}
	}
	if dictCol < 0 || dictColMs < 0 {
		t.Fatal("fixture lacks ts_dict/ts_dict_ms — regenerate it")
	}
	for _, idx := range []int{dictCol, dictColMs} {
		pr := fr.ColumnPages(0, idx)
		if pr == nil {
			t.Fatalf("col %d: no pages", idx)
		}
		_, pure, err := pr.DictionaryIfPure()
		pr.Close()
		if err != nil || !pure {
			t.Fatalf("col %d: chunk is not pure-dictionary (pure=%v err=%v) — regenerate the fixture",
				idx, pure, err)
		}
	}

	// ts_dict holds base+0 .. base+9 as MICROS. The probe is the engine
	// value the planner would build, so it can never equal a stored micro.
	const base = 1755000000000
	present := []EqProbe{{ColName: "ts_dict", Value: int64(base + 5)}}
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		if CanDictPruneRowGroup(fr, rg, present) {
			t.Errorf("rg%d: dictionary probe pruned a micros TIMESTAMP row group that contains the value; "+
				"the probe is in engine millis and the dictionary is in file micros", rg)
		}
	}

	// Control: the same probe against the MILLIS twin must still prune when
	// the value is genuinely absent. Declining the foreign-unit case must
	// not turn dictionary pruning off wholesale.
	absent := []EqProbe{{ColName: "ts_dict_ms", Value: int64(base + 9999)}}
	pruned := 0
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		if CanDictPruneRowGroup(fr, rg, absent) {
			pruned++
		}
	}
	if pruned != fr.NumRowGroups() {
		t.Errorf("millis control: pruned %d/%d row groups for an absent value, want all — "+
			"dictionary pruning stopped engaging", pruned, fr.NumRowGroups())
	}
	presentMs := []EqProbe{{ColName: "ts_dict_ms", Value: int64(base + 5)}}
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		if CanDictPruneRowGroup(fr, rg, presentMs) {
			t.Errorf("millis control rg%d: pruned a row group that contains the value", rg)
		}
	}
}
