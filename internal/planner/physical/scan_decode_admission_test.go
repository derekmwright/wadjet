// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The read-ahead admission (scan_decode_admission.go) admits a decode against
// footerDecodeEstimate, so the estimate has to cover what a decode really
// holds — the PEAK, not the final batch. These gates hold it to a measurement
// on every column type the native decoder reads.

func writeAdmissionFixture(t *testing.T, schema parquet.Schema, rows []map[string]any, rowGroup int) *parquet.FileReader {
	t.Helper()
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, parquet.WriterConfig{RowGroupSize: rowGroup})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	fr, err := parquet.OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return fr
}

// TestTheFooterEstimateCoversADecodesPeak decodes one row group of one column
// at a time — every flat type of the type matrix, all-NULL columns of three
// carriers, and 512-byte strings — and asserts, per column:
//
//   - the decoded half of the estimate is at least the decoded batch's
//     MemBytes (the charge trackScanBatch forces after the decode);
//   - decoded + transient is at least EVERYTHING the decode allocated, garbage
//     included (runtime TotalAlloc across the call): an upper bound on its
//     live peak that needs no sampling.
//
// One column at a time because that is the unit the decoder runs
// concurrently; the estimate sums the GOMAXPROCS largest columns' transients
// for exactly that reason. The allocation half is skipped under the race
// detector, which changes what a call allocates.
func TestTheFooterEstimateCoversADecodesPeak(t *testing.T) {
	type fixture struct {
		name string
		fr   *parquet.FileReader
		cols []parquet.Column
	}
	var fixtures []fixture
	tm := typematrix.Schema()
	tmFR := writeAdmissionFixture(t, tm, typematrix.Data(typematrix.Rows), typematrix.RowGroup)
	for _, c := range tm.Columns {
		fixtures = append(fixtures, fixture{"typemx." + c.Name, tmFR, []parquet.Column{c}})
	}
	nulls := parquet.Schema{Columns: []parquet.Column{
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "i", Type: parquet.TypeInt64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Nullable: true, Precision: 38, Scale: 10},
	}}
	nrows := make([]map[string]any, 2200)
	for i := range nrows {
		nrows[i] = map[string]any{"s": nil, "i": nil, "d": nil}
	}
	nullFR := writeAdmissionFixture(t, nulls, nrows, 1100)
	for _, c := range nulls.Columns {
		fixtures = append(fixtures, fixture{"all-NULL " + c.Name, nullFR, []parquet.Column{c}})
	}
	long := parquet.Schema{Columns: []parquet.Column{{Name: "pad", Type: parquet.TypeString}}}
	lrows := make([]map[string]any, 1024)
	for i := range lrows {
		lrows[i] = map[string]any{"pad": fmt.Sprintf("%0512d", i)}
	}
	fixtures = append(fixtures, fixture{"512-byte strings", writeAdmissionFixture(t, long, lrows, 512), long.Columns})

	for _, f := range fixtures {
		decoded, transient, ok := footerDecodeEstimate(f.fr, 0, f.cols)
		if !ok {
			t.Errorf("%s: the footer gave no estimate for a PLAIN column", f.name)
			continue
		}
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		b, err := scan.ReadRowGroupNative(f.fr, 0, f.cols, nil)
		runtime.ReadMemStats(&m1)
		if err != nil {
			t.Fatalf("%s: decode: %v", f.name, err)
		}
		alloc := int64(m1.TotalAlloc - m0.TotalAlloc)
		t.Logf("%-22s decoded=%7d; estimate decoded=%7d transient=%7d sum=%7d; allocated during the decode=%7d",
			f.name, b.MemBytes(), decoded, transient, decoded+transient, alloc)
		if decoded < b.MemBytes() {
			t.Errorf("%s: decoded estimate %d is under the decoded batch's %d bytes", f.name, decoded, b.MemBytes())
		}
		if !raceEnabled && decoded+transient < alloc {
			t.Errorf("%s: estimate %d is under the %d bytes the decode allocated: it would admit a decode "+
				"whose peak it does not cover", f.name, decoded+transient, alloc)
		}
	}
}

// A byte-array chunk with a dictionary page decodes to its entries EXPANDED per
// row, which no footer field bounds (measured on DuckDB's c_mktsegment: 2,514
// uncompressed bytes decoding to 21,196). The estimate must decline rather than
// guess, and a declined estimate puts the scan on demand.
func TestADictionaryEncodedByteArrayHasNoFooterEstimate(t *testing.T) {
	raw, err := os.ReadFile("../../../benchmarks/tpch/duckdb-data/customer.parquet")
	if err != nil {
		t.Skipf("DuckDB fixture: %v", err)
	}
	fr, err := parquet.OpenFileReaderFromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	var seg parquet.Column
	found := false
	for _, c := range fr.Schema().Columns {
		if c.Name == "c_mktsegment" {
			seg, found = c, true
		}
	}
	if !found {
		t.Fatal("fixture has no c_mktsegment")
	}
	if md := fr.RowGroupMeta(0).Columns; len(md) == 0 {
		t.Fatal("no column chunks")
	}
	if _, _, ok := footerDecodeEstimate(fr, 0, []parquet.Column{seg}); ok {
		t.Fatal("the footer estimate answered for a dictionary-encoded string chunk, whose decoded size it cannot know")
	}
}

// Without a budget the rule costs nothing: every read-ahead is admitted and
// the ledger is not touched, not even transiently.
func TestWithoutABudgetEveryReadAheadIsAdmittedForFree(t *testing.T) {
	tracker := memory.NewTracker("no-budget", 0)
	inner := &scanSourceInner{memTracker: tracker}
	if !inner.admitReadAhead(0) {
		t.Fatal("a read-ahead without a budget was not admitted")
	}
	if tracker.Peak() != 0 {
		t.Fatalf("the ledger moved (peak %d) for a read-ahead with no budget", tracker.Peak())
	}
	if inner.onDemand.Load() {
		t.Fatal("a scan without a budget was put on demand")
	}
}
