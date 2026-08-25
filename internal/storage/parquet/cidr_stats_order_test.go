package parquet

import (
	"bytes"
	"testing"
)

// TestCidrStatsSortKeyOrdersLikeInet pins the ADR-0018 §6 example directly
// against the local sort-key helper (#523): "9.0.0.0/8" is BELOW
// "10.0.0.0/8" in PostgreSQL's inet order and ABOVE it as text.
func TestCidrStatsSortKeyOrdersLikeInet(t *testing.T) {
	lo, ok := CidrStatsSortKey("9.0.0.0/8")
	if !ok {
		t.Fatal("CidrStatsSortKey(9.0.0.0/8) ok = false")
	}
	hi, ok := CidrStatsSortKey("10.0.0.0/8")
	if !ok {
		t.Fatal("CidrStatsSortKey(10.0.0.0/8) ok = false")
	}
	if !("9.0.0.0/8" > "10.0.0.0/8") {
		t.Fatal("test setup: \"9.0.0.0/8\" is not above \"10.0.0.0/8\" as text — the case this test exists for")
	}
	if lo >= hi {
		t.Errorf("CidrStatsSortKey(9.0.0.0/8) = %x, want it BELOW CidrStatsSortKey(10.0.0.0/8) = %x", lo, hi)
	}
}

// TestCidrStatsSortKeyRefusesGarbage pins that an unparseable value is
// refused rather than silently keyed as something — the fallback
// updateStatsCIDR takes on this signal, not a value it could be misread as.
func TestCidrStatsSortKeyRefusesGarbage(t *testing.T) {
	if _, ok := CidrStatsSortKey("not a cidr"); ok {
		t.Error("CidrStatsSortKey(\"not a cidr\") ok = true, want false")
	}
}

// cidrStatsTestFile writes a single-row-group file with one CIDR column
// (plus an id column so the schema is not degenerate) and returns the raw
// bytes, for the tests below to read back and mutate.
func cidrStatsTestFile(t *testing.T, cidrVals []string) []byte {
	t.Helper()
	schema := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "c_cidr", Type: TypeCIDR},
	}}
	rows := make([]map[string]any, len(cidrVals))
	for i, v := range cidrVals {
		rows[i] = map[string]any{"id": int64(i), "c_cidr": v}
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// TestCIDRRowGroupStatsAreInetOrdered is the #523 regression: a fresh file's
// CIDR min/max are the row group's true PostgreSQL inet-order extremes, not
// the text-order ones, and the footer carries the promise a reader checks
// before trusting them. Includes a HOST-BEARING prefix (address bits set
// past the mask, the shape #492's original CidrSortKey fix was for) among
// the rows, per the issue's own regression ask.
func TestCIDRRowGroupStatsAreInetOrdered(t *testing.T) {
	// Text order would put "10.0.0.0/24" and "192.168.188.190/24" as the
	// extremes (digit '1' < '9'); inet order puns "9.0.0.0/8" below every
	// 10.x/192.x address and "192.168.188.190/24" above them all.
	vals := []string{"10.0.0.0/24", "192.168.188.190/24", "9.0.0.0/8", "172.16.0.5/16"}
	data := cidrStatsTestFile(t, vals)

	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	fr := r.FileReader()
	if !fr.cidrStatsAreInetOrder() {
		t.Fatal("a fresh file with only valid CIDR values must carry CidrStatsOrderKey=inet")
	}

	stats := fr.RowGroupStats(0)
	cs, ok := stats.Columns["c_cidr"]
	if !ok || !cs.HasStats {
		t.Fatalf("c_cidr stats = %+v, want HasStats", cs)
	}
	wantMin, ok := CidrStatsSortKey("9.0.0.0/8")
	if !ok {
		t.Fatal("CidrStatsSortKey(9.0.0.0/8) ok = false")
	}
	wantMax, ok := CidrStatsSortKey("192.168.188.190/24")
	if !ok {
		t.Fatal("CidrStatsSortKey(192.168.188.190/24) ok = false")
	}
	gotMin, ok := cs.MinValue.(CidrInetBound)
	if !ok || gotMin.Key != wantMin {
		t.Errorf("MinValue = %#v, want the inet-order minimum (9.0.0.0/8)'s sort key, boxed as CidrInetBound", cs.MinValue)
	}
	gotMax, ok := cs.MaxValue.(CidrInetBound)
	if !ok || gotMax.Key != wantMax {
		t.Errorf("MaxValue = %#v, want the inet-order maximum (192.168.188.190/24)'s sort key, boxed as CidrInetBound", cs.MaxValue)
	}
	// The box also carries the winning rows' TEXT, which is what the
	// CATALOG persists — the Key is binary and JSON cannot hold it.
	if gotMin.Text != "9.0.0.0/8" {
		t.Errorf("MinValue.Text = %q, want the winning row's address text %q", gotMin.Text, "9.0.0.0/8")
	}
	if gotMax.Text != "192.168.188.190/24" {
		t.Errorf("MaxValue.Text = %q, want the winning row's address text %q", gotMax.Text, "192.168.188.190/24")
	}
}

// TestCIDRRowGroupStatsWithheldOnUnparseableValue: a single unparseable CIDR
// value anywhere in the file suppresses CidrStatsOrderKey for the WHOLE
// file, and a reader withholds that column's stats entirely rather than
// trust a bound the writer could not fully verify.
func TestCIDRRowGroupStatsWithheldOnUnparseableValue(t *testing.T) {
	data := cidrStatsTestFile(t, []string{"10.0.0.0/24", "not-an-address", "192.168.1.0/24"})

	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	fr := r.FileReader()
	if fr.cidrStatsAreInetOrder() {
		t.Fatal("a file with an unparseable CIDR value must not carry CidrStatsOrderKey=inet")
	}
	cs, ok := fr.RowGroupStats(0).Columns["c_cidr"]
	if !ok {
		t.Fatal("c_cidr missing from RowGroupStats entirely")
	}
	if cs.HasStats {
		t.Errorf("c_cidr stats = %+v, want withheld (HasStats=false)", cs)
	}
}

// TestCIDRRowGroupStatsWithheldOnOldFooter is the other #523 regression
// fixture: an OLD file — simulated by stripping CidrStatsOrderKey from a
// file that would otherwise prune — must still withhold, never compare a
// text-order bound as if it were inet-ordered.
func TestCIDRRowGroupStatsWithheldOnOldFooter(t *testing.T) {
	data := cidrStatsTestFile(t, []string{"10.0.0.0/24", "9.0.0.0/8", "192.168.188.190/24"})

	stripped, err := StripCidrStatsOrder(data)
	if err != nil {
		t.Fatalf("StripCidrStatsOrder: %v", err)
	}

	r, err := NewReaderFromBytes(stripped)
	if err != nil {
		t.Fatalf("NewReaderFromBytes (stripped): %v", err)
	}
	fr := r.FileReader()
	if fr.cidrStatsAreInetOrder() {
		t.Fatal("a stripped file must not report CidrStatsOrderKey=inet")
	}
	cs, ok := fr.RowGroupStats(0).Columns["c_cidr"]
	if !ok {
		t.Fatal("c_cidr missing from RowGroupStats entirely")
	}
	if cs.HasStats {
		t.Errorf("c_cidr stats on a stripped (old-style) footer = %+v, want withheld", cs)
	}

	// The un-stripped file, read fresh, still prunes — the strip changed
	// only the copy under test.
	r2, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("NewReaderFromBytes (original): %v", err)
	}
	if !r2.FileReader().cidrStatsAreInetOrder() {
		t.Fatal("the original file lost its CidrStatsOrderKey flag — StripCidrStatsOrder mutated its input")
	}
}

// TestStripCidrStatsOrderErrorsWithoutTheKey mirrors StripDeclaredSchema's
// own symmetry check: stripping a key that is not there is a named error,
// not a silent no-op.
func TestStripCidrStatsOrderErrorsWithoutTheKey(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: TypeInt64}}}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRows([]map[string]any{{"id": int64(1)}}); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := StripCidrStatsOrder(buf.Bytes()); err == nil {
		t.Fatal("want an error stripping CidrStatsOrderKey from a file with no CIDR column")
	}
}
