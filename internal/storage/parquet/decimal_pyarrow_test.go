package parquet

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The FOREIGN-FILE case #707 exists for, against the reference implementation
// rather than against our own writer.
//
// "Two files of one table declare one DECIMAL column at different scales" is
// not a hypothetical: registering a table's files from a producer that wrote
// `decimal128(15, 4)` against a catalog column declared `DECIMAL(15,2)` is an
// ordinary thing to do, and PyArrow is the producer most likely to have done
// it. The unit tests beside this one build their fixture with wadjet's own
// writer, so they cannot say whether the READER agrees with the format about
// what such a file holds — only PyArrow can (CLAUDE.md, "Parquet Package
// Safety": bit-exact verification against a reference implementation).
//
// It also closes the loop on #456: PyArrow parses the Apache `created_by`
// convention, and this asserts that it reads back the version wadjet stamped.
func TestPyArrowDecimalFileReadsAtTheCatalogsScale(t *testing.T) {
	if exec.Command("python3", "-c", "import pyarrow").Run() != nil {
		t.Skip("python3 with pyarrow is not importable here")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "foreign.parquet")
	out, err := exec.Command("python3", "-c", pyArrowForeignScaleScript, path).CombinedOutput()
	if err != nil {
		t.Fatalf("pyarrow write failed: %v\n%s", err, out)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// The file describes itself as (15,4) — the premise, asserted rather than
	// assumed, so this cannot pass because PyArrow happened to write (15,2).
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening the PyArrow file: %v", err)
	}
	var own Column
	for _, c := range r.Schema().Columns {
		if c.Name == "a" {
			own = c
		}
	}
	if own.Type != TypeDecimal || own.Scale != 4 {
		t.Fatalf("the PyArrow file declares column a as %v(%d,%d), want DECIMAL(15,4)",
			own.Type, own.Precision, own.Scale)
	}

	// Read against a catalog that declares (15,2). PyArrow wrote 12.7500,
	// 12.7550 and -12.7550; at the catalog's scale those are 1275, 1276 and
	// -1276 — PostgreSQL's assignment cast, half away from zero.
	catalog := []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "a", Type: TypeDecimal, Precision: 15, Scale: 2, Nullable: true},
	}
	rows, err := r.ReadRowsAs(catalog, nil)
	if err != nil {
		t.Fatalf("ReadRowsAs over the PyArrow file: %v", err)
	}
	want := []string{"1275", "1276", "-1276"}
	if len(rows) != len(want) {
		t.Fatalf("%d rows, want %d", len(rows), len(want))
	}
	for i, w := range want {
		d, ok := decBoxCarrier(rows[i]["a"])
		if !ok {
			t.Fatalf("row %d column a is %#v, want a DECIMAL box", i, rows[i]["a"])
		}
		if d.String() != w {
			t.Errorf("row %d carrier = %s, want %s at the catalog's scale 2 — PyArrow wrote "+
				"this column at scale 4 and the catalog declares scale 2 (#707)",
				i, d.String(), w)
		}
	}

	// #456, from the other side: PyArrow reads back what wadjet stamped.
	ours := filepath.Join(dir, "ours.parquet")
	var buf bytes.Buffer
	w, err := NewWriter(&buf, Schema{Columns: catalog}, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{{"id": int64(1), "a": Decimal128{Lo: 1275}}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ours, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := exec.Command("python3", "-c", pyArrowCreatedByScript, ours).CombinedOutput()
	if err != nil {
		t.Fatalf("pyarrow read failed: %v\n%s", err, got)
	}
	if s := strings.TrimSpace(string(got)); s != CreatedBy() {
		t.Errorf("pyarrow read created_by = %q, want %q (#456)", s, CreatedBy())
	}
}

// pyArrowForeignScaleScript writes one DECIMAL(15,4) column holding three values
// whose rescale to scale 2 exercises both rounding directions: exact, half
// away from zero, and half away from zero negative.
const pyArrowForeignScaleScript = `
import decimal, sys, pyarrow as pa, pyarrow.parquet as pq
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("a", pa.decimal128(15, 4), nullable=True),
])
tbl = pa.table({
    "id": pa.array([1, 2, 3], type=pa.int64()),
    "a": pa.array([decimal.Decimal("12.7500"),
                   decimal.Decimal("12.7550"),
                   decimal.Decimal("-12.7550")], type=pa.decimal128(15, 4)),
}, schema=schema)
pq.write_table(tbl, sys.argv[1])
`

// pyArrowCreatedByScript prints the footer's created_by, which is the field
// #456 put a version into and the field PyArrow's own writers use the same way.
const pyArrowCreatedByScript = `
import sys, pyarrow.parquet as pq
print(pq.ParquetFile(sys.argv[1]).metadata.created_by)
`
