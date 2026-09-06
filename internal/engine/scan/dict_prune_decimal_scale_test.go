package scan

import (
	"bytes"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
	gp "github.com/parquet-go/parquet-go"
)

// #916: dictionary pruning must reconcile the equality probe's catalog DECIMAL
// carrier and the file dictionary's carrier to a common scale before comparing.
// A file at DECIMAL(18,0) holding 12 read under catalog DECIMAL(9,2) matches
// 12.00; the prune must not declare the matching row group empty.
func TestAdversarialScanDecimalDictionaryScale(t *testing.T) {
	type record struct {
		D int64 `parquet:"d,dict,decimal(0:18)"`
	}
	var buf bytes.Buffer
	w := gp.NewGenericWriter[record](&buf)
	if _, e := w.Write([]record{{12}}); e != nil {
		t.Fatal(e)
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := pqt.OpenFileReaderFromBytes(buf.Bytes())
	if e != nil {
		t.Fatal(e)
	}
	b, e := ReadRowGroupNative(f, 0, []pqt.Column{{Name: "d", Type: pqt.TypeDecimal, Precision: 9, Scale: 2}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("unpruned rows: %v", b.ToRows())
	lit, ok := kernel.StatsDomainValue(pqt.TypeDecimal, 2, "12.00")
	if !ok {
		t.Fatal("literal conversion failed")
	}
	t.Logf("catalog literal: %v", lit)
	// The planner carries the catalog DECIMAL declaration onto the probe so
	// the file (18,0) dictionary and the (9,2) carrier reconcile before the
	// comparison (dict_prune.go). Without the reconcile, 1200 is searched for
	// in a dictionary holding 12 and the matching row group is pruned.
	if CanDictPruneRowGroup(f, 0, []EqProbe{{ColName: "d", Value: lit, Scale: 2, Precision: 9}}) {
		t.Error("dictionary prunes matching decimal after file-to-catalog rescaling")
	}
	// A value the file genuinely cannot hold IS provably absent and must still
	// prune: 13.00 has no carrier the (18,0) dictionary {12} can equal.
	absent, _ := kernel.StatsDomainValue(pqt.TypeDecimal, 2, "13.00")
	if !CanDictPruneRowGroup(f, 0, []EqProbe{{ColName: "d", Value: absent, Scale: 2, Precision: 9}}) {
		t.Error("dictionary failed to prune a genuinely absent decimal (13.00 vs {12})")
	}
}

// #916 (negative + differing scale): a negative file carrier at one scale must
// reconcile against a catalog probe at another before the equality comparison,
// both for a present match (no prune) and a genuine absence (prune).
func TestAdversarialScanDecimalDictionaryScaleNegative(t *testing.T) {
	type record struct {
		D int64 `parquet:"d,dict,decimal(0:18)"`
	}
	var buf bytes.Buffer
	w := gp.NewGenericWriter[record](&buf)
	if _, e := w.Write([]record{{-7}}); e != nil {
		t.Fatal(e)
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := pqt.OpenFileReaderFromBytes(buf.Bytes())
	if e != nil {
		t.Fatal(e)
	}
	present, _ := kernel.StatsDomainValue(pqt.TypeDecimal, 3, "-7.000") // carrier -7000 at scale 3
	if CanDictPruneRowGroup(f, 0, []EqProbe{{ColName: "d", Value: present, Scale: 3, Precision: 9}}) {
		t.Error("dictionary prunes matching -7 against catalog -7.000")
	}
	absent, _ := kernel.StatsDomainValue(pqt.TypeDecimal, 3, "-7.500")
	if !CanDictPruneRowGroup(f, 0, []EqProbe{{ColName: "d", Value: absent, Scale: 3, Precision: 9}}) {
		t.Error("dictionary failed to prune genuinely absent -7.500 against {-7}")
	}
}
