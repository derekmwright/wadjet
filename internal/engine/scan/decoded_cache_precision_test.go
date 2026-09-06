package scan

import (
	"bytes"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

func decCachePrecFile(t *testing.T, cols []pqt.Column, rows []map[string]any) *pqt.FileReader {
	t.Helper()
	var buf bytes.Buffer
	w, e := pqt.NewWriter(&buf, pqt.Schema{Columns: cols}, pqt.DefaultWriterConfig())
	if e != nil {
		t.Fatal(e)
	}
	if e = w.WriteRows(rows); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := pqt.OpenFileReaderFromBytes(buf.Bytes())
	if e != nil {
		t.Fatal(e)
	}
	return f
}

// #914: a value served from the decoded cache must apply the SAME DECIMAL
// precision/overflow check the cold decode applies. Warming the cache under a
// wide declaration must not let a later narrow read serve a value that
// overflows its declared precision — whether an invalid value is accepted must
// not depend on cache history.
func TestAdversarialScanCachePrecision(t *testing.T) {
	f := decCachePrecFile(t, []pqt.Column{{Name: "d", Type: pqt.TypeDecimal, Precision: 9, Scale: 0}}, []map[string]any{{"d": "1234"}})
	f.SetCacheIdentity("immutable-fixture")
	broad := []pqt.Column{{Name: "d", Type: pqt.TypeDecimal, Precision: 9, Scale: 2}}
	narrow := []pqt.Column{{Name: "d", Type: pqt.TypeDecimal, Precision: 3, Scale: 2}}
	if _, e := ReadRowGroupNative(f, 0, narrow, nil); e == nil {
		t.Fatal("cold read unexpectedly accepted 1234 as DECIMAL(3,2)")
	} else {
		t.Logf("cold read refuses: %v", e)
	}
	cache := NewDecodedChunkCache(1 << 20)
	for i := 0; i < 2; i++ {
		if _, e := ReadRowGroupNativeCached(f, 0, broad, nil, cache); e != nil {
			t.Fatal(e)
		}
	}
	b, e := ReadRowGroupNativeCached(f, 0, narrow, nil, cache)
	if e == nil {
		t.Errorf("warm cache accepts DECIMAL(3,2): %v; stats=%+v", b.ToRows(), cache.Stats())
	} else {
		t.Logf("warm cache refuses: %v", e)
	}
}
