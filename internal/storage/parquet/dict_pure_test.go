package parquet

import (
	"os"
	"testing"
)

// DictionaryIfPure must refuse to certify chunks with PLAIN fallback data
// pages — the mixed-encoding layout in testdata/dict_fallback.parquet is
// exactly the adversarial case: a dictionary page EXISTS, but data pages
// beyond the overflow point are PLAIN, so dictionary membership proves
// nothing about the chunk's values. A wrongly-certified chunk would let
// the scan's dictionary-probe pruning silently drop matching rows.
func TestDictionaryIfPureRejectsFallback(t *testing.T) {
	f, err := os.Open("testdata/dict_fallback.parquet")
	if err != nil {
		t.Skip("fixture not present")
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	fr, err := OpenFileReader(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}

	pure, impure := 0, 0
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		for col := range fr.Leaves() {
			pr := fr.ColumnPages(rg, col)
			dict, ok, err := pr.DictionaryIfPure()
			if err != nil {
				t.Fatalf("rg %d col %d: %v", rg, col, err)
			}
			if ok {
				if dict == nil || dict.NumValues == 0 {
					t.Fatalf("rg %d col %d: certified pure with empty dictionary", rg, col)
				}
				pure++
			} else {
				impure++
			}
			pr.Close()
		}
	}
	t.Logf("pure=%d impure=%d", pure, impure)
	// The fixture is BUILT to contain fallback chunks; if every chunk
	// certifies pure, the detector is broken in the dangerous direction.
	if impure == 0 {
		t.Fatal("no chunk rejected — fallback detection is not working")
	}
	// And a reader whose position was walked must still decode normally
	// afterwards: DictionaryIfPure must not advance reader state.
	pr := fr.ColumnPages(0, 0)
	defer pr.Close()
	if _, _, err := pr.DictionaryIfPure(); err != nil {
		t.Fatal(err)
	}
	d, err := pr.NextDictionary()
	if err != nil {
		t.Fatalf("NextDictionary after walk: %v", err)
	}
	_ = d
	pg, err := pr.NextPage()
	if err != nil {
		t.Fatalf("NextPage after walk: %v", err)
	}
	if pg == nil {
		t.Fatal("no page after DictionaryIfPure walk — reader state was consumed")
	}
}
