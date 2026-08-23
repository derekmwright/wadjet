package scan

import (
	"math/big"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Parquet stores a DECIMAL over any of four physical types. Our writer emits
// INT64; PyArrow emits the narrowest that fits, so DECIMAL(9,2) is INT32,
// DECIMAL(18,4) is INT64 and DECIMAL(38,10) is FIXED_LEN_BYTE_ARRAY. The
// native scan refused everything but INT64 outright ("decimal column %q:
// unsupported physical encoding"), which made a pyarrow-written decimal
// column unreadable on this path — and the refusal was reached BEFORE the
// non-INT64 decode that had been added for exactly those physicals, so that
// decode was unreachable.
//
// Fixtures: internal/storage/parquet/testdata/gen_decimal_physicals.py
// (PyArrow, store_decimal_as_integer=True). The expected values come out of
// the file's own unscaled_* columns, so this compares against the reference
// writer's encoding, not against a value recomputed here.

const (
	decimalPhysicalsFixture     = "../../storage/parquet/testdata/decimal_physicals.parquet"
	decimalPhysicalsDictFixture = "../../storage/parquet/testdata/decimal_physicals_dict.parquet"
)

func TestNativeScanDecodesEveryDecimalPhysical(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{"plain", decimalPhysicalsFixture},
		{"dictionary", decimalPhysicalsDictFixture},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile(tc.path)
			if err != nil {
				t.Skipf("fixture missing (regen with testdata/gen_decimal_physicals.py): %v", err)
			}
			fr, err := pqt.OpenFileReaderFromBytes(data)
			if err != nil {
				t.Fatalf("opening fixture: %v", err)
			}

			schema := []pqt.Column{
				{Name: "d9_2", Type: pqt.TypeDecimal, Nullable: true, Precision: 9, Scale: 2},
				{Name: "d18_4", Type: pqt.TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
				{Name: "d38_10", Type: pqt.TypeDecimal, Nullable: true, Precision: 38, Scale: 10},
				{Name: "unscaled_9_2", Type: pqt.TypeInt64, Nullable: true},
				{Name: "unscaled_18_4", Type: pqt.TypeInt64, Nullable: true},
				{Name: "unscaled_38_10", Type: pqt.TypeString, Nullable: true},
			}
			// The three physicals really are different in the file.
			wantPhys := map[string]pqt.PhysicalType{
				"d9_2":   pqt.PhysicalInt32,
				"d18_4":  pqt.PhysicalInt64,
				"d38_10": pqt.PhysicalFixedLenByteArray,
			}
			for _, leaf := range fr.Leaves() {
				if want, ok := wantPhys[leaf.Name]; ok {
					if leaf.Type == nil || *leaf.Type != want {
						t.Fatalf("fixture: %s is %v, want %s (regenerate it)", leaf.Name, leaf.Type, want)
					}
				}
			}

			b, err := ReadRowGroupNative(fr, 0, schema, nil)
			if err != nil {
				t.Fatalf("ReadRowGroupNative: %v", err)
			}
			if b == nil || b.Len == 0 {
				t.Fatal("no rows decoded")
			}

			for _, pair := range []struct{ dec, unscaled int }{{0, 3}, {1, 4}, {2, 5}} {
				dv, uv := b.Columns[pair.dec], b.Columns[pair.unscaled]
				for row := 0; row < b.Len; row++ {
					if uv.Nulls.IsNullFast(row) {
						if !dv.Nulls.IsNullFast(row) {
							t.Errorf("%s row %d: want NULL, got %v",
								schema[pair.dec].Name, row, dv.DecimalData.Data[row])
						}
						continue
					}
					if dv.Nulls.IsNullFast(row) {
						t.Fatalf("%s row %d: decoded NULL, want a value", schema[pair.dec].Name, row)
					}
					want := new(big.Int)
					if pair.unscaled == 5 {
						if _, ok := want.SetString(uv.BytesData.StringValue(row), 10); !ok {
							t.Fatalf("fixture: unparsable unscaled value %q", uv.BytesData.StringValue(row))
						}
					} else {
						want.SetInt64(uv.Int64Data[row])
					}
					if got := int128ToBig(dv.DecimalData.Data[row]); got.Cmp(want) != 0 {
						t.Errorf("%s row %d: unscaled = %s, want %s",
							schema[pair.dec].Name, row, got, want)
					}
				}
			}
		})
	}
}

// TestRowReaderDecodesEveryDecimalPhysical is the same claim on the other
// read path: resolveDictForRows had no TypeDecimal case, so a dict-encoded
// DECIMAL over INT32/INT64 hit the BYTE_ARRAY default and errored out
// ("declares 6 entries but decoded -1") instead of decoding.
func TestRowReaderDecodesEveryDecimalPhysical(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{"plain", decimalPhysicalsFixture},
		{"dictionary", decimalPhysicalsDictFixture},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile(tc.path)
			if err != nil {
				t.Skipf("fixture missing (regen with testdata/gen_decimal_physicals.py): %v", err)
			}
			r, err := pqt.NewReaderFromBytes(data)
			if err != nil {
				t.Fatalf("opening fixture: %v", err)
			}
			rows, err := r.ReadRowsAs([]pqt.Column{
				{Name: "d9_2", Type: pqt.TypeDecimal, Nullable: true, Precision: 9, Scale: 2},
				{Name: "d18_4", Type: pqt.TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
				{Name: "unscaled_9_2", Type: pqt.TypeInt64, Nullable: true},
				{Name: "unscaled_18_4", Type: pqt.TypeInt64, Nullable: true},
			}, []string{"d9_2", "d18_4", "unscaled_9_2", "unscaled_18_4"})
			if err != nil {
				t.Fatalf("ReadRowsAs: %v", err)
			}
			if len(rows) == 0 {
				t.Fatal("no rows decoded")
			}
			for i, row := range rows {
				for _, pair := range [][2]string{{"d9_2", "unscaled_9_2"}, {"d18_4", "unscaled_18_4"}} {
					want, present := row[pair[1]]
					got, gotPresent := row[pair[0]]
					if present != gotPresent {
						t.Errorf("row %d %s: present=%v, want %v", i, pair[0], gotPresent, present)
						continue
					}
					if !present {
						continue
					}
					if got != want {
						t.Errorf("row %d %s: unscaled = %v, want %v", i, pair[0], got, want)
					}
				}
			}
		})
	}
}

func int128ToBig(v batch.Int128) *big.Int {
	hi := big.NewInt(v.Hi)
	hi.Lsh(hi, 64)
	lo := new(big.Int).SetUint64(v.Lo)
	return hi.Add(hi, lo)
}
