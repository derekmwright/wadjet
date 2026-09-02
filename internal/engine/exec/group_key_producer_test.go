package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The gate #788 needed: for EVERY type and EVERY producer, one value has ONE
// merge key.
//
// A HashAggregate produces the bytes the k-way merger compares from four
// different places, holding the same value in three different Go boxes (see
// group_key_encoder.go). Nothing in the tree compared them. A DATE key was
// `14610` from the int-keyed drain and `"\n2010-01-01"` from the boxed
// remainder, the merge never combined the two, and the query answered 772-841
// rows for 421 groups with sum(n) unchanged — right totals, wrong grouping,
// invisible to every row-count and every value assertion.
//
// This is the seam test that would have caught it, and it is a type SWEEP
// rather than a DATE test because the defect is structural: any type whose two
// boxings disagree has it, and the next one added has it on the day it is
// added. IPv4 and MAC are the two the sweep found sharing DATE's shape.
//
// It deliberately does NOT go through a query. A query reaches these producers
// only under a memory budget, a key-path migration and a partitioned plan all
// at once — which is why #788 survived four investigation rounds — and a gate
// whose trigger is a condition rather than a shape cannot be trusted to fire.
// wadjet.TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget is the end-to-end
// arm; this one is decidable.
func TestEveryGroupKeyProducerWritesTheSameBytes(t *testing.T) {
	for _, s := range gkpSamples() {
		t.Run(s.typ.String(), func(t *testing.T) {
			col := s.column()
			v := s.vector(t)
			box := v.GetValue(0)
			if box == nil {
				t.Fatalf("the sample stored a NULL — this cell would compare the null marker with itself")
			}

			// Producer B: the compact / str-generic drain, whose keyValues
			// were boxed at consume time by Vector.GetValue.
			// (aggregate_partial_drain_cursor.go's arena build.)
			want := appendSerializedKey(nil, []any{box}, []batch.TypeID{s.typ}, []parquet.Column{col})
			got := map[string][]byte{"boxed-GetValue": want}

			// Producer E: the ONE exported producer, used by the
			// coordinator's cross-worker re-aggregation and by the set-op
			// key (ADR-0023 item 5).
			got["AppendBoxedGroupKey"] = AppendBoxedGroupKey(nil, box, &col)

			if batch.IntStorageType(s.typ) {
				storage := s.storage(t, v)

				// Producer A: the int-mode and packed-int drains, which hold
				// the int64 STORAGE and never box it
				// (partialGroupCursor.appendIntModeSortKey).
				got["int-mode drain"] = appendTypedIntKey(nil, storage, s.typ)

				// Producer C: migrateToGenericMap, which re-boxes an
				// int-keyed group's key as the RAW int64 and hands it to the
				// boxed path — `ext.keyValues = []any{intKey}` (aggregate.go).
				// This is the producer that made patching the int drain alone
				// insufficient: the digits reach the merger from here too.
				got["migrated raw int"] = appendSerializedKey(nil, []any{storage},
					[]batch.TypeID{s.typ}, []parquet.Column{col})

				// The inverse boxing is what lets the boxed producers arrive
				// at the same integer at all, so assert it directly: a
				// GetValue box re-reads as the storage it came from.
				if iv, ok := batch.KeyStorageInt(box, s.typ); !ok || iv != storage {
					t.Errorf("batch.KeyStorageInt(%#v, %s) = %d, %v; want %d, true — "+
						"GetValue's boxing and its inverse have drifted apart",
						box, s.typ, iv, ok, storage)
				}
			}

			if genericKeyBoxingDeferrable(s.typ) {
				// Producer D: the deferred generic SoA path, which keeps no
				// box at all and rebuilds one from the BINARY hash key at
				// drain time (decodeSerializedKey).
				bin := appendColumnValue([]byte{0}, v, 0, s.typ)
				vals := decodeSerializedKey(string(bin), []batch.TypeID{s.typ})
				got["decoded from the binary key"] = appendSerializedKey(nil, vals,
					[]batch.TypeID{s.typ}, []parquet.Column{col})
			}

			for name, k := range got {
				if string(k) != string(want) {
					t.Errorf("producer %q writes %q; the boxed producer writes %q — "+
						"two byte strings for one value is two definitions of equality, "+
						"and the k-way merger keeps their groups apart (#788)",
						name, k, want)
				}
			}
			if len(got) < 2 {
				t.Fatalf("only %d producer(s) reached this type — the cell compared nothing", len(got))
			}
		})
	}
}

// TestGroupKeyProducerSweepCoversEveryDeclaredType fails when a type is added
// to the engine and not to the sweep above. Coverage that depends on
// remembering is coverage that is lost.
func TestGroupKeyProducerSweepCoversEveryDeclaredType(t *testing.T) {
	sampled := map[parquet.TypeID]bool{}
	for _, s := range gkpSamples() {
		sampled[s.typ] = true
	}
	// The four container types key through appendContainerKeyValue /
	// appendKeyElem, whose own gates are named in ADR-0023's Consequences
	// (exec.TestContainerGroupByAcrossASpillMatchesMemory,
	// TestMergeKeyKeepsBoxedTypesDistinct, and the embedded-API arm). They are
	// listed rather than skipped so the list itself is asserted.
	elsewhere := map[parquet.TypeID]string{
		parquet.TypeArray:  "aggregate_container_key_test.go",
		parquet.TypeMap:    "aggregate_container_key_test.go",
		parquet.TypeRow:    "aggregate_container_key_test.go",
		parquet.TypeVector: "aggregate_container_key_test.go",
	}
	for typ := parquet.TypeBool; typ <= parquet.TypeVector; typ++ {
		if sampled[typ] {
			if where, ok := elsewhere[typ]; ok {
				t.Errorf("%s is both sampled here and declared covered by %s — pick one", typ, where)
			}
			continue
		}
		if _, ok := elsewhere[typ]; ok {
			continue
		}
		t.Errorf("%s has no group-key producer sample: add one to gkpSamples, or list the "+
			"gate that covers it in `elsewhere`", typ)
	}
}

// TestAnUnstorableBoxKeysApartFromEveryRealValue attempts the impossibility
// appendGroupKeyColumn's int arm asserts (method 10 of the correctness-fix
// protocol): a box that no column of that type can hold.
//
// The claim is that batch.KeyStorageInt answers ok=false for one, and the
// design decision is that such a box is NOT guessed into an integer, because a
// wrong integer is a wrong GROUP — '2026-02-30' keying as 2026-03-02 would
// merge two groups that are not one. It falls through to the length-prefixed
// text encoding, which cannot collide with the bare-digit integer arm.
func TestAnUnstorableBoxKeysApartFromEveryRealValue(t *testing.T) {
	unstorable := []struct {
		typ parquet.TypeID
		box any
	}{
		{parquet.TypeDate, "2026-02-30"},   // a date the calendar does not have (#560)
		{parquet.TypeDate, "not a date"},   //
		{parquet.TypeIPv4, "999.1.1.1"},    //
		{parquet.TypeMAC, "aa:bb"},         //
		{parquet.TypeInt64, "12345"},       // text where an integer is stored
		{parquet.TypeDate, float64(14610)}, // a float box: never GetValue's, never a migration's
	}
	for _, u := range unstorable {
		if _, ok := batch.KeyStorageInt(u.box, u.typ); ok {
			t.Errorf("%s accepted the box %#v as storage — a box no column of that type "+
				"can hold must not be guessed into an integer", u.typ, u.box)
		}
	}

	// And the encoding stays injective across the fallback: the text form of a
	// number is not the integer arm's bytes for that number.
	digits := appendGroupKeyColumn(nil, int64(14610), parquet.TypeDate, nil)
	text := appendGroupKeyColumn(nil, "14610", parquet.TypeDate, nil)
	if string(digits) == string(text) {
		t.Errorf("an unstorable DATE box %q keys identically to the stored day count %d",
			"14610", 14610)
	}
}

// gkpSample is one type's fixture: a column declaration and a stored value.
type gkpSample struct {
	typ   parquet.TypeID
	scale int
	set   any // what SetValue stores
}

func (s gkpSample) column() parquet.Column {
	return parquet.Column{Name: "k", Type: s.typ, Nullable: true, Scale: s.scale, Precision: 38}
}

func (s gkpSample) vector(t *testing.T) *batch.Vector {
	t.Helper()
	v := batch.NewVectorWithScale(s.typ, 1, s.scale)
	v.SetValue(0, s.set)
	if v.Nulls.IsNull(0) {
		t.Fatalf("SetValue(%#v) on a %s vector stored NULL — the sample is not a value of this type", s.set, s.typ)
	}
	return v
}

// storage reads back the int64 the column actually stores, which is what the
// int-keyed drain holds.
func (s gkpSample) storage(t *testing.T, v *batch.Vector) int64 {
	t.Helper()
	switch s.typ {
	case parquet.TypeBool:
		if v.BoolData[0] {
			return 1
		}
		return 0
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		return int64(v.Int32Data[0])
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		return v.Int64Data[0]
	}
	t.Fatalf("%s is IntStorageType but has no storage accessor here", s.typ)
	return 0
}

// gkpSamples carries one value per flat type. The values are chosen so the two
// boxings CANNOT agree by accident: every formatted type's display form is a
// different byte string from its storage digits, and the integers are negative
// or wide enough that a width mistake shows.
func gkpSamples() []gkpSample {
	return []gkpSample{
		{typ: parquet.TypeBool, set: true},
		{typ: parquet.TypeInt32, set: int32(-7)},
		{typ: parquet.TypeInt64, set: int64(-1234567890123)},
		{typ: parquet.TypeFloat32, set: float32(-1.5)},
		{typ: parquet.TypeFloat64, set: float64(-2.25)},
		{typ: parquet.TypeString, set: "a\x00b"},
		{typ: parquet.TypeBytes, set: []byte{0, 1, 2}},
		{typ: parquet.TypeTimestamp, set: int64(1262304000000)},
		// The three whose GetValue box is a FORMATTED string over integer
		// storage — the shape #788 is.
		{typ: parquet.TypeIPv4, set: "10.0.0.1"},
		{typ: parquet.TypeMAC, set: "aa:bb:cc:dd:ee:ff"},
		{typ: parquet.TypeDate, set: "2010-01-01"},
		{typ: parquet.TypeIPv6, set: "2001:db8::1"},
		{typ: parquet.TypeCIDR, set: "10.0.0.1/32"},
		{typ: parquet.TypePort, set: int32(443)},
		{typ: parquet.TypeProtocol, set: int32(6)},
		{typ: parquet.TypeDuration, set: int64(1500000000)},
		{typ: parquet.TypeUUID, set: "123e4567-e89b-12d3-a456-426614174000"},
		{typ: parquet.TypeDecimal, scale: 4, set: batch.ParseDecimalString("-12.3400", 4)},
	}
}
