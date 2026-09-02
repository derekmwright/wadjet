package exec

import (
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ONE merge-key encoder per column TYPE (ADR-0023 item 5, ADR-0026).
//
// A group's merge key is what the k-way merger compares, so it is a
// DEFINITION OF EQUALITY: two producers that write different bytes for one
// value are two definitions, and the merger keeps their groups apart. ADR-0023
// item 5 says a key producer is SHARED and states it for the one consumer
// outside this package; #788 is the same rule INSIDE one operator, where a
// HashAggregate has four producers of the same key:
//
//   - the int-mode SoA drain and the packed-int drain, which have the key's
//     int64 STORAGE (partialGroupCursor.appendIntModeSortKey);
//   - the compact and str/generic drains, which have the key as an `any`
//     boxed at consume time by (*batch.Vector).GetValue (appendSerializedKey);
//   - migrateToGenericMap, which re-boxes an int-keyed group's key as the RAW
//     int64 and hands it to that same boxed path;
//   - decodeSerializedKey, which re-boxes from the binary hash key.
//
// Those three boxes are not one encoding. GetValue FORMATS the three
// int-stored types whose storage is not their text — DATE, IPv4, MAC — so the
// int drain wrote `14610` for a DATE while the boxed remainder wrote
// `\n2010-01-01`, the merge never combined them, and every DATE group came out
// TWICE with the right total and the wrong grouping (#788: 421 groups, 772-841
// rows, sum unchanged). The DATE output column's SetValue accepts an integer
// and a date string alike, so both rows even rendered the same date.
//
// The encoding is the value's STORAGE, not its display: it is what the hot
// int path already writes for INT32/PORT/PROTOCOL and what the coordinator's
// own cross-worker re-aggregation already writes for DATE/IPv4/MAC
// (coordinator.go's keyEncoders read Int32Data/Int64Data), and ADR-0023 item 1
// says a key is keyed on what the comparator compares rather than on how a
// value displays. The VALUE a group emits is untouched by all of this — it
// stays the boxed round trip of ADR-0023 items 2-3, and nothing decodes a
// value out of a key.
//
// Dispatch is on the DECLARED TYPE, never on the Go box: the box is exactly
// what disagreed.

// appendGroupKeyColumn writes one group-key column's merge bytes for the boxed
// value v of declared type t. meta carries the same column's full declared
// metadata, needed one level down for a container's element/field types.
//
// Every producer of a merge key in this package goes through here or through
// appendTypedIntKey, which is this function's int-storage arm.
func appendGroupKeyColumn(buf []byte, v any, t batch.TypeID, meta *parquet.Column) []byte {
	if v != nil {
		if batch.IntStorageType(t) {
			if iv, ok := batch.KeyStorageInt(v, t); ok {
				return appendTypedIntKey(buf, iv, t)
			}
			// A box no column of type t can hold. It is NOT guessed into an
			// integer — a wrong integer is a wrong GROUP, and the whole point
			// of this function is that a key never keys on a coincidence of
			// boxing. It falls through to the generic text encoding, which is
			// injective over the values that reach it and cannot collide with
			// the integer arm (that arm writes bare digits; the text arm
			// length-prefixes). TestAnUnstorableBoxKeysApartFromEveryRealValue
			// is the fixture that attempts it (method 10).
		}
		if t == batch.TypeCIDR {
			// PostgreSQL's inet order, not the raw stored text: '10.0.0.1'
			// and '10.0.0.1/32' are one inet value (#492, #520). The boxed
			// `any` has no type tag of its own — a CIDR boxes as a plain Go
			// string — so the re-key has to happen where the declared type is
			// still in hand.
			if s, ok := v.(string); ok {
				return appendKeyValue(buf, kernel.CidrOrderKey(s))
			}
		}
	}
	return appendGroupKeyColumnMeta(buf, v, meta)
}

// appendGroupKeyColumnMeta is the arm for a column whose declared TypeID the
// caller does not have (appendSerializedKey's types slice may be shorter than
// its values). Only a container consults its metadata: appendKeyValueWithMeta
// recurses into element/field types to re-key a CIDR leaf, and every other
// leaf keeps appendKeyValue's encoding exactly.
func appendGroupKeyColumnMeta(buf []byte, v any, meta *parquet.Column) []byte {
	if meta != nil {
		switch meta.Type {
		case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow:
			return appendKeyValueWithMeta(buf, v, meta)
		}
	}
	return appendKeyValue(buf, v)
}

// appendTypedIntKey writes one int-stored column value to buf from its int64
// STORAGE. It is appendGroupKeyColumn's int arm, called directly by the int-
// and packed-key drains, which already hold the storage and never box it.
//
// batch.IntStorageType names exactly the types that reach here, and
// batch.KeyStorageInt is the inverse boxing that lets the boxed producers
// arrive at the same integer.
func appendTypedIntKey(buf []byte, v int64, t batch.TypeID) []byte {
	switch t {
	case batch.TypeBool:
		return strconv.AppendBool(buf, v != 0)
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return strconv.AppendInt(buf, int64(int32(v)), 10)
	default:
		// int64 + every wider-or-equal type stored as int64 (timestamp,
		// ipv4, mac, duration).
		return strconv.AppendInt(buf, v, 10)
	}
}
