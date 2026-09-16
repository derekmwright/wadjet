// SPDX-License-Identifier: MIT

package ingest

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The programmatic boundary the SQL doors sit above. checkType admits int,
// int8, int16, int32, int64, uint8, uint16 and uint32 for an INT32 / PORT /
// PROTOCOL column, and until CodeQL #36/#37 nothing on this path re-checked
// that the NUMBER fits: an int64(3000000000) was admitted here, wrapped into
// -1294967296 by the writer's toInt32 and written to the file, with no error
// anywhere. The five narrow boxes were worse — they had no arm in the writer's
// converter at all and stored 0.
//
// PostgreSQL raises 22003 for the same assignment.
func TestTheIngestBoundaryRefusesAnInt32ItCannotHold(t *testing.T) {
	// PORT and PROTOCOL have LEFT this test: their range is the TYPE's, not
	// the carrier's, and it is checked wherever a value enters the type — by
	// cast or by write — which includes this Go-box door (ADR-0012's
	// 2026-09-15 entry; review NT round 2, B2). TestTheIngestBoundaryHoldsPortAndProtocolToTheirOwnRange
	// below is their half.
	for _, colType := range []parquet.TypeID{parquet.TypeInt32} {
		col := parquet.Column{Name: "a", Type: colType, Nullable: true}
		for _, box := range []any{int64(3000000000), int64(-2147483649), int(3000000000), int64(-1) << 40} {
			err := checkType(col, box)
			if err == nil {
				t.Errorf("%s: ingest accepted %v (%T); no int32 holds it", colType, box, box)
				continue
			}
			if s := sqlerr.StateOf(err); s != "22003" {
				t.Errorf("%s: refusing %v carried SQLSTATE %q, want 22003: %v", colType, box, s, err)
			}
		}
		for _, box := range []any{
			int8(5), int16(-7), uint8(9), uint16(11), uint32(13),
			int32(0), int64(2147483647), int64(-2147483648), int(1),
		} {
			if err := checkType(col, box); err != nil {
				t.Errorf("%s: ingest refused %v (%T), which an int32 holds: %v", colType, box, box, err)
			}
		}
	}
}

// TestTheIngestBoundaryHoldsPortAndProtocolToTheirOwnRange is the other half:
// the two types with a range of their own are held to it at the Go-BOX door,
// the same way the text door and the CAST hold them.
func TestTheIngestBoundaryHoldsPortAndProtocolToTheirOwnRange(t *testing.T) {
	for _, c := range []struct {
		typ parquet.TypeID
		hi  int64
	}{{parquet.TypePort, 65535}, {parquet.TypeProtocol, 255}} {
		col := parquet.Column{Name: "a", Type: c.typ, Nullable: true}
		for _, box := range []any{int32(0), int64(c.hi), int(1), uint8(9), float64(c.hi)} {
			if err := checkType(col, box); err != nil {
				t.Errorf("%s: refused %v (%T), which the type holds: %v", c.typ, box, box, err)
			}
		}
		for _, box := range []any{int32(c.hi + 1), int64(c.hi + 1), int(-1), int32(-1), float64(c.hi + 1)} {
			err := checkType(col, box)
			if err == nil {
				t.Errorf("%s: accepted %v (%T); no %s is that", c.typ, box, box, c.typ)
				continue
			}
			if s := sqlerr.StateOf(err); s != "22003" {
				t.Errorf("%s: refusing %v carried SQLSTATE %q, want 22003: %v", c.typ, box, s, err)
			}
		}
	}
}
