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
	for _, colType := range []parquet.TypeID{parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol} {
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
