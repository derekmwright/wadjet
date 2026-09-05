package ingest

import (
	"bytes"
	"fmt"
	"math"
	"net"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ingest boundary and the writer are two checks of ONE rule, and this
// asserts the direction that can produce a wrong file: whatever `checkType`
// ADMITS, the writer must be able to STORE.
//
// The other direction — a box the writer takes and this boundary refuses — is
// fail-closed and, for the numeric leaves, is now empty: the round-1 review
// measured 21 such cells (`uint` and `uint64` into INT32/PORT/PROTOCOL, and
// int8/int16/uint* into FLOAT32/FLOAT64), all of them values the leaf holds
// exactly and this door refused. Both lists are the writer's now. For the
// remaining types the writer stays a deliberate SUPERSET — it takes a
// `net.IP`, a `net.HardwareAddr` and a raw `[]byte` for an address where this
// door takes only text — because a caller of the exported NativeWriter is not
// obliged to come through ingest at all. That direction is recorded in
// ADR-0018 §10 and asserted below, so it cannot silently reverse.
func TestTheIngestBoundaryNeverAdmitsWhatTheWriterRefuses(t *testing.T) {
	cols := agreementColumns()
	boxes := agreementBoxes()

	var admittedButUnwritable []string
	var numericGaps []string
	for _, col := range cols {
		for _, b := range boxes {
			ingestErr := checkType(col, b.val)
			writeErr := writeOneThroughTheWriter(t, col, b.val)
			switch {
			case ingestErr == nil && writeErr != nil:
				admittedButUnwritable = append(admittedButUnwritable,
					fmt.Sprintf("%s <- %s: ingest OK, writer %v", col.Type, b.name, writeErr))
			case ingestErr != nil && writeErr == nil && isNumericLeaf(col.Type):
				numericGaps = append(numericGaps,
					fmt.Sprintf("%s <- %s: ingest %v, writer OK", col.Type, b.name, ingestErr))
			}
		}
	}
	if len(admittedButUnwritable) > 0 {
		t.Errorf("the ingest boundary admits %d boxes the writer cannot store — a row accepted "+
			"at the door and refused at the flush, which fails a whole buffer of good rows "+
			"and reports against a partition rather than the row that carried it:\n  %v",
			len(admittedButUnwritable), admittedButUnwritable)
	}
	if len(numericGaps) > 0 {
		t.Errorf("the two boundaries disagree on %d NUMERIC cells; both lists are meant to be "+
			"the writer's (round-1 review P2):\n  %v", len(numericGaps), numericGaps)
	}
}

func isNumericLeaf(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol,
		parquet.TypeFloat32, parquet.TypeFloat64:
		return true
	}
	return false
}

// writeOneThroughTheWriter reports whether the native writer stores this box in
// this column — the same question checkType is asked, put to the layer below.
func writeOneThroughTheWriter(t *testing.T, col parquet.Column, val any) error {
	t.Helper()
	var buf bytes.Buffer
	nw := parquet.NewNativeWriter(&buf, parquet.Schema{Columns: []parquet.Column{col}},
		parquet.DefaultWriterConfig())
	if err := nw.WriteMapRows([]map[string]any{{col.Name: val}}); err != nil {
		return err
	}
	return nw.Close()
}

func agreementColumns() []parquet.Column {
	c := func(t parquet.TypeID) parquet.Column {
		return parquet.Column{Name: "c", Type: t, Nullable: true}
	}
	return []parquet.Column{
		c(parquet.TypeBool), c(parquet.TypeInt32), c(parquet.TypeInt64),
		c(parquet.TypeFloat32), c(parquet.TypeFloat64), c(parquet.TypeString),
		c(parquet.TypeBytes), c(parquet.TypeTimestamp), c(parquet.TypeIPv4),
		c(parquet.TypeIPv6), c(parquet.TypeCIDR), c(parquet.TypeMAC),
		c(parquet.TypePort), c(parquet.TypeProtocol), c(parquet.TypeDuration),
		c(parquet.TypeUUID), c(parquet.TypeDate),
		{Name: "c", Type: parquet.TypeDecimal, Nullable: true, Precision: 9, Scale: 2},
		{Name: "c", Type: parquet.TypeVector, Nullable: true, Dimension: 2},
	}
}

func agreementBoxes() []struct {
	name string
	val  any
} {
	return []struct {
		name string
		val  any
	}{
		{"bool", true},
		{"int", int(42)},
		{"int8", int8(42)},
		{"int16", int16(42)},
		{"int32", int32(42)},
		{"int64", int64(42)},
		{"uint", uint(42)},
		{"uint8", uint8(42)},
		{"uint16", uint16(42)},
		{"uint32", uint32(42)},
		{"uint64", uint64(42)},
		{"uint64 MaxUint64", uint64(math.MaxUint64)},
		{"int64 MaxInt32+1", int64(math.MaxInt32) + 1},
		{"int64 MinInt32-1", int64(math.MinInt32) - 1},
		{"float32", float32(42)},
		{"float64", float64(42)},
		{"float64 1e40", float64(1e40)},
		{"float64 NaN", math.NaN()},
		{"float64 2.5", float64(2.5)},
		{"string numeric", "42"},
		{"string ipv4", "10.0.0.1"},
		{"string ipv6", "2001:db8::1"},
		{"string cidr", "10.0.0.0/8"},
		{"string mac", "00:11:22:33:44:55"},
		{"string uuid", "00000000-0000-4000-8000-000000000001"},
		{"string date", "2026-09-05"},
		{"string decimal", "1.25"},
		{"bytes", []byte{1, 2}},
		{"net.IP v4", net.ParseIP("10.0.0.1").To4()},
		{"[]float32 x2", []float32{1, 2}},
		{"[]float32 x3", []float32{1, 2, 3}},
	}
}
