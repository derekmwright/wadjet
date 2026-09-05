package ingest

import (
	"bytes"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ingest boundary and the writer are two checks of ONE rule, and this
// asserts the direction that can produce a wrong file or a lost buffer:
// whatever `checkType` ADMITS, the writer must be able to STORE.
//
// Every column SHAPE, including the containers and the containers nested in
// containers. The first cut of this gate listed the 19 flat types and nothing
// else, and the three it omitted were exactly the three where the property was
// false: `checkType`'s container arm returned before the value check and
// `validateNestedLeaf` was a four-case switch, so an out-of-range INT32 inside
// an ARRAY, a short VECTOR inside an ARRAY and a `[]float32` where an INT64
// belongs all passed the door and killed the whole buffer at the flush — 99
// good rows lost, with the error naming a partition rather than the INSERT
// (round-2 review B1). A gate whose corpus omits the shapes the rule is
// hardest for is not a gate.
//
// The other direction — a box the writer takes and this boundary refuses — is
// fail-closed and, for the numeric leaves, is empty: both lists are the
// writer's. For the remaining types the writer stays a deliberate SUPERSET (a
// net.IP, a net.HardwareAddr and a raw []byte for an address column, where the
// door takes only text; an `int` for a DURATION), because a caller of the
// exported NativeWriter is not obliged to come through ingest at all. That is
// recorded in ADR-0018 §10 and counted below, so it cannot silently reverse.
func TestTheIngestBoundaryNeverAdmitsWhatTheWriterRefuses(t *testing.T) {
	cols := agreementColumns()
	boxes := agreementBoxes()

	var admittedButUnwritable []string
	var numericGaps []string
	cells := 0
	for _, col := range cols {
		for _, b := range boxes {
			cells++
			ingestErr := checkType(col.col, b.val)
			writeErr := writeOneThroughTheWriter(t, col.col, b.val)
			switch {
			case ingestErr == nil && writeErr != nil:
				admittedButUnwritable = append(admittedButUnwritable,
					fmt.Sprintf("%s <- %s: ingest OK, writer %v", col.name, b.name, writeErr))
			case ingestErr != nil && writeErr == nil && isNumericLeaf(col.col.Type):
				numericGaps = append(numericGaps,
					fmt.Sprintf("%s <- %s: ingest %v, writer OK", col.name, b.name, ingestErr))
			}
		}
	}
	if len(admittedButUnwritable) > 0 {
		t.Errorf("the ingest boundary admits %d of %d cells the writer cannot store — a row accepted "+
			"at the door and refused at the flush, which fails a whole buffer of good rows "+
			"and reports against a partition rather than the row that carried it:\n  %v",
			len(admittedButUnwritable), cells, admittedButUnwritable)
	}
	if len(numericGaps) > 0 {
		t.Errorf("the two boundaries disagree on %d NUMERIC cells; both lists are meant to be "+
			"the writer's (round-1 review P2):\n  %v", len(numericGaps), numericGaps)
	}
	t.Logf("%d cells over %d column shapes x %d box classes", cells, len(cols), len(boxes))
}

// The five container cells the round-2 review measured losing a whole buffer,
// named individually so a regression says WHICH shape came back rather than
// only that the count moved.
func TestTheIngestBoundaryRefusesABadValueInsideAContainer(t *testing.T) {
	elem := func(c parquet.Column) *parquet.Column { return &c }
	i32 := parquet.Column{Name: "element", Type: parquet.TypeInt32, Nullable: true}
	for _, tc := range []struct {
		name string
		col  parquet.Column
		val  any
	}{
		{"ARRAY(INT32) out of range", arrayOf(i32), []any{int64(3000000000)}},
		{"ROW{f INT32} out of range",
			parquet.Column{Name: "c", Type: parquet.TypeRow, Nullable: true,
				Fields: []parquet.Column{{Name: "f", Type: parquet.TypeInt32, Nullable: true}}},
			map[string]any{"f": int64(3000000000)}},
		{"MAP<STRING,INT32> out of range", mapOf(parquet.TypeInt32), map[string]any{"k": int64(3000000000)}},
		{"ARRAY(VECTOR(2)) short",
			arrayOf(parquet.Column{Name: "element", Type: parquet.TypeVector, Nullable: true, Dimension: 2}),
			[]any{[]float32{1}}},
		{"ARRAY(INT64) wrong box",
			arrayOf(parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}),
			[]any{[]float32{1}}},
		// The nested-in-nested twin: the walk has to reach the bottom.
		{"ARRAY(ARRAY(INT32)) out of range",
			arrayOf(parquet.Column{Name: "element", Type: parquet.TypeArray, Nullable: true,
				ElementType: elem(i32)}),
			[]any{[]any{int64(3000000000)}}},
		// And a bad LITERAL, not only a bad number.
		{"ARRAY(IPV4) bad literal",
			arrayOf(parquet.Column{Name: "element", Type: parquet.TypeIPv4, Nullable: true}),
			[]any{"999.1.1.1"}},
		{"MAP<STRING,DATE> bad literal", mapOf(parquet.TypeDate), map[string]any{"k": "2026-02-30"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkType(tc.col, tc.val); err == nil {
				t.Fatalf("the ingest door admitted %v; the writer answers %v",
					tc.val, writeOneThroughTheWriter(t, tc.col, tc.val))
			}
			if err := writeOneThroughTheWriter(t, tc.col, tc.val); err == nil {
				t.Fatalf("the writer stored %v; this cell is no longer a refusal on either side "+
					"and the gate is asserting nothing", tc.val)
			}
		})
	}
}

// And the value the whole boundary exists for: a buffer of good rows carrying
// ONE bad container value must lose nothing, because the bad row is refused at
// the door rather than at the flush.
func TestOneBadContainerRowDoesNotTakeTheBufferWithIt(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		arrayOf(parquet.Column{Name: "element", Type: parquet.TypeInt32, Nullable: true}),
	}}
	good := make([]map[string]any, 0, 99)
	for i := 0; i < 99; i++ {
		good = append(good, map[string]any{"id": int64(i), "c": []any{int64(i)}})
	}
	bad := map[string]any{"id": int64(99), "c": []any{int64(3000000000)}}

	// Every good row is admitted, and the bad one is refused HERE, naming its
	// own column — not at a flush that would take the other 99 with it.
	for i, r := range good {
		for _, col := range schema.Columns {
			if err := checkType(col, r[col.Name]); err != nil {
				t.Fatalf("good row %d, column %q refused: %v", i, col.Name, err)
			}
		}
	}
	refused := false
	for _, col := range schema.Columns {
		if err := checkType(col, bad[col.Name]); err != nil {
			refused = true
		}
	}
	if !refused {
		t.Fatal("the bad container row passed the ingest door; at the flush it takes all 99 good rows with it")
	}
}

func arrayOf(elemCol parquet.Column) parquet.Column {
	e := elemCol
	return parquet.Column{Name: "c", Type: parquet.TypeArray, Nullable: true, ElementType: &e}
}

func mapOf(valType parquet.TypeID) parquet.Column {
	entry := parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
		{Name: "key", Type: parquet.TypeString},
		{Name: "value", Type: valType, Nullable: true},
	}}
	return parquet.Column{Name: "c", Type: parquet.TypeMap, Nullable: true, ElementType: &entry}
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

type agreementColumn struct {
	name string
	col  parquet.Column
}

// agreementColumns is every column SHAPE this writer can be handed: all 22
// types, the parameterized DECIMALs, the three containers, containers nested in
// containers, and the two container-of-parameterized shapes whose leaf carries
// a width or a scale.
func agreementColumns() []agreementColumn {
	c := func(n string, t parquet.TypeID) agreementColumn {
		return agreementColumn{n, parquet.Column{Name: "c", Type: t, Nullable: true}}
	}
	elem := func(x parquet.Column) *parquet.Column { return &x }
	i32 := parquet.Column{Name: "element", Type: parquet.TypeInt32, Nullable: true}

	out := []agreementColumn{
		c("BOOL", parquet.TypeBool),
		c("INT32", parquet.TypeInt32),
		c("INT64", parquet.TypeInt64),
		c("FLOAT32", parquet.TypeFloat32),
		c("FLOAT64", parquet.TypeFloat64),
		c("STRING", parquet.TypeString),
		c("BYTES", parquet.TypeBytes),
		c("TIMESTAMP", parquet.TypeTimestamp),
		c("IPV4", parquet.TypeIPv4),
		c("IPV6", parquet.TypeIPv6),
		c("CIDR", parquet.TypeCIDR),
		c("MAC", parquet.TypeMAC),
		c("PORT", parquet.TypePort),
		c("PROTOCOL", parquet.TypeProtocol),
		c("DURATION", parquet.TypeDuration),
		c("UUID", parquet.TypeUUID),
		c("DATE", parquet.TypeDate),
		{"DECIMAL(9,2)", parquet.Column{Name: "c", Type: parquet.TypeDecimal, Nullable: true, Precision: 9, Scale: 2}},
		{"DECIMAL(38,10)", parquet.Column{Name: "c", Type: parquet.TypeDecimal, Nullable: true, Precision: 38, Scale: 10}},
		{"VECTOR(2)", parquet.Column{Name: "c", Type: parquet.TypeVector, Nullable: true, Dimension: 2}},

		// The three the first cut omitted, and the three where the property
		// was false.
		{"ARRAY(INT32)", arrayOf(i32)},
		{"ROW{f INT32}", parquet.Column{Name: "c", Type: parquet.TypeRow, Nullable: true,
			Fields: []parquet.Column{{Name: "f", Type: parquet.TypeInt32, Nullable: true}}}},
		{"MAP<STRING,INT32>", mapOf(parquet.TypeInt32)},

		// Nested, so the walk is asserted to reach the bottom.
		{"ARRAY(ARRAY(INT32))", arrayOf(parquet.Column{Name: "element", Type: parquet.TypeArray,
			Nullable: true, ElementType: elem(i32)})},
		{"ARRAY(ROW{f INT32})", arrayOf(parquet.Column{Name: "element", Type: parquet.TypeRow,
			Nullable: true, Fields: []parquet.Column{{Name: "f", Type: parquet.TypeInt32, Nullable: true}}})},
		{"MAP<STRING,ARRAY(INT32)>", mapOf(parquet.TypeArray)},

		// A leaf carrying a WIDTH and a leaf carrying a SCALE, inside a
		// container: the two parameters a container walk can drop.
		{"ARRAY(VECTOR(2))", arrayOf(parquet.Column{Name: "element", Type: parquet.TypeVector,
			Nullable: true, Dimension: 2})},
		{"ARRAY(DECIMAL(9,2))", arrayOf(parquet.Column{Name: "element", Type: parquet.TypeDecimal,
			Nullable: true, Precision: 9, Scale: 2})},
		{"ARRAY(IPV4)", arrayOf(parquet.Column{Name: "element", Type: parquet.TypeIPv4, Nullable: true})},
		{"ARRAY(DATE)", arrayOf(parquet.Column{Name: "element", Type: parquet.TypeDate, Nullable: true})},
	}
	// MAP<STRING,ARRAY(INT32)>'s value column needs its own element type,
	// which mapOf cannot express from a TypeID alone.
	for i := range out {
		if out[i].name == "MAP<STRING,ARRAY(INT32)>" {
			v := out[i].col.ElementType.Fields[1]
			v.ElementType = elem(i32)
			out[i].col.ElementType.Fields[1] = v
		}
	}
	return out
}

type agreementBox struct {
	name string
	val  any
}

// agreementBoxes is every box class a caller can hand over, INCLUDING the
// container-shaped ones — a container column's cells are the interesting half
// and a flat column handed a `[]any` is the other half of the same question.
func agreementBoxes() []agreementBox {
	return []agreementBox{
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
		{"int64 MaxInt64", int64(math.MaxInt64)},
		{"float32", float32(42)},
		{"float64", float64(42)},
		{"float64 1e40", float64(1e40)},
		{"float64 NaN", math.NaN()},
		{"float64 +Inf", math.Inf(1)},
		{"float64 2.5", float64(2.5)},
		{"string numeric", "42"},
		{"string ipv4", "10.0.0.1"},
		{"string ipv4 bad", "999.1.1.1"},
		{"string ipv6", "2001:db8::1"},
		{"string ipv6 bad", "zz::1"},
		{"string cidr", "10.0.0.0/8"},
		{"string mac", "00:11:22:33:44:55"},
		{"string mac bad", "gg:11"},
		{"string uuid", "00000000-0000-4000-8000-000000000001"},
		{"string uuid bad", "not-a-uuid"},
		{"string date", "2026-09-05"},
		{"string date impossible", "2026-02-30"},
		{"string timestamp", "2026-09-05T12:00:00Z"},
		{"string timestamp bad", "nope"},
		{"string duration nanos", "3600000000000"},
		{"string duration go", "1h"},
		{"string decimal", "1.25"},
		{"string decimal wide", "123456789012345678901234567890.5"},
		{"bytes", []byte{1, 2}},
		{"bytes 16", make([]byte, 16)},
		{"net.IP v4", net.ParseIP("10.0.0.1").To4()},
		{"net.IP v6", net.ParseIP("2001:db8::1").To16()},
		{"time.Time", time.Unix(0, 0).UTC()},
		{"time.Duration", time.Second},
		{"[]float32 x2", []float32{1, 2}},
		{"[]float32 x3", []float32{1, 2, 3}},
		{"[]float64 x2", []float64{1, 2}},
		{"[]any int64", []any{int64(1)}},
		{"[]any out of range", []any{int64(3000000000)}},
		{"[]any nested", []any{[]any{int64(3000000000)}}},
		{"[]any vector short", []any{[]float32{1}}},
		{"[]any bad ipv4", []any{"999.1.1.1"}},
		{"[]any bad date", []any{"2026-02-30"}},
		{"[]int64", []int64{1, 2, 3}},
		{"map k=int64", map[string]any{"k": int64(1)}},
		{"map k=out of range", map[string]any{"k": int64(3000000000)}},
		{"map k=bad date", map[string]any{"k": "2026-02-30"}},
		{"map f=out of range", map[string]any{"f": int64(3000000000)}},
	}
}
