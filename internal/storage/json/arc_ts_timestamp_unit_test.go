// SPDX-License-Identifier: MIT

package json

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// tsUnitTexts are spellings every reader path types as TIMESTAMP, each paired
// with the epoch MILLISECONDS a TIMESTAMP literal of the same text stores
// (parquet.ParseTimestampMillis — the engine's one carrier, #1266). The
// offsets are discarded, as PostgreSQL's `timestamp` input discards them.
var tsUnitTexts = []string{
	"2024-06-15T12:30:45Z",
	"2024-06-15 12:30:45.5",
	"1969-07-20T20:17:40.123Z",
	"1969-12-31T23:59:59.999",
	"9999-12-31 23:59:59.999",
	"1600-02-29 06:00:00.25",
	"2001-02-03T04:05:06+05:30",
	"2001-02-03T04:05:06.75-07:00",
	"1970-01-01T00:00:00.0005Z", // sub-millisecond: floored, like the literal
	"1969-12-31T23:59:59.9995Z", // …toward the past, not toward zero
}

func tsUnitWant(t *testing.T, text string) int64 {
	t.Helper()
	ms, err := parquet.ParseTimestampMillis(text)
	if err != nil {
		t.Fatalf("the literal refuses %q: %v", text, err)
	}
	return ms
}

// TestArcTSEveryJSONReaderPathStoresEpochMillis: at 260fc569 the columnar
// decode (which the streaming reader, and so read_json, runs) stored
// t.UnixMicro() for a scalar field and handed SetValue t.UnixMicro() for a
// timestamp nested in an array or object — both 1000x the carrier. The eager
// reader was already right (FromRows reads the text through
// batch.timestampTextMillis), and is here as the control.
func TestArcTSEveryJSONReaderPathStoresEpochMillis(t *testing.T) {
	var body strings.Builder
	for _, s := range tsUnitTexts {
		fmt.Fprintf(&body, "{\"ts\":%q,\"arr\":[%q],\"obj\":{\"t\":%q}}\n", s, s, s)
	}
	data := []byte(body.String())

	type next interface {
		Next() (*batch.RecordBatch, error)
	}
	paths := map[string]func() (next, error){
		"eager":    func() (next, error) { return NewReaderFromBytes(data) },
		"columnar": func() (next, error) { return NewColumnarReader(data) },
		"stream":   func() (next, error) { return NewStreamReader(bytes.NewReader(data)) },
	}
	for name, open := range paths {
		t.Run(name, func(t *testing.T) {
			r, err := open()
			if err != nil {
				t.Fatal(err)
			}
			row, nested := 0, 0
			for {
				rb, err := r.Next()
				if err != nil {
					t.Fatal(err)
				}
				if rb == nil {
					break
				}
				idx := map[string]int{}
				for i, c := range rb.Schema {
					idx[c.Name] = i
				}
				tsCol := rb.Columns[idx["ts"]]
				if rb.Schema[idx["ts"]].Type != parquet.TypeTimestamp {
					t.Fatalf("ts inferred as %v, want TIMESTAMP", rb.Schema[idx["ts"]].Type)
				}
				for i := 0; i < rb.Len; i++ {
					text := tsUnitTexts[row]
					want := tsUnitWant(t, text)
					if got := tsCol.Int64Data[i]; got != want {
						t.Errorf("%q stored %d (%s), the literal stores %d (%s)",
							text, got, batch.FormatTimestamp(got), want, batch.FormatTimestamp(want))
					}
					// Nested: only the columnar decode types the element, so
					// only a TIMESTAMP element is held to the carrier.
					if c, ok := idx["arr"]; ok {
						if el := rb.Schema[c].ElementType; el != nil && el.Type == parquet.TypeTimestamp {
							nested++
							arr, _ := rb.Columns[c].GetValue(i).([]any)
							if len(arr) != 1 || fmt.Sprint(arr[0]) != fmt.Sprint(want) {
								t.Errorf("array element %q stored %v, the literal stores %d", text, arr, want)
							}
						}
					}
					if c, ok := idx["obj"]; ok && rb.Schema[c].Type == parquet.TypeRow {
						for _, f := range rb.Schema[c].Fields {
							if f.Name == "t" && f.Type == parquet.TypeTimestamp {
								nested++
								obj, _ := rb.Columns[c].GetValue(i).(map[string]any)
								if fmt.Sprint(obj["t"]) != fmt.Sprint(want) {
									t.Errorf("object field %q stored %v, the literal stores %d", text, obj["t"], want)
								}
							}
						}
					}
					row++
				}
			}
			if row != len(tsUnitTexts) {
				t.Fatalf("read %d rows, want %d", row, len(tsUnitTexts))
			}
			// The nested cells are the coerceToColumn arm, which only the
			// columnar decode reaches; they must not pass by never running.
			if name != "eager" && nested != 2*len(tsUnitTexts) {
				t.Fatalf("%d nested TIMESTAMP cells checked, want %d", nested, 2*len(tsUnitTexts))
			}
		})
	}
}
