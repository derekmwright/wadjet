// SPDX-License-Identifier: MIT

package csv

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestArcTSEveryCSVReaderPathStoresEpochMillis: at 260fc569 writeCSVValue
// stored t.UnixMicro() into the TIMESTAMP carrier, which every reader of it
// takes as epoch MILLISECONDS, so `2024-06-15 12:30:45` read as
// 56425-08-29 08:30:00 (#1266). Both constructors share the write; each is held
// to what a TIMESTAMP literal of the same text stores
// (parquet.ParseTimestampMillis), offsets discarded as PostgreSQL's
// `timestamp` input discards them, sub-millisecond digits floored.
func TestArcTSEveryCSVReaderPathStoresEpochMillis(t *testing.T) {
	texts := []string{
		"2024-06-15 12:30:45",
		"2024-06-15 12:30:45.5",
		"1969-07-20 20:17:40.123",
		"1969-12-31T23:59:59.999",
		"9999-12-31 23:59:59.999",
		"1600-02-29 06:00:00.25",
		"2001-02-03T04:05:06+05:30",
		"2001-02-03T04:05:06.75-07:00",
		"2024-06-15T12:30:45Z",
		"1970-01-01T00:00:00.0005Z",
		"1969-12-31T23:59:59.9995Z",
	}
	var body strings.Builder
	body.WriteString("ts\n")
	for _, s := range texts {
		body.WriteString("\"" + s + "\"\n")
	}
	paths := map[string]func() (*Reader, error){
		"eager":  func() (*Reader, error) { return NewReader([]byte(body.String()), DefaultConfig()) },
		"stream": func() (*Reader, error) { return NewStreamReader(strings.NewReader(body.String()), DefaultConfig()) },
	}
	for name, open := range paths {
		t.Run(name, func(t *testing.T) {
			r, err := open()
			if err != nil {
				t.Fatal(err)
			}
			row := 0
			for {
				rb, err := r.Next()
				if err != nil {
					t.Fatal(err)
				}
				if rb == nil {
					break
				}
				if rb.Schema[0].Type != parquet.TypeTimestamp {
					t.Fatalf("ts inferred as %v, want TIMESTAMP", rb.Schema[0].Type)
				}
				for i := 0; i < rb.Len; i++ {
					want, err := parquet.ParseTimestampMillis(texts[row])
					if err != nil {
						t.Fatalf("the literal refuses %q: %v", texts[row], err)
					}
					if got := rb.Columns[0].Int64Data[i]; got != want {
						t.Errorf("%q stored %d (%s), the literal stores %d (%s)", texts[row],
							got, batch.FormatTimestamp(got), want, batch.FormatTimestamp(want))
					}
					row++
				}
			}
			if row != len(texts) {
				t.Fatalf("read %d rows, want %d", row, len(texts))
			}
		})
	}
}
