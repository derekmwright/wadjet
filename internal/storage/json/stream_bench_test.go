// SPDX-License-Identifier: MIT

package json

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// The StreamReader (what read_json runs) over the column kinds whose values
// past the sample are checked by a parse of their own — timestamp and inet
// strings, ARRAY and ROW values — so a check that costs a second parse per
// value shows here (it did not in the row/columnar benchmarks, whose
// columns are bigint, text, double and boolean). 10 000 rows: 100 sampled,
// 9 900 checked.
func benchStream(b *testing.B, row string) {
	b.Helper()
	var sb strings.Builder
	for i := 0; i < 10000; i++ {
		fmt.Fprintf(&sb, row, i, i%250)
		sb.WriteByte('\n')
	}
	data := []byte(sb.String())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r, err := NewStreamReader(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
		for {
			bt, err := r.Next()
			if err != nil {
				b.Fatal(err)
			}
			if bt == nil {
				break
			}
		}
	}
}

func BenchmarkStreamReader_TimestampInet(b *testing.B) {
	benchStream(b, `{"id":%d,"ts":"2024-01-02T03:04:05Z","ip":"10.0.0.%d"}`)
}

func BenchmarkStreamReader_Nested(b *testing.B) {
	benchStream(b, `{"id":%d,"arr":[1,2,3],"obj":{"x":%d,"y":"s"}}`)
}
