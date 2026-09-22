// SPDX-License-Identifier: MIT

package csv

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// BenchmarkStreamReader is read_csv's reader over bigint, double precision,
// boolean, timestamp and text columns, 10 000 rows (100 sampled, 9 900 read
// past the sample).
func BenchmarkStreamReader(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("id,score,active,ts,name\n")
	for i := 0; i < 10000; i++ {
		fmt.Fprintf(&sb, "%d,%d.%d,%t,2024-01-02T03:04:05Z,user_%d\n", i, i, i%10, i%2 == 0, i)
	}
	data := []byte(sb.String())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r, err := NewStreamReader(bytes.NewReader(data), DefaultConfig())
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
