package json

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// drainReader pulls all rows from either reader type via a Next() func.
func drainBatches(tb testing.TB, next func() (interface {
	ToRows() []map[string]any
}, error)) []map[string]any {
	tb.Helper()
	var rows []map[string]any
	for {
		b, err := next()
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil || len(b.ToRows()) == 0 {
			return rows
		}
		rows = append(rows, b.ToRows()...)
	}
}

// streamRows drains a StreamReader.
func streamRows(tb testing.TB, sr *StreamReader) []map[string]any {
	tb.Helper()
	var rows []map[string]any
	for {
		b, err := sr.Next()
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil {
			return rows
		}
		rows = append(rows, b.ToRows()...)
	}
}

// eagerRows drains the eager ColumnarReader.
func eagerRows(tb testing.TB, data []byte) []map[string]any {
	tb.Helper()
	r, err := NewColumnarReader(data)
	if err != nil {
		tb.Fatal(err)
	}
	var rows []map[string]any
	for {
		b, err := r.Next()
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil {
			return rows
		}
		rows = append(rows, b.ToRows()...)
	}
}

// Issue #130 regression: the stream reader must produce batches identical
// to the eager reader's for the same input — including nested values with
// interior nulls (the PR #123 corpus), across every chunk boundary. Tiny
// chunk sizes force objects to split mid-string, mid-escape, and mid-nest.
func TestStreamReader_MatchesEagerAcrossChunkBoundaries(t *testing.T) {
	corpora := map[string]string{
		"jsonl_flat": `{"a":1,"b":"x"}
{"a":2,"b":"y"}
{"a":3,"b":null}`,
		"array_flat": `[{"a":1,"b":"x"},{"a":2,"b":"y"},{"a":3}]`,
		"nested_null_corpus": `[{"meta":{"name":"alpha"}},{"meta":null},{"meta":{"name":"beta"}},{"other":1}]`,
		"arrays_and_escapes": `[{"tags":["a","b"],"s":"he said \"hi\\\" there"},{"tags":[],"s":","},{"tags":null,"s":"}{"}]`,
		"deep_nesting":       `{"r":{"x":{"y":[1,2,{"z":"w"}]}}}` + "\n" + `{"r":null}`,
	}
	// Many rows so multiple batches + many refills happen.
	var big strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&big, `{"id":%d,"name":"row-%d","vals":[%d,%d],"meta":{"grp":"g%d"}}`+"\n", i, i, i, i*2, i%5)
	}
	corpora["large_multibatch"] = big.String()

	for name, input := range corpora {
		for _, chunk := range []int{7, 64, 1024, streamChunkBytes} {
			t.Run(fmt.Sprintf("%s_chunk%d", name, chunk), func(t *testing.T) {
				want := eagerRows(t, []byte(input))
				sr, err := newStreamReaderSized(strings.NewReader(input), chunk)
				if err != nil {
					t.Fatalf("NewStreamReader: %v", err)
				}
				got := streamRows(t, sr)
				if len(got) != len(want) {
					t.Fatalf("rows = %d, want %d", len(got), len(want))
				}
				for i := range want {
					if fmt.Sprintf("%v", got[i]) != fmt.Sprintf("%v", want[i]) {
						t.Fatalf("row %d:\n got %#v\nwant %#v", i, got[i], want[i])
					}
				}
			})
		}
	}
}

func TestStreamReader_EmptyAndTruncated(t *testing.T) {
	for _, input := range []string{"", "   \n\t", "[]"} {
		sr, err := newStreamReaderSized(strings.NewReader(input), 16)
		if err != nil {
			t.Fatalf("empty input %q: %v", input, err)
		}
		b, err := sr.Next()
		if err != nil || b != nil {
			t.Fatalf("empty input %q: Next = %v, %v", input, b, err)
		}
	}

	// Truncated object must error, not silently drop the tail.
	sr, err := newStreamReaderSized(strings.NewReader(`{"a":1}`+"\n"+`{"a":2,"b":"trunc`), 8)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sr.Next()
	if err == nil {
		t.Fatal("truncated tail object must surface an error (silent row drop)")
	}
}

// The whole point of #130: the buffered window stays bounded by
// chunk + largest object, regardless of total input size.
func TestStreamReader_WindowStaysBounded(t *testing.T) {
	const chunk = 4 << 10
	var big bytes.Buffer
	for i := 0; i < 20000; i++ {
		fmt.Fprintf(&big, `{"id":%d,"pad":"%s"}`+"\n", i, strings.Repeat("x", 100))
	}
	total := big.Len()
	sr, err := newStreamReaderSized(bytes.NewReader(big.Bytes()), chunk)
	if err != nil {
		t.Fatal(err)
	}
	maxWindow := 0
	for {
		b, err := sr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if len(sr.buf) > maxWindow {
			maxWindow = len(sr.buf)
		}
		if b == nil {
			break
		}
	}
	// Sample phase buffers ~100 objects (~12KB here); afterwards the window
	// must never approach the input size.
	if maxWindow > total/10 {
		t.Fatalf("window grew to %d bytes for a %d-byte input — not streaming", maxWindow, total)
	}
}
