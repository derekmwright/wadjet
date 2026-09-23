// SPDX-License-Identifier: MIT

package csv

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

// TestArcFR2AMultibyteDelimiterIsOneCharacter: the reader's delimiter is a
// RUNE, and a field boundary is its whole UTF-8 sequence. At 784aac60 the
// scanner took byte(delimiter): `§` (C2 A7) split on A7 and left C2 on every
// field, and `界` never matched at all. Base 962117da (encoding/csv) split
// both correctly. The inputs put the delimiter's bytes across a read
// boundary too, through a one-byte reader.
func TestArcFR2AMultibyteDelimiterIsOneCharacter(t *testing.T) {
	for _, sep := range []rune{'|', '\t', ';', '§', '界', '😀'} {
		for _, arm := range []string{"eager", "stream", "stream_one_byte_reads"} {
			t.Run(fmt.Sprintf("%U/%s", sep, arm), func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.Delimiter = sep
				data := fmt.Sprintf("a%cb%cc\nx%c\"y%cq\"%cz\n", sep, sep, sep, sep, sep)
				var r *Reader
				var err error
				switch arm {
				case "eager":
					r, err = NewReader([]byte(data), cfg)
				case "stream":
					r, err = NewStreamReader(strings.NewReader(data), cfg)
				default:
					r, err = NewStreamReader(&oneByteReader{s: data}, cfg)
				}
				if err != nil {
					t.Fatal(err)
				}
				b, err := r.Next()
				if err != nil {
					t.Fatal(err)
				}
				var names, vals []string
				for i, c := range r.Schema() {
					names = append(names, c.Name)
					vals = append(vals, fmt.Sprint(b.Columns[i].GetValue(0)))
				}
				want := fmt.Sprintf("[a b c] [x y%cq z]", sep)
				if got := fmt.Sprint(names, " ", vals); got != want {
					t.Fatalf("names/values %q, want %q", got, want)
				}
			})
		}
	}
}

type oneByteReader struct {
	s string
	i int
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	p[0] = r.s[r.i]
	r.i++
	return 1, nil
}
