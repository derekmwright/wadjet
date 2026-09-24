// SPDX-License-Identifier: MIT

package json

import (
	"bytes"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Arc CW (ADR-0045 §2): a nested JSON value the reader holds in a TEXT column
// or a TEXT field/element stores its JSON text. It stored Go's rendering of
// the decoded box (`map[z:w]`) — on the streaming reader read_json uses, for
// a value nested deeper than the sampled shape types — until a container
// written into a text vector became a refusal; then the read refused. Both
// the eager and the streaming reader are held to the JSON text.
func TestArcCWNestedJSONInATextSlotIsItsJSONText(t *testing.T) {
	data := []byte(`{"r":{"x":{"y":[1,2,{"z":"w"}]}},"o":{"k":[1,"a"]}}` + "\n" + `{"r":null,"o":{"k":"s"}}` + "\n")
	type next interface {
		Next() (*batch.RecordBatch, error)
	}
	for name, open := range map[string]func() (next, error){
		"eager":  func() (next, error) { return NewReaderFromBytes(data) },
		"stream": func() (next, error) { return NewStreamReader(bytes.NewReader(data)) },
	} {
		t.Run(name, func(t *testing.T) {
			r, err := open()
			if err != nil {
				t.Fatal(err)
			}
			var all []string
			for {
				rb, err := r.Next()
				if err != nil {
					t.Fatal(err)
				}
				if rb == nil {
					break
				}
				// Only the TEXT leaves: a ROW or ARRAY the reader typed is a
				// container and prints as one.
				var leaves func(v any)
				leaves = func(v any) {
					switch x := v.(type) {
					case string:
						all = append(all, x)
					case []any:
						for _, e := range x {
							leaves(e)
						}
					case map[string]any:
						for _, e := range x {
							leaves(e)
						}
					}
				}
				for c := range rb.Schema {
					for i := 0; i < rb.Len; i++ {
						leaves(rb.Columns[c].GetValue(i))
					}
				}
			}
			got := strings.Join(all, " ; ")
			if strings.Contains(got, "map[") || strings.Contains(got, "[1 ") {
				t.Errorf("a nested value stored Go's rendering: %s", got)
			}
			if !strings.Contains(got, `{"z":"w"}`) || !strings.Contains(got, `[1,"a"]`) {
				t.Errorf("the nested values' JSON text is missing: %s", got)
			}
		})
	}
}
