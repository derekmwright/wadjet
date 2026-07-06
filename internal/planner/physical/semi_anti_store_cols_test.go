package physical

import (
	"reflect"
	"sort"
	"testing"
)

func TestSemiAntiBuildStoreCols(t *testing.T) {
	tests := []struct {
		name      string
		rightKeys []string
		filter    string
		want      []string // nil = expect nil; keys prefix is order-stable, filter cols sorted for comparison
	}{
		{
			name:      "empty filter is key-only, stores nothing",
			rightKeys: []string{"l_orderkey"},
			filter:    "",
			want:      nil,
		},
		{
			name:      "q21 shape: key plus one filter column",
			rightKeys: []string{"l_orderkey"},
			filter:    "l_suppkey <> l_suppkey",
			want:      []string{"l_orderkey", "l_suppkey"},
		},
		{
			name:      "filter column equal to the key dedups",
			rightKeys: []string{"id"},
			filter:    "id != id",
			want:      []string{"id"},
		},
		{
			name:      "multi-condition filter",
			rightKeys: []string{"k"},
			filter:    "a > a AND b <= b",
			want:      []string{"k", "a", "b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SemiAntiBuildStoreCols(tt.rightKeys, tt.filter)
			if tt.want == nil {
				if got != nil {
					t.Fatalf("got %v, want nil", got)
				}
				return
			}
			// Keys come first in input order; filter columns (map-ordered)
			// follow — compare the tail as a set.
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			nk := len(tt.rightKeys)
			if tt.want[0] == tt.rightKeys[0] && !reflect.DeepEqual(got[:min(nk, len(got))], tt.rightKeys[:min(nk, len(got))]) {
				// only check key prefix when keys survive dedup as a prefix
				t.Fatalf("key prefix of %v != %v", got, tt.rightKeys)
			}
			gs, ws := append([]string{}, got...), append([]string{}, tt.want...)
			sort.Strings(gs)
			sort.Strings(ws)
			if !reflect.DeepEqual(gs, ws) {
				t.Fatalf("got %v, want (as set) %v", got, tt.want)
			}
		})
	}
}
