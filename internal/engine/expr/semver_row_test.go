package expr

import (
	"reflect"
	"testing"
)

func TestSemverParseRow(t *testing.T) {
	for _, tc := range []struct {
		input any
		want  any
	}{
		{"v1.2.3-rc.1+b7", map[string]any{"major": int64(1), "minor": int64(2), "patch": int64(3), "prerelease": "rc.1", "build": "b7"}},
		{"1.2.3", map[string]any{"major": int64(1), "minor": int64(2), "patch": int64(3), "prerelease": "", "build": ""}},
		{"01.2.3", nil}, {"latest", nil}, {nil, nil},
	} {
		if got := fnSemverParse([]any{tc.input}); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parse(%v)=%v want %v", tc.input, got, tc.want)
		}
		if tc.want != nil || tc.input == nil {
			if got := fnSemverParseStrict([]any{tc.input}); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("strict(%v)=%v want %v", tc.input, got, tc.want)
			}
		}
	}
	for _, name := range []string{"semver_parse", "semver_parse_strict"} {
		d, c := DefaultRegistry.ReturnType(name).Resolve(1, nil)
		if c != Decided || !reflect.DeepEqual(d.RowFields(), semverRowFields()) {
			t.Errorf("%s declaration %+v", name, d)
		}
	}
}
