// SPDX-License-Identifier: MIT

package sql

import "testing"

// TestArcRDSignedNamedArgument: a signed number after a table function's
// `name =` is ONE value, as a signed positional argument has been since
// generate_series(1, 0, -1). At 9420d256 `sample_size=-1` stored "-" as the
// value and appended "1" to the positional arguments (#1242, arc RD).
func TestArcRDSignedNamedArgument(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"SELECT * FROM read_csv('/tmp/a.csv', sample_size=-1)", "-1"},
		{"SELECT * FROM read_csv('/tmp/a.csv', sample_size = -1)", "-1"},
		{"SELECT * FROM read_csv('/tmp/a.csv', sample_size = +5)", "+5"},
		{"SELECT * FROM read_json('/tmp/a.json', sample_size = 7, header = true)", "7"},
		{"SELECT * FROM read_csv('/tmp/a.csv', sample_size = '-1')", "-1"},
	} {
		parsed, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		info, err := ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		tr := info.Tables[0]
		if len(tr.FuncArgs) != 1 || tr.FuncNamedArgs["sample_size"] != tc.want {
			t.Errorf("%s: args %v named %v, want sample_size=%s and one positional argument", tc.sql, tr.FuncArgs, tr.FuncNamedArgs, tc.want)
		}
	}
}
