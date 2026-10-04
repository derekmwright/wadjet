// SPDX-License-Identifier: MIT

package batch

import "testing"

// The printer of a column created from an unconstrained numeric (ADR-0024
// §10): the stored scale's trailing zeros go, the value stays.
func TestTrimDecimalText(t *testing.T) {
	for in, want := range map[string]string{
		"1.2500000000":                    "1.25",
		"1.0000000000":                    "1",
		"0.0000000000":                    "0",
		"-0.5000000000":                   "-0.5",
		"0.0000000001":                    "0.0000000001",
		"1234567890.0000000000":           "1234567890",
		"12345678901234567890.1234567890": "12345678901234567890.123456789",
		"10":                              "10",
		"100":                             "100",
		"-7":                              "-7",
		"abc":                             "abc",
		"1.5e10":                          "1.5e10",
		"":                                "",
	} {
		if got := TrimDecimalText(in); got != want {
			t.Errorf("TrimDecimalText(%q) = %q, want %q", in, got, want)
		}
	}
}
