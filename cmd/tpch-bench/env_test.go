package main

import "testing"

// TPCH_STREAMING_EXCHANGE must default ON (matching the wadjet serve
// default): the prior default-off parse silently ran every benchmark
// 2026-07-02..07-12 on the synchronous S3 shuffle path (−27.7% left on
// the table at SF100).
func TestEnvBoolDefaultOn(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"", true}, // unset = engine default = on
		{"1", true},
		{"true", true},
		{"TRUE", true},
		{"0", false},
		{"false", false},
		{"False", false},
	}
	for _, c := range cases {
		t.Setenv("TPCH_TEST_BOOL", c.val)
		if got := envBoolDefaultOn("TPCH_TEST_BOOL"); got != c.want {
			t.Errorf("envBoolDefaultOn(%q) = %v, want %v", c.val, got, c.want)
		}
	}
}
