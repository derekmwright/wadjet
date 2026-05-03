package main

import "testing"

func TestParseGoMemLimit(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"-1", 0, false},
		{"2147483648", 2 * 1024 * 1024 * 1024, true},
		{"2GiB", 2 * 1024 * 1024 * 1024, true},
		{"2GB", 2 * 1000 * 1000 * 1000, true},
		{"2G", 2 * 1000 * 1000 * 1000, true},
		{"2g", 2 * 1000 * 1000 * 1000, true},
		{"512MiB", 512 * 1024 * 1024, true},
		{"512MB", 512 * 1000 * 1000, true},
		{"4KiB", 4 * 1024, true},
		{"4K", 4 * 1000, true},
		{"1TiB", 1024 * 1024 * 1024 * 1024, true},
		{"  2GiB  ", 2 * 1024 * 1024 * 1024, true},
		{"2X", 0, false},
		{"GiB", 0, false},
		{"abc", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseGoMemLimit(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("parseGoMemLimit(%q) = (%d, %v), want (%d, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
