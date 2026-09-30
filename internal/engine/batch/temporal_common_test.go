// SPDX-License-Identifier: MIT

package batch

import "testing"

// DateMidnightMillis is exact on both sides of 1970 (#1378): day -1
// is 1969-12-31 00:00:00, never a truncation toward zero.
func TestDateMidnightMillisAcrossTheEpoch(t *testing.T) {
	for days, want := range map[int64]int64{-1: -86_400_000, 0: 0, 1: 86_400_000, -719_528: -62_167_219_200_000, 2_932_896: 253_402_214_400_000} {
		if got := DateMidnightMillis(days); got != want {
			t.Errorf("DateMidnightMillis(%d) = %d, want %d", days, got, want)
		}
	}
	if c, ok := TemporalCommonType(TypeDate, TypeTimestamp); !ok || c != TypeTimestamp {
		t.Errorf("TemporalCommonType(DATE, TIMESTAMP) = (%v, %v)", c, ok)
	}
}
