// SPDX-License-Identifier: MIT

package exec

import (
	"bytes"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A DATE KEY AT ITS PAIR'S TIMESTAMP RUNG IS ITS MIDNIGHT (#1378): the bytes
// a DATE side keys as are exactly the bytes the TIMESTAMP of its midnight
// keys as itself — the hash join's build and probe, the bloom filter and the
// shuffle partition hash all read this one producer — and a TIMESTAMP later
// that day is a different key. The integer fast path, which reads each
// side's raw integer (days on one side, milliseconds on the other), is not
// taken for the pair. At v0.25.2 the encoder had no arm for the pair and the
// join keyed each side at its own encoding.
func TestArcDTDateKeyAtTimestampIsItsMidnight(t *testing.T) {
	const day = 19724 // 2024-01-02
	d := batch.NewVector(batch.TypeDate, 1)
	d.Int32Data[0] = day
	ts := batch.NewVector(batch.TypeTimestamp, 2)
	ts.Int64Data[0] = int64(day) * 86_400_000              // 2024-01-02 00:00:00
	ts.Int64Data[1] = int64(day)*86_400_000 + 12*3_600_000 // 2024-01-02 12:00:00

	if !canEncodeKeyAt(batch.TypeDate, batch.TypeTimestamp) {
		t.Fatalf("canEncodeKeyAt(DATE, TIMESTAMP) = false: the rung has no encoder arm")
	}
	var got []byte
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("AppendWidenedKeyValue(DATE → TIMESTAMP) raised: %v", r)
			}
		}()
		got = AppendWidenedKeyValue(nil, d, 0, batch.TypeTimestamp)
	}()
	midnight := AppendWidenedKeyValue(nil, ts, 0, batch.TypeTimestamp)
	noon := AppendWidenedKeyValue(nil, ts, 1, batch.TypeTimestamp)
	if !bytes.Equal(got, midnight) {
		t.Errorf("DATE 2024-01-02 keys as %v, its midnight TIMESTAMP as %v: the two must be one key", got, midnight)
	}
	if bytes.Equal(got, noon) {
		t.Errorf("DATE 2024-01-02 keys as 2024-01-02 12:00:00 (%v): the promotion is not a truncation", noon)
	}
	// Before 1970: DATE 1969-12-31 (day -1) keys as that day's midnight,
	// -86 400 000 ms (batch.DateMidnightMillis), not as 23:59:59.999 (ms -1).
	pre := batch.NewVector(batch.TypeDate, 1)
	pre.Int32Data[0] = -1
	preTS := batch.NewVector(batch.TypeTimestamp, 2)
	preTS.Int64Data[0] = -86_400_000
	preTS.Int64Data[1] = -1
	if got := AppendWidenedKeyValue(nil, pre, 0, batch.TypeTimestamp); !bytes.Equal(got, AppendWidenedKeyValue(nil, preTS, 0, batch.TypeTimestamp)) ||
		bytes.Equal(got, AppendWidenedKeyValue(nil, preTS, 1, batch.TypeTimestamp)) {
		t.Errorf("DATE 1969-12-31 keys as %v: want the bytes of 1969-12-31 00:00:00, not of epoch ms -1", got)
	}
	types := []batch.TypeID{batch.TypeTimestamp}
	for _, own := range []batch.TypeID{batch.TypeDate, batch.TypeTimestamp} {
		if joinKeyUsesIntPath(types, 0, own) {
			t.Errorf("joinKeyUsesIntPath(resolved TIMESTAMP, own %v) = true: the int path reads a day count "+
				"on one side and milliseconds on the other", own)
		}
	}
}
