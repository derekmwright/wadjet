package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// distinctSet holds per-(group, aggregate) COUNT(DISTINCT)/APPROX_DISTINCT
// state. Int-class columns use an open-addressing int64 set — the previous
// map[string]struct{} over binary-serialized values paid a string
// allocation, an AES string hash, and map overhead PER ROW, and the
// clone-merge union re-paid all of it per element (ClickBench Q05,
// COUNT(DISTINCT UserID) at 100M: 73% of a 14s profile inside the serial
// string-map merge). Non-int columns keep the serialized-string map.
type distinctSet struct {
	ints *i64Set
	strs map[string]struct{}
}

// newDistinctSetFor picks the representation from the input column type.
func newDistinctSetFor(typ batch.TypeID) *distinctSet {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration,
		batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return &distinctSet{ints: newI64Set(16)}
	default:
		return &distinctSet{strs: make(map[string]struct{})}
	}
}

// addInt inserts v; returns true when newly added. Only valid on int-mode sets.
func (d *distinctSet) addInt(v int64) bool { return d.ints.insert(v) }

// addStr inserts the serialized key; returns true when newly added.
func (d *distinctSet) addStr(k string) bool {
	if _, dup := d.strs[k]; dup {
		return false
	}
	d.strs[k] = struct{}{}
	return true
}

func (d *distinctSet) count() int {
	if d == nil {
		return 0
	}
	if d.ints != nil {
		return d.ints.n
	}
	return len(d.strs)
}

// memBytes estimates retained bytes (slots or map entries + overhead).
func (d *distinctSet) memBytes() int64 {
	if d == nil {
		return 0
	}
	if d.ints != nil {
		return 48 + int64(len(d.ints.slots))*8
	}
	var b int64 = 48
	for k := range d.strs {
		b += int64(len(k)) + 48
	}
	return b
}

// mergeFrom unions o into d. Both sides come from the same aggregate spec,
// so representations always match; a nil/empty other is a no-op.
func (d *distinctSet) mergeFrom(o *distinctSet) {
	if o == nil {
		return
	}
	if d.ints != nil && o.ints != nil {
		s := o.ints
		if s.hasZero {
			d.ints.insert(0)
		}
		for _, v := range s.slots {
			if v != 0 {
				d.ints.insert(v)
			}
		}
		return
	}
	if d.strs != nil {
		for k := range o.strs {
			d.strs[k] = struct{}{}
		}
	}
}

// i64Set is an open-addressing int64 hash set. Zero is the empty-slot
// sentinel; an explicit flag tracks whether 0 itself is a member.
type i64Set struct {
	slots   []int64
	mask    uint64
	n       int
	hasZero bool
}

func newI64Set(capHint int) *i64Set {
	size := 16
	for size < capHint*2 {
		size <<= 1
	}
	return &i64Set{slots: make([]int64, size), mask: uint64(size - 1)}
}

// mix64 is the splitmix64 finalizer — full-avalanche int64 hashing.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// insert adds v, returning true when newly added.
func (s *i64Set) insert(v int64) bool {
	if v == 0 {
		if s.hasZero {
			return false
		}
		s.hasZero = true
		s.n++
		return true
	}
	if uint64(s.n+1)*4 >= uint64(len(s.slots))*3 { // 75% load
		s.grow()
	}
	i := mix64(uint64(v)) & s.mask
	for {
		sv := s.slots[i]
		if sv == v {
			return false
		}
		if sv == 0 {
			s.slots[i] = v
			s.n++
			return true
		}
		i = (i + 1) & s.mask
	}
}

func (s *i64Set) grow() {
	old := s.slots
	s.slots = make([]int64, len(old)*2)
	s.mask = uint64(len(s.slots) - 1)
	for _, v := range old {
		if v == 0 {
			continue
		}
		i := mix64(uint64(v)) & s.mask
		for s.slots[i] != 0 {
			i = (i + 1) & s.mask
		}
		s.slots[i] = v
	}
}
