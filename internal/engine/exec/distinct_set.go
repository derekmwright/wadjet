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
// string-map merge). Non-int columns use an arena-backed open-addressing
// set for the same reasons — the map path allocated a string per ROW
// (duplicates included) before it could even probe (ClickBench Q06,
// COUNT(DISTINCT SearchPhrase) at 100M).
type distinctSet struct {
	ints *i64Set
	strs *strSet
}

// newDistinctSetFor picks the representation from the input column type.
func newDistinctSetFor(typ batch.TypeID) *distinctSet {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration,
		batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return &distinctSet{ints: newI64Set(16)}
	default:
		return &distinctSet{strs: newStrSet(16)}
	}
}

// addInt inserts v; returns true when newly added. Only valid on int-mode sets.
func (d *distinctSet) addInt(v int64) bool { return d.ints.insert(v) }

// addStr inserts the serialized value; returns true when newly added.
// k may be a transient view (scratch buffer) — bytes are copied into the
// set's arena only on insert.
func (d *distinctSet) addStr(k []byte) bool { return d.strs.insert(k) }

func (d *distinctSet) count() int {
	if d == nil {
		return 0
	}
	if d.ints != nil {
		return d.ints.n
	}
	return d.strs.n
}

// memBytes estimates retained bytes (slots or arena + entries + overhead).
func (d *distinctSet) memBytes() int64 {
	if d == nil {
		return 0
	}
	if d.ints != nil {
		return 48 + int64(len(d.ints.slots))*8
	}
	return 48 + int64(cap(d.strs.arena)) + int64(len(d.strs.entries))*12
}

// mergeFrom unions o into d. Both sides come from the same aggregate spec,
// so representations always match; a nil/empty other is a no-op.
func (d *distinctSet) mergeFrom(o *distinctSet) {
	if o == nil {
		return
	}
	if d.ints != nil && o.ints != nil {
		s := o.ints
		// Pre-grow to the merged cardinality upper bound: growing through
		// doubling mid-merge re-hashes the table log2 times and holds two
		// copies live at each step — at Q05 scale (90M distinct across 16
		// clones) the doubling transients were a multi-GB heap spike that
		// evicted the query's own input page cache.
		d.ints.reserve(d.ints.n + s.n)
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
	if d.strs != nil && o.strs != nil {
		d.strs.reserve(d.strs.n + o.strs.n)
		for _, e := range o.strs.entries {
			if e.l >= 0 {
				d.strs.insert(o.strs.arena[e.off : e.off+e.l])
			}
		}
	}
}

// strSet is an arena-backed open-addressing byte-string set: entries hold
// (offset, length, hash tag) into a shared append-only arena. Probing is
// alloc-free (hash + tag/length prefilter + byte compare); insert appends
// the bytes once. l == -1 marks an empty slot.
type strSet struct {
	entries []strSetEntry
	mask    uint64
	n       int
	arena   []byte
}

type strSetEntry struct {
	off int32
	l   int32
	tag uint32
}

func newStrSet(capHint int) *strSet {
	size := 16
	for size < capHint*2 {
		size <<= 1
	}
	s := &strSet{entries: make([]strSetEntry, size), mask: uint64(size - 1)}
	for i := range s.entries {
		s.entries[i].l = -1
	}
	return s
}

// reserve grows the entry table once for n total members (see i64Set.reserve).
func (s *strSet) reserve(n int) {
	need := 16
	for uint64(n+1)*4 >= uint64(need)*3 {
		need <<= 1
	}
	if need <= len(s.entries) {
		return
	}
	old := s.entries
	s.entries = make([]strSetEntry, need)
	for i := range s.entries {
		s.entries[i].l = -1
	}
	s.mask = uint64(need - 1)
	for _, e := range old {
		if e.l == -1 {
			continue
		}
		i := strHash(s.arena[e.off:e.off+e.l]) & s.mask
		for s.entries[i].l != -1 {
			i = (i + 1) & s.mask
		}
		s.entries[i] = e
	}
}

// insert adds k (copying into the arena), returning true when newly added.
func (s *strSet) insert(k []byte) bool {
	if uint64(s.n+1)*4 >= uint64(len(s.entries))*3 { // 75% load
		s.grow()
	}
	h := strHash(k)
	tag := uint32(h)
	i := h & s.mask
	for {
		e := &s.entries[i]
		if e.l == -1 {
			off := int32(len(s.arena))
			s.arena = append(s.arena, k...)
			*e = strSetEntry{off: off, l: int32(len(k)), tag: tag}
			s.n++
			return true
		}
		if e.tag == tag && int(e.l) == len(k) && string(s.arena[e.off:e.off+e.l]) == string(k) {
			return false
		}
		i = (i + 1) & s.mask
	}
}

func (s *strSet) grow() {
	old := s.entries
	s.entries = make([]strSetEntry, len(old)*2)
	for i := range s.entries {
		s.entries[i].l = -1
	}
	s.mask = uint64(len(s.entries) - 1)
	for _, e := range old {
		if e.l == -1 {
			continue
		}
		i := strHash(s.arena[e.off:e.off+e.l]) & s.mask
		for s.entries[i].l != -1 {
			i = (i + 1) & s.mask
		}
		s.entries[i] = e
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

// reserve grows the table once so n total members fit under the 75%%
// load factor without incremental doubling.
func (s *i64Set) reserve(n int) {
	need := 16
	for uint64(n+1)*4 >= uint64(need)*3 {
		need <<= 1
	}
	if need <= len(s.slots) {
		return
	}
	old := s.slots
	s.slots = make([]int64, need)
	s.mask = uint64(need - 1)
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
