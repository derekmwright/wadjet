package sql

import (
	"strings"
	"testing"
)

// TestEverySlotFamilyIsReservedAndEveryReservationHasAProducer asserts the
// table in BOTH directions, the way the ANALYZE coverage gate does.
//
// The API exists to stop a family's name and its reservation drifting apart,
// and they had already drifted: SlotCovarState read "__covar_stat" while the
// reservation and worker/var_fold.go used "__covar_state". It was latent only
// because nothing called the constructor. A one-directional test would not have
// seen it — the prefix was reserved, it was the CONSTANT that was wrong.
func TestEverySlotFamilyIsReservedAndEveryReservationHasAProducer(t *testing.T) {
	// Every family constant mints a name the reservation claims.
	for _, f := range allSlotFamilies {
		name := SlotName(f, 0)
		got := ReservedSlotFamily(name)
		if got == "" {
			t.Errorf("SlotName(%q, 0) = %q, which the reservation does NOT claim — a family "+
				"the planner mints and the namespace does not cover is a name a user can "+
				"still collide with", string(f), name)
			continue
		}
		if got != string(f) {
			t.Errorf("SlotName(%q, 0) = %q, claimed by family %q — the constant and the "+
				"reservation have drifted", string(f), name, got)
		}
	}

	// And every reservation is one some family or documented suffix-minted slot
	// produces, so the list cannot accumulate prefixes nothing uses.
	produced := map[string]bool{}
	for _, f := range allSlotFamilies {
		produced[string(f)] = true
	}
	// The five slots minted with a SUFFIX rather than an index. They have no
	// SlotName producer by construction and are named here so "unclaimed"
	// cannot quietly mean "forgotten".
	for _, f := range suffixMintedFamilies {
		produced[string(f)] = true
	}
	for _, p := range reservedSlotPrefixes {
		if !produced[p] {
			t.Errorf("reservation %q has no SlotName producer and is not one of the "+
				"suffix-minted slots — either give it a family constant or drop it", p)
		}
	}
	// Neither direction may pass vacuously.
	if len(allSlotFamilies) == 0 || len(reservedSlotPrefixes) == 0 {
		t.Fatal("the slot table is empty; this gate would pass on any tree")
	}
}

// TestReservedSlotFamilyMatchesCaseInsensitively: column resolution is
// case-insensitive, so a user writing __WIN_0 reaches the same slot.
func TestReservedSlotFamilyMatchesCaseInsensitively(t *testing.T) {
	for _, name := range []string{"__win_0", "__WIN_0", "  __Win_0  "} {
		if ReservedSlotFamily(name) != string(SlotWindowOutput) {
			t.Errorf("ReservedSlotFamily(%q) did not claim the window family", name)
		}
	}
	for _, name := range []string{"_win_0", "__mycol", "__window", "id", ""} {
		if got := ReservedSlotFamily(name); got != "" {
			t.Errorf("ReservedSlotFamily(%q) = %q; the reservation is a named list, not a "+
				"ban on leading underscores", name, got)
		}
	}
}

// TestRefuseReservedSlotNameIsReservedNameSQLSTATE pins the SQLSTATE. 42939 is
// PostgreSQL's reserved_name; 42601 (syntax_error) says the query is
// malformed, which it is not.
func TestRefuseReservedSlotNameIsReservedNameSQLSTATE(t *testing.T) {
	err := RefuseReservedSlotName("__win_0", "column of table t")
	if err == nil {
		t.Fatal("a reserved name was admitted")
	}
	if !strings.Contains(err.Error(), "__win_") {
		t.Errorf("the refusal does not name the family it collided with: %v", err)
	}
	type coded interface{ SQLState() string }
	c, ok := err.(coded)
	if !ok {
		t.Fatalf("the refusal carries no SQLSTATE: %T", err)
	}
	if c.SQLState() != "42939" {
		t.Errorf("SQLSTATE = %q, want 42939 (reserved_name)", c.SQLState())
	}
}

// TestSlotAllocatorExcludesSeededAndIssued is the allocator's contract, and it
// is stated as the two halves that were each got wrong independently.
func TestSlotAllocatorExcludesSeededAndIssued(t *testing.T) {
	// A scope holding a STORED column of the family, exactly as a table
	// written before the namespace was reserved provides.
	a := NewSlotAllocator("id", "__win_0", "plain")

	const n = 5
	got := make([]string, 0, n)
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		name, ok := a.Next(SlotWindowOutput)
		if !ok {
			t.Fatalf("allocation %d failed in a scope of three names", i)
		}
		if strings.EqualFold(name, "__win_0") {
			t.Errorf("allocation %d returned %q, which is SEEDED — a stored column and the "+
				"planner's slot would be one name", i, name)
		}
		if seen[strings.ToLower(name)] {
			t.Errorf("allocation %d returned %q, ALREADY ISSUED by this allocator — two "+
				"windows would write one column and the second would read the first's value",
				i, name)
		}
		seen[strings.ToLower(name)] = true
		got = append(got, name)
	}
	if len(seen) != n {
		t.Fatalf("allocated %d slots but only %d distinct: %v", n, len(seen), got)
	}
	if issued := a.Issued(); len(issued) != n {
		t.Errorf("Issued() reports %d slots, want %d: %v", len(issued), n, issued)
	}
}

// TestSlotAllocatorTestCatchesAnAllocatorThatForgets is the mutation proof: it
// drives an allocator that records seeds but NOT what it issued — the exact
// defect both authors wrote — and asserts that the contract above would reject
// it. Without this, the test above could pass on an allocator whose issued-set
// was dead code.
func TestSlotAllocatorTestCatchesAnAllocatorThatForgets(t *testing.T) {
	forgetful := NewSlotAllocator("id", "__win_0", "plain")

	// Stub the issued half faithfully: search from zero each time, skipping
	// only the SEEDED names. That is what both real bugs did — each caller ran
	// its own fresh search and neither told the other what it had taken — and
	// it is why a monotonic cursor alone is not the contract.
	nextForgetful := func() string {
		for i := 0; ; i++ {
			name := SlotName(SlotWindowOutput, i)
			if !forgetful.taken[strings.ToLower(name)] {
				return name // deliberately NOT recorded as taken
			}
		}
	}

	seen := map[string]bool{}
	duplicated := false
	for i := 0; i < 3; i++ {
		name := nextForgetful()
		if seen[strings.ToLower(name)] {
			duplicated = true
		}
		seen[strings.ToLower(name)] = true
	}
	if !duplicated {
		t.Fatal("the forgetful allocator did NOT repeat a name, so the contract test above " +
			"would pass on an allocator whose issued-set is dead code; this gate proves " +
			"nothing as written")
	}
}

// TestSlotAllocatorIsPerFamily: allocating in one family must not renumber
// another, or a group-key allocation would push a window's slot along.
func TestSlotAllocatorIsPerFamily(t *testing.T) {
	a := NewSlotAllocator()
	w, _ := a.Next(SlotWindowOutput)
	g, _ := a.Next(SlotGroupKey)
	if w != SlotName(SlotWindowOutput, 0) {
		t.Errorf("first window slot = %q, want %q", w, SlotName(SlotWindowOutput, 0))
	}
	if g != SlotName(SlotGroupKey, 0) {
		t.Errorf("first group-key slot = %q, want %q — the families share a cursor", g,
			SlotName(SlotGroupKey, 0))
	}
}

// TestSlotAllocatorSeedIsCaseInsensitive: column resolution is, so a stored
// __WIN_0 has to block __win_0.
func TestSlotAllocatorSeedIsCaseInsensitive(t *testing.T) {
	a := NewSlotAllocator("__WIN_0")
	name, ok := a.Next(SlotWindowOutput)
	if !ok {
		t.Fatal("allocation failed")
	}
	if strings.EqualFold(name, "__win_0") {
		t.Errorf("allocated %q against a seed of __WIN_0; a consumer looking the slot up "+
			"would find the stored column", name)
	}
}

// TestSlotAllocatorSeedAfterAllocationKeepsIssued: a scope can learn a name
// late (a schema resolved after planning began), and that must not un-issue
// what is already handed out.
func TestSlotAllocatorSeedAfterAllocationKeepsIssued(t *testing.T) {
	a := NewSlotAllocator()
	first, _ := a.Next(SlotWindowOutput)
	a.Seed("late_column", SlotName(SlotWindowOutput, 3))
	for i := 0; i < 4; i++ {
		name, ok := a.Next(SlotWindowOutput)
		if !ok {
			t.Fatalf("allocation %d failed", i)
		}
		if name == first {
			t.Errorf("re-issued %q after a late Seed", name)
		}
		if name == SlotName(SlotWindowOutput, 3) {
			t.Errorf("issued %q, which was seeded after allocation began", name)
		}
	}
}
