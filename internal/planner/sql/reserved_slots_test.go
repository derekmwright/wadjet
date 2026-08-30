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
	for _, s := range []string{
		"__default__", "__precomp_agg_", "__row_loc", "__rowcount_only__", "__subsume_f",
	} {
		produced[s] = true
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
