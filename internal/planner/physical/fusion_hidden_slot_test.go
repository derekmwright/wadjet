package physical

import "testing"

// A FUSION ABSORBS A JOIN'S RULES OR IT DOES NOT ABSORB THE JOIN — #988.
//
// `fuseJoinStages` replaces a broadcast join with a `FusedJoinSpec`, and that
// struct is the whole record of the join it replaces. It has a field for the
// keys, the build alias, the filters and the build schema — and for NONE of:
// `HiddenJoinCols` (the `__key_N` slot a decorrelated LATERAL minted, which the
// join owes a drop of, ADR-0026 §3c), `LateralPadMarker`, or
// `LateralEmptyDefaults` (the per-column empty-input values the operator above
// the join applies). Absorbing such a stage publishes a reserved name to the
// client and leaves an unmatched outer row's defaults unapplied.
//
// The rule is the one the `NullAwareAnti` and `ProjectExprs` guards beside it
// already state: not fusing is the honest alternative to carrying a rule
// nowhere. `ChainedJoinSpec` — the DOWNSTREAM fusion's spec — grew all three
// fields instead, which is why that pass absorbs these and this one declines.
//
// It is gated HERE, on the pass, rather than through SQL, and deliberately: no
// statement I could build reaches `fuseJoinStages` with a lateral join as the
// candidate (measured on four arms with the guard disabled — a lateral's join
// feeding another join's probe fuses through `fuseStageChains` instead, which
// carries the fields). A guard whose condition cannot be reached by a fixture
// is a claim; this makes it a gate, at the level the condition is written.
func TestFuseJoinStagesDeclinesAJoinWhoseRulesTheSpecCannotCarry(t *testing.T) {
	// The same shape fusion_byte_budget_test.go builds: a leaf broadcast join
	// feeding the outer join's PROBE side, small enough to fuse.
	build := func(mark func(*Stage)) []Stage {
		stages := []Stage{
			{ID: "scan-probe", Type: "scan"},
			{
				ID: "join-outer", Type: "broadcast_join",
				Dependencies: []string{"scan-probe", "join-leaf"},
				LeftDepStage: "join-leaf", RightDepStage: "scan-probe",
				JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			},
			{
				ID: "join-leaf", Type: "broadcast_join",
				Dependencies: []string{"scan-leaf-probe", "scan-leaf-build"},
				LeftDepStage: "scan-leaf-probe", RightDepStage: "scan-leaf-build",
				JoinLeftKeys: []string{"k"}, JoinRightKeys: []string{"k"},
			},
			{ID: "scan-leaf-probe", Type: "scan"},
			{ID: "scan-leaf-build", Type: "scan", EstimatedBytes: 512},
		}
		for i := range stages {
			if stages[i].ID == "join-leaf" {
				mark(&stages[i])
			}
		}
		return stages
	}
	absorbed := func(t *testing.T, out []Stage) bool {
		t.Helper()
		for _, s := range out {
			if s.ID == "join-leaf" {
				return false
			}
		}
		return true
	}

	for _, tc := range []struct {
		name string
		mark func(*Stage)
		want bool // want the leaf ABSORBED
	}{
		{
			// CONTROL: a leaf carrying none of the three fields fuses, which
			// is what says the cells below decline for their own reason and
			// not because the shape stopped being fusable.
			name: "control: a plain leaf is absorbed",
			mark: func(*Stage) {},
			want: true,
		},
		{
			name: "a leaf that minted a hidden slot is not absorbed",
			mark: func(s *Stage) {
				s.HiddenJoinCols = []HiddenJoinCol{{Ordinal: 0, Name: "__key_0"}}
			},
		},
		{
			name: "a leaf carrying a lateral pad marker is not absorbed",
			mark: func(s *Stage) { s.LateralPadMarker = "s.__key_0" },
		},
		{
			// Both together — the shape #988's second lateral actually has.
			name: "a leaf carrying both is not absorbed",
			mark: func(s *Stage) {
				s.HiddenJoinCols = []HiddenJoinCol{{Ordinal: 0, Name: "__key_0"}}
				s.LateralPadMarker = "s.__key_0"
				s.LateralDropMarker = true
				s.LateralEmptyDefaults = []LateralEmptyDefaultSpec{{Column: "n", ExprSQL: "0"}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := fuseJoinStages(build(tc.mark))
			if got := absorbed(t, out); got != tc.want {
				if tc.want {
					t.Fatalf("the leaf was NOT absorbed; this cell's whole job is to show "+
						"the shape is fusable (%d stages out)", len(out))
				}
				t.Fatalf("the leaf WAS absorbed — `FusedJoinSpec` has no field for what it " +
					"carries, so the rule stops running and a reserved name reaches the " +
					"client (#988)")
			}
		})
	}
}
