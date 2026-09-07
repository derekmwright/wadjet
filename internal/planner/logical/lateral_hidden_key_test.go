package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A decorrelated LATERAL publishes its correlation key under a HIDDEN SLOT —
// #956, #767's mirror, ADR-0026 §3c.
//
// The shape test, at the layer that decides it, so a change to the injection
// is caught without standing up a cluster. The VALUES are gated on four arms
// by `coordinator.TestArcJ1ALateralKeyIsPublishedUnderAHiddenSlot`.
//
// Three answers are possible and each is a cell:
//
//   - the SELECT list publishes the key under its own name (or through a
//     star) — nothing is injected, and the join keys on the lateral's alias
//     plus that name;
//   - it publishes the key's VALUE under an alias — nothing is injected
//     either, and the join keys on the alias (H1's #767 fix);
//   - it does not publish the key at all, or publishes something ELSE under
//     the key's name — the key is minted into `__key_N` and the join keys on
//     that. `__key_N` is reserved, so no alias can shadow it.
func TestALateralKeyIsPublishedUnderAHiddenSlot(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		// wantKey is the build-side spelling the join condition must end in.
		wantKey string
		// wantPublish is the aggregate's GroupByPublish entry, "" for none.
		wantPublish string
	}{
		{"an aggregate aliased like the key mints one (#956)",
			`SELECT d.k, s.g FROM typemx_dim d JOIN LATERAL (` +
				`SELECT MAX(t.id) AS g FROM typemx t WHERE t.g = d.k) s ON true`,
			"s.__key_0", "__key_0"},
		{"an inner alias shadowing the key mints one (#767 mirror)",
			`SELECT o.customer, li.order_id FROM lat_ord o JOIN LATERAL (` +
				`SELECT amount AS order_id FROM lat_item WHERE order_id = o.id) li ON true`,
			"li.__key_0", ""},
		{"a key the list does not publish at all mints one",
			`SELECT d.k, s.c FROM typemx_dim d JOIN LATERAL (` +
				`SELECT COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			"s.__key_0", "__key_0"},
		{"ctl the key under its OWN name keys on the lateral's alias",
			`SELECT d.k, s.g FROM typemx_dim d JOIN LATERAL (` +
				`SELECT t.g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			"s.g", ""},
		{"ctl the key under an ALIAS keys on the alias",
			`SELECT d.k, s.gg FROM typemx_dim d JOIN LATERAL (` +
				`SELECT t.g AS gg, COUNT(*) AS c FROM typemx t WHERE t.g = d.k GROUP BY t.g) s ON true`,
			"s.gg", ""},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := plansql.Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := plansql.ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			plan, err := BuildFromSelect(info)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			join := findLateralJoin(plan)
			if join == nil {
				t.Fatal("no join in the plan")
			}
			if !strings.HasSuffix(strings.TrimSpace(join.JoinCond), tc.wantKey) {
				t.Errorf("join condition %q does not key on %q — the build side does not "+
					"publish what the join reads", join.JoinCond, tc.wantKey)
			}
			agg := findAggregateBelow(join.Children[1])
			switch {
			case tc.wantPublish == "":
				if agg != nil && len(agg.GroupByPublish) > 0 && agg.GroupByPublish[0] != "" {
					t.Errorf("group key published as %q; this shape needs no slot",
						agg.GroupByPublish[0])
				}
			case agg == nil:
				t.Errorf("no aggregate below the lateral, so nothing publishes %q", tc.wantPublish)
			case len(agg.GroupByPublish) == 0 || agg.GroupByPublish[0] != tc.wantPublish:
				t.Errorf("GroupByPublish = %v, want [%q] — the aggregate must publish the key "+
					"under the slot while still resolving it by %v",
					agg.GroupByPublish, tc.wantPublish, agg.GroupBy)
			}
		})
	}
}

// A minted slot is in the RESERVED namespace, which is what makes it a slot
// rather than a name: the reservation refuses a user who writes one, so
// nothing a query can spell collides with it (ADR-0026 §2a).
func TestAMintedLateralKeyIsInTheReservedNamespace(t *testing.T) {
	if got := plansql.ReservedSlotFamily(plansql.SlotName(plansql.SlotCorrKey, 0)); got == "" {
		t.Fatal("__key_0 is not in the reserved namespace, so an alias could shadow it")
	}
}

func findLateralJoin(n *Node) *Node {
	if n == nil {
		return nil
	}
	if n.Type == NodeJoin && len(n.Children) == 2 {
		return n
	}
	for _, c := range n.Children {
		if f := findLateralJoin(c); f != nil {
			return f
		}
	}
	return nil
}

func findAggregateBelow(n *Node) *Node {
	if n == nil {
		return nil
	}
	if n.Type == NodeAggregate {
		return n
	}
	if len(n.Children) != 1 {
		return nil
	}
	return findAggregateBelow(n.Children[0])
}
