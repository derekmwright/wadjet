package physical

import (
	"errors"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrLateralProjectionDistributed marks a plan the stage DAG refuses because a
// STAR reads a decorrelated LATERAL whose block PROJECTION is not the column
// list its stage emits.
//
// A Project emits no stage. On the distributed path a lateral's SELECT list is
// therefore not a relation of its own: the stage under it — the Aggregate, or
// the Scan — is what materializes, and a `SELECT *` above the join publishes
// THAT stream. Where the two agree the star is right and runs distributed;
// where they differ the client is handed a relation the query did not write:
//
//	SELECT * FROM lat_ord o LEFT JOIN LATERAL (
//	  SELECT order_id, order_id AS oid, COUNT(*) AS n FROM lat_item
//	  WHERE order_id = o.id GROUP BY order_id) s ON true
//	PostgreSQL  id, customer, total, order_id, oid, n
//	the stream  id, customer, total, order_id,      n   ← `oid` is not a stage column
//
// Three ways a projection leaves its stream behind, and all three are one test
// — is every published name a column of the stream, once:
//
//   - a source column published TWICE (`order_id, order_id AS oid`): the
//     stream carries one of it, so the second is missing;
//   - a RENAME the stream does not carry (`order_id AS oid`, `COUNT(*) + 1 AS
//     n` over an aggregate that publishes `__agg_0`): the stream carries the
//     source name, so the client reads the wrong NAME or nothing;
//   - a duplicate published NAME, which one stream column cannot answer to
//     twice.
//
// The MINTED correlation slot is excluded from the comparison, because the
// join drops it: `SELECT COUNT(*) AS n` publishes `__key_0, n` over a stream of
// `order_id, n`, the rename is invisible below the drop, and that shape runs
// distributed exactly as it did.
//
// The refusal is a HANDOFF and not the query's outcome: `Coordinator.
// ExecuteSQL` routes it to the coordinator-local single-process pipeline, where
// the lateral's Project is a real operator and every published column is a real
// column, and the query answers PostgreSQL's rows. Before it, one spelling was
// LOUD (the join's empty-build task declared the projection while its siblings
// declared the stream — ADR-0010's `one stage's files describe one relation`)
// and the other silently dropped the column.
//
// It is scoped to a STAR because that is the consumer with no column list of
// its own: a named SELECT list above the join asks for its columns by name and
// the gather resolves them, which is why `SELECT s.oid` is right on every arm
// today and is not routed.
//
// K3 REMOVES THIS ROUTING. The structural fix is #984 — a stage declares the
// block's PROJECTION rather than its stream — and the day it lands every shape
// here runs distributed and this refusal, its counter and its gate go with it.
var ErrLateralProjectionDistributed = errors.New(
	"a star over this LATERAL reads columns its stage does not publish")

// refuseLateralProjection walks the plan for a star that reads a decorrelated
// lateral whose projection its stage cannot state.
//
// `projected` is the whole star test: a Project between the root and the join
// means the statement named its columns, and a named list is resolved by name
// on both paths. Only a join whose own output IS the statement's output — a
// bare star, a qualified star that never expanded, a derived table's or a
// CTE's star, all of which leave no Project above the join — is at risk.
func (p *Planner) refuseLateralProjection(node *logical.Node) error {
	return lateralProjectionWalk(node, false)
}

func lateralProjectionWalk(n *logical.Node, projected bool) error {
	if n == nil {
		return nil
	}
	if n.Type == logical.NodeProject {
		projected = true
	}
	if !projected {
		if missing, stream := lateralProjectionNotInStream(n); missing != "" {
			return fmt.Errorf("%w: the subquery publishes %q, which the stage "+
				"emitting it does not carry (its columns are %v) — a Project emits no "+
				"stage, so a star over this join would publish the stage's stream "+
				"instead of the columns the query wrote",
				ErrLateralProjectionDistributed, missing, stream)
		}
	}
	for _, child := range n.Children {
		if err := lateralProjectionWalk(child, projected); err != nil {
			return err
		}
	}
	return nil
}

// lateralProjectionNotInStream returns the first column this join's LATERAL
// side publishes that its stage does not, with the stream's own list beside it;
// "" when the two agree or when this is not a decorrelated lateral join.
func lateralProjectionNotInStream(n *logical.Node) (string, []string) {
	side := lateralSideOf(n)
	if side < 0 {
		return "", nil
	}
	sub := n.Children[side]
	if sub == nil || sub.Type != logical.NodeProject {
		// No projection to disagree with: the lateral's own SELECT list was
		// elided into the operator below, so the stream IS the block.
		return "", nil
	}
	stream := lateralStreamNames(sub)
	if len(stream) == 0 {
		return "", nil // a stream this pass cannot state says nothing
	}
	slots := make(map[string]bool, len(n.HiddenJoinCols))
	for _, h := range n.HiddenJoinCols {
		slots[strings.ToLower(lateralBareName(h))] = true
	}
	have := make(map[string]bool, len(stream))
	for _, s := range stream {
		have[strings.ToLower(lateralBareName(s))] = true
	}
	seen := make(map[string]bool, len(sub.Projections))
	for _, name := range emittedColumnNames(sub) {
		bare := strings.ToLower(lateralBareName(name))
		if bare == "" || slots[bare] {
			// The correlation slot the join is about to drop. Its rename is
			// invisible to every consumer, so it cannot be a divergence.
			continue
		}
		if seen[bare] {
			return name, stream // one stream column cannot answer to it twice
		}
		seen[bare] = true
		if !have[bare] {
			return name, stream
		}
	}
	return "", nil
}

// lateralStreamNames is what the STAGE under this lateral's projection emits:
// the first thing below it that is not a Project, because a Project emits no
// stage.
func lateralStreamNames(n *logical.Node) []string {
	for cur := n; cur != nil; {
		if cur.Type != logical.NodeProject {
			return emittedColumnNames(cur)
		}
		if len(cur.Children) != 1 {
			return nil
		}
		cur = cur.Children[0]
	}
	return nil
}

// lateralBareName drops a qualifier: the projection spells `s.oid` where the
// stream spells `oid`, and that is not a divergence.
func lateralBareName(s string) string {
	s = strings.TrimSpace(s)
	if dot := strings.LastIndexByte(s, '.'); dot >= 0 && dot < len(s)-1 {
		return s[dot+1:]
	}
	return s
}
