package tpch

// postgresJoinArmCases asks PostgreSQL what a reference resolves to when it is
// carried ACROSS a join — a filter above the join naming a CTE's renamed
// column (#700, #726) and a qualified reference to one of two arms that
// publish the same name (#742, #747, #751).
//
// This arm runs through the EMBEDDED API, so it is the SINGLE-process half of
// those questions; the DAG halves are gated in
// `coordinator.TestCTEFilterAboveJoinThreeArms` and
// `coordinator.TestSiblingWindowSubqueriesUnderAJoinKeepTheirOwnValues`, which
// run the same shapes on the broadcast and the forced-shuffle lowerings. Both
// are needed: the defects in this family are lowering-specific, and an entry
// here that agrees says nothing about the other two arms.
//
// Written over nation and region, whose sizes make the join shape obvious:
// 25 nations across 5 regions.
func postgresJoinArmCases() []pgCase {
	var out []pgCase
	add := func(name, sql string) {
		out = append(out, pgCase{name: name, sql: sql})
	}
	// There is no `pin` helper here any more: every entry in this file is
	// GATED. The last pinned one was #751, deleted when its fix landed — a
	// pin whose issue stops diverging FAILS, which is how it left.

	// --- A WHERE above a JOIN naming a CTE's RENAMED column (#653, #700, #726).
	//
	// `rk > 2` keeps regions 3 and 4, which is 2 of the 5 — a literal inside
	// the range, so an engine that dropped the predicate and one that made it
	// UNKNOWN on every row give visibly different answers.
	const cte = `WITH c AS (SELECT n_nationkey AS k, n_regionkey AS rk, n_name AS nm FROM nation) `
	// The same CTE with a COMPUTED published column beside the renames.
	const cte2 = `WITH c AS (SELECT n_nationkey AS k, n_regionkey AS rk, ` +
		`n_regionkey * 2 AS dv FROM nation) `
	add("JoinArmCTEFilterInner", cte+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey WHERE c.rk > 2`)
	add("JoinArmCTEFilterInnerSwapped", cte+
		`SELECT COUNT(*) AS n FROM region r JOIN c ON c.rk = r.r_regionkey WHERE c.rk > 2`)
	add("JoinArmCTEFilterLeft", cte+
		`SELECT COUNT(*) AS n FROM c LEFT JOIN region r ON c.rk = r.r_regionkey WHERE c.rk > 2`)
	add("JoinArmCTEFilterRight", cte+
		`SELECT COUNT(*) AS n FROM c RIGHT JOIN region r ON c.rk = r.r_regionkey WHERE c.rk > 2`)
	add("JoinArmCTEFilterFull", cte+
		`SELECT COUNT(*) AS n FROM c FULL JOIN region r ON c.rk = r.r_regionkey WHERE c.rk > 2`)
	add("JoinArmCTEFilterBareName", cte+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey WHERE rk > 2`)
	add("JoinArmCTEFilterConjoined", cte+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`WHERE c.rk > 2 AND r.r_regionkey < 4`)
	add("JoinArmCTEFilterSemi", cte+
		`SELECT COUNT(*) AS n FROM c WHERE c.rk IN (SELECT r_regionkey FROM region) AND c.rk > 2`)
	add("JoinArmCTEFilterAnti", cte+
		`SELECT COUNT(*) AS n FROM c WHERE c.rk NOT IN (SELECT r_regionkey FROM region WHERE r_regionkey > 90) `+
		`AND c.rk > 2`)
	add("JoinArmDerivedFilter",
		`SELECT COUNT(*) AS n FROM (SELECT n_nationkey AS k, n_regionkey AS rk FROM nation) c `+
			`JOIN region r ON c.rk = r.r_regionkey WHERE c.rk > 2`)
	add("JoinArmCTEReferencedTwice", cte+
		`SELECT COUNT(*) AS n FROM c JOIN (SELECT k AS j FROM c) x ON c.k = x.j WHERE c.rk > 2`)
	// The ROW spelling, so the gate sees the VALUES and not only a count.
	add("JoinArmCTEFilterRows", cte+
		`SELECT c.k AS ck, c.rk AS crk, c.nm AS cnm FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`WHERE c.rk > 2 ORDER BY c.k`)

	// --- The same filter above a CHAIN of joins, which is where #700/#726
	// actually live. The one-join sweep above passes everywhere and the
	// two-join one did not: the column pruner partitions a join's needs using
	// the names the subtree's SCANS store, and a CTE's alias is stored by no
	// scan, so it went to neither side and the INNER join's OutputFilter
	// dropped it. The single path raised `filter column "c.rk" does not exist
	// in the input schema`; the shuffled DAG answered zero in silence.
	add("JoinArmCTEFilterTwoJoinChain", cte+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.rk > 2`)
	add("JoinArmCTEFilterTwoJoinChainCTESecond", cte+
		`SELECT COUNT(*) AS n FROM region r JOIN c ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.rk > 2`)
	add("JoinArmCTEFilterThreeJoinChain", cte+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey JOIN nation n3 ON c.k = n3.n_nationkey `+
		`WHERE c.rk > 2`)
	add("JoinArmCTEFilterTwoJoinChainBare", cte+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey WHERE rk > 2`)
	add("JoinArmCTEFilterTwoJoinChainConjoined", cte+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.rk > 2 AND n2.n_nationkey < 20`)
	add("JoinArmDerivedFilterTwoJoinChain",
		`SELECT COUNT(*) AS n FROM (SELECT n_nationkey AS k, n_regionkey AS rk FROM nation) c `+
			`JOIN region r ON c.rk = r.r_regionkey JOIN nation n2 ON c.k = n2.n_nationkey `+
			`WHERE c.rk > 2`)
	// The ROW spelling through the chain, so the values are compared and not
	// only a count.
	add("JoinArmCTEFilterTwoJoinChainRows", cte+
		`SELECT c.k AS ck, c.rk AS crk, c.nm AS cnm FROM c `+
		`JOIN region r ON c.rk = r.r_regionkey JOIN nation n2 ON c.k = n2.n_nationkey `+
		`WHERE c.rk > 2 ORDER BY c.k`)
	// The CTE on the BUILD side of the first join of a chain, which is the
	// spelling where the join qualifies its columns and the re-spelled filter
	// names the bare one.
	add("JoinArmCTEFilterChainCTEOnBuild",
		`WITH c AS (SELECT n_nationkey AS k, n_name AS v FROM nation) `+
			`SELECT COUNT(*) AS n FROM nation t JOIN c ON c.k = t.n_nationkey `+
			`JOIN nation u ON c.k = u.n_nationkey WHERE c.v > 'M'`)

	// The CTE BODY is a window, then an aggregate: the filter names an output
	// that lives in a hidden slot, and one that is an aggregate's output.
	add("JoinArmCTEBodyWindow",
		`WITH c AS (SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) `+
			`SELECT COUNT(*) AS n FROM c JOIN region r ON c.k = r.r_regionkey WHERE c.w > 2`)
	add("JoinArmCTEBodyAggregate",
		`WITH c AS (SELECT n_regionkey AS gk, COUNT(*) AS cnt FROM nation GROUP BY n_regionkey) `+
			`SELECT COUNT(*) AS n FROM c JOIN region r ON c.gk = r.r_regionkey WHERE c.cnt > 4`)

	// --- A CTE publishing a COMPUTED column above a join CHAIN.
	//
	// Every chain entry above publishes a plain RENAME, and a rename resolves
	// back to a source column through every DAG resolver. A COMPUTED output
	// has to be MATERIALIZED by some fragment or it exists nowhere, and that
	// is a different question — the one the chain entries above were blind to
	// while the class was still open (#700/#726 round 2).
	add("JoinArmComputedChainArith", cte2+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.dv > 4`)
	add("JoinArmComputedChainThreeJoin", cte2+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey JOIN nation n3 ON c.k = n3.n_nationkey `+
		`WHERE c.dv > 4`)
	add("JoinArmComputedChainBare", cte2+
		`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey WHERE dv > 4`)
	add("JoinArmComputedChainProjecting", cte2+
		`SELECT c.k AS ck, c.dv AS dv FROM c JOIN region r ON c.rk = r.r_regionkey `+
		`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.dv > 4 ORDER BY c.k`)
	add("JoinArmComputedChainCase",
		`WITH c AS (SELECT n_nationkey AS k, n_regionkey AS rk, `+
			`CASE WHEN n_regionkey > 2 THEN n_regionkey ELSE 0 END AS dv FROM nation) `+
			`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
			`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.dv > 0`)
	add("JoinArmComputedChainNestedProject",
		`WITH c AS (SELECT k, rk, v * 2 AS dv FROM `+
			`(SELECT n_nationkey AS k, n_regionkey AS rk, n_regionkey AS v FROM nation) z) `+
			`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
			`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.dv > 4`)
	add("JoinArmComputedChainDerived",
		`SELECT COUNT(*) AS n FROM (SELECT n_nationkey AS k, n_regionkey AS rk, `+
			`n_regionkey * 2 AS dv FROM nation) c JOIN region r ON c.rk = r.r_regionkey `+
			`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.dv > 4`)
	add("JoinArmComputedChainOverWindow",
		`WITH c AS (SELECT n_nationkey AS k, n_regionkey AS rk, `+
			`SUM(n_regionkey) OVER () + 0 AS dv FROM nation) `+
			`SELECT COUNT(*) AS n FROM c JOIN region r ON c.rk = r.r_regionkey `+
			`JOIN nation n2 ON c.k = n2.n_nationkey WHERE c.dv > 4`)
	add("JoinArmComputedChainOverAggregate",
		`WITH c AS (SELECT n_regionkey AS rk, SUM(n_nationkey) * 2 AS dv FROM nation `+
			`GROUP BY n_regionkey) SELECT COUNT(*) AS n FROM c `+
			`JOIN region r ON c.rk = r.r_regionkey JOIN region r2 ON c.rk = r2.r_regionkey `+
			`WHERE c.dv > 4`)

	// --- Two arms publishing ONE name, and a qualified reference to each.
	//
	// Every sibling block mints its window slot from a counter that restarts
	// per SELECT block, so two of them held the same `__win_0` and one
	// window's value was published under both output columns (#747). The
	// values are chosen apart — SUM(n_regionkey) = 50 over 25 nations,
	// MIN(n_regionkey) = 0, MAX = 4 — because two equal windows make the
	// collapse invisible.
	add("JoinArmSiblingWindowsTwo",
		`SELECT p.w AS pw, q.w AS qw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, MIN(n_regionkey) OVER () AS w FROM nation) q `+
			`ON p.k = q.k ORDER BY p.k`)
	add("JoinArmSiblingWindowsTwoDistinctNames",
		`SELECT p.w AS pw, q.w2 AS qw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, MIN(n_regionkey) OVER () AS w2 FROM nation) q `+
			`ON p.k = q.k ORDER BY p.k`)
	// The CTE spelling of the two blocks just above. It was pinned (#753):
	// the single-process path published p's window under BOTH output columns
	// while the DERIVED spelling agreed, because the join qualified the build
	// arm's duplicate column with the alias of the SCAN below the CTE rather
	// than with the CTE's own name, and `q.w` matched neither spelling. The
	// arm is named `q` now (`joinArmAlias`) and the pin is gone.
	add("JoinArmSiblingWindowsInCTEs",
		`WITH p AS (SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation), `+
			`q AS (SELECT n_nationkey AS k, MIN(n_regionkey) OVER () AS w FROM nation) `+
			`SELECT p.w AS pw, q.w AS qw, p.k AS k FROM p JOIN q ON p.k = q.k ORDER BY p.k`)
	// The control: two sibling blocks with NO window resolve correctly today
	// and must keep doing so, which is what says the repair moved the SLOT
	// and not the name resolution.
	add("JoinArmSiblingsNoWindow",
		`SELECT p.w AS pw, q.w AS qw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, n_regionkey + 1 AS w FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 3 AS w FROM nation) q `+
			`ON p.k = q.k ORDER BY p.k`)

	// --- The join-OF-JOINS shapes. Every one of these diverged until the
	// column pruner learned that a subtree also publishes the names its own
	// Projects MINT: a derived alias is stored by no scan, so it belonged to
	// neither side of the partition and the INNER join's OutputFilter dropped
	// it. They are GATED now, and they are the entries that keep that closed.
	add("JoinArmSiblingWindowsThree",
		`SELECT p.w AS pw, q.w AS qw, s.w AS sw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, MIN(n_regionkey) OVER () AS w FROM nation) q ON p.k = q.k JOIN `+
			`(SELECT n_nationkey AS k, MAX(n_regionkey) OVER () AS w FROM nation) s ON p.k = s.k `+
			`ORDER BY p.k`)
	// The same shape with NO window anywhere and a distinct alias per arm,
	// which is what says the collapse was never a name collision. It answered
	// NULL for the first two columns.
	add("JoinArmDerivedComputedUnderJoinOfJoins",
		`SELECT p.w1 AS pw, q.w2 AS qw, s.w3 AS sw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, n_regionkey + 1 AS w1 FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 3 AS w2 FROM nation) q ON p.k = q.k JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 5 AS w3 FROM nation) s ON p.k = s.k `+
			`ORDER BY p.k`)
	// #742's shape on THIS path. The single-process engine answers PostgreSQL's
	// value now; both stage-DAG arms still answer the other arm's column, which
	// the two-path gate pins — an entry that agrees here is no evidence about
	// them.
	add("JoinArmQualifiedRefAcrossArms",
		`SELECT x.k AS k, x.w AS xw FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () + 0 AS w FROM nation) x JOIN `+
			`nation y ON x.k = y.n_nationkey JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 3 AS w FROM nation) z ON x.k = z.k ORDER BY x.k`)

	// --- A join CHAIN over DERIVED arms (#755, #766, #753).
	//
	// This arm is the SINGLE-process half; the shuffled lowering is where all
	// three were filed, and `coordinator.TestDerivedArmsAboveAJoinChainThreeArms`
	// runs the same shapes on the two DAG lowerings. Both are needed and
	// neither says anything about the other.
	add("JoinArmDerivedArmsChainBaseBetween",
		`SELECT p.w AS pw, q.w AS qw FROM `+
			`(SELECT n_nationkey AS k, n_regionkey + 1 AS w FROM nation) p `+
			`JOIN nation y ON p.k = y.n_nationkey `+
			`JOIN (SELECT n_nationkey AS k, n_regionkey * 3 AS w FROM nation) q ON p.k = q.k `+
			`ORDER BY p.k`)
	add("JoinArmDerivedArmsChainThreeDistinct",
		`SELECT p.w1 AS pw, q.w2 AS qw, s.w3 AS sw FROM `+
			`(SELECT n_nationkey AS k, n_regionkey + 1 AS w1 FROM nation) p `+
			`JOIN (SELECT n_nationkey AS k, n_regionkey * 3 AS w2 FROM nation) q ON p.k = q.k `+
			`JOIN (SELECT n_nationkey AS k, n_regionkey * 5 AS w3 FROM nation) s ON p.k = s.k `+
			`ORDER BY p.k`)
	// A BARE window arm beside a computed one, both publishing `w`: the
	// qualified reference has to reach the arm it names and not the first arm
	// that answers to the bare spelling.
	add("JoinArmWindowArmBesideComputedArmProbe",
		`SELECT p.k AS k, p.w AS pw FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p `+
			`JOIN nation y ON p.k = y.n_nationkey `+
			`JOIN (SELECT n_nationkey AS k, n_regionkey * 3 AS w FROM nation) q ON p.k = q.k `+
			`ORDER BY p.k`)
	add("JoinArmWindowArmBesideComputedArmBuild",
		`SELECT p.k AS k, q.w AS qw FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p `+
			`JOIN nation y ON p.k = y.n_nationkey `+
			`JOIN (SELECT n_nationkey AS k, n_regionkey * 3 AS w FROM nation) q ON p.k = q.k `+
			`ORDER BY p.k`)
	// PROJECTING the computed column through the chain, whose COUNT twin was
	// already right — the asymmetry #766 is about.
	add("JoinArmComputedChainNestedAggProjecting",
		`WITH c AS (SELECT k, sv * 2 AS dv FROM `+
			`(SELECT n_regionkey AS k, SUM(n_nationkey) AS sv FROM nation GROUP BY n_regionkey) z) `+
			`SELECT c.dv AS d FROM c JOIN region r ON c.k = r.r_regionkey `+
			`JOIN region r2 ON c.k = r2.r_regionkey WHERE c.dv > 4 ORDER BY c.k`)
	add("JoinArmComputedChainNestedAggCount",
		`WITH c AS (SELECT k, sv * 2 AS dv FROM `+
			`(SELECT n_regionkey AS k, SUM(n_nationkey) AS sv FROM nation GROUP BY n_regionkey) z) `+
			`SELECT COUNT(*) AS n FROM c JOIN region r ON c.k = r.r_regionkey `+
			`JOIN region r2 ON c.k = r2.r_regionkey WHERE c.dv > 4`)
	add("JoinArmTwoArmsOneAliasProjecting",
		`SELECT x.w AS xw FROM (SELECT n_nationkey AS k, n_regionkey AS w FROM nation) x `+
			`JOIN (SELECT n_nationkey AS k, n_nationkey * 3 AS w FROM nation) z ON x.k = z.k `+
			`JOIN nation u ON x.k = u.n_nationkey WHERE x.w > 2 ORDER BY x.k`)
	add("JoinArmComputedChainOverWindowProjecting",
		`WITH c AS (SELECT n_nationkey AS k, SUM(n_regionkey) OVER () + 0 AS dv FROM nation) `+
			`SELECT c.dv AS d FROM c JOIN nation t ON c.k = t.n_nationkey `+
			`JOIN nation u ON c.k = u.n_nationkey WHERE c.dv > 4 ORDER BY c.k`)

	// A sibling NESTED inside a sibling (#751), gated rather than pinned since
	// 2026-09-01: `joinArmAlias` reads a DERIVED table's own alias off the
	// arm's subtree root now, so the join no longer qualifies that arm's
	// duplicate column by a name the query never wrote.
	add("JoinArmSiblingNestedInSibling",
		`SELECT p.w AS pw, q.w AS qw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p JOIN `+
			`(SELECT x.k, x.w FROM (SELECT n_nationkey AS k, MIN(n_regionkey) OVER () AS w FROM nation) x) q `+
			`ON p.k = q.k ORDER BY p.k`)
	// The composition the same arc closed: a window in the SELECT LIST above a
	// join one of whose ARMS is itself a window (#772). The two mint one
	// `__win_0` — the builder's counter is per SELECT block — and the outer
	// one's renumbering used to be applied DOWNWARD into the arm.
	add("JoinArmWindowAboveAWindowArm",
		`SELECT p.k AS k, p.w AS pw, q.w AS qw, SUM(q.w) OVER () AS s FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 100 AS w FROM nation) q `+
			`ON p.k = q.k ORDER BY p.k`)
	add("JoinArmWindowAboveAWindowArmOverItsOwnArm",
		`SELECT p.k AS k, p.w AS pw, q.w AS qw, SUM(p.w) OVER () AS s FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 100 AS w FROM nation) q `+
			`ON p.k = q.k ORDER BY p.k`)
	// An arm that is ITSELF A JOIN, in the DERIVED spelling (#773).
	add("JoinArmIsItselfAJoin",
		`SELECT t.k AS k, t.w AS tw, m.w AS mw FROM `+
			`(SELECT n_nationkey AS k, n_regionkey AS w FROM nation) t JOIN `+
			`(SELECT g.k AS k, g.w AS w FROM `+
			`(SELECT n_nationkey AS k, n_regionkey * 100 AS w FROM nation) g `+
			`JOIN nation h ON g.k = h.n_nationkey) m ON t.k = m.k ORDER BY t.k`)
	// A WINDOW above a GROUP BY (#737): the computed key is a NAME there, and
	// so is an aggregate inside the window's own spec.
	add("JoinArmWindowAboveAGroupBy",
		`SELECT n_regionkey + 1 AS gk, ROW_NUMBER() OVER (ORDER BY n_regionkey + 1) AS rn `+
			`FROM nation GROUP BY n_regionkey + 1 ORDER BY gk`)
	add("JoinArmWindowOverAnAggregateOutput",
		`SELECT n_regionkey + 1 AS gk, COUNT(*) AS n, SUM(COUNT(*)) OVER () AS s `+
			`FROM nation GROUP BY n_regionkey + 1 ORDER BY gk`)
	return out
}
