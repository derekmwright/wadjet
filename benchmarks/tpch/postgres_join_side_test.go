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
	pin := func(name, sql, bug, issue string) {
		out = append(out, pgCase{name: name, sql: sql, knownBug: bug, issue: issue})
	}

	// --- A WHERE above a JOIN naming a CTE's RENAMED column (#653, #700, #726).
	//
	// `rk > 2` keeps regions 3 and 4, which is 2 of the 5 — a literal inside
	// the range, so an engine that dropped the predicate and one that made it
	// UNKNOWN on every row give visibly different answers.
	const cte = `WITH c AS (SELECT n_nationkey AS k, n_regionkey AS rk, n_name AS nm FROM nation) `
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

	// The CTE BODY is a window, then an aggregate: the filter names an output
	// that lives in a hidden slot, and one that is an aggregate's output.
	add("JoinArmCTEBodyWindow",
		`WITH c AS (SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) `+
			`SELECT COUNT(*) AS n FROM c JOIN region r ON c.k = r.r_regionkey WHERE c.w > 2`)
	add("JoinArmCTEBodyAggregate",
		`WITH c AS (SELECT n_regionkey AS gk, COUNT(*) AS cnt FROM nation GROUP BY n_regionkey) `+
			`SELECT COUNT(*) AS n FROM c JOIN region r ON c.gk = r.r_regionkey WHERE c.cnt > 4`)

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
	// The CTE spelling of the two blocks just above. The DERIVED spelling
	// agrees on this path and this one does not, which is the tell that what
	// is left is the CTE reference's materialization and not the slot.
	pin("JoinArmSiblingWindowsInCTEs",
		`WITH p AS (SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation), `+
			`q AS (SELECT n_nationkey AS k, MIN(n_regionkey) OVER () AS w FROM nation) `+
			`SELECT p.w AS pw, q.w AS qw, p.k AS k FROM p JOIN q ON p.k = q.k ORDER BY p.k`,
		pgBugWadjet+" two sibling CTEs whose bodies each hold a window: the single-process path "+
			"publishes p's window under BOTH output columns, while the identical DERIVED-table "+
			"spelling agrees and both stage-DAG arms agree",
		"#753")
	// The control: two sibling blocks with NO window resolve correctly today
	// and must keep doing so, which is what says the repair moved the SLOT
	// and not the name resolution.
	add("JoinArmSiblingsNoWindow",
		`SELECT p.w AS pw, q.w AS qw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, n_regionkey + 1 AS w FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 3 AS w FROM nation) q `+
			`ON p.k = q.k ORDER BY p.k`)

	// --- The residuals, pinned so the day they agree this file FAILS.
	pin("JoinArmSiblingWindowsThree",
		`SELECT p.w AS pw, q.w AS qw, s.w AS sw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, MIN(n_regionkey) OVER () AS w FROM nation) q ON p.k = q.k JOIN `+
			`(SELECT n_nationkey AS k, MAX(n_regionkey) OVER () AS w FROM nation) s ON p.k = s.k `+
			`ORDER BY p.k`,
		pgBugWadjet+" a derived table's computed column read above a JOIN OF JOINS takes the "+
			"last arm's value on the single-process path; both stage-DAG arms answer it correctly",
		"#753")
	pin("JoinArmDerivedComputedUnderJoinOfJoins",
		`SELECT p.w1 AS pw, q.w2 AS qw, s.w3 AS sw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, n_regionkey + 1 AS w1 FROM nation) p JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 3 AS w2 FROM nation) q ON p.k = q.k JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 5 AS w3 FROM nation) s ON p.k = s.k `+
			`ORDER BY p.k`,
		pgBugWadjet+" the same shape with no window anywhere and a DISTINCT alias per arm, so the "+
			"collapse is not a name collision: the single-process path answers NULL for the "+
			"first two columns",
		"#753")
	pin("JoinArmSiblingNestedInSibling",
		`SELECT p.w AS pw, q.w AS qw, p.k AS k FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () AS w FROM nation) p JOIN `+
			`(SELECT x.k, x.w FROM (SELECT n_nationkey AS k, MIN(n_regionkey) OVER () AS w FROM nation) x) q `+
			`ON p.k = q.k ORDER BY p.k`,
		pgBugWadjet+" a sibling nested inside a sibling: the single-process path answers the OUTER "+
			"sibling's window for the inner one; both stage-DAG arms are right",
		"#751")
	pin("JoinArmQualifiedRefAcrossArms",
		`SELECT x.k AS k, x.w AS xw FROM `+
			`(SELECT n_nationkey AS k, SUM(n_regionkey) OVER () + 0 AS w FROM nation) x JOIN `+
			`nation y ON x.k = y.n_nationkey JOIN `+
			`(SELECT n_nationkey AS k, n_regionkey * 3 AS w FROM nation) z ON x.k = z.k ORDER BY x.k`,
		pgBugWadjet+" a QUALIFIED reference resolves through the other arm's identically-named "+
			"column: `x.w` answers z's `n_regionkey * 3` on every execution path",
		"#742")

	return out
}
