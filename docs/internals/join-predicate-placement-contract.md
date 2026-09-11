# Join predicate placement contract

Source: internal/planner/logical/join_predicates.go — joinKind, moved 2026-09-11 (#1026)

This file holds the two analyses that decide where a join's predicates may
legally live: which of a join's inputs can be NULL-padded (so a WHERE
predicate must not be pushed below it unexamined), and which ON-clause
conjuncts the physical planner's key parser is able to represent at all.

Both existed only implicitly before, and both were wrong:

	#335 — pushFilterThroughJoin pushed a WHERE predicate to whichever join
	       child owned its columns, without looking at the join type. Over a
	       LEFT JOIN that pushed the predicate below the NULL-padding, so
	       `... LEFT JOIN region r ON ... WHERE r.r_regionkey = 2` filtered
	       region to one row and then padded every unmatched nation back in:
	       25 rows out of a 5-row answer, and 25 again for the IS NULL
	       anti-join idiom whose answer is 0.

	#336 — an ON conjunct comparing two COLUMNS across the join
	       (`a.s_suppkey < b.s_suppkey`) stayed in JoinCond, where
	       parseJoinKeys keeps only the parts containing "=" and drops the
	       rest without a word. The same conjunct in WHERE is honoured, so
	       ON residuals and WHERE residuals were two paths with two answers.

	#351 — the same analysis, one level finer. An ON conjunct can BE an
	       equality and still have no key representation, because the join
	       executor matches on column NAMES: `n.n_regionkey = r.r_regionkey
	       + 3` reached it with "r.r_regionkey + 3" as a key column, which
	       resolves to nothing and matches nothing — 0 rows for a 10-row
	       query. The residual test is therefore on the OPERANDS, not on
	       the operator alone.
