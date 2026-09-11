# Scalar aggregate semijoin reduction

Source: internal/planner/logical/scalar_agg_semijoin.go — reduceDecorrelatedScalarAggs, moved 2026-09-11 (#1026)

reduceDecorrelatedScalarAggs semijoin-reduces the aggregate input of each
decorrelated correlated-scalar-subquery join (marked ScalarDecorrelated by
tryDecorrelateScalarSubquery).

Decorrelation turns

	WHERE l_quantity < (SELECT 0.2*avg(l_quantity) FROM lineitem
	                    WHERE l_partkey = p_partkey)

into a LEFT JOIN against "SELECT l_partkey, avg(l_quantity) FROM lineitem
GROUP BY l_partkey" — an aggregate over the ENTIRE inner table, even when
the outer side keeps a fraction of the keys. At SF100 Q17 this is a full
600M-row lineitem scan shuffled into a 20M-group aggregate of which ~0.1%
of groups survive the part filter (~43s of Q17's 92.5s, and its 12 scan
tasks saturate the dispatch slots, queueing the main probe join behind
them). Q20 and Q02 have the same shape.

The reduction: find the outer branch that produces the correlation
columns (the filtered part scan in Q17/Q02, the partsupp⊳part semijoin in
Q20), clone it, and semijoin the aggregate's input against its distinct
keys. Because the pass runs AFTER predicate pushdown, the branch carries
its final filters.

Validity: the reduction only removes aggregate groups whose keys cannot
appear in any outer row that survives the branch's own
filters/semijoins. Those outer rows die regardless of the scalar's value
(the branch is part of the outer plan; its filters apply conjunctively),
so removed groups are never observed. This holds for every aggregate
function including count: absent groups produce NULL through the LEFT
JOIN either way.
