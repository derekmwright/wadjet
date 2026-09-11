# Two level conversion timing

Source: internal/engine/exec/agg_index.go — convertsToTwoLevel, moved 2026-09-11 (#1026)

convertsToTwoLevel decides, at the END of a batch, whether a flat index
should become bucketed. Two conditions:

  - size: live >= twoLevelConvertAt — the measured structural crossover,
    below which a flat rehash is still a cache-resident scatter and
    bucketing is overhead (see two_level_hash.go for the curve).
  - imminent rehash: live + incoming crosses the flat table's 70% load
    factor. That is the only moment the conversion is free: it rehashes
    the entries grow() was about to rehash, into the capacity grow() was
    about to allocate, and the flat doubling then never happens. Anywhere
    else in the fill the conversion REPLACES NOTHING and the table still
    owes its doubling — the ≈10:1 overhead-to-benefit ratio the SF100
    profile found, and the near-unique-key regression it produced (Q18,
    +87%).

incoming is what the caller is about to insert: the consume path passes
the new-group count of the batch just finished, as the estimate of the
next batch's; the merge path passes the incoming aggregate's group count,
which is exact.

The growth-rate test this replaces (newGroups*4 >= rows, "still filling")
could not veto the losing bet it was written for: a near-unique key mints
a group on every row, so it passed unconditionally, on every batch, for
exactly the shape that pays the most. The load-factor test subsumes its
real intent — a saturated table adds no groups, so it can never cross,
so it can never convert.

Both are per-BATCH tests on numbers the consume loop already has; nothing
here runs per row.
