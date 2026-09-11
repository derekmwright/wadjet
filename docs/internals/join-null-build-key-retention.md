# Join null build key retention

Source: internal/engine/exec/join.go — HashJoin.nullBuildKey, moved 2026-09-11 (#1026)

nullBuildKey records a build row whose join key is NULL — a row the hash
index must NOT hold, because NULL equals nothing and no probe may match it.

Skipping the index insert is the whole of "must not match". Skipping the
row is a different claim, and two consumers need it not to be made:

  - A RIGHT / FULL OUTER / RIGHT ANTI join owes every unmatched build row a
    NULL-padded output row, and FlushUnmatched / FlushAntiMatched enumerate
    the ARENA. The integer key paths used to `continue` past the arena
    append as well, so those rows were invisible to the flush and vanished
    — while the serialized-key path appended them and answered correctly,
    which is why the same query was right with a TEXT key and wrong with a
    BIGINT one (#496). storeRows=true appends the row with arenaNext = -1:
    a chain of one that no hash bucket points at. arenaMatched is sized
    from len(arena) after every append, so the extra entries are safe.

  - A null-aware anti join needs to know the build contained a NULL AT ALL,
    because that alone makes `NOT IN`'s answer UNKNOWN for every probe row
    it did not otherwise match (#507). That is recorded on every path,
    including the key-only builds that store no rows.

buildRows counts it: it is a real build row. (nullBuildKeyOnly is the
key-only variant, for the builds that store no rows and already count every
arriving row in bulk.)

Caller must hold h.mu on the paths that take it.
