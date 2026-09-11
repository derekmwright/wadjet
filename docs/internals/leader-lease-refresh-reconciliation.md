# Leader lease refresh reconciliation

Source: internal/coordinator/leader.go — reclaimLease, moved 2026-09-11 (#1026)

reclaimLease decides what a FAILED refresh actually MEANS by asking the
store who holds the lease now. A CAS rejection says only that the key is
not at the revision this instance recorded, and there are three states
behind that, which are not the same thing:

  - The key is GONE. The KV bucket's TTL is 5s and the refresh ticks every
    2s, so a tick the runtime delivers late — a GC pause, a busy host, a
    slow round trip — lets the entry age out. The CAS then reports
    `wrong last sequence: 0`: the subject holds no message at all. Nobody
    else holds the lease either, because there is nothing to hold. Treating
    that as "lost" resigned leadership over an empty key and then waited a
    full standby poll before even LOOKING at it (#559). The right move is to
    race the standbys for it immediately: Create is atomic, so exactly one
    instance wins and split-brain is impossible.
  - Someone else holds it. Then leadership really is lost, and the standby
    path is correct.
  - WE hold it, at a revision this instance did not record — the update
    landed but its acknowledgement did not come back. Adopting the store's
    revision is the whole repair; resigning would have been a flap over a
    lease that was never in doubt.

Any other error leaves the question unanswered, and an unanswered question
about who is leader is answered by standing down.
