# In subquery post build budget charge

Source: internal/engine/expr/expr_subquery.go — func (e *InSubquery) chargeMemory() {, moved 2026-09-11 (#1026)

chargeMemory reserves the membership set's estimated heap footprint
against Budget (ADR-0006, #528). A nil Budget (every compile path except
CompileWithBudget) is a no-op, exactly the pre-#528 behavior — this is
what makes the fix opt-in rather than a change to every existing caller.

Called once, from inside resolveSlow's already-held resolveMu, after the
set this query holds has been decided, so the estimate matches exactly
what stays reachable for the life of this InSubquery.

AFTER, which bounds what this can do. The map is already built and already
resident by the time a byte is charged, so a subquery large enough to
exhaust the machine exhausts it before Reserve is ever called: this does
not PREVENT that OOM, and #528's issue text describing it as doing so is
wrong. What it does do is make the set VISIBLE to the task's budget — it
counts against every later allocation the task makes, and a set that is
over budget on its own turns into a query error instead of a permanently
unaccounted resident map that every other operator then has to fit
alongside. That is worth having and is what ADR-0006 asks of a structure
that cannot spill; it is not the same claim.

Charging as the set is BUILT — a Reserve per N rows inside resolveSlow's
accumulation loop, refusing partway — is what would actually bound peak
resident bytes. It needs resolveSlow to have somewhere to put a partial
failure and a caller that can act on one, so it is a change to this
type's contract rather than to this function. Not attempted here.
