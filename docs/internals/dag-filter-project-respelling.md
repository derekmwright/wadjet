# Dag filter project respelling

Source: internal/planner/logical/filter_project_pushdown.go — ResolveFilterThroughProjects, moved 2026-09-11 (#1026)

ResolveFilterThroughProjects re-spells a predicate that sits ABOVE one or
more Projects into the names their INPUT carries. It is the stage DAG's
half of the question the Filter-Project swap answers for the single-process
pipeline, and it exists because the two paths lower a Project differently.

pushdownPredicates SWAPS a Filter below a Project and substitutes each
reference to a renamed or computed output with its defining expression. It
DECLINES that swap for a Project tagged with a CTEName — a materialization
fence, because the single-process planner replays ONE cached result for
every reference of a CTE and a predicate pushed inside it would apply to
all of them — and it never applies at all when the Filter's child is a
JOIN, whichever kind of subquery the rename came from. Declining is right
in both cases: the predicate does not move.

What is wrong on the DAG is the SPELLING. An ordinary Project emits NO
STAGE there (docs/internals/native-dag-execution.md §Derived-table
aliases), so the predicate walkStages attaches to the producing stage is
evaluated against a schema carrying SOURCE column names. A reference to the
alias resolves to nothing, `expr.ColRef.Eval` answers nil, the predicate is
UNKNOWN on every row, and a WHERE that admits only TRUE drops all of them —
silently, for every type (#653). Every other consumer of a derived name on
the DAG has a resolver for exactly this reason; the filter had none.

So the predicate stays where it is and only its spelling changes, which is
sound whatever the Project is tagged with: substitution evaluates the exact
defining expression the Project would have produced, NULLs included. The
walk descends a join ONE ARM AT A TIME with that arm's scope names, so a
reference qualified to the other arm is left alone (projRefs), and it stops
at the first Project whose output the substitution cannot express — an
aggregate output, a volatile function — because a stage that emits such a
column emits it under the alias, which is the name the predicate already
carries.

It also stops at a Sort or a LIMIT. Those DO emit stages, carrying the
names above them, so a predicate re-spelled past one would name a column
the stage below the Project has and the stage the filter lands on does not.

AMBIGUITY is not this pass's to report. A bare name two relations in scope
both carry is rejected by physical.validate before any of this runs
("column reference %q is ambiguous", 42702, on both paths), so a decline
here can only be a shape the resolver leaves alone — never a name the query
failed to disambiguate.

Returns (nil, false) when nothing changed; the caller then ships the
predicate exactly as it did before.
aliases lists the OUTPUT names the rewrite substituted away, lowercased —
the spellings the predicate carried before this pass touched it. The DAG
needs them to decide whether the producing fragment carries the alias or
the source column (physical.resolveFilterAliasSpelling, #656).
