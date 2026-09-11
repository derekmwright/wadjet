# Scalar placeholder token substitution

Source: internal/coordinator/scalar_extract.go — replacePlaceholders, moved 2026-09-11 (#1026)

replacePlaceholders rewrites every `:scalar_N` placeholder in s with the
literal its producer yielded.

A PLACEHOLDER IS A TOKEN, NOT A PREFIX, and the substitution has to say so.
This used to be `strings.ReplaceAll` per entry over a map range: `:scalar_1`
then matches INSIDE `:scalar_10`, leaving that producer's literal with a
stray digit stuck to it, and which columns are corrupted depends on Go's map
iteration order — so the same query answered differently run to run, and
differently on two DAG arms of one run. Measured with twelve SELECT-list
subqueries: `s9` came back 49990149970 for PostgreSQL's 4999015006 — the
digits of `:scalar_1`'s value with `:scalar_10`'s trailing `0` appended — on
both arms, silently, with no counter moving.

Ten placeholders in one stage were not reachable from ordinary SQL before
#659's SELECT-list lowering: a filter carries one or two. The lowering made
them reachable with ten select items, which is why a latent defect in a
three-line helper became a wrong number.

So the scan is single-pass and token-aware: at each `:scalar_` it takes the
MAXIMAL run of digits, which is the whole placeholder name and cannot be a
prefix of another. A name with no literal is left exactly as it was — the
planner's dependency wiring refuses a placeholder no producer answers, and
silently emptying it here would hide that.
