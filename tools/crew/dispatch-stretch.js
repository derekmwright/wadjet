// One bounded stretch of the queue, run as a Workflow script (docs/testing/CREW.md).
//
// The loop lives HERE, in code, not in a model's head: a mid-tier dispatcher is
// reliable at one judgment at a time and unreliable at holding a long horizon.
// Every agent below does one bounded thing and returns a typed result; the
// script decides what happens next, counts the rounds, and stops at the first
// escalation trigger. Run by the architect:
//
//   Workflow({ scriptPath: "tools/crew/dispatch-stretch.js", args: { ... } })
//
// args = {
//   base:     "<sha main is at when the stretch starts>",
//   notesDir: "<absolute dir outside the repo for result notes>",
//   items: [ { id: "1558", title: "...", risk: "docs" | "scoped" | "risky",
//              brief: "<what to do, the repro, the files in scope, what is out>" } ],
//   // risk is the architect's call per item: "risky" (a shared primitive, a pipeline breaker,
//   // a planner seam, anything the brief cannot fully bound) builds on the strong coding
//   // model; "scoped" (bounded files, a stated repro, a stated fix shape) builds on the
//   // mid-tier model; "docs" (documentation only, no code) builds on the fast model.
//   // Omitted = "risky". The reviewer is always the strong model. The dispatcher's own
//   // steps (verify a hand-back, land a commit) are fast decisions and run on the fast model.
//   maxItems: 3,  // optional; a stretch is bounded
//   land: false   // optional; build, verify and review only (a trial or a tier comparison)
// }
//
// Returns { landed, escalation, dropped, ledger }. The architect appends
// `ledger` to the metrics ledger and rules on `escalation` before the next stretch.

export const meta = {
  name: 'dispatch-stretch',
  description: 'Run one bounded stretch of the queue: build, verify, review, land, one item at a time, stopping at the first escalation',
  whenToUse: 'The architect has a sorted, briefed list of ready items (standalone issues or the next slices of the flagship epic) and wants them built, reviewed and landed on local main without relaying each hand-back.',
  phases: [
    { title: 'Build', detail: 'one worker per item, in its own worktree: strong model for risky items, mid-tier for scoped, fast for docs' },
    { title: 'Verify', detail: 'dispatcher re-checks the hand-back with its own commands', model: 'haiku' },
    { title: 'Review', detail: 'reviewer checks the note against the code', model: 'opus' },
    { title: 'Land', detail: 'dispatcher lands on local main: no push, no tag', model: 'haiku' },
  ],
}

const TRIGGERS = `Escalation triggers (stop and report; do not work around):
1. a new design record (ADR) or an amendment to one is needed;
2. (counted by the script, not by you);
3. the change alters something a user would see in an answer, an error or a wire type beyond what the brief states, or drops user data;
4. a public statement about quality, exactness or security would have to be written or reworded (README, docs claims, release notes);
5. a gate or test failed for a reason your own change does not explain, a conflict is not mechanical, or the branch you started from moved;
6. the brief and the runbooks do not cover the situation, or a rule would have to bend.`

const BUILD = {
  type: 'object',
  properties: {
    status: { type: 'string', enum: ['done', 'blocked'] },
    trigger: { type: 'integer', description: 'escalation trigger number when blocked' },
    reason: { type: 'string' },
    branch: { type: 'string' },
    commit: { type: 'string', description: 'full sha of the tip commit' },
    files: { type: 'array', items: { type: 'string' } },
    note: { type: 'string', description: 'absolute path of the result note' },
    claims: { type: 'array', items: { type: 'string' }, description: 'each claim names the test, record or measurement that backs it' },
    negativeControl: { type: 'string', description: 'the test that failed with the fix reverted, and the failure it printed' },
  },
  required: ['status'],
}
const CHECK = {
  type: 'object',
  properties: {
    verified: { type: 'boolean' },
    failures: { type: 'array', items: { type: 'string' } },
    trigger: { type: 'integer', description: 'set only if an escalation trigger fired' },
  },
  required: ['verified', 'failures'],
}
const REVIEW = {
  type: 'object',
  properties: {
    verdict: { type: 'string', enum: ['CLEAN', 'BLOCK'] },
    required: { type: 'array', items: { type: 'string' }, description: 'changes required before landing; only regressions, wrong values and unbacked claims block' },
    brokeEarlierFix: { type: 'boolean' },
    trigger: { type: 'integer' },
    reason: { type: 'string' },
  },
  required: ['verdict', 'required'],
}
const LAND = {
  type: 'object',
  properties: {
    landed: { type: 'boolean' },
    main: { type: 'string', description: 'sha of main after landing' },
    gate: { type: 'string', description: 'the gate command run and its one-line result' },
    trigger: { type: 'integer' },
    reason: { type: 'string' },
  },
  required: ['landed'],
}

function esc(trigger, reason) {
  const e = new Error(reason)
  e.esc = { trigger, reason }
  return e
}

const cap = (args && args.maxItems) || 3
const items = ((args && args.items) || []).slice(0, cap)
const dropped = ((args && args.items) || []).slice(cap).map(i => i.id)
if (dropped.length) log(`stretch is bounded at ${cap} items; not started: ${dropped.join(', ')}`)

const landed = []
const ledger = []
let escalation = null

for (const item of items) {
  let round = 0
  const workerModel = { docs: 'haiku', scoped: 'sonnet' }[item.risk] || 'opus'
  let build = null
  let required = []
  let prevCount = null
  try {
    while (true) {
      round++
      const fixing = round > 1
      build = await agent(
        `You are a WORKER on issue/line ${item.id}: ${item.title}

${item.brief}

${fixing
  ? `This is fix round ${round}. Start from commit ${build.commit} (git switch -c crew/${item.id}-r${round} ${build.commit}). Make exactly these required changes, nothing else:\n- ${required.join('\n- ')}`
  : `Work on a new branch crew/${item.id}-r1 from ${args.base}.`}

Rules:
- Root cause over workaround: keep the reproduction, find the cause, record it.
- Every new test has a negative control: revert the fix, watch the test fail for the right reason, restore. Report it.
- A failed test is never rerun unchanged and never called a flake. If an existing test fails BECAUSE it pins the behaviour or plan shape your fix intentionally changes, update that pin and list it in the note (test name, old assertion, new assertion, why); the reviewer judges it. Any other failure, one your diff does not explain or in a test of something you did not mean to change, is trigger 5: stop and report.
- Commit only the files you name (never "git add -A"); never push; never touch another branch.
- Before handing back, write a result note to ${args.notesDir}/${item.id}_r${round}.md: what changed, the cause, each claim with the test or measurement behind it, what you did NOT do.

${TRIGGERS}
If a trigger fires, return status "blocked" with its number and the reason.`,
        { label: `build:${item.id} r${round}`, phase: 'Build', model: workerModel, isolation: 'worktree', schema: BUILD })
      ledger.push({ item: item.id, role: 'worker', model: workerModel, risk: item.risk || 'risky', round, outcome: build ? build.status : 'died' })
      if (!build) throw esc(5, `worker for ${item.id} died in round ${round}`)
      if (build.status === 'blocked') throw esc(build.trigger || 6, build.reason || 'worker blocked without a reason')

      const check = await agent(
        `You are the DISPATCHER verifying a worker's hand-back for ${item.id}. A hand-back is model output, not truth: check each fact below with your own commands and report only what the commands show. Do not fix anything.

Hand-back: ${JSON.stringify(build)}

Check: (1) commit ${build.commit} exists and is the tip of ${build.branch}; (2) it descends from ${args.base}; (3) it touches only the files the hand-back names; (4) the note ${build.note} exists and states a cause and its claims; (5) the note records a negative control; (6) no "Co-Authored-By" or AI trailer, and a Signed-off-by is present.

${TRIGGERS}`,
        { label: `verify:${item.id} r${round}`, phase: 'Verify', model: 'haiku', schema: CHECK })
      ledger.push({ item: item.id, role: 'dispatcher-verify', model: 'haiku', round, outcome: check ? (check.verified ? 'verified' : 'failed') : 'died' })
      if (!check) throw esc(5, `verification for ${item.id} died in round ${round}`)
      if (check.trigger) throw esc(check.trigger, check.failures.join('; '))

      let brokeEarlier = false
      if (check.verified) {
        const review = await agent(
          `You are a read-only REVIEWER of commit ${build.commit} (branch ${build.branch}) for ${item.id}: ${item.title}

Brief the worker was given:
${item.brief}

Verify the builder's note (${build.note}) AGAINST THE CODE, not the code against the note. For each claim, find the test or measurement and confirm it shows what is claimed; revert the fix in a scratch checkout and confirm the new test fails for the stated reason. Only regressions, wrong values and unbacked claims block; style does not. ${fixing ? `Round ${round - 1} required: ${required.join(' | ')} — say whether this round broke an earlier fix.` : ''}

${TRIGGERS}
Edit nothing in the branch.`,
          { label: `review:${item.id} r${round}`, phase: 'Review', model: 'opus', isolation: 'worktree', schema: REVIEW })
        ledger.push({ item: item.id, role: 'reviewer', model: 'opus', round, outcome: review ? review.verdict : 'died', required: review ? review.required.length : null })
        if (!review) throw esc(5, `review for ${item.id} died in round ${round}`)
        if (review.trigger) throw esc(review.trigger, review.reason || review.required.join('; '))
        if (review.verdict === 'CLEAN') break
        required = review.required
        brokeEarlier = !!review.brokeEarlierFix
      } else {
        required = check.failures
      }

      // The round-loop rule: rounds one and two get a fix and a retry.
      if (round >= 3) throw esc(2, `third failed round on ${item.id}: ${required.join('; ')}`)
      if (brokeEarlier) throw esc(2, `round ${round} on ${item.id} broke an earlier round's fix`)
      if (prevCount !== null && required.length > prevCount) throw esc(2, `round ${round} on ${item.id} found more problems (${required.length}) than the last (${prevCount})`)
      prevCount = required.length
      log(`${item.id}: round ${round} failed with ${required.length} required change(s); retrying`)
    }

    if (args.land === false) {
      landed.push({ id: item.id, commit: build.commit, main: null, rounds: round, note: build.note, gate: 'not landed (land: false)' })
      log(`${item.id}: reviewed CLEAN after ${round} round(s); not landed`)
      continue
    }
    const land = await agent(
      `You are the DISPATCHER landing ${item.id} on LOCAL main. Reviewed commit: ${build.commit}.

1. Confirm the main checkout is clean and main is an ancestor of ${build.commit}; if main moved in a way that needs more than a fast-forward, stop (trigger 5).
2. Run the gate ONCE on the reviewed commit: the packages \`task affected BASE=${args.base}\` names, through \`task test-quiet\`. A failure is never rerun: stop (trigger 5) and quote the failing test names.
3. git merge --ff-only ${build.commit} into main.
Do not push. Do not tag. Do not close issues or reword any public text.

${TRIGGERS}`,
      { label: `land:${item.id}`, phase: 'Land', model: 'haiku', schema: LAND })
    ledger.push({ item: item.id, role: 'dispatcher-land', model: 'haiku', round, outcome: land && land.landed ? 'landed' : 'not-landed' })
    if (!land) throw esc(5, `landing agent for ${item.id} died`)
    if (!land.landed) throw esc(land.trigger || 5, land.reason || 'not landed, no reason given')
    landed.push({ id: item.id, commit: build.commit, main: land.main, rounds: round, note: build.note, gate: land.gate })
    log(`${item.id}: landed after ${round} round(s)`)
  } catch (e) {
    if (!e.esc) throw e
    escalation = { item: item.id, round, trigger: e.esc.trigger, reason: e.esc.reason, lastCommit: build && build.commit, note: build && build.note }
    ledger.push({ item: item.id, role: 'escalation', trigger: e.esc.trigger, round })
    log(`ESCALATION on ${item.id} (trigger ${e.esc.trigger}): ${e.esc.reason}`)
    break // landing is one at a time and the architect rules before anything else starts
  }
}

const notStarted = items.slice(landed.length + (escalation ? 1 : 0)).map(i => i.id).concat(dropped)
return { landed, escalation, notStarted, ledger }
