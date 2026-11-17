// Self-test for dispatch-stretch.js: runs the script against a scripted mock of agent().
//   node tools/crew/dispatch-stretch.selftest.js
// Run it after ANY edit to the script or its prompts; a prompt edit can break the template
// literals, and the round logic has been wrong in ways only a scenario shows.
const fs = require('fs'), path = require('path')
const src = fs.readFileSync(path.join(__dirname, 'dispatch-stretch.js'), 'utf8').replace('export const meta', 'const meta')
async function run(args, plan) {
  const calls = []
  const agent = async (prompt, o) => { calls.push(o.label + '=' + o.model); return plan(o, prompt, calls) }
  const r = await new Function('args', 'agent', 'log', 'return (async()=>{' + src + '})()')(args, agent, () => {})
  return { r, calls }
}
const B = { status: 'done', commit: 'c1', branch: 'x', note: '/n/x' }, V = { verified: true, failures: [] }, C = { verdict: 'CLEAN', required: [] }
const base = (items, extra) => Object.assign({ base: 'b', notesDir: '/n', land: false, items }, extra)
let failed = 0
const check = (name, ok, got) => { console.log((ok ? 'ok   ' : 'FAIL ') + name + (ok ? '' : '  ' + JSON.stringify(got))); if (!ok) failed++ }
;(async () => {
  let t = await run(base([{ id: '1', title: 't', brief: 'b', builder: 'haiku' }, { id: '2', title: 't', brief: 'b', builder: 'sonnet' }, { id: '3', title: 't', brief: 'b' }]),
    o => o.phase === 'Build' ? B : o.phase === 'Verify' ? V : C)
  check('builders route by tier; review is opus; verify is haiku', t.calls.join(' ') === 'build:1 r1=haiku verify:1 r1=haiku review:1 r1=opus build:2 r1=sonnet verify:2 r1=haiku review:2 r1=opus build:3 r1=opus verify:3 r1=haiku review:3 r1=opus' && t.r.landed.length === 3, t.calls)

  let n = 0
  t = await run(base([{ id: '1', title: 't', brief: 'b' }]), o => o.phase === 'Build' ? B : o.phase === 'Verify' ? (++n < 3 ? { verified: false, failures: ['note missing'] } : V) : C)
  check('two failed hand-back checks then clean lands, and are not blocked reviews', t.r.landed.length === 1 && !t.r.escalation && t.r.landed[0].rounds === 3, t.r)

  t = await run(base([{ id: '1', title: 't', brief: 'b' }]), o => o.phase === 'Build' ? B : o.phase === 'Verify' ? { verified: false, failures: ['x'] } : C)
  check('three failed hand-back checks escalate on trigger 6', t.r.escalation && t.r.escalation.trigger === 6, t.r.escalation)

  t = await run(base([{ id: '1', title: 't', brief: 'b' }, { id: '2', title: 't', brief: 'b' }]), o => o.phase === 'Build' ? B : o.phase === 'Verify' ? V : { verdict: 'BLOCK', required: ['a'] })
  check('a third blocked review stops on trigger 2 and the next item is not started', t.r.escalation.trigger === 2 && t.r.escalation.round === 3 && t.r.notStarted.join() === '2', t.r)

  let k = 0
  t = await run(base([{ id: '1', title: 't', brief: 'b' }]), o => o.phase === 'Build' ? B : o.phase === 'Verify' ? V : { verdict: 'BLOCK', required: ++k === 1 ? ['a'] : ['a', 'b'] })
  check('a review that finds more than the last escalates early', t.r.escalation.trigger === 2 && t.r.escalation.round === 2, t.r.escalation)

  t = await run(base([{ id: '1', title: 't', brief: 'b', resume: { commit: 'cc', branch: 'br', note: '/n/n', base: 'OLD', round: 3, ruling: 'RULING' } }]),
    (o, p) => o.phase === 'Verify' ? (p.includes('descends from OLD') && p.includes('RULING') ? V : { verified: false, failures: ['prompt lacks base or ruling'] }) : (p.includes('RULING') ? C : { verdict: 'BLOCK', required: ['no ruling'] }))
  check('resume at review: no build, own base, ruling reaches verifier and reviewer, round kept', t.calls.join(' ') === 'verify:1 r3=haiku review:1 r3=opus' && t.r.landed[0].commit === 'cc' && t.r.landed[0].rounds === 3, t)

  t = await run(base([{ id: '1', title: 't', brief: 'b', builder: 'sonnet', resume: { commit: 'cc', branch: 'br', note: '/n/n', round: 2, required: ['fix A'] } }]),
    (o, p) => o.phase === 'Build' ? (p.includes('fix A') && p.includes('crew/1-r3 cc') && p.includes('/n/1_r3.md') ? B : { status: 'blocked', trigger: 6, reason: 'bad fix prompt' }) : o.phase === 'Verify' ? V : C)
  check('resume with required: next round is a fix round numbered after the resumed one', t.calls[0] === 'build:1 r3=sonnet' && t.r.landed[0].rounds === 3, t)

  t = await run(base([{ id: 's', title: 't', brief: 'b', reviewOnly: true, resume: { commit: 'cc', branch: 'br', note: '/n/n' } }]), o => o.phase === 'Verify' ? V : { verdict: 'BLOCK', required: ['x'], trigger: 3 })
  check('review-only item returns the verdict and neither fixes nor escalates', t.r.reviews[0].verdict === 'BLOCK' && !t.r.escalation && t.calls.length === 2, t.r)

  t = await run(base([{ id: '1', title: 't', brief: 'b' }]), o => o.phase === 'Build' ? B : { verified: true, failures: [], trigger: 3, reason: 'WHY' })
  check('a verifier trigger carries its reason', t.r.escalation.reason === 'WHY', t.r.escalation)

  t = await run(base([{ id: '1', title: 't', brief: 'b' }]), (o, p) => p.startsWith('You were launched by the architect') ? (o.phase === 'Build' ? B : o.phase === 'Verify' ? V : C) : null)
  check('every agent prompt opens with the authorization preamble', t.r.landed.length === 1, t.r)

  t = await run(base([{ id: '1', title: 't', brief: 'b' }, { id: '2', title: 't', brief: 'b' }, { id: '3', title: 't', brief: 'b' }], { maxItems: 2 }), o => o.phase === 'Build' ? B : o.phase === 'Verify' ? V : C)
  check('a stretch is bounded and reports what it did not start', t.r.landed.length === 2 && t.r.notStarted.join() === '3', t.r)

  console.log(failed ? failed + ' FAILED' : 'all passed')
  process.exit(failed ? 1 : 0)
})()
