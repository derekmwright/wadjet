// Emits internal/engine/expr/testdata/node_semver_gte0.tsv from node-semver
// 7.7.3 itself. One line per range: the range text, a TAB, then one T/F per
// version in the order of the `versions` list below.
const semver = require('/home/linuxbrew/.linuxbrew/lib/node_modules/npm/node_modules/semver');
const ranges = [];
for (const lo of ['0', '0.x', '0.0', '0.0.x', '0.0.0', '*', 'x', '1', '1.0.0', '0.0.1']) {
  for (const hi of ['0.0.0-alpha', '0.0.0-0', '0.0.0', '0.0.1-alpha', '1.0.0-alpha', '0.1.0-beta', '2.0.0-x', '0.0.0-alpha.1']) {
    ranges.push(lo + ' - ' + hi);
  }
}
ranges.push('>=0.0.0 <=0.0.0-alpha', '>=0.0.0 <0.0.0-alpha', '>=0.0.0', '>=0.0.0-0',
  '>=0.0.0 <=1.0.0-alpha', '>=0.0.0 || <=0.0.0-alpha', '* <=0.0.0-alpha',
  '>=0.0.0 >=0.0.0-alpha', '<=0.0.0-alpha', '<=0.0.0-alpha >=0.0.0',
  '0.0.0 - 0.0.0', '^0.0.0-alpha', '~0.0.0-alpha', '>=0.0.0-alpha <=0.0.0-alpha',
  '0 - 0', '0.x', '^0.x', '*', '>=0.0.0 <1.0.0-0');
const versions = [];
for (const core of ['0.0.0', '0.0.1', '0.1.0', '1.0.0', '2.0.0']) {
  for (const suf of ['', '-alpha', '-0', '-alpha.1', '-beta', '+b']) versions.push(core + suf);
}
const out = [];
out.push('# node-semver ' + require('/home/linuxbrew/.linuxbrew/lib/node_modules/npm/node_modules/semver/package.json').version);
out.push('#versions\t' + versions.join(','));
for (const r of ranges) {
  const bits = versions.map(v => semver.satisfies(v, r) ? 'T' : 'F').join('');
  out.push(r + '\t' + bits + '\t' + JSON.stringify(new semver.Range(r).range));
}
console.log(out.join('\n'));
