#!/usr/bin/env python3
"""Check catalog identifiers, explicit references and added page entries."""
import csv
from pathlib import Path
import re
import subprocess

catalog = Path('docs/adr/0012-divergences')
rows = {}
for path in sorted(catalog.glob('*.md')):
    if path.name == 'README.md':
        continue
    pairs = [(int(m[1]), line) for line in path.read_text().splitlines()
             if (m := re.match(r'\| \*\*r(\d+)\*\*', line))]
    ids = [n for n, _ in pairs]
    assert ids == sorted(set(ids)), str(path) + ': ids duplicate or unordered'
    rows[path.stem] = dict(pairs)
pattern = re.compile(r'(' + '|'.join(rows) + r')(?:\.md)?(?:#|\s+)r(\d+)\b')
refs, missing = [], []
for path in sorted(list(Path('docs').rglob('*.md')) + list(Path('.').rglob('*.go'))):
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if path.suffix == '.go' and not line.lstrip().startswith('//'):
            continue
        for match in pattern.finditer(line):
            family, n = match[1], int(match[2])
            exists = n in rows[family]
            historical = str(path) == 'docs/adr/0012-amendments.md' and (family, n) in {
                ('aggregates-windows', 12), ('lateral-subqueries', 6)}
            status = 'exists' if exists else ('explicit retirement history' if historical else 'missing')
            refs.append([str(path), number, match[0], status, line.strip(), rows[family].get(n, '')])
            if status == 'missing':
                missing.append(refs[-1])
with open('hk_author/catalog_references.tsv', 'w') as stream:
    writer = csv.writer(stream, delimiter='\t')
    writer.writerow(['file', 'line', 'reference', 'status', 'context', 'catalog row'])
    writer.writerows(refs)
assert not missing, missing
page = Path('docs/postgres-differences.md').read_text()
page_refs = set()
for family in rows:
    for match in re.finditer(re.escape(family) + r'#(r\d+(?:, r\d+|[–-]r?\d+)*)', page):
        nums = re.findall(r'r?(\d+)', match[1])
        if re.search('[–-]', match[1]):
            nums = list(range(int(nums[0]), int(nums[-1]) + 1))
        page_refs.update((family, int(n)) for n in nums)
added = []
for family, family_rows in rows.items():
    base = subprocess.check_output(['git', 'show', f'db4c0a7d:{catalog}/{family}.md'], text=True)
    old = set(map(int, re.findall(r'^\| \*\*r(\d+)\*\*', base, re.M)))
    for n, line in family_rows.items():
        if n not in old and line.split(' | ')[4] in ('kept superset', 'value divergence'):
            assert (family, n) in page_refs, (family, n)
            added.append(f'{family}#r{n}')
print(f'{sum(map(len, rows.values()))} catalog rows: unique and ordered within each family')
print(f'{len(refs)} explicit references: no missing live row; 2 explicit retirement records retained')
print(f'{len(added)} added kept-superset/value-divergence rows all appear on the differences page')
print('Added rows: ' + ', '.join(added))
