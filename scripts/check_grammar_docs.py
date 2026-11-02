"""Check grammar indexes, point specifications, identities and local links.

This structural check does not prove the linguistic correctness of a point.
"""
from __future__ import annotations

import ast
import collections
import re
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parent.parent
GRAMMAR = ROOT / 'docs' / 'grammar'
ENTRY = re.compile(r'^- \[([ ~x])\] \[(.*?)\]\((points/[^)]+\.md)\)$', re.M)


def check() -> list[str]:
    errors: list[str] = []
    entries: dict[str, list[tuple[str, str, str]]] = {}
    paths: list[str] = []
    names: list[str] = []
    for index in ('next', 'implemented', 'later'):
        path = GRAMMAR / f'{index}.md'
        text = path.read_text(encoding='utf-8')
        rows = ENTRY.findall(text)
        entries[index] = rows
        for line in text.splitlines():
            if line.startswith('- [') and not ENTRY.fullmatch(line):
                errors.append(f'{path}: invalid index entry: {line}')
        for status, name, relative in rows:
            paths.append(relative)
            names.append(name)
            if index == 'implemented' and status != 'x':
                errors.append(f'{path}: incomplete entry {name}')
            if index in {'next', 'later'} and status == 'x':
                errors.append(f'{path}: complete entry {name}')
            point = GRAMMAR / relative
            if not re.fullmatch(r'[a-z0-9-]+\.md', point.name):
                errors.append(f'{point}: filename must use lowercase ASCII, digits and hyphens')
            if not point.is_file():
                errors.append(f'{path}: missing {relative}')
                continue
            body = point.read_text(encoding='utf-8')
            if not body.startswith(f'# {name}\n'):
                errors.append(f'{point}: title differs from index')
            if not re.search(rf'^Status: \[{re.escape(status)}\]$', body, re.M):
                errors.append(f'{point}: status differs from index')
            for filename in re.findall(r'\]\(../../../src/grammar_rules/rules/(\w+)\.py\)', body):
                rule = ROOT / 'src' / 'grammar_rules' / 'rules' / f'{filename}.py'
                if not rule.is_file():
                    continue  # The link check below reports the missing file.
                tree = ast.parse(rule.read_text(encoding='utf-8'))
                ids = [keyword.value.value for node in ast.walk(tree)
                       if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                       and node.func.id == 'GrammarRule' for keyword in node.keywords
                       if keyword.arg == 'id' and isinstance(keyword.value, ast.Constant)]
                for identity in ids:
                    if f'`{identity}`' not in body:
                        errors.append(f'{point}: missing registry identity {identity}')
    for label, values in [('point path', paths), ('entry name', names)]:
        for value, count in collections.Counter(values).items():
            if count > 1:
                errors.append(f'duplicate {label}: {value}')
    indexed = set(paths)
    for point in (GRAMMAR / 'points').glob('*.md'):
        relative = point.relative_to(GRAMMAR).as_posix()
        if relative not in indexed:
            errors.append(f'unindexed specification: {relative}')
    for path in [GRAMMAR / 'README.md', *(GRAMMAR / f'{i}.md' for i in entries), *(GRAMMAR / 'points').glob('*.md')]:
        for target in re.findall(r'\]\(([^)]+)\)', path.read_text(encoding='utf-8')):
            target = unquote(target.split('#', 1)[0])
            if target and not re.match(r'^[a-z]+:', target) and not (path.parent / target).exists():
                errors.append(f'{path}: broken link {target}')
    print('Indexes:', ', '.join(f'{k}={len(v)}' for k, v in entries.items()))
    counts = collections.Counter(status for rows in entries.values() for status, _, _ in rows)
    print('Statuses:', ', '.join(f'[{s}]={counts[s]}' for s in (' ', '~', 'x')))
    return errors


if __name__ == '__main__':
    failures = check()
    if failures:
        print('\n'.join(failures))
        raise SystemExit(1)
    print('Grammar documentation structural checks passed.')
