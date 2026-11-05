"""Print the next available grammar point as JSON."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GRAMMAR_DIR = ROOT / 'docs' / 'grammar'
QUEUES = (('next.md', ' '), ('later.md', ' '), ('partially-implemented.md', '~'))
ENTRY = re.compile(r'^- \[([ ~])\] \[(.*?)\]\((points/[^)]+\.md)\)$')


def next_point(queue: str, status: str = ' ') -> dict[str, str] | None:
    for line in queue.splitlines():
        match = ENTRY.fullmatch(line)
        if match and match[1] == status:
            return {'name': match[2], 'path': f'docs/grammar/{match[3]}'}
    return None


def select_next(grammar_dir: Path = GRAMMAR_DIR) -> dict[str, str] | None:
    for filename, status in QUEUES:
        result = next_point((grammar_dir / filename).read_text(encoding='utf-8'), status)
        if result is not None:
            return result
    return None


if __name__ == '__main__':
    result = select_next()
    sys.stdout.buffer.write((json.dumps(result, ensure_ascii=False) + '\n').encode('utf-8'))
