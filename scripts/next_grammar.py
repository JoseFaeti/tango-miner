"""Print the first unchecked grammar point as JSON."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUEUE = ROOT / 'docs' / 'grammar' / 'next.md'
ENTRY = re.compile(r'^- \[ \] \[(.*?)\]\((points/[^)]+\.md)\)$')


def next_point(queue: str) -> dict[str, str] | None:
    for line in queue.splitlines():
        match = ENTRY.fullmatch(line)
        if match:
            return {'name': match[1], 'path': f'docs/grammar/{match[2]}'}
    return None


if __name__ == '__main__':
    result = next_point(QUEUE.read_text(encoding='utf-8'))
    sys.stdout.buffer.write((json.dumps(result, ensure_ascii=False) + '\n').encode('utf-8'))
