"""Sample grammar shadow proposals for local, human review without mining."""
import argparse
from collections import Counter, defaultdict
import hashlib
import heapq
import json
from pathlib import Path
import sys


def sample(path, per_stratum, seed):
    """Select a deterministic bounded sample per construction and evidence."""
    if per_stratum < 1:
        raise ValueError('per-stratum must be positive')
    heaps = defaultdict(list)
    totals = Counter()
    serial = 0
    with Path(path).open(encoding='utf-8') as stream:
        for number, line in enumerate(stream, 1):
            row = json.loads(line)
            for proposal in row['proposals']:
                serial += 1
                identity = proposal['id']
                if not isinstance(identity, str) or not identity:
                    raise ValueError(f'line {number}: invalid proposal ID')
                key = (proposal['construction_id'], tuple(proposal['evidence']))
                totals[key] += 1
                rank = int.from_bytes(hashlib.sha256((seed + '\0' + identity).encode('utf-8')).digest(), 'big')
                # A line number makes ties comparable without comparing dictionaries.
                item = (-rank, -serial, identity, row, proposal)
                heap = heaps[key]
                if len(heap) < per_stratum:
                    heapq.heappush(heap, item)
                elif item[:3] > heap[0][:3]:
                    heapq.heapreplace(heap, item)
    output = []
    for key in sorted(heaps):
        for _, _, identity, row, proposal in sorted(heaps[key], key=lambda x: (-x[0], -x[1], x[2])):
            output.append({
                'id': identity, 'sentence_id': row['sentence_id'],
                'source': row.get('source'), 'tag': row.get('tag'),
                'text': row['normalized_sentence'], 'span': proposal['span'],
                'surface': proposal['surface'], 'construction_id': key[0],
                'evidence': list(key[1]), 'component_token_ids': proposal['component_token_ids'],
                'tokens': row['tokens'], 'rule_version': row['rule_version'],
                'rule_sha256': row.get('rule_sha256'), 'stratum_population': totals[key],
                'reviewed': False, 'judgment': None, 'note': '',
            })
    return output, totals


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True, help='run grammar-shadow.jsonl')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--per-stratum', type=int, default=5)
    parser.add_argument('--seed', default='grammar-review-v1')
    args = parser.parse_args(argv)
    try:
        rows, totals = sample(args.input, args.per_stratum, args.seed)
        with args.output.open('x', encoding='utf-8', newline='\n') as stream:
            for row in rows:
                stream.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
        print(f'Sampled {len(rows)} proposals from {sum(totals.values())} in {len(totals)} strata.')
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
