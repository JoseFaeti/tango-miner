"""Replay one registered grammar rule against a shadow JSONL archive."""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.grammar_rules.registry import RULES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    parser.add_argument('--rule', required=True, choices=[r.id for r in RULES])
    parser.add_argument('--surface-contains', help='only count matches containing this surface text')
    parser.add_argument('--limit', type=int, default=15, help='maximum examples to print')
    parser.add_argument('--max-lines', type=int, default=None, help='optional scan limit')
    args = parser.parse_args()
    rule = next(r for r in RULES if r.id == args.rule)
    counts = Counter()
    examples = []
    with args.archive.open(encoding='utf-8') as stream:
        for line_number, line in enumerate(stream, 1):
            if args.max_lines is not None and line_number > args.max_lines:
                break
            row = json.loads(line)
            counts['sentences'] += 1
            if row.get('alignment_error'):
                counts['alignment_errors'] += 1
                continue
            text, tokens = row['normalized_sentence'], row['tokens']
            for start, end in rule.match(tokens):
                if args.surface_contains and args.surface_contains not in text[start:end]:
                    continue
                counts['matches'] += 1
                if len(examples) < max(0, args.limit):
                    examples.append((text[start:end], text))
    print(f"{args.rule}: {counts['matches']} matches in {counts['sentences']} sentences; "
          f"{counts['alignment_errors']} alignment errors. Raw rule replay; expression precedence and example eligibility are not applied.")
    for surface, text in examples:
        print(f'{surface} | {text}')


if __name__ == '__main__':
    main()
