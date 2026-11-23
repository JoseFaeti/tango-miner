"""Profile registered grammar rules on bounded archived tokens or sentence examples."""
import argparse
import hashlib
import json
import sys
from itertools import islice
from pathlib import Path
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    parser.add_argument('--max-lines', type=int, default=20000)
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--sentence-csv', action='store_true', help='retokenize archived examples with the current tokenizer')
    parser.add_argument('--per-tag', type=int, default=150, help='maximum examples per tag for --sentence-csv')
    args = parser.parse_args()
    if min(args.max_lines, args.repeat, args.per_tag) < 1:
        parser.error('limits must be positive')
    if args.sentence_csv:
        from scripts.benchmark_grammar_dispatch import sample
        sentences, tags = sample(args.archive, args.max_lines, args.per_tag)
        aligned = [aligned_tokens(sentence.text, sentence.tokens) for sentence in sentences]
        tokens = [items for items, error in aligned if not error]
        count = len(sentences)
        print(f'Source tags: {tags}')
    else:
        with args.archive.open(encoding='utf-8') as stream:
            rows = [json.loads(line) for line in islice(stream, args.max_lines)]
        tokens = [row['tokens'] for row in rows if not row.get('alignment_error')]
        count = len(rows)
    print(f'Sentences: {count}; aligned: {len(tokens)}; repetitions: {args.repeat}')
    results = []
    for rule in RULES:
        start = perf_counter()
        digest = hashlib.sha256()
        count = 0
        for _ in range(args.repeat):
            for index, sentence in enumerate(tokens):
                matches = list(rule.match(sentence))
                for a, b in matches:
                    digest.update(f'{index}:{a}:{b};'.encode('ascii'))
                count += len(matches)
        elapsed = perf_counter() - start
        results.append((elapsed, rule.id, count, digest.hexdigest()[:16]))
    for elapsed, rule_id, count, digest in sorted(results, reverse=True):
        print(f'{rule_id}: {elapsed:.3f}s; matches {count}; spans {digest}')
    print('Timings include result collection and checksum computation; compare identical samples and repetitions.')


if __name__ == '__main__':
    main()
