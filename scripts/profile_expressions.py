"""Time expression matching on a bounded sample of archived sentences."""
import argparse
import csv
import hashlib
from pathlib import Path
import sys
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.Expressions import find_expressions
from src.steps.TokenizeStep.index import tokenize_raw_sentences


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv', type=Path)
    parser.add_argument('--limit', type=int, default=20000)
    parser.add_argument('--repeat', type=int, default=3)
    args = parser.parse_args()
    if min(args.limit, args.repeat) < 1:
        parser.error('limit and repeat must be positive')
    with args.csv.open(encoding='utf-8-sig', newline='') as stream:
        texts = [row['sentence'] for _, row in zip(range(args.limit), csv.DictReader(stream))]
    sentences = tokenize_raw_sentences(texts, tag='sample', source_path=args.csv)
    for _ in range(args.repeat):
        started = perf_counter()
        matches = [find_expressions(sentence) for sentence in sentences]
        elapsed = perf_counter() - started
        digest = hashlib.sha256(repr(matches).encode('utf-8')).hexdigest()[:16]
        print(f'{len(sentences)} sentences, {sum(map(len, matches))} matches: {elapsed:.3f}s; checksum {digest}')
    print('Matching only; excludes tokenization, position aggregation and checksum. Archived examples are a selected sample.')


if __name__ == '__main__':
    main()
