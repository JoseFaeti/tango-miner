"""Time word-stat aggregation on a bounded sample of archived sentences."""
import argparse
import csv
from pathlib import Path
import sys
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.steps.BuildWordStatsStep.index import PotentialFormResolver, build_word_stats
from src.steps.MergeTokensStep.index import get_jmdict
from src.steps.TokenizeStep.index import get_tokenizer, tokenize_raw_sentences


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
    dictionary, tokenizer = get_jmdict(), get_tokenizer()
    for _ in range(args.repeat):
        resolver = PotentialFormResolver(dictionary, tokenizer)
        started = perf_counter()
        words, segmented = build_word_stats(sentences, potential_resolver=resolver)
        elapsed = perf_counter() - started
        print(f'{len(segmented)} sentences, {len(words)} candidates: {elapsed:.3f}s')
    print('Raw token sample; excludes tokenization and dictionary setup, includes potential resolution.')


if __name__ == '__main__':
    main()
