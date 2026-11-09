"""Profile token merging on a bounded set of archived sentence examples."""
import argparse
import cProfile
import csv
from dataclasses import asdict, is_dataclass
import hashlib
import json
from pathlib import Path
import sys
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.steps.MergeTokensStep.index import combine_tokens_jmdict, get_jmdict
from src.steps.TokenizeStep.index import tokenize_raw_sentences


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv', type=Path, help='archive sentence CSV with sentence and tag columns')
    parser.add_argument('--limit', type=int, default=5000)
    parser.add_argument('--per-tag', type=int, default=150)
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--max-lookahead', type=int, default=10)
    parser.add_argument('--reserve-grammar', action='store_true')
    parser.add_argument('--diagnostics', action='store_true')
    parser.add_argument('--profile', type=Path, help='write cProfile data')
    args = parser.parse_args()
    if min(args.limit, args.per_tag, args.repeat, args.max_lookahead) < 1:
        parser.error('limits must be positive')
    texts, seen, counts = [], set(), {}
    with args.csv.open(encoding='utf-8-sig', newline='') as stream:
        for row in csv.DictReader(stream):
            text, tag = row['sentence'], row['tag']
            if text in seen or not 4 <= len(text) <= 200 or counts.get(tag, 0) >= args.per_tag:
                continue
            texts.append(text)
            seen.add(text)
            counts[tag] = counts.get(tag, 0) + 1
            if len(texts) >= args.limit:
                break
    sentences = tokenize_raw_sentences(texts, tag=None, source_path=args.csv)
    dictionary = get_jmdict()
    if args.reserve_grammar:
        stage = GrammarStep(prefer_expressions=True, profiling=False, max_lookahead=args.max_lookahead)
        artifact = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact(sentences)))
        sentences = artifact.data
    digest = hashlib.sha256()
    rejection_count = 0
    merge_seconds = 0.0
    def encode(value):
        if is_dataclass(value):
            return asdict(value)
        raise TypeError(type(value).__name__)
    def run():
        nonlocal rejection_count, merge_seconds
        for sentence in sentences:
            rejected = [] if args.diagnostics else None
            merge_started = perf_counter()
            merged = combine_tokens_jmdict(sentence.tokens, dictionary,
                                           max_lookahead=args.max_lookahead, rejected=rejected)
            merge_seconds += perf_counter() - merge_started
            payload = json.dumps([merged, rejected], ensure_ascii=True, sort_keys=True,
                                 separators=(',', ':'), default=encode)
            digest.update(payload.encode('ascii'))
            rejection_count += len(rejected) if rejected is not None else 0
    profiler = cProfile.Profile() if args.profile else None
    started = perf_counter()
    if profiler:
        profiler.enable()
    for _ in range(args.repeat):
        run()
    if profiler:
        profiler.disable()
        profiler.dump_stats(args.profile)
    print(f'Sentences: {len(sentences)}; tags: {len(counts)}; repetitions: {args.repeat}; '
          f'grammar reservation: {args.reserve_grammar}; diagnostics: {args.diagnostics}; '
          f'max lookahead: {args.max_lookahead}')
    print(f'Merge seconds: {merge_seconds:.3f}; elapsed including checksum: {perf_counter() - started:.3f}; '
          f'rejected: {rejection_count}; '
          f'output SHA-256: {digest.hexdigest()}')
    print('Merge timing excludes setup, grammar reservation, JSON serialization and hashing.')


if __name__ == '__main__':
    main()
