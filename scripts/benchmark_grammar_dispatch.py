"""Compare grammar dispatch paths on bounded, retokenized archived examples."""
import argparse
import csv
import json
import sys
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.dispatch import RuleDispatch
from src.grammar_rules.registry import RULES
from src.GrammarCache import _encode
from src.steps.MergeTokensStep.index import get_jmdict
from src.steps.TokenizeStep.index import tokenize_raw_sentences


def sample(path, limit, per_tag):
    rows, seen, counts = [], set(), {}
    with path.open(encoding='utf-8-sig', newline='') as stream:
        for row in csv.DictReader(stream):
            text, tag = row['sentence'], row['tag']
            if text in seen or not 4 <= len(text) <= 200 or counts.get(tag, 0) >= per_tag:
                continue
            rows.append((text, tag))
            seen.add(text)
            counts[tag] = counts.get(tag, 0) + 1
            if len(rows) >= limit:
                break
    tokenized = tokenize_raw_sentences([text for text, _ in rows], tag=None, source_path=path)
    return [replace(sentence, tag=tag) for sentence, (_, tag) in zip(tokenized, rows)], len(counts)


def proposals(tokens, candidates):
    return [(rule.id, start, end) for rule in candidates for start, end in rule.match(tokens)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv', type=Path)
    parser.add_argument('--limit', type=int, default=5000)
    parser.add_argument('--per-tag', type=int, default=150)
    parser.add_argument('--repeat', type=int, default=3)
    args = parser.parse_args()
    if min(args.limit, args.per_tag, args.repeat) < 1:
        parser.error('limits must be positive')
    sentences, tags = sample(args.csv, args.limit, args.per_tag)
    if not sentences:
        parser.error('sample contains no eligible sentences')
    if not sentences:
        parser.error('sample contains no eligible sentences')
    aligned = [aligned_tokens(sentence.text, sentence.tokens) for sentence in sentences]
    tokens = [items for items, error in aligned if not error]
    if len(tokens) != len(sentences):
        raise ValueError('Sample contains unaligned sentences')
    dispatch = RuleDispatch(RULES)
    get_jmdict()  # Both stage paths start with a warm dictionary object.
    print(f'Sentences: {len(sentences)}; tags: {tags}; rules: {len(RULES)}; repetitions: {args.repeat}')
    expected = None
    for repetition in range(args.repeat):
        for indexed in ((False, True) if repetition % 2 == 0 else (True, False)):
            start = perf_counter()
            candidates = [dispatch.candidates(row) if indexed else RULES for row in tokens]
            dispatch_seconds = perf_counter() - start
            start = perf_counter()
            result = [proposals(row, rules) for row, rules in zip(tokens, candidates)]
            matcher_seconds = perf_counter() - start
            if expected is None:
                expected = result
            elif result != expected:
                raise AssertionError('Rule IDs, spans or proposal order differ')
            stage = GrammarStep(prefer_expressions=True, profiling=True, indexed_dispatch=indexed)
            start = perf_counter()
            artifact = stage.process(Artifact(sentences))
            total_seconds = perf_counter() - start
            reserved = ExcludeGrammarTokensStep(stage).process(artifact)
            output = {'stage': _encode(stage), 'matches': stage.matches,
                      'tokens': [sentence.tokens for sentence in reserved.data]}
            encoded = json.dumps(output, ensure_ascii=True, sort_keys=True, separators=(',', ':'))
            digest = sha256(encoded.encode('ascii')).hexdigest()
            if repetition == 0 and not indexed:
                expected_digest = digest
            elif digest != expected_digest:
                raise AssertionError('Occurrences, examples, order or lexical reservation differ')
            calls = sum(map(len, candidates))
            print(f'{"indexed" if indexed else "original"} {repetition + 1}: '
                  f'calls {calls}; candidates/sentence {calls / len(tokens):.2f}; '
                  f'dispatch {dispatch_seconds:.4f}s; matchers {matcher_seconds:.4f}s; '
                  f'stage matching {stage._performance_entries["matching"]:.4f}s; '
                  f'grammar {total_seconds:.4f}s; SHA-256 {digest}')
    print('Stage timing excludes tokenization, preliminary alignment, reservation and checksumming; '
          'it includes stage alignment and expression precedence with a warm in-process dictionary. '
          'Dispatch and matcher timings are separate passes; proposal collection is included.')


if __name__ == '__main__':
    main()
