"""Build a bounded raw-source benchmark and compare Japanese analyzers offline.

The analyzer comparison is a shadow experiment, not the Tango Miner pipeline.
Raw line character offsets are Python Unicode offsets. No Anki access occurs.
"""
import argparse
import hashlib
import heapq
import importlib.metadata
import importlib.util
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.compare_runs import completed_runs, load_run
from scripts.evaluate_review import read_jsonl, write_jsonl

TAG = re.compile(r'\[([^\]]+)\]')
JAPANESE = re.compile(r'[ぁ-んァ-ヿ一-龯]')


def digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def source_group(path):
    # Multiple files with the same project tag stay in the same partition.
    matches = TAG.findall(Path(path).name)
    return (matches[-1] if matches else Path(path).stem).casefold()


def choose_groups(manifest, seed, count):
    groups = defaultdict(list)
    for item in manifest:
        groups[source_group(item['path'])].append(item)
    ordered = sorted(groups, key=lambda g: digest(seed + '\0' + g))[:count]
    # Deterministic, grouped holdout, with at least one holdout when possible.
    holdout_count = max(1, len(ordered) // 5) if len(ordered) > 1 else 0
    return [(group, groups[group], 'holdout' if index < holdout_count else 'development')
            for index, group in enumerate(ordered)]


def collect_raw_lines(run, seed, max_groups, per_group):
    if max_groups < 1 or per_group < 1:
        raise ValueError('max-groups and per-group must be positive')
    encoding = run['config']['input']['encoding']
    result = []
    seen = set()
    for group, files, partition in choose_groups(run['manifest'], seed, max_groups):
        ranked = []
        for item in files:
            path = Path(item['path'])
            raw = path.read_bytes()
            actual = hashlib.sha256(raw).hexdigest()
            if actual != item['sha256']:
                raise ValueError(f'Input hash mismatch for source {digest(group)[:12]}')
            for number, line in enumerate(raw.decode(encoding).splitlines(), 1):
                # Raw lines deliberately include short lines the pipeline may
                # discard; bounded length prevents giant model inputs.
                if not 1 <= len(line) <= 300 or not JAPANESE.search(line):
                    continue
                identity = digest(actual + '\0' + str(number) + '\0' + line)
                if identity in seen:
                    continue
                seen.add(identity)
                rank = int(digest(seed + '\0' + identity), 16)
                candidate = (-rank, identity, number, line, actual)
                if len(ranked) < per_group:
                    heapq.heappush(ranked, candidate)
                elif candidate > ranked[0]:
                    heapq.heapreplace(ranked, candidate)
        for _, identity, number, line, file_hash in sorted(ranked, reverse=True):
            result.append({'id': identity[:24], 'partition': partition,
                           'group_id': digest(group)[:16], 'source_sha256': file_hash,
                           'line_number': number, 'text': line, 'kind': 'sampled_raw_line'})
    return result


def align(text, tokens):
    """Align token surfaces in order, allowing only whitespace gaps."""
    cursor, output = 0, []
    for token in tokens:
        surface = token['surface']
        # Sudachi can return zero-length nodes around normalized punctuation.
        # They have no source span and cannot be candidate boundaries.
        if not surface:
            continue
        start = text.find(surface, cursor) if surface else -1
        if start < 0 or text[cursor:start].strip():
            raise ValueError('Cannot safely align analyzer token to source')
        end = start + len(surface)
        output.append({**token, 'span': [start, end]})
        cursor = end
    if text[cursor:].strip():
        raise ValueError('Analyzer omitted non-whitespace source text')
    return output


def sudachi_tokens(tokenizer, text, mode):
    return align(text, [{'surface': m.surface(), 'lemma': m.dictionary_form(),
                         'normalized': m.normalized_form(), 'reading': m.reading_form(),
                         'pos': list(m.part_of_speech()), 'oov': m.is_oov()}
                        for m in tokenizer.tokenize(text, mode)])


def unidic_tokens(tagger, text):
    result = []
    for node in tagger(text):
        feature = node.feature
        result.append({'surface': node.surface, 'lemma': getattr(feature, 'lemma', None),
                       'reading': getattr(feature, 'kana', None),
                       'pos': [getattr(feature, 'pos1', None), getattr(feature, 'pos2', None),
                               getattr(feature, 'pos3', None), getattr(feature, 'pos4', None),
                               getattr(feature, 'cType', None), getattr(feature, 'cForm', None)],
                       'oov': node.is_unk})
    return align(text, result)


def analyze(samples, analyzer, metadata):
    rows = []
    for case in samples.values():
        try:
            tokens = analyzer(case['text'])
            rows.append({'id': case['id'], 'analyzer': metadata, 'tokens': tokens, 'error': None})
        except Exception as exc:
            rows.append({'id': case['id'], 'analyzer': metadata, 'tokens': [],
                         'error': f'{type(exc).__name__}: {exc}'})
    return rows


def compare(sample, left, right):
    if set(left) != set(sample) or set(right) != set(sample):
        raise ValueError('Each analysis must contain exactly the benchmark IDs')
    groups = defaultdict(Counter)
    for key, case in sample.items():
        a, b = left[key], right[key]
        c = groups[case['partition']]
        c['sentences'] += 1
        if a.get('error') or b.get('error'):
            c['analyzer_errors'] += 1
            continue
        spans_a = {tuple(t['span']) for t in a['tokens']}
        spans_b = {tuple(t['span']) for t in b['tokens']}
        c['identical_span_sets' if spans_a == spans_b else 'different_span_sets'] += 1
        c['left_tokens'] += len(spans_a)
        c['right_tokens'] += len(spans_b)
        c['shared_spans'] += len(spans_a & spans_b)
    return {key: dict(value) for key, value in sorted(groups.items())}


def focus_report(sample, analyses):
    """Evaluate only the deliberately naive exact-token proposal heuristic."""
    if set(analyses) != set(sample):
        raise ValueError('Each analysis must contain exactly the benchmark IDs')
    counts = Counter()
    for key, case in sample.items():
        expected = case.get('expected_interpretation')
        if expected not in ('supported', 'fragment', 'other_analysis'):
            raise ValueError(f'{key}: invalid expected interpretation')
        span = case.get('focus_span')
        text = case.get('text')
        if (not isinstance(span, list) or len(span) != 2
                or any(not isinstance(n, int) for n in span) or not isinstance(text, str)
                or not 0 <= span[0] < span[1] <= len(text)
                or text[span[0]:span[1]] != case.get('focus')):
            raise ValueError(f'{key}: focus must match its source span')
        row = analyses[key]
        if row.get('error'):
            counts['analysis_errors'] += 1
            continue
        exact = any(t['span'] == case['focus_span'] for t in row['tokens'])
        if expected == 'supported':
            counts['supported_exact' if exact else 'supported_not_exact'] += 1
        else:
            counts['unsupported_exact' if exact else 'unsupported_not_exact'] += 1
    return dict(counts)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    sample = sub.add_parser('sample')
    sample.add_argument('--run', type=Path)
    sample.add_argument('--runs-dir', type=Path, default=Path('runs'))
    sample.add_argument('--output', type=Path, required=True)
    sample.add_argument('--seed', default='tango-raw-v1')
    sample.add_argument('--max-groups', type=int, default=24)
    sample.add_argument('--per-group', type=int, default=3)
    parse = sub.add_parser('analyze')
    parse.add_argument('--sample', type=Path, required=True)
    parse.add_argument('--output', type=Path, required=True)
    parse.add_argument('--analyzer', choices=('sudachi-a', 'sudachi-b', 'sudachi-c', 'unidic'), required=True)
    diff = sub.add_parser('compare')
    diff.add_argument('--sample', type=Path, required=True)
    diff.add_argument('--left', type=Path, required=True)
    diff.add_argument('--right', type=Path, required=True)
    labels = sub.add_parser('label-template')
    labels.add_argument('--sample', type=Path, required=True)
    labels.add_argument('--output', type=Path, required=True)
    focus = sub.add_parser('focus-report')
    focus.add_argument('--sample', type=Path, required=True)
    focus.add_argument('--analysis', type=Path, required=True)
    args = p.parse_args(argv)
    try:
        if args.command == 'sample':
            runs = [args.run] if args.run else completed_runs(args.runs_dir)
            if not runs:
                raise ValueError('No completed run found')
            run = load_run(runs[-1])
            rows = collect_raw_lines(run, args.seed, args.max_groups, args.per_group)
            for row in rows:
                row.update(run_id=run['path'].name, seed=args.seed)
            write_jsonl(args.output, rows)
            print(f'Sampled {len(rows)} raw lines from {len({r["group_id"] for r in rows})} source groups.')
        elif args.command == 'analyze':
            cases = read_jsonl(args.sample)
            if args.analyzer.startswith('sudachi'):
                from sudachipy import dictionary, tokenizer
                if Path.cwd().resolve() != ROOT:
                    raise ValueError('Run Sudachi analysis from the project root so relative user dictionary paths match the configuration')
                mode = getattr(tokenizer.Tokenizer.SplitMode, args.analyzer[-1].upper())
                engine = dictionary.Dictionary(config_path=str(ROOT / 'sudachi.config.json'), dict='full').create()
                spec = importlib.util.find_spec('sudachidict_full')
                dictionary_path = Path(spec.origin).parent / 'resources' / 'system.dic'
                config = json.loads((ROOT / 'sudachi.config.json').read_text(encoding='utf-8'))
                user_hashes = [file_hash(Path(path) if Path(path).is_absolute() else ROOT / path)
                               for path in config.get('userDict', [])]
                meta = {'name': args.analyzer, 'sudachipy': importlib.metadata.version('SudachiPy'),
                        'dictionary': importlib.metadata.version('SudachiDict-full'),
                        'dictionary_sha256': file_hash(dictionary_path),
                        'config_sha256': file_hash(ROOT / 'sudachi.config.json'),
                        'user_dictionary_sha256': user_hashes}
                function = lambda text: sudachi_tokens(engine, text, mode)
            else:
                import fugashi
                import unidic
                engine = fugashi.Tagger()
                if (len(engine.dictionary_info) != 1
                        or Path(engine.dictionary_info[0]['filename']).resolve() != (Path(unidic.DICDIR) / 'sys.dic').resolve()):
                    raise ValueError('Fugashi did not load the expected UniDic system dictionary')
                meta = {'name': 'unidic', 'fugashi': importlib.metadata.version('fugashi'),
                        'unidic': importlib.metadata.version('unidic'),
                        'dictionary_files_sha256': {p.name: file_hash(p) for p in sorted(Path(unidic.DICDIR).iterdir()) if p.is_file()}}
                function = lambda text: unidic_tokens(engine, text)
            rows = analyze(cases, function, meta)
            write_jsonl(args.output, rows)
            print(f'Analyzed {len(rows)} lines; {sum(bool(r["error"]) for r in rows)} alignment or analyzer errors.')
        elif args.command == 'label-template':
            cases = read_jsonl(args.sample)
            write_jsonl(args.output, [{'id': key, 'partition': row['partition'], 'text': row['text'],
                'reviewed': False, 'annotations': [], 'note': ''} for key, row in cases.items()])
            print(f'Created {len(cases)} empty source-aligned labels.')
        elif args.command == 'focus-report':
            print(json.dumps(focus_report(read_jsonl(args.sample), read_jsonl(args.analysis)),
                             ensure_ascii=False, indent=2))
        else:
            print(json.dumps(compare(read_jsonl(args.sample), read_jsonl(args.left), read_jsonl(args.right)),
                             ensure_ascii=False, indent=2))
    except (OSError, ValueError, KeyError, ImportError, UnicodeError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
