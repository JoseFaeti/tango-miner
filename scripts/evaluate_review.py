"""Create and score a manually labeled, offline sample of archived examples.

Never runs the pipeline or contacts Anki. Labels and predictions are separate
JSONL files so an experiment cannot accidentally inspect its answer key.
"""
import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.compare_runs import completed_runs, load_run
from src.JMDict import JMDict


def write_jsonl(path, rows):
    # Exclusive creation protects human labels and earlier experiments.
    with Path(path).open('x', encoding='utf-8', newline='\n') as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')


def read_jsonl(path):
    result = {}
    with Path(path).open(encoding='utf-8-sig') as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            key = row.get('id')
            if not isinstance(key, str) or not key or key in result:
                raise ValueError(f'{path}:{number}: missing or duplicate id')
            result[key] = row
    return result


def selected_examples(run):
    files = list(run['path'].glob('*.sentence.csv'))
    if len(files) != 1:
        raise ValueError('Expected exactly one sentence CSV')
    examples = {}
    with files[0].open(encoding='utf-8-sig', newline='') as stream:
        for row in csv.DictReader(stream):
            if row.get('word') and row.get('sentence'):
                examples.setdefault(row['word'], {'sentence': row['sentence'],
                    'surface': row.get('surface_form', ''), 'tag': row.get('tag', '')})
    return examples


def sample_candidates(rows, examples, per_stratum, seed):
    """Sample equally from frequency-ranked thirds within each output status."""
    if per_stratum < 1:
        raise ValueError('per-stratum must be positive')
    selected = []
    for status in ('valid', 'dropped/review'):
        eligible = [(word, item) for word, item in rows.items()
                    if item['status'] == status and word in examples]
        eligible.sort(key=lambda pair: (-pair[1]['frequency'], pair[0]))
        for band in range(3):
            group = eligible[len(eligible) * band // 3:len(eligible) * (band + 1) // 3]
            group.sort(key=lambda pair: hashlib.sha256(
                (str(seed) + '\0' + status + '\0' + pair[0]).encode('utf-8')).digest())
            selected.extend((word, item, f'{status}:{("high", "middle", "low")[band]}')
                            for word, item in group[:per_stratum])
    return selected


def alternatives(dictionary, word):
    result = []
    for entry in dictionary.lookup_word(word):
        result.append({'entry_id': entry.get('ent_seq'),
                       'kanji': [k['form'] for k in entry['kanji']],
                       'readings': [r['form'] for r in entry['reading']],
                       'senses': [{'pos': s['pos'], 'gloss': s['gloss'],
                                   'stagk': s.get('stagk', []), 'stagr': s.get('stagr', [])}
                                  for s in entry['senses']]})
    return result


def create_sample(run, dictionary, per_stratum, seed):
    examples = selected_examples(run)
    rows = []
    for word, item, stratum in sample_candidates(run['rows'], examples, per_stratum, seed):
        # This identifies a sampled candidate and its selected example, not a
        # stable corpus occurrence ID or an aligned character span.
        identity = hashlib.sha256((run['path'].name + '\0' + word).encode('utf-8')).hexdigest()[:24]
        rows.append({'id': identity, 'candidate': word, 'frequency': item['frequency'],
                     'stratum': stratum, 'pipeline_status': item['status'],
                     'pipeline_definition': item['definition'],
                     **examples[word], 'dictionary_entries': alternatives(dictionary, word)})
    return rows


def evaluate(sample, labels, predictions):
    unknown = (set(labels) | set(predictions)) - set(sample)
    if unknown:
        raise ValueError('Labels or predictions contain IDs absent from the sample')
    for key, pred in predictions.items():
        if pred.get('decision') not in ('select', 'abstain'):
            raise ValueError(f'{key}: prediction decision must be select or abstain')
        if pred['decision'] == 'select' and (not isinstance(pred.get('target'), str)
                                             or not pred['target']
                                             or not isinstance(pred.get('entry_id'), str)
                                             or not pred['entry_id']):
            raise ValueError(f'{key}: selected target and entry_id must be nonempty strings')
    counts = defaultdict(Counter)
    for key, case in sample.items():
        label = labels.get(key, {})
        verdict = label.get('baseline_correct', '')
        if verdict not in ('', 'correct', 'incorrect', 'uncertain', 'not_applicable'):
            raise ValueError(f'{key}: invalid baseline_correct')
        pred = predictions.get(key)
        for group in ('all', case['stratum']):
            c = counts[group]
            c['sampled'] += 1
            if verdict:
                c['baseline_' + verdict] += 1
            target = label.get('gold_target', '')
            entry = label.get('gold_entry_id', '')
            if target and entry:
                c['scorable'] += 1
                if pred is None or pred.get('decision') == 'abstain':
                    c['abstain_or_missing'] += 1
                elif pred.get('decision') == 'select':
                    if pred.get('target') == target and str(pred.get('entry_id', '')) == str(entry):
                        c['correct'] += 1
                    else:
                        c['incorrect'] += 1
    return {group: dict(count) for group, count in sorted(counts.items())}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    make = sub.add_parser('sample')
    make.add_argument('--run', type=Path)
    make.add_argument('--runs-dir', type=Path, default=Path('runs'))
    make.add_argument('--dictionary', type=Path, help='override archived JMdict path')
    make.add_argument('--output', type=Path, required=True)
    make.add_argument('--per-stratum', type=int, default=20)
    make.add_argument('--seed', default='tango-review-v1')
    template = sub.add_parser('label-template')
    template.add_argument('--sample', type=Path, required=True)
    template.add_argument('--output', type=Path, required=True)
    prompts = sub.add_parser('prompts')
    prompts.add_argument('--sample', type=Path, required=True)
    prompts.add_argument('--output', type=Path, required=True)
    score = sub.add_parser('score')
    score.add_argument('--sample', type=Path, required=True)
    score.add_argument('--labels', type=Path, required=True)
    score.add_argument('--predictions', type=Path, help='JSONL from the system being evaluated')
    make.add_argument('--allow-dictionary-mismatch', action='store_true',
                      help='allow a JMdict that differs from the archived hash')
    args = parser.parse_args(argv)
    try:
        if args.command == 'sample':
            paths = completed_runs(args.runs_dir) if args.run is None else [args.run]
            if not paths:
                raise ValueError('No completed runs found')
            run = load_run(paths[-1])
            path = args.dictionary or Path(run['config']['dictionary']['jmdict_path']).expanduser()
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            dictionary_hash = digest.hexdigest()
            recorded_hash = run['summary'].get('dependencies', {}).get('files', {}).get('jmdict', {}).get('sha256')
            if recorded_hash != dictionary_hash and not args.allow_dictionary_mismatch:
                raise ValueError('JMdict hash differs from or is missing in the archive; use the archived dictionary or --allow-dictionary-mismatch')
            sample = create_sample(run, JMDict(path), args.per_stratum, args.seed)
            for row in sample:
                row.update(run_id=run['path'].name, dictionary_sha256=dictionary_hash, seed=args.seed)
            write_jsonl(args.output, sample)
            example_words = selected_examples(run)
            print(f"Sampled {len(sample)} candidates from {run['path'].name}; "
                  f"{sum(1 for w in run['rows'] if w not in example_words)} candidates lack selected examples.")
        elif args.command == 'label-template':
            sample = read_jsonl(args.sample)
            write_jsonl(args.output, [{'id': key, 'candidate': row['candidate'],
                'sentence': row['sentence'], 'baseline_correct': '', 'gold_target': '',
                'gold_entry_id': '', 'review_note': ''} for key, row in sample.items()])
            print(f'Created {len(sample)} empty labels.')
        elif args.command == 'prompts':
            sample = read_jsonl(args.sample)
            keys = ('id', 'candidate', 'sentence', 'surface', 'dictionary_entries')
            write_jsonl(args.output, [{key: row[key] for key in keys} for row in sample.values()])
            print(f'Created {len(sample)} evaluation inputs without labels or pipeline results.')
        else:
            result = evaluate(read_jsonl(args.sample), read_jsonl(args.labels),
                              read_jsonl(args.predictions) if args.predictions else {})
            print(json.dumps(result, ensure_ascii=False, indent=2))
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
