"""Query a completed run with bounded output; never runs mining or contacts Anki."""
import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

from compare_runs import completed_runs, load_run, plain_examples


def clip(value, width):
    value = ' '.join(value.split())
    return value if len(value) <= width else value[:max(0, width - 1)] + '…'


def summarize_occurrences(path, words, sample_limit, width):
    counts = defaultdict(Counter)
    samples = defaultdict(list)
    with path.open(encoding='utf-8') as stream:
        for line in stream:
            row = json.loads(line)
            word = row.get('candidate')
            if word not in words:
                continue
            components = row.get('components', [])
            key = tuple((c.get('lemma', ''), tuple(c.get('pos', [])[:2])) for c in components)
            counts[word][key] += 1
            if len(samples[word]) < sample_limit:
                samples[word].append(clip(row.get('sentence', ''), width))
    return counts, samples


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', nargs='?', type=Path, help='run directory; defaults to latest completed')
    parser.add_argument('--runs-dir', type=Path, default=Path('runs'))
    parser.add_argument('--word', action='append', default=[], help='exact candidate; repeat as needed')
    parser.add_argument('--contains', help='substring to find in candidate names')
    parser.add_argument('--status', choices=('all', 'valid', 'review'), default='all', help='review includes invalid')
    parser.add_argument('--limit', type=int, default=10, help='maximum candidates')
    parser.add_argument('--examples', type=int, default=1, help='maximum displayed examples per candidate')
    parser.add_argument('--width', type=int, default=160, help='maximum characters per definition or example')
    parser.add_argument('--occurrences', action='store_true', help='summarize recorded occurrence analyses for exact --word values')
    parser.add_argument('--rejected-merges', action='store_true', help='summarize rejected dictionary matches for exact --word surfaces')
    args = parser.parse_args(argv)
    if args.limit < 0 or args.examples < 0 or args.width < 1:
        parser.error('limit/examples must be nonnegative and width must be positive')
    if (args.occurrences or args.rejected_merges) and not args.word:
        parser.error('--occurrences and --rejected-merges require at least one --word')
    try:
        if args.run is None:
            runs = completed_runs(args.runs_dir)
            if not runs:
                raise ValueError(f'No completed runs in {args.runs_dir}')
            args.run = runs[-1]
        run = load_run(args.run)
        rows = run['rows']
        selected = [(w, v) for w, v in rows.items()
                    if (not args.word or w in args.word)
                    and (not args.contains or args.contains in w)
                    and (args.status == 'all' or (v['status'] == 'valid') == (args.status == 'valid'))]
        selected.sort(key=lambda pair: (-pair[1]['frequency'], pair[0]))
        print(f"Run: {run['path']} | matching candidates: {len(selected)} | showing: {min(args.limit, len(selected))}")
        for word, item in selected[:args.limit]:
            print(f"{word} | {item['status']} | frequency {item['frequency']}")
            print(f"  definition: {clip(plain_examples(item['definition']), args.width) or '(unresolved)'}")
            if item['status'] != 'valid' and item['definition']:
                print('  review: definition present; CSV does not record the reason')
            # Each CSV example is separated by two HTML line breaks.
            examples = re.split(r'(?:<br\s*/?>\s*){2}', item['examples'], flags=re.I)
            for example in examples[:args.examples]:
                print(f"  example: {clip(plain_examples(example), args.width)}")
        missing = [w for w in args.word if w not in rows]
        if missing:
            print('Absent: ' + ', '.join(missing))
        if args.occurrences:
            path = args.run / 'occurrences.jsonl'
            if not path.is_file():
                raise ValueError('Occurrence diagnostics are unavailable in this run; use --occurrence-diagnostics on a future run')
            words = set(args.word)
            counts, samples = summarize_occurrences(path, words, args.examples, args.width)
            for word in args.word[:args.limit]:
                print(f"{word} occurrences: {sum(counts[word].values())}; top analyses (at most {args.limit}):")
                for analysis, count in counts[word].most_common(args.limit):
                    print(f"  {count}: {clip(str(analysis), args.width)}")
                for sample in samples[word]:
                    print(f"  sample: {sample}")
        candidate_path = args.run / 'candidate-diagnostics.jsonl'
        if args.word and candidate_path.is_file():
            with candidate_path.open(encoding='utf-8') as stream:
                for line in stream:
                    row = json.loads(line)
                    if row.get('candidate') in args.word[:args.limit]:
                        print(f"{row['candidate']} selected entry: {row.get('selected_dictionary_entry')} | supported: {row.get('definition_supported_occurrences')} | other: {row.get('other_occurrences')}")
                        if row.get('review_reasons'):
                            print(f"  review reasons: {', '.join(row['review_reasons'])}")
        if args.rejected_merges:
            path = args.run / 'rejected-merges.jsonl'
            if not path.is_file():
                raise ValueError('Rejected merge diagnostics are unavailable in this run')
            counts, samples = Counter(), defaultdict(list)
            with path.open(encoding='utf-8') as stream:
                for line in stream:
                    row = json.loads(line)
                    if row['surface'] in args.word:
                        counts[(row['surface'], row['reason'])] += 1
                        if len(samples[row['surface']]) < args.examples:
                            samples[row['surface']].append(clip(row['sentence'], args.width))
            for word in args.word[:args.limit]:
                print(f"{word} rejected merges: {sum(count for (surface, _), count in counts.items() if surface == word)}")
                for (surface, reason), count in counts.most_common():
                    if surface == word:
                        print(f"  {count}: {reason}")
                for sample in samples[word]:
                    print(f"  sample: {sample}")
        return 0
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
