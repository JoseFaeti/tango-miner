"""Score independently reviewed source-aligned learning candidates offline."""
import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.evaluate_review import read_jsonl


def validate_candidate(candidate, text):
    span = candidate.get('span')
    if (not isinstance(span, list) or len(span) != 2
            or any(type(n) is not int for n in span)
            or not 0 <= span[0] < span[1] <= len(text)
            or candidate.get('surface') != text[span[0]:span[1]]
            or not isinstance(candidate.get('target'), str) or not candidate['target']):
        raise ValueError('candidate needs a valid source span, matching surface and target')
    return (span[0], span[1], candidate['target'])


def audit(sample):
    groups, texts = defaultdict(set), defaultdict(set)
    for row in sample.values():
        if row.get('partition') not in ('development', 'holdout'):
            raise ValueError('invalid partition')
        groups[row['group_id']].add(row['partition'])
        texts[row['text']].add(row['partition'])
    return {'groups_crossing_partitions': sum(len(x) > 1 for x in groups.values()),
            'exact_texts_crossing_partitions': sum(len(x) > 1 for x in texts.values())}


def score(sample, labels, predictions):
    if set(labels) != set(sample) or set(predictions) != set(sample):
        raise ValueError('labels and predictions must contain exactly the sample IDs')
    totals = defaultdict(Counter)
    for key, case in sample.items():
        label, prediction = labels[key], predictions[key]
        text = case['text']
        if label.get('text') != text or label.get('partition') != case['partition']:
            raise ValueError(f'{key}: label text or partition does not match sample')
        c = totals[case['partition']]
        c['sample_lines'] += 1
        if label.get('reviewed') is not True:
            c['unreviewed_lines'] += 1
            continue
        if label.get('exhaustive') is not True:
            c['non_exhaustive_lines'] += 1
            continue
        annotations = label.get('annotations')
        if not isinstance(annotations, list):
            raise ValueError(f'{key}: annotations must be a list')
        gold = []
        for annotation in annotations:
            alternatives = annotation.get('alternatives')
            if not isinstance(alternatives, list) or not alternatives:
                raise ValueError(f'{key}: each annotation needs alternatives')
            gold.append({validate_candidate(a, text) for a in alternatives})
        if prediction.get('error'):
            c['prediction_error_lines'] += 1
            continue
        if prediction.get('abstain') is True:
            c['abstained_lines'] += 1
            continue
        candidates = prediction.get('candidates')
        if not isinstance(candidates, list):
            raise ValueError(f'{key}: candidates must be a list, or abstain/error must be set')
        proposed = [validate_candidate(p, text) for p in candidates]
        if len(set(proposed)) != len(proposed):
            raise ValueError(f'{key}: duplicate predictions')
        # Maximum bipartite matching prevents one proposal from satisfying
        # multiple overlapping gold items and handles alternative answers.
        matched = {}
        def augment(index, visited):
            for candidate in gold[index]:
                if candidate not in proposed or candidate in visited:
                    continue
                visited.add(candidate)
                if candidate not in matched or augment(matched[candidate], visited):
                    matched[candidate] = index
                    return True
            return False
        hits = sum(augment(i, set()) for i in range(len(gold)))
        c['scored_lines'] += 1
        c['gold_candidates'] += len(gold)
        c['predicted_candidates'] += len(proposed)
        c['matched_candidates'] += hits
    return {'audit': audit(sample), 'partitions': {k: dict(v) for k, v in sorted(totals.items())}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sample', type=Path, required=True)
    parser.add_argument('--labels', type=Path, required=True)
    parser.add_argument('--predictions', type=Path, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(score(read_jsonl(args.sample), read_jsonl(args.labels),
                               read_jsonl(args.predictions)), ensure_ascii=False, indent=2))
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
