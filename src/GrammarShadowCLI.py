"""Generate tentative grammar proposals from an offline analyzer comparison."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.evaluate_review import read_jsonl, write_jsonl
from src.GrammarShadow import analyze_sentence, fingerprint
from src.TokenizedSentence import TokenizedSentence


def build_rows(sample, analyses):
    if set(sample) != set(analyses):
        raise ValueError('analysis must contain exactly the sample IDs')
    output = []
    rule_hash = fingerprint()
    for index, (key, case) in enumerate(sample.items()):
        analysis = analyses[key]
        if analysis.get('error'):
            output.append({'id': key, 'error': analysis['error'], 'proposals': []})
            continue
        tokens = analysis.get('tokens')
        if not isinstance(tokens, list):
            raise ValueError(f'{key}: tokens must be a list')
        sentence = TokenizedSentence(case['text'], None, Path(case.get('group_id', 'unknown')), tokens)
        row = analyze_sentence(sentence, index, source_id=key)
        # The benchmark input is the original raw line, not normalized text.
        row['raw_sentence'] = case['text']
        row['raw_mapping_status'] = 'identity_raw_input'
        for proposal in row['proposals']:
            proposal['raw_span'] = list(proposal['span'])
        row.update(id=key, partition=case['partition'], analyzer=analysis.get('analyzer'),
                   rule_sha256=rule_hash)
        output.append(row)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sample', type=Path, required=True)
    parser.add_argument('--analysis', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        rows = build_rows(read_jsonl(args.sample), read_jsonl(args.analysis))
        write_jsonl(args.output, rows)
        print(f"Wrote {len(rows)} lines, {sum(len(r['proposals']) for r in rows)} tentative proposals.")
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
