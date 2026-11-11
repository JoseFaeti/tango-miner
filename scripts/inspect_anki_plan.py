"""Summarize field and sentence changes in an archived Anki preview."""
import argparse
import json
from collections import Counter
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('--examples', type=int, default=5)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text(encoding='utf-8'))
    fields, combinations, sentence_changes = Counter(), Counter(), Counter()
    examples = []
    for item in plan['modify']:
        changes = item['fields']
        fields.update(changes.keys())
        combinations[', '.join(sorted(changes))] += 1
        if item.get('tags_added') or item.get('tags_removed'):
            fields['tags'] += 1
        if 'Sentence' in changes:
            sentence_changes['changed'] += 1
            if len(examples) < args.examples:
                examples.append(item['expression'])
    print(f"Added: {len(plan['add'])}; modified: {len(plan['modify'])}; deleted: {len(plan['delete'])}")
    for title, counts in [('Changed fields', fields), ('Field combinations', combinations), ('Sentence changes', sentence_changes)]:
        print(f'{title}:')
        for key, count in counts.most_common(): print(f'  {key}: {count}')
    print('Sentence-change examples:', ', '.join(examples) or 'none')


if __name__ == '__main__':
    main()
