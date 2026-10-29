"""Search JSONL diagnostics without loading the file into memory or running mining."""
import argparse
import json
import sys
from pathlib import Path


def values_at(value, parts):
    """Follow dotted fields through dictionaries and arrays."""
    if not parts:
        return [value]
    if isinstance(value, list):
        return [result for item in value for result in values_at(item, parts)]
    if isinstance(value, dict) and parts[0] in value:
        return values_at(value[parts[0]], parts[1:])
    return []


def render(value):
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path, help='JSONL diagnostic file')
    parser.add_argument('--field', help='dotted field to search; arrays are traversed')
    match = parser.add_mutually_exclusive_group(required=True)
    match.add_argument('--contains', help='case-sensitive substring')
    match.add_argument('--exact', help='exact value, rendered as text')
    parser.add_argument('--show', action='append', default=[], help='dotted field to display; repeat as needed')
    parser.add_argument('--limit', type=int, default=10, help='maximum matching records displayed')
    parser.add_argument('--width', type=int, default=300, help='maximum characters per displayed record')
    args = parser.parse_args(argv)
    if args.limit < 0 or args.width < 1:
        parser.error('--limit must be nonnegative and --width must be positive')
    count = 0
    try:
        with args.file.open(encoding='utf-8-sig') as stream:
            for line_number, line in enumerate(stream, 1):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f'{args.file}:{line_number}: {exc.msg}') from exc
                candidates = values_at(row, args.field.split('.')) if args.field else [row]
                matched = any((args.contains in render(v) if args.contains is not None
                               else args.exact == render(v)) for v in candidates)
                if not matched:
                    continue
                count += 1
                if count <= args.limit:
                    output = {field: values_at(row, field.split('.')) for field in args.show} if args.show else row
                    text = render(output)
                    if len(text) > args.width:
                        text = text[:args.width - 1] + '…'
                    print(f'{line_number}: {text}')
        print(f'Matches: {count}; shown: {min(count, args.limit)}')
        return 0
    except (OSError, ValueError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
