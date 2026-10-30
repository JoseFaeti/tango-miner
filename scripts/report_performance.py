"""Show performance measurements alongside existing run records."""
import argparse
import json
from pathlib import Path


def load(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None


def report(root):
    summary = load(root / 'summary.json')
    perf = load(root / 'performance.json')
    print(f'Run: {root} | status: {summary.get("status", "unavailable") if isinstance(summary, dict) else "unavailable"}')
    stages = sorted((root / 'steps').glob('*.json'))
    if not stages:
        print('Stage records: unavailable')
    named_steps = {}
    for path in stages:
        row = load(path)
        if not isinstance(row, dict):
            print(f'{path.name}: unavailable')
            continue
        named_steps[str(row.get('step_number', ''))] = row
        duration = row.get('duration_seconds')
        duration = f'{duration:.3f}s' if isinstance(duration, (int, float)) else 'unavailable'
        print(f'  {row.get("name", path.name)}: {duration} | {row.get("status", "unavailable")} | cache: {row.get("cache", "unavailable")}')
    if not isinstance(perf, dict) or perf.get('schema_version') not in (1, 2, 3):
        print('Detailed measurements: unavailable or unsupported schema')
    else:
        grammar = perf.get('grammar', {})
        if perf.get('schema_version') in (1, 2):
            print('Grammar measurements: ' + (json.dumps(grammar, ensure_ascii=False, sort_keys=True) if grammar else 'unavailable'))
        for index, profile in sorted(perf.get('steps', {}).items(), key=lambda item: int(item[0])):
            if perf.get('schema_version') == 3:
                row = named_steps.get(index, {})
                print(f"  {profile.get('name', row.get('name', 'Step ' + index))} named timings:")
                entries = profile.get('entries_seconds', {})
                if 'total' not in entries:
                    total = row.get('duration_seconds')
                    print(f"    total: {total:.3f}s" if isinstance(total, (int, float)) else '    total: unavailable')
                for name, seconds in entries.items():
                    print(f'    {name}: {seconds:.3f}s')
            else:
                cpu = profile.get('cpu_seconds')
                if isinstance(cpu, (int, float)):
                    print(f'  Step {index} CPU: {cpu:.3f}s')
    for name in ('config.json', 'input-manifest.json', 'notes.txt'):
        if not (root / name).is_file():
            print(f'{name}: unavailable')
    if not isinstance(summary, dict) or 'dependencies' not in summary:
        print('Dependencies: unavailable')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('runs', nargs='+', type=Path)
    args = parser.parse_args()
    for root in args.runs:
        report(root)
    if len(args.runs) > 1:
        for name in ('config.json', 'input-manifest.json'):
            values = [load(root / name) for root in args.runs]
            print(f'{name} identical: ' + ('unavailable' if any(v is None for v in values) else str(all(v == values[0] for v in values[1:]))))
        summaries = [load(root / 'summary.json') for root in args.runs]
        dependencies = [s.get('dependencies') if isinstance(s, dict) else None for s in summaries]
        print('Dependencies identical: ' + ('unavailable' if any(v is None for v in dependencies) else str(all(v == dependencies[0] for v in dependencies[1:]))))
        for root in args.runs:
            try:
                revision = next((line for line in (root / 'notes.txt').read_text(encoding='utf-8').splitlines() if line.startswith('Git revision:')), 'Git revision: unavailable')
            except OSError:
                revision = 'Git revision: unavailable'
            print(f'{root}: {revision}; patch: {"present" if (root / "changes.patch").is_file() else "absent or unavailable"}')
        print('Compare cache states, patches, machine load and instrumentation before interpreting timings.')


if __name__ == '__main__':
    main()
