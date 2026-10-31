"""Bounded, read-only queries over archived pipeline records."""
import argparse
import json
import sys
from pathlib import Path

from src.RunComparison import completed_runs, load_json


def _optional(path):
    try:
        return load_json(path)
    except FileNotFoundError:
        return None


def _stages(summary):
    return {row['name']: row for row in summary.get('stages', []) if isinstance(row, dict) and 'name' in row}


def _baseline(run, explicit=None):
    if explicit:
        return explicit
    runs = completed_runs(run.parent)
    if run in runs:
        index = runs.index(run)
        return runs[index - 1] if index else None
    earlier = [p for p in runs if p.name < run.name]
    return earlier[-1] if earlier else None


def _compatibility(run, baseline, current, previous):
    result = {}
    for name, filename in [('inputs_identical', 'input-manifest.json'), ('config_identical', 'config.json')]:
        a, b = _optional(run / filename), _optional(baseline / filename)
        result[name] = a == b if a is not None and b is not None else None
    a, b = current.get('dependencies'), previous.get('dependencies')
    result['dependencies_identical'] = a == b if a is not None and b is not None else None
    return result


def query(command, run, baseline=None, regressions=False, threshold=25.0, changes_only=False):
    summary = load_json(run / 'summary.json')
    stages = _stages(summary)
    result = {'run': run.name, 'status': summary.get('status', 'unavailable')}
    if command == 'summary':
        result['duration_seconds'] = summary.get('duration_seconds')
        row = stages.get('AddDefinitionsStep', {})
        result['candidates_at_definition'] = {k: row[k] for k in ('valid_candidates', 'review_candidates', 'invalid_candidates') if k in row}
        result['anki_status'] = (summary.get('anki') or {}).get('status', 'unavailable')
    elif command == 'anki':
        anki = summary.get('anki') or {}
        result['anki_status'] = anki.get('status', 'unavailable')
        plan = anki.get('plan')
        result['planned_changes'] = ({k: v for k, v in plan.items() if k in ('add', 'modify', 'delete') and (not changes_only or v)} if isinstance(plan, dict) else None)
        result['details'] = 'anki-plan.json' if (run / 'anki-plan.json').is_file() else None
    elif command == 'diff':
        comparison = stages.get('CompareOutputStep', {}).get('comparison')
        result['comparison'] = comparison or {'status': 'unavailable'}
        if comparison and comparison.get('baseline'):
            old = run.parent / comparison['baseline']
            previous = _optional(old / 'summary.json')
            result['compatibility'] = _compatibility(run, old, summary, previous) if previous else None
        result['details'] = 'csv-changes.json' if (run / 'csv-changes.json').is_file() else None
    elif command in ('performance', 'warnings'):
        baseline = _baseline(run, baseline)
        previous = _optional(baseline / 'summary.json') if baseline else None
        result['baseline'] = baseline.name if baseline else None
        result['compatibility'] = _compatibility(run, baseline, summary, previous) if previous else None
        old_stages = _stages(previous) if previous else {}
        timings = []
        for name, row in stages.items():
            value = row.get('duration_seconds')
            old = old_stages.get(name, {}).get('duration_seconds')
            if not isinstance(value, (int, float)):
                continue
            change = (value / old - 1) * 100 if isinstance(old, (int, float)) and old > 0 else None
            if not regressions or (change is not None and change >= threshold):
                timings.append({'stage': name, 'seconds': value, 'previous_seconds': old, 'change_percent': round(change, 1) if change is not None else None})
        if command == 'performance':
            result['threshold_percent'] = threshold if regressions else None
            result['timings'] = sorted(timings, key=lambda row: row['seconds'], reverse=True)
            result['note'] = 'Single-run timings; check compatibility, cache state and machine load.'
        else:
            warnings = []
            if not previous:
                warnings.append({'id': 'baseline_unavailable', 'message': 'No baseline summary available'})
            if summary.get('error'):
                warnings.append({'id': 'pipeline_failed', 'message': summary['error']})
            for key, value in (result['compatibility'] or {}).items():
                if value is not True:
                    warnings.append({'id': key, 'message': 'Different' if value is False else 'Unavailable'})
            for row in timings:
                if row['change_percent'] is not None and row['change_percent'] >= threshold:
                    warnings.append({'id': 'stage_runtime:' + row['stage'], 'message': f"{row['change_percent']:+.1f}% versus one baseline run"})
            result['warnings'] = warnings
            result['note'] = 'Only recorded failures, compatibility and single-run timing signals are checked.'
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(prog='tango_miner.py report', description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    for name in ('summary', 'warnings', 'diff', 'anki', 'performance'):
        p = subs.add_parser(name)
        p.add_argument('--run', type=Path, help='run directory; defaults to latest completed run')
        p.add_argument('--runs-dir', type=Path, default=Path('runs'), help='archive root for automatic selection')
        p.add_argument('--json', action='store_true', help='emit JSON')
        if name in ('warnings', 'performance'):
            p.add_argument('--baseline', type=Path, help='baseline run directory; defaults to preceding completed run')
            p.add_argument('--threshold', type=float, default=25.0, help='single-run timing increase percent (default 25)')
        if name == 'performance':
            p.add_argument('--regressions', action='store_true', help='show only timings above threshold')
        if name == 'diff':
            p.add_argument('--previous', action='store_true', help='use the run’s recorded CSV comparison baseline')
        if name == 'anki':
            p.add_argument('--changes-only', action='store_true', help='omit zero planned change counts')
    args = parser.parse_args(argv)
    if getattr(args, 'threshold', 0) < 0:
        parser.error('--threshold must be non-negative')
    try:
        runs = completed_runs(args.runs_dir) if args.run is None else []
        if args.run is None and not runs:
            raise ValueError(f'No completed runs in {args.runs_dir}')
        run = (args.run or runs[-1]).resolve()
        result = query(args.command, run, getattr(args, 'baseline', None), getattr(args, 'regressions', False), getattr(args, 'threshold', 25.0), getattr(args, 'changes_only', False))
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            for key, value in result.items():
                print(f"{key}: {json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value if value is not None else 'unavailable'}")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0
