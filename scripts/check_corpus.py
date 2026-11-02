"""Run a corpus check with Anki disabled, a saved log and compact comparison."""
import argparse
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.RunConfig import resolve_config
from scripts.compare_runs import compact_report, completed_runs, load_run


def config_toml(config):
    """Serialize the resolved, validated settings without optional None values."""
    data = asdict(config)
    result = [f"config_version = {data.pop('config_version')}"]
    for section, fields in data.items():
        result.append(f'\n[{section}]')
        for key, value in fields.items():
            if value is not None:
                result.append(f'{key} = {json.dumps(value, ensure_ascii=False)}')
    return '\n'.join(result) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=ROOT / 'config.toml')
    parser.add_argument('--input', type=Path, help='override configured corpus path')
    parser.add_argument('--output-dir', type=Path, required=True, help='directory for run archive and log')
    parser.add_argument('--baseline', type=Path, help='completed run to compare against')
    parser.add_argument('--word', action='append', default=[], help='candidate to include in the compact comparison')
    parser.add_argument('--occurrence-diagnostics', action='store_true', help='save detailed occurrence data (can be very large)')
    parser.add_argument('--retokenize', action='store_true', help='regenerate raw tokens')
    parser.add_argument('--no-grammar', action='store_true', help='disable grammar cards for this check')
    args = parser.parse_args(argv)
    try:
        config = resolve_config(args.config)
        config = replace(config, anki=replace(config.anki, enabled=False))
        if not args.input and not config.input.path:
            raise ValueError('Set input.path in the config or pass --input')
        baseline = load_run(args.baseline) if args.baseline else None
        output = args.output_dir.resolve()
        output.mkdir(parents=True, exist_ok=True)
        archives = output / 'runs'
        old = set(completed_runs(archives)) if archives.exists() else set()
        fd, log_name = tempfile.mkstemp(prefix='tango-check-', suffix='.log', dir=output)
        with tempfile.TemporaryDirectory(prefix='tango-check-config-') as temp:
            config_path = Path(temp) / 'config.toml'
            config_path.write_text(config_toml(config), encoding='utf-8')
            # Validate the exact file passed to the child and fail closed.
            if resolve_config(config_path).anki.enabled:
                raise ValueError('Could not disable Anki for the check')
            command = [sys.executable, '-X', 'utf8', str(ROOT / 'tango_miner.py'),
                       '--config', str(config_path), '--output', str(output), '--silent']
            if args.input:
                command += ['--input', str(args.input.resolve())]
            if args.occurrence_diagnostics:
                command.append('--occurrence-diagnostics')
            if args.retokenize:
                command.append('--retokenize')
            if args.no_grammar:
                command.append('--no-grammar')
            print(f'Running with Anki disabled. Log: {log_name}', flush=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as log:
                result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=False)
        if result.returncode:
            print(f'Pipeline failed with exit code {result.returncode}; see log: {log_name}', file=sys.stderr)
            return result.returncode if result.returncode > 0 else 1
        new = set(completed_runs(archives)) - old
        if len(new) != 1:
            raise ValueError(f'Expected one new completed archive, found {len(new)}; see log: {log_name}')
        run_path, = new
        run = load_run(run_path)
        print(f'Completed: {run_path}')
        if baseline:
            compact_report(baseline, run, args.word, 5)
        else:
            valid = sum(r['status'] == 'valid' for r in run['rows'].values())
            print(f"{valid} valid, {len(run['rows']) - valid} dropped/review; Anki: {run['summary'].get('anki', {}).get('status', 'unknown')}")
        return 0
    except (OSError, ValueError, KeyError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
