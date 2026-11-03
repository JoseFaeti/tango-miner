"""Shared command-line behavior for the exposed script bridges."""
import contextlib
import os
from pathlib import Path
import runpy
import sys
import traceback


_SIMPLE_HELP = {
    'next_grammar': 'Print the next unchecked grammar point as JSON.',
    'check_grammar_docs': 'Check grammar indexes and point documentation.',
    'run_tests': 'Run tests quietly; pass unittest names or discovery arguments.',
}


def _execute(command, help_requested):
    if help_requested and command in _SIMPLE_HELP:
        print(f'usage: python -m tango_miner.{command} [--help] [--silent]'
              + (' [unittest arguments]' if command == 'run_tests' else ''))
        print(_SIMPLE_HELP[command])
        return
    if command == 'inspect_run':
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
    try:
        runpy.run_module(f'scripts.{command}', run_name='__main__')
    except SystemExit as exc:
        if help_requested and exc.code in (None, 0):
            print('  --silent  suppress all console output')
        raise
    if help_requested:
        print('  --silent  suppress all console output')


def run(command):
    # Do not consume arguments after the conventional end-of-options marker.
    args = sys.argv[1:]
    end = args.index('--') if '--' in args else len(args)
    options, rest = args[:end], args[end:]
    silent = '--silent' in options
    help_requested = '--help' in options
    sys.argv[1:] = [arg for arg in options if arg != '--silent'] + rest
    if not silent:
        _execute(command, help_requested)
        return

    # Redirect descriptors as well as Python streams, including subprocess output.
    sys.stdout.flush()
    sys.stderr.flush()
    saved = [os.dup(1), os.dup(2)]
    try:
        with open(os.devnull, 'w', encoding='utf-8') as sink:
            os.dup2(sink.fileno(), 1)
            os.dup2(sink.fileno(), 2)
            with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                try:
                    _execute(command, help_requested)
                except SystemExit as exc:
                    if exc.code is not None and not isinstance(exc.code, int):
                        print(exc.code, file=sys.stderr)
                        raise SystemExit(1)
                    raise
                except Exception:
                    traceback.print_exc()
                    raise SystemExit(1)
    finally:
        for fd, original in enumerate(saved, start=1):
            os.dup2(original, fd)
            os.close(original)
