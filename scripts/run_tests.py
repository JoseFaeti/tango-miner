"""Run unittest quietly, printing failure details only when the run fails.

Without arguments, discover the full suite. Pass unittest names or discovery
arguments to select tests, for example: python scripts/run_tests.py tests.test_run_report
"""
import contextlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Make project modules importable when invoked as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main():
    if any(arg in ("-h", "--help") for arg in sys.argv[1:]):
        unittest.main(module=None, argv=sys.argv, exit=False)
        return 0
    output = io.StringIO()
    # Capture import-time warnings and output as well as test output. unittest's
    # buffer retains each failing test's stdout and stderr in its traceback.
    captured = io.StringIO()
    # Child processes can bypass Python's redirected streams, so capture their
    # inherited file descriptors too. Restore them before reporting failures.
    sys.stdout.flush()
    sys.stderr.flush()
    saved = [os.dup(1), os.dup(2)]
    sink = tempfile.TemporaryFile(mode="w+b")
    os.dup2(sink.fileno(), 1)
    os.dup2(sink.fileno(), 2)
    exit_code = None
    try:
        with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
            runner = unittest.TextTestRunner(stream=output, verbosity=0, buffer=True)
            arguments = sys.argv if len(sys.argv) > 1 else [sys.argv[0], "discover", "-s", "tests"]
            program = unittest.main(module=None, argv=arguments, testRunner=runner, exit=False)
    except SystemExit as exc:
        exit_code = exc.code or 0
    finally:
        os.dup2(saved[0], 1)
        os.dup2(saved[1], 2)
        for descriptor in saved:
            os.close(descriptor)
        sink.close()
    if exit_code is not None:
        if exit_code:
            sys.stderr.write(captured.getvalue())
        return exit_code
    if not program.result.wasSuccessful():
        sys.stderr.write(output.getvalue())
        from scripts.check_jmdict import check_dictionary
        matches, message = check_dictionary()
        if not matches:
            sys.stderr.write(message + "\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
