"""Console rendering for pipeline lifecycle and progress events."""
import re
import os
import shutil
import unicodedata
import time
from contextlib import contextmanager, redirect_stdout


_LABELS = {
    "GatherInputFilesStep": "Finding input files",
    "ReadFilesStep": "Reading files",
    "ExtractSentencesStep": "Extracting sentences",
    "NormalizeSentences": "Normalizing sentences",
    "RawTokenizeStep": "Tokenizing",
    "GrammarStep": "Finding grammar occurrences",
    "GrammarShadowStep": "Writing grammar proposals",
    "ExcludeGrammarTokensStep": "Reserving grammar tokens",
    "MergeTokensStep": "Merging token compounds",
    "OccurrenceDiagnosticsStep": "Writing occurrence diagnostics",
    "GrammarPositionStep": "Calculating grammar positions",
    "BuildWordStatsStep": "Collecting tokens",
    "FilterFrequencyStep": "Filtering useful vocabulary",
    "AddDefinitionsStep": "Adding definitions",
    "AddGrammarCardsStep": "Adding grammar cards",
    "ScoreWordStep": "Calculating scores",
    "AttachSentencesStep": "Adding sentences",
    "WriteOutputStep": "Writing output",
    "CompareOutputStep": "Comparing output",
    "AddWordsToAnkiStep": "Sending words to Anki",
    "DumpSentences": "Writing sentence debug output",
}


class _StepOutput:
    def __init__(self, display):
        self.display = display

    def write(self, text):
        if text:
            self.display.before_step_output()
        if self.display.log is not None:
            self.display.log.write(text)
            self.display.log.flush()
        return self.display.stream.write(text)

    def flush(self):
        self.display.stream.flush()

    def __getattr__(self, name):
        return getattr(self.display.stream, name)


class ConsoleProgress:
    def __init__(self, stream, total, verbose=False, log=None):
        self.stream, self.total, self.verbose, self.log = stream, total, verbose, log
        self.width = 0
        self.visible = False
        self.started_at = None
        self.index = 0
        self.label = ""
        self.detail = ""

    @property
    def prefix(self):
        elapsed = max(0, int(time.monotonic() - self.started_at)) if self.started_at is not None else 0
        minutes, seconds = divmod(elapsed, 60)
        return f"[{minutes:02d}:{seconds:02d} {self.index}/{self.total}] {self.label}"

    def _line(self, text):
        # Reserve the last column to avoid terminal wrapping, which a carriage
        # return cannot undo. Japanese characters usually occupy two columns.
        try:
            columns = os.get_terminal_size(self.stream.fileno()).columns
        except (AttributeError, OSError, ValueError):
            columns = shutil.get_terminal_size(fallback=(120, 24)).columns
        limit = max(1, columns - 1)
        text = " ".join(str(text).splitlines())
        chars = []
        width = 0
        for char in text:
            size = 0 if unicodedata.combining(char) else (2 if unicodedata.east_asian_width(char) in "WF" else 1)
            if width + size > limit:
                break
            chars.append(char)
            width += size
        self.stream.write("\r" + "".join(chars) + " " * max(0, self.width - width))
        self.stream.flush()
        self.width = max(self.width, width)
        self.visible = True

    def before_step_output(self):
        if self.visible:
            self.stream.write("\n")
            self.stream.flush()
            self.visible = False
            self.width = 0

    def _log_line(self, text):
        if self.log is not None:
            self.log.write(" ".join(str(text).splitlines()) + "\n")
            self.log.flush()

    def stage(self, index, step, record):
        if record["status"] == "running":
            name = type(step).__name__
            label = _LABELS.get(name, re.sub(r"(?<!^)(?=[A-Z])", " ", name.removesuffix("Step")))
            if self.started_at is None:
                self.started_at = time.monotonic()
            self.index = index
            self.label = label
            self.detail = ""
            self._log_line(self.prefix + "...")
            self._line(self.prefix + "...")
        else:
            status = record["status"]
            duration = record.get("duration_seconds", 0)
            suffix = f"{status} in {duration:.1f} seconds."
            detail = record.get("message", self.detail)
            summary = f"{self.prefix}... {suffix}" + (f" {detail}" if detail else "")
            self._log_line(summary)
            if self.verbose or status == "failed":
                if self.visible:
                    self.stream.write("\r" + " " * self.width + "\r")
                    self.visible = False
                    self.width = 0
                self.stream.write(summary + "\n")
                self.stream.flush()
            else:
                self._line(summary)

    def progress(self, step, amount, total, duration=None, additional_text=""):
        self.detail = additional_text
        if total and duration is None and not (amount == 0 and total == 1 and additional_text):
            text = f"{self.prefix}... {amount / total:.1%}"
        else:
            text = self.prefix + "..."
        if additional_text:
            text += " " + additional_text
        self._line(text)

    @contextmanager
    def capture_step_output(self):
        try:
            with redirect_stdout(_StepOutput(self)):
                yield
        finally:
            if self.visible:
                if self.verbose:
                    self.before_step_output()
                else:
                    self.stream.write("\r" + " " * self.width + "\r")
                    self.stream.flush()
                    self.visible = False
                    self.width = 0
