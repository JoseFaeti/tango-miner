import time

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from src.steps.ProcessingStep import ProcessingStep

class PipelineStep(ABC):
    _progress_handler = None
    _processing_step = None
    elapsed_time = 0


    def add_performance_time(self, name, seconds):
        """Accumulate a named elapsed-time measurement when profiling is enabled."""
        if getattr(self, '_performance_enabled', False):
            if not name or name == 'total':
                raise ValueError('Phase name must be nonempty and cannot be total')
            entries = self._performance_entries
            entries[name] = entries.get(name, 0.0) + seconds

    @contextmanager
    def performance_phase(self, name):
        """Measure a named phase, including partial work when it fails."""
        if not getattr(self, '_performance_enabled', False):
            yield
            return
        start = time.perf_counter()
        try:
            yield
        finally:
            self.add_performance_time(name, time.perf_counter() - start)

    @abstractmethod
    def process(self, input_data: Any) -> Any:
        """
        Take input_data (object, path, whatever),
        return output_data (object, path, whatever).
        """
        raise NotImplementedError()


    def progress(self, current, total, message=""):
        if self._progress_handler:
            if current >= total:
                self.done(message)
            else:
                self._progress_handler(ProgressEvent(self._processing_step, current, total, message=message))


    def done(self, message=""):
        if self.start_time:
            self.end_time = time.perf_counter()
            self.elapsed_time = max(0.0, self.end_time - self.start_time - getattr(self, 'wait_seconds', 0.0))
            # print(f"yay self.start_time! {self.start_time} {self.end_time} {self.elapsed_time}")

        if self._progress_handler:
            self._progress_handler(ProgressEvent(self._processing_step, 1, 1, self.elapsed_time, message))


@dataclass
class ProgressEvent:
    step: ProcessingStep
    current: int
    total: int
    duration: float | None = None
    message: str = ""


class NoOpStep(PipelineStep):
    def process(self, input_data: Any) -> Any:
        return input_data


class DebugStep(PipelineStep):
    def __init__(self, name: str):
        self.name = name

    def process(self, input_data: Any) -> Any:
        # print(f"[{self.name}] received:", input_data)
        return input_data
