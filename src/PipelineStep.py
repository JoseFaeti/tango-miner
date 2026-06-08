import time

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from src.steps.ProcessingStep import ProcessingStep

class PipelineStep(ABC):
    _progress_handler = None
    _processing_step = None
    elapsed_time = 0


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
            self.elapsed_time = self.end_time - self.start_time
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
