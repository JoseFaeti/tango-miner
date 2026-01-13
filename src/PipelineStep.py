from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from .ProcessingStep import ProcessingStep

class PipelineStep(ABC):
    def __init__(self):
        self._progress_handler = None


    @abstractmethod
    def process(self, input_data: Any) -> Any:
        """
        Take input_data (object, path, whatever),
        return output_data (object, path, whatever).
        """
        pass


    def progress(self, step, current, total, message=""):
        if self._progress_handler:
            self._progress_handler(ProgressEvent(step, current, total, message))


@dataclass
class ProgressEvent:
    step: ProcessingStep
    current: int
    total: int
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
