from abc import ABC, abstractmethod
from typing import Any

class PipelineStep(ABC):
    @abstractmethod
    def process(self, input_data: Any) -> Any:
        """
        Take input_data (object, path, whatever),
        return output_data (object, path, whatever).
        """
        pass


class NoOpStep(PipelineStep):
    def process(self, input_data: Any) -> Any:
        return input_data


class DebugStep(PipelineStep):
    def __init__(self, name: str):
        self.name = name

    def process(self, input_data: Any) -> Any:
        print(f"[{self.name}] received:", input_data)
        return input_data
