from typing import Iterable, Any, List
from src.PipelineStep import PipelineStep

class Pipeline:
    def __init__(self, steps: Iterable[PipelineStep]):
        self.steps: List[PipelineStep] = list(steps)

    def run(self, initial_input: Any) -> Any:
        data = initial_input

        for step in self.steps:
            data = step.process(data)

        return data
