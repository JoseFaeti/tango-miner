import time
from typing import Iterable, Any, List

from src.PipelineStep import PipelineStep, ProgressEvent


class Pipeline:
    def __init__(self, steps: Iterable[PipelineStep], on_progress=None):
        self.steps: List[PipelineStep] = list(steps)
        self.on_progress = on_progress

        for step in self.steps:
            step._progress_handler = self._handle_progress


    def run(self, initial_input: Any) -> Any:
        self.start_time = time.perf_counter()

        data = initial_input
        tmpdir = data.tmpdir

        for step in self.steps:
            step.start_time = time.perf_counter()

            data = step.process(data)
            data.tmpdir = tmpdir
        
        self.end_time = time.perf_counter()
        self.duration = self.end_time - self.start_time
            
        return data


    def _handle_progress(self, event: ProgressEvent):
        if self.on_progress:
            self.on_progress(event.step, event.current, event.total, duration=event.duration, additional_text=event.message)
