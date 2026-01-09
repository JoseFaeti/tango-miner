from typing import Iterable, Any, List

from .PipelineStep import PipelineStep, ProgressEvent


class Pipeline:
    def __init__(self, steps: Iterable[PipelineStep], on_progress=None):
        self.steps: List[PipelineStep] = list(steps)
        self.on_progress = on_progress

        for step in steps:
            step._progress_handler = self._handle_progress


    def run(self, initial_input: Any) -> Any:
        data = initial_input
        tmpdir = data.tmpdir

        for step in self.steps:
            data = step.process(data)
            data.tmpdir = tmpdir
            
        return data


    def _handle_progress(self, event: ProgressEvent):
        if self.on_progress:
            self.on_progress(event.step, event.current, event.total)
