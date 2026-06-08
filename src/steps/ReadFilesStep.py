from pathlib import Path

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep


class ReadFilesStep(PipelineStep):
    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding
        self._processing_step = ProcessingStep.READ_FILES

    def process(self, artifact: Artifact) -> Artifact:
        files: list[Path] = artifact.data
        total = len(files)
        results: list[tuple[Path, str]] = []

        for i, path in enumerate(files):
            self.progress(i, total, path.name)
            text = path.read_text(encoding=self.encoding)
            results.append((path, text))

        self.done()

        return Artifact(results)