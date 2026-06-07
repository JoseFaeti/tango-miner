from pathlib import Path

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep


class ReadFilesStep(PipelineStep):
    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    def process(self, artifact: Artifact) -> Artifact:
        files: list[Path] = artifact.data
        total = len(files)
        results: list[tuple[Path, str]] = []

        for i, path in enumerate(files):
            self.progress(ProcessingStep.TOKENIZING, i, total, path.name)
            text = path.read_text(encoding=self.encoding)
            results.append((path, text))

        return Artifact(results)