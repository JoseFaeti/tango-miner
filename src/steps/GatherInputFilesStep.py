from pathlib import Path

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep

ALLOWED_FILE_EXTENSIONS = {
    ".tsv",
    ".txt",
    ".csv",
    ".pdf",
    ".xml",
    ".html",
    ".srt",
}


class GatherInputFilesStep(PipelineStep):
    def __init__(self, input_path: Path, include_subdirectories: bool = False, extensions=None):
        self.input_path = Path(input_path)
        self.include_subdirectories = include_subdirectories
        self.extensions = ALLOWED_FILE_EXTENSIONS if extensions is None else set(extensions)

    def process(self, artifact: Artifact) -> Artifact:
        if self.input_path.is_file():
            files = [self.input_path]
        elif self.input_path.is_dir():
            files = list(_iter_input_files(self.input_path, self.include_subdirectories, self.extensions))
        else:
            raise FileNotFoundError(f"Input path not found: {self.input_path}")

        return Artifact(files)


def _iter_input_files(directory: Path, include_subdirectories: bool, extensions=ALLOWED_FILE_EXTENSIONS):
    for f in directory.iterdir():
        if f.is_file() and f.suffix.lower() in extensions:
            yield f
        elif include_subdirectories and f.is_dir():
            yield from _iter_input_files(f, include_subdirectories=True, extensions=extensions)