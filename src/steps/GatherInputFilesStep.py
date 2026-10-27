from pathlib import Path
import re

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
    def __init__(self, input_path: Path, include_subdirectories: bool = False, extensions=None, excluded_directory=None, manifest_path=None):
        self.input_path = Path(input_path)
        self.excluded_directory = Path(excluded_directory).resolve() if excluded_directory else None
        self.manifest_path = manifest_path
        self.include_subdirectories = include_subdirectories
        self.extensions = ALLOWED_FILE_EXTENSIONS if extensions is None else set(extensions)

    def process(self, artifact: Artifact) -> Artifact:
        if self.input_path.is_file():
            files = [self.input_path]
        elif self.input_path.is_dir():
            files = list(_iter_input_files(self.input_path, self.include_subdirectories, self.extensions, self.excluded_directory))
        else:
            raise FileNotFoundError(f"Input path not found: {self.input_path}")

        if self.manifest_path is not None:
            from src.RunArchive import write_input_manifest
            write_input_manifest(files, self.manifest_path)
        return Artifact(files)


def _is_run_archive(directory: Path) -> bool:
    """Identify our saved runs without excluding unrelated folders named runs."""
    return (directory.parent.name.lower() == "runs"
            and re.match(r"^\d{4}-\d{2}-\d{2}_\d{4}_.+", directory.name) is not None
            and (directory / "config.json").is_file()
            and (directory / "notes.txt").is_file())


def _iter_input_files(directory: Path, include_subdirectories: bool, extensions=ALLOWED_FILE_EXTENSIONS, excluded_directory=None):
    for f in directory.iterdir():
        if f.is_file() and f.suffix.lower() in extensions:
            yield f
        elif (include_subdirectories and f.is_dir() and f.resolve() != excluded_directory
              and not _is_run_archive(f)):
            yield from _iter_input_files(f, include_subdirectories=True, extensions=extensions, excluded_directory=excluded_directory)