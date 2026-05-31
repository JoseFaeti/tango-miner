import appdirs

from pathlib import Path

from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .ProcessingStep import ProcessingStep
from .TokenizeStep import tokenize

ALLOWED_FILE_EXTENSIONS = {
    ".tsv",
    ".txt",
    ".csv",
    ".pdf",
    ".xml",
    ".html",
    ".srt",
}


class TokenizeDirectoryStep(PipelineStep):
    def __init__(self, directory, include_subdirectories=False):
        self.directory = directory
        self.include_subdirectories = include_subdirectories

    def process(self, artifact: Artifact) -> Artifact:
        combined_tokens = {}
        combined_sentences = []
        cache_dir = Path(appdirs.user_cache_dir("tango_miner"))

        total_files = count_files(self.directory, self.include_subdirectories)
        total_tokens = 0
        file_index = 0
        current_file = None

        for _dir_path, file_path in iter_input_files(self.directory, self.include_subdirectories):
            current_file = Path(file_path)

            self.progress(
                ProcessingStep.TOKENIZING,
                file_index,
                total_files,
                f"{total_tokens} tokens ({current_file.name})",
            )

            def file_progress(step, current, total, message=""):
                self.progress(
                    step,
                    (current / total * 100) + 100 * file_index,
                    100 * total_files,
                    f"{total_tokens} tokens ({current_file.name})",
                )

            combined_tokens, combined_sentences = tokenize(
                file_path,
                combined_tokens,
                combined_sentences,
                cache_dir=cache_dir,
                progress_handler=file_progress,
            )

            file_index += 1
            total_tokens = len(combined_tokens)

        self.progress(
            ProcessingStep.TOKENIZING,
            1,
            1,
            f"{total_tokens} tokens, {len(combined_sentences)} sentences from {file_index} files",
        )

        return Artifact(combined_tokens, sentences=combined_sentences)


def iter_input_files(directory: Path, include_subdirectories: bool):
    """
    Yields (directory_path, file_path) tuples for all processable files.
    """
    for f in directory.iterdir():
        if f.is_file() and f.suffix.lower() in ALLOWED_FILE_EXTENSIONS:
            yield directory, f
        elif include_subdirectories and f.is_dir():
            yield from iter_input_files(f, include_subdirectories=True)


def count_files(directory: Path, include_subdirectories: bool) -> int:
    return sum(1 for _ in iter_input_files(directory, include_subdirectories))