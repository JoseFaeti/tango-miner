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
  ".srt"}

class TokenizeDirectoryStep(PipelineStep):
  def __init__(self, directory, include_subdirectories=False):
      self.directory = directory
      self.include_subdirectories = include_subdirectories

  def process(self, artifact: Artifact) -> Artifact:
      combined_tokens = {}
      cache_dir = Path(appdirs.user_cache_dir("tango_miner"))

      self.total = count_files(self.directory, self.include_subdirectories)
      self.total_tokens = 0
      self.index = 0
      self.current_file = None

      for dir_path, file_path in iter_input_files(self.directory, include_subdirectories=self.include_subdirectories):
          self.current_file = Path(file_path)

          self.progress(
              ProcessingStep.TOKENIZING,
              self.index,
              self.total,
              f'{len(combined_tokens)} tokens ({self.current_file.name})'
          )

          combined_tokens = tokenize(file_path, None, combined_tokens, cache_dir=cache_dir, progress_handler=self.tokenizer_progress)
          self.index += 1
          self.total_tokens = len(combined_tokens)
          
      self.progress(
          ProcessingStep.TOKENIZING,
          1,
          1,
          f'{len(combined_tokens)} tokens from {self.index} files'
      )

      return Artifact(combined_tokens)


  def tokenizer_progress(self, step, current, total, message=""):
      self.progress(step, (current/total*100) + 100 * self.index, 100 * self.total, f'{self.total_tokens} tokens ({self.current_file.name})')

  
def iter_input_files(directory: Path, *, include_subdirectories: bool):
    """
    Recursively yields (directory_path, file_path) tuples.

    directory_path: the folder containing the file
    file_path: the actual file
    """
    for f in directory.iterdir():
        if f.is_file() and f.suffix.lower() in ALLOWED_FILE_EXTENSIONS:
            yield directory, f
        elif include_subdirectories and f.is_dir():
            # Recurse into subdirectory
            yield from iter_input_files(f, include_subdirectories=True)


def count_files(directory: Path, include_subdirectories: bool):
    total = 0
    for _ in iter_input_files(directory, include_subdirectories=include_subdirectories):
        total += 1
    return total
