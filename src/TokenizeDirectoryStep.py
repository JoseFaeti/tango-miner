import appdirs

from collections import OrderedDict
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
  def __init__(self, directory):
      self.directory = directory

  def process(self, artifact: Artifact) -> Artifact:
      combined_tokens = OrderedDict()
      cache_dir = Path(appdirs.user_cache_dir("tango_miner"))

      files = [
          f for f in self.directory.iterdir()
          if f.is_file() and f.suffix.lower() in ALLOWED_FILE_EXTENSIONS
      ]

      total = len(files)

      for i, file in enumerate(files, start=0):
          self.progress(
              ProcessingStep.TOKENIZING,
              i,
              total,
              str(file.name)
          )

          combined_tokens = tokenize(file, None, combined_tokens, cache_dir=cache_dir)
          
      self.progress(
          ProcessingStep.TOKENIZING,
          total,
          total
      )

      return Artifact(combined_tokens)

  