from collections import OrderedDict
from .Artifact import Artifact
from .PipelineStep import PipelineStep
from .TokenizeStep import tokenize

class TokenizeDirectoryStep(PipelineStep):
	def __init__(self, directory):
		self.directory = directory

	def process(self, artifact: Artifact) -> Artifact:
			combined_tokens = OrderedDict()
			single_file_mode = True

			# process directory contents
			for file in self.directory.iterdir():
				if file.is_file() and file.suffix.lower() in {".tsv"}:#, ".txt", ".csv", ".pdf", ".xml", ".html", ".srt"}:
					if single_file_mode:
						print(f"Tokenizing {file}...")
						combined_tokens = tokenize(file, None, combined_tokens)	
			# tokenize(artifact.data, output_path, progress_handler=self.progress)
			return Artifact(combined_tokens)
	