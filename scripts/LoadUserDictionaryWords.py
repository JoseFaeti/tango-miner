
from src.Artifact import Artifact
from src.PipelineStep import PipelineStep

class LoadUserDictionaryWords(PipelineStep):
	def __init__(self):
		pass

	def process(self, artifact: Artifact) -> Artifact:
		return Artifact()