from pathlib import Path

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.ProcessingStep import ProcessingStep
from src.JMDict import JMDict


class AddDictionaryEntries(PipelineStep):
		def __init__(self, on_entry_processed=None):
				self.on_entry_processed = on_entry_processed

		def process(self, artifact: Artifact) -> Artifact:
				add_dictionary_entries(artifact.data, self.progress, on_entry_processed=self.on_entry_processed)
				return artifact

def add_dictionary_entries(input, progress_handler=None, on_entry_processed=None):
		jmdict = JMDict(Path.home() / "JMdict_e.xml")

		total = len(input)
		found = 0

		for i, word in enumerate(input):
				result = jmdict.get_best_entry(word)

				if result:
						found += 1

				if on_entry_processed:
						on_entry_processed(word, result)

				progress_handler(
						ProcessingStep.DEFINITIONS,
						i,
						total,
						f'{i}/{total} {found} definitions found'
				)