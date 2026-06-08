import appdirs
from pathlib import Path

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep

class DumpSentences(PipelineStep):
	def process(self, artifact: Artifact) -> Artifact:
		base_dir = Path(appdirs.user_cache_dir("tango_miner")) / 'debug'
		base_dir.mkdir(parents=True, exist_ok=True)

		output_path = base_dir / 'sentences.txt'

		with output_path.open("w", encoding="utf-8") as f:
		    for path, sentences in artifact.data:
		        f.write(f"### {path}\n")

		        for sentence in sentences:
		            sentence = sentence.strip()
		            if sentence:
		                f.write(sentence + "\n")

		        f.write("\n\n------------------------\n\n")  # separator between files

		self.done(f"sentences dumped to: {output_path}")

		return artifact