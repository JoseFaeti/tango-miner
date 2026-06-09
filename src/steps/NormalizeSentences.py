import re
import unicodedata

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep

_RE_SPACES = re.compile(r"[ \t\u3000]+")
_RE_WHITESPACE = re.compile(r"\s+")
_RE_JP_PUNCT_SPACES = re.compile(r"\s*([、。！？「」『』])\s*")

class NormalizeSentences(PipelineStep):
	def process(self, artifact: Artifact) -> Artifact:
		self._processing_step = ProcessingStep.SENTENCE_NORMALIZATION

		files: list[tuple[Path, list[str]]] = artifact.data
		results: list[tuple[Path, list[str]]] = []

		for path, sentences in files:
			self.progress(len(results), len(files))

			normalized_sentences = [
				normalize_sentence(sentence)
				for sentence in sentences
			]

			results.append((path, normalized_sentences))

		self.done()

		return Artifact(results)


def normalize_sentence(text: str) -> str:
	text = unicodedata.normalize("NFKC", text)
	text = _RE_SPACES.sub("　", text)
	text = _RE_WHITESPACE.sub("　", text)
	text = _RE_JP_PUNCT_SPACES.sub(r"\1", text)

	# fix broken bracket cases
	if text.startswith("「") and "」" not in text:
		text = text[1:]
	if text.startswith("『") and "』" not in text:
		text = text[1:]
	if text.endswith("」") and "「" not in text:
		text = text[:-1]
	if text.endswith("』") and "『" not in text:
		text = text[:-1]

	# compress long punctuation runs
	text = re.sub(r"・{3,}", "・・・", text)
	text = re.sub(r"ー{3,}", "ーーー", text)

	return text.strip()