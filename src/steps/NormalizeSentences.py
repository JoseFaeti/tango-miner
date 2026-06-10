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
	text = text.strip()
	text = unicodedata.normalize("NFKC", text)
	text = _RE_SPACES.sub("　", text)
	text = _RE_WHITESPACE.sub("　", text)
	text = _RE_JP_PUNCT_SPACES.sub(r"\1", text)

	if text.startswith((">", ")", "∠", "*", "、")):
		text = text[1:]

	# remove opening and closing brackets and such
	while True:
		new_text = fix_broken_brackets(text)
		if new_text == text:
				break
		text = new_text

	# compress long punctuation runs
	text = re.sub(r"・{3,}", "・・・", text)
	text = re.sub(r"ー{3,}", "ーーー", text)
	text = re.sub(r"⋯+", "⋯", text)

	return text.strip()


BRACKET_PAIRS = [
		("「", "」"),
		("『", "』"),
		("（", "）"),
		("(", ")"),
		("［", "］"),
		("[", "]"),
		("【", "】"),
		("〈", "〉"),
		("《", "》"),
		("〔", "〕"),
		("｛", "｝"),
		("{", "}"),
		("＜", "＞"),
		("<", ">"),
		("“", "”"),
		("‘", "’"),
		("\"", "\""),
		("'", "'"),
]


def fix_broken_brackets(text: str) -> str:
		for opening, closing in BRACKET_PAIRS:
				# Opening bracket at start without matching closing
				if text.startswith(opening) and closing not in text:
						text = text[len(opening):]

				# Closing bracket at end without matching opening
				if text.endswith(closing) and opening not in text:
						text = text[:-len(closing)]

				# Sentence fully wrapped by exactly one pair
				if (
						text.startswith(opening)
						and text.endswith(closing)
						and text.count(opening) == 1
						and text.count(closing) == 1
				):
						text = text[len(opening):-len(closing)]

		return text