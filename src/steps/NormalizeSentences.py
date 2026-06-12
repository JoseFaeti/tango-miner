import re
import unicodedata

from src.Artifact import Artifact
from src.PipelineStep import PipelineStep
from src.steps.ProcessingStep import ProcessingStep

_RE_WHITESPACE = re.compile(r"\s+")
# _RE_JP_PUNCT_SPACES = re.compile(r"\s*([、。！？「」『』（）])\s*")

class NormalizeSentences(PipelineStep):
	def process(self, artifact: Artifact) -> Artifact:
		self._processing_step = ProcessingStep.SENTENCE_NORMALIZATION

		files: list[tuple[Path, list[str]]] = artifact.data
		results: list[tuple[Path, list[str]]] = []

		for path, sentences in files:
			self.progress(len(results), len(files))

			normalized_sentences = []

			for sentence in sentences:
				normalized = normalize_sentence(sentence)

				if len(normalized) >= 5 and has_at_least_n_hiragana(normalized, 2):
					normalized_sentences.append(normalized)

			results.append((path, normalized_sentences))

		self.done(f"{sum(len(sentences) for _, sentences in results)} sentences normalized.")

		return Artifact(results)


def normalize_sentence(text: str) -> str:
	text = _RE_WHITESPACE.sub("", text)
	text = unicodedata.normalize("NFC", text)
	# text = _RE_JP_PUNCT_SPACES.sub(r"\1", text)

	# unwanted characters
	text = re.sub(r"→+", "", text)

	# remove script control HEX characters
	text = re.sub(r"[A-F0-9]{2}\s", "", text)

	unwanted_prefix_chars = {"×", ">", ")", "）", "∠", "*", "、", "＊", "▶", "・", "∨", "◎"}

	while text and text[0] in unwanted_prefix_chars:
			text = text[1:]

	# remove opening and closing brackets and such
	while True:
		new_text = fix_broken_brackets(text)
		if new_text == text:
				break
		text = new_text

	# limit any character to at most 3 copies
	text = re.sub(r"(.)\1{3,}", r"\1\1\1", text)
	text = re.sub(r"⋯+", "…", text)
	text = re.sub(r"…+", "…", text)
	text = re.sub(r"‥+", "…", text)

	# normalize punctuation
	text = re.sub(r"・・・", "…", text)
	text = re.sub(r"～～～", "～", text)
	text = re.sub(r"…。", "…", text)

	return text


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


def contains_japanese_script(text: str) -> bool:
    return any(
        "\u3040" <= c <= "\u309F"  # Hiragana
        or "\u30A0" <= c <= "\u30FF"  # Katakana
        or "\u4E00" <= c <= "\u9FFF"  # Kanji
        for c in text
    )


def has_at_least_n_hiragana(text: str, n: int) -> bool:
    count = 0

    for c in text:
        if "\u3040" <= c <= "\u309F":
            count += 1
            if count >= n:
                return True

    return False