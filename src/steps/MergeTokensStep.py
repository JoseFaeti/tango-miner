from pathlib import Path

from src.Artifact import Artifact
from src.JMDict import JMDict
from src.PipelineStep import PipelineStep
from src.TokenizedSentence import TokenizedSentence
from src.TokenProvenance import token_components
from src.steps.ProcessingStep import ProcessingStep


PROGRESS_INTERVAL = 200

USEFUL_KANA_ONLY_POS = [
    "expression",
    "adverb (fukushi)",
    "conjunction",
    "interjection",
    "pronoun",
    "pre-noun adjectival",
]

_jmdict = None
_jmdict_path = None


def get_jmdict(path: str | Path | None = None) -> JMDict:
    """Load the dictionary for the requested path, reusing the last instance."""
    global _jmdict, _jmdict_path
    resolved_path = (Path(path).expanduser() if path is not None else Path.home() / "JMdict_e.xml").resolve()
    if _jmdict is None or _jmdict_path != resolved_path:
        dictionary = JMDict(resolved_path)
        _jmdict, _jmdict_path = dictionary, resolved_path
    return _jmdict


class MergeTokensStep(PipelineStep):
    """Merge adjacent Sudachi tokens when their sequence is a JMdict entry."""

    def __init__(self, max_lookahead: int = 10, dictionary_path: str | Path | None = None):
        self._processing_step = ProcessingStep.TOKEN_MERGING
        self.max_lookahead = max_lookahead
        self.dictionary_path = dictionary_path

    def process(self, artifact: Artifact) -> Artifact:
        tokenized_sentences: list[TokenizedSentence] = artifact.data
        jmdict = get_jmdict(self.dictionary_path)
        merged_sentences = []

        for index, sentence in enumerate(tokenized_sentences, 1):
            merged_sentences.append(
                TokenizedSentence(
                    text=sentence.text,
                    tag=sentence.tag,
                    origin=sentence.origin,
                    tokens=combine_tokens_jmdict(sentence.tokens, jmdict, max_lookahead=self.max_lookahead),
                )
            )
            if index % PROGRESS_INTERVAL == 0 and index < len(tokenized_sentences):
                self.progress(index, len(tokenized_sentences))

        self.done(f"{len(merged_sentences)} token streams merged.")
        return Artifact(merged_sentences)


def _get_field(obj, field: str, default=None):
    if isinstance(obj, dict):
        return obj.get(field, default)
    return getattr(obj, field, default)


def _has_useful_kana_only_sense(definitions: list) -> bool:
    for definition in definitions:
        for sense in _get_field(definition, "senses", []):
            pos = _get_field(sense, "pos", [])
            if isinstance(pos, str):
                pos = [pos]
            if any(
                target in part.lower()
                for part in pos
                for target in USEFUL_KANA_ONLY_POS
            ):
                return True
    return False


def _is_prefixed_kana_noun(token_list: list[dict], definitions: list) -> bool:
    """Recognize a lexical kana noun split after a Sudachi prefix."""
    if len(token_list) < 2 or token_list[0]["pos"][0] != "接頭辞":
        return False

    for definition in definitions:
        for sense in _get_field(definition, "senses", []):
            pos = _get_field(sense, "pos", [])
            if isinstance(pos, str):
                pos = [pos]
            if any(part.lower().startswith("noun") for part in pos):
                return True
    return False


def has_kanji(text: str) -> bool:
    return any("一" <= char <= "龯" for char in text)


def combine_tokens_jmdict(
    token_list: list[dict],
    jmdict: JMDict,
    max_lookahead: int = 10,
) -> list[dict]:
    """Greedily merge adjacent lexical compounds recognized by JMdict."""
    merged_tokens = []
    n = len(token_list)
    i = 0

    while i < n:
        match_found = False
        for j in range(min(n, i + max_lookahead), i + 1, -1):
            combined_surface = "".join(token["surface"] for token in token_list[i:j])
            # A merged card must be grounded in the text the learner will see.
            # Matching a concatenation of dictionary lemmas can invent an
            # unrelated word from inflected or grammatical tokens: for example,
            # おまち (polite stem of 待つ) used to become おまつ (black pine).
            # Surface matching still permits genuine compounds such as
            # おまもり, while leaving non-exact sequences as their original
            # tokens for high-recall mining.
            token_string = combined_surface
            definitions = jmdict.lookup_word(token_string)
            if not definitions:
                continue

            if (
                not has_kanji(token_string)
                and not _has_useful_kana_only_sense(definitions)
                and not _is_prefixed_kana_noun(token_list[i:j], definitions)
            ):
                continue

            match_word = jmdict.get_best_entry(token_string)
            if not match_word:
                continue

            jmdict_pos = "exp"
            if hasattr(jmdict, "get_pos"):
                jmdict_pos = jmdict.get_pos(match_word)
            elif hasattr(match_word, "pos"):
                jmdict_pos = match_word.pos

            if isinstance(jmdict_pos, str):
                new_pos = [jmdict_pos, "*", "*", "*", "*", "*"]
            elif isinstance(jmdict_pos, list):
                new_pos = jmdict_pos + ["*"] * max(0, 6 - len(jmdict_pos))
            else:
                new_pos = list(token_list[j - 1]["pos"])

            lemma_str = match_word if isinstance(match_word, str) else token_string
            merged_tokens.append({
                "surface": combined_surface,
                "lemma": lemma_str,
                "base_form": lemma_str,
                "reading": jmdict.get_reading(match_word, token_string),
                "pos": new_pos,
                "components": tuple(
                    component
                    for token in token_list[i:j]
                    for component in token_components(token)
                ),
            })
            i = j
            match_found = True
            break

        if not match_found:
            merged_tokens.append(token_list[i])
            i += 1

    return merged_tokens
