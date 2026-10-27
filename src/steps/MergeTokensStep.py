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


def _is_split_kana_noun(tokens: list[dict], definitions: list) -> bool:
    """Recover a noun split into unchanged noun and noun-suffix surfaces."""
    if len(tokens) != 2:
        return False
    first, second = tokens
    surface = first["surface"] + second["surface"]
    if (len(surface) < 3 or not all("ぁ" <= char <= "ん" for char in surface)
            or first["pos"][0] != "名詞" or first["pos"][1] != "普通名詞"
            or tuple(second["pos"][:2]) != ("接尾辞", "名詞的")
            or any(token["surface"] != token["lemma"] for token in tokens)):
        return False
    return any(
        part.lower().startswith("noun")
        for definition in definitions
        for sense in _get_field(definition, "senses", [])
        for part in _get_field(sense, "pos", [])
    )


def _crosses_incompatible_grammar(tokens: list[dict], entry) -> bool:
    """Reject lexical readings contradicted by a clear Sudachi boundary."""
    if len(tokens) != 2 or not isinstance(entry, dict):
        return False
    first, second = tokens
    senses = entry.get("senses", [])
    tags = [part.lower() for sense in senses for part in sense.get("pos", [])]
    if not tags:
        return False

    # Negative auxiliary + connective particle is not a lexical conjunction.
    # Entries with an auxiliary/expression sense (e.g. ないで) remain eligible.
    if ((first["pos"][0] == "助動詞"
         or (first["pos"][0] == "形容詞" and first["lemma"] == "ない"))
            and second["pos"][0] == "助詞"
            and all(tag == "conjunction" for tag in tags)):
        return True

    # A noun followed by the inflected stem of する is not a noun or
    # adverb formed by joining the two surfaces (こと + し != ことし).
    if (first["pos"][0] == "名詞" and second["pos"][0] == "動詞"
            and second["lemma"] == "する" and second["surface"] != "する"
            and all(tag.startswith("noun") or tag.startswith("adverb") for tag in tags)
            and not any("verb" in tag and not tag.startswith("adverb") or "expression" in tag for tag in tags)):
        return True
    return False


def _repair_kotoshi_boundaries(token_list: list[dict]) -> list[dict]:
    """Split ことし when the following token confirms する inflection."""
    result = []
    for index, token in enumerate(token_list):
        following = token_list[index + 1] if index + 1 < len(token_list) else None
        if (token["surface"] == "ことし" and token["lemma"] == "ことし"
                and token["pos"][0] == "名詞" and following
                and following["surface"] in {"て", "ても", "ない", "なくても", "なくちゃ", "なければ", "ちゃあ"}):
            evidence = token_components(token)
            result.extend([
                {"surface": "こと", "lemma": "こと", "base_form": "こと", "reading": "こと",
                 "pos": ("名詞", "普通名詞", "一般", "*", "*", "*"), "components": evidence},
                {"surface": "し", "lemma": "する", "base_form": "する", "reading": "する",
                 "pos": ("動詞", "非自立可能", "*", "*", "サ行変格", "連用形-一般"), "components": evidence},
            ])
        else:
            result.append(token)
    return result


def _repair_imperative(token_list: list[dict], index: int, jmdict) -> dict:
    """Repair a potential-lemma analysis of a clearly delimited imperative."""
    token = token_list[index]
    lemma = token["lemma"]
    if (index + 1 >= len(token_list) or token["pos"][0] != "動詞"
            or not lemma.endswith("る") or token["surface"] != lemma[:-1]
            or token_list[index + 1]["surface"] not in {"。", "！", "!", "？", "?", "、", "…", "って"}
            or jmdict.lookup_word(lemma)):
        return token
    from src.steps.TokenizeStep import get_tokenizer, TOKENIZER_MODE, kata_to_hira
    nodes = get_tokenizer().tokenize(lemma, TOKENIZER_MODE)
    if len(nodes) != 1 or nodes[0].dictionary_form() != lemma:
        return token
    base = nodes[0].normalized_form()
    endings = {"う": "える", "く": "ける", "ぐ": "げる", "す": "せる", "つ": "てる",
               "ぬ": "ねる", "ぶ": "べる", "む": "める", "る": "れる"}
    if not base or base[-1] not in endings or base[:-1] + endings[base[-1]] != lemma:
        return token
    # Restrict to godan verbs: the imperative of an ichidan verb differs.
    base_nodes = get_tokenizer().tokenize(base, TOKENIZER_MODE)
    if len(base_nodes) != 1 or "五段" not in base_nodes[0].part_of_speech()[4]:
        return token
    reading = kata_to_hira(base_nodes[0].reading_form())
    entries = jmdict.lookup_word(base)
    if not any(isinstance(entry, dict) and any(r["form"] == reading for r in entry.get("reading", []))
               and any("verb" in tag.lower() and not tag.lower().startswith(("adverb", "noun", "auxiliary"))
                       for sense in entry.get("senses", []) for tag in sense.get("pos", []))
               for entry in entries):
        return token
    return {**token, "lemma": base, "base_form": base, "reading": reading,
            "reassigned_from": lemma, "components": token_components(token)}


def has_kanji(text: str) -> bool:
    return any("一" <= char <= "龯" for char in text)


def combine_tokens_jmdict(
    token_list: list[dict],
    jmdict: JMDict,
    max_lookahead: int = 10,
) -> list[dict]:
    """Greedily merge adjacent lexical compounds recognized by JMdict."""
    token_list = _repair_kotoshi_boundaries(token_list)
    token_list = [_repair_imperative(token_list, i, jmdict) for i in range(len(token_list))]
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
                and not _is_split_kana_noun(token_list[i:j], definitions)
            ):
                continue

            match_word = jmdict.get_best_entry(token_string)
            if not match_word:
                continue
            if _crosses_incompatible_grammar(token_list[i:j], match_word):
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
