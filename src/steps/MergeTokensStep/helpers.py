"""Dictionary eligibility and token utilities for lexical merging."""

USEFUL_KANA_ONLY_POS = [
    "expression",
    "adverb (fukushi)",
    "conjunction",
    "interjection",
    "pronoun",
    "pre-noun adjectival",
]

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

def _is_contracted_kana_verb(tokens: list[dict], entry) -> bool:
    """Keep an attested abbreviated verb split into a verb and auxiliary."""
    if len(tokens) != 2 or not isinstance(entry, dict):
        return False
    verb, auxiliary = tokens
    if (verb['pos'][:1] not in (('動詞',), ['動詞'])
            or auxiliary['pos'][:1] not in (('助動詞',), ['助動詞'])
            or auxiliary['lemma'] not in {'とく', 'どく', 'ちゃう', 'じゃう', 'てる', 'でる'}
            or auxiliary['surface'] != auxiliary['lemma']):
        return False
    return any('abbreviation' in sense.get('misc', [])
               and any('verb' in tag.lower() and not tag.lower().startswith('adverb')
                       for tag in sense.get('pos', []))
               for sense in entry.get('senses', []))

def has_kanji(text: str) -> bool:
    return any("一" <= char <= "龯" for char in text)


def is_dictionary_expression(entry) -> bool:
    """Return whether a dictionary entry has an expression sense."""
    return isinstance(entry, dict) and any(
        "expression" in tag.lower() for sense in entry.get("senses", [])
        for tag in sense.get("pos", []))
