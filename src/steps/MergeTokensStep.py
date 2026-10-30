from pathlib import Path
import json

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

    def __init__(self, max_lookahead: int = 10, dictionary_path: str | Path | None = None, diagnostics_path: Path | None = None):
        self.diagnostics_path = diagnostics_path
        self._processing_step = ProcessingStep.TOKEN_MERGING
        self.max_lookahead = max_lookahead
        self.dictionary_path = dictionary_path

    def process(self, artifact: Artifact) -> Artifact:
        tokenized_sentences: list[TokenizedSentence] = artifact.data
        jmdict = get_jmdict(self.dictionary_path)
        merged_sentences = []

        temporary = Path(self.diagnostics_path).with_suffix(".jsonl.tmp") if self.diagnostics_path is not None else None
        if temporary is not None:
            temporary.write_text("", encoding="utf-8")
        for index, sentence in enumerate(tokenized_sentences, 1):
            sentence_events = [] if temporary is not None else None
            merged_sentences.append(
                TokenizedSentence(
                    text=sentence.text,
                    tag=sentence.tag,
                    origin=sentence.origin,
                    tokens=combine_tokens_jmdict(sentence.tokens, jmdict, max_lookahead=self.max_lookahead, rejected=sentence_events),
                )
            )
            if temporary is not None and sentence_events:
                with temporary.open("a", encoding="utf-8", newline="\n") as stream:
                    for event in sentence_events:
                        event.update(sentence_index=index - 1, sentence=sentence.text, source=str(sentence.origin) if sentence.origin is not None else None)
                        stream.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            if index % PROGRESS_INTERVAL == 0 and index < len(tokenized_sentences):
                self.progress(index, len(tokenized_sentences))

        if temporary is not None:
            temporary.replace(self.diagnostics_path)
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


def _is_inflection_misread_as_noun(tokens, start, end, entry):
    """Keep a verb and passive auxiliary when a following inflection confirms it."""
    if end - start != 2 or end >= len(tokens) or not isinstance(entry, dict):
        return False
    verb, auxiliary = tokens[start:end]
    following = tokens[end]
    tags = [tag.lower() for sense in entry.get('senses', []) for tag in sense.get('pos', [])]
    return (bool(tags) and all(tag.startswith('noun') for tag in tags)
            and verb['pos'][0] == '動詞' and len(verb['pos']) > 5
            and verb['pos'][5].startswith('未然形')
            and auxiliary['pos'][0] == '助動詞' and auxiliary['lemma'] in {'れる', 'られる'}
            and following['lemma'] in {'た', 'て'}
            and following['pos'][0] in {'助動詞', '助詞'})


def _is_auxiliary_oru_misread(tokens, start, end):
    """A continuative verb followed by non-independent おる is not 追手."""
    if start == 0 or end - start != 2:
        return False
    previous = tokens[start - 1]
    verb, particle = tokens[start:end]
    auxiliary = (verb['lemma'] == 'おる' and tuple(verb['pos'][:2]) == ('動詞', '非自立可能')
                 and particle['surface'] == 'て' and particle['pos'][0] == '助詞')
    if not auxiliary:
        return False

    def continuative(token):
        return (token['pos'][0] == '動詞' and len(token['pos']) > 5
                and token['pos'][5].startswith('連用形'))

    if continuative(previous):
        return True
    # A connective て/で can separate the lexical verb from auxiliary おる.
    return (start >= 2 and previous['surface'] in {'て', 'で'}
            and tuple(previous['pos'][:2]) == ('助詞', '接続助詞')
            and continuative(tokens[start - 2]))


def _repair_tsurete_motion(tokens, index, jmdict):
    """Identify taking someone along only with an object and motion verb."""
    token = tokens[index]
    if (token['lemma'] != 'つれる' or token['pos'][0] != '動詞'
            or index + 2 >= len(tokens) or tokens[index + 1]['surface'] != 'て'
            or tokens[index + 1]['pos'][0] != '助詞'
            or tokens[index + 2]['lemma'] not in {'いく', '行く', 'くる', '来る'}
            or tokens[index + 2]['pos'][0] != '動詞'):
        return token
    prior = tokens[max(0, index - 6):index]
    object_positions = [i for i, t in enumerate(prior)
                        if t['surface'] == 'を' and t['pos'][0] == '助詞']
    if not object_positions or any(t['surface'] in {'。', '！', '？', '!', '?', '…'}
                                   for t in prior[object_positions[-1] + 1:]):
        return token
    entries = jmdict.lookup_word('連れる')
    if not any(any(r['form'] == 'つれる' for r in e.get('reading', []))
               and any(tag.lower().startswith('ichidan verb') for sense in e.get('senses', [])
                       for tag in sense.get('pos', [])) for e in entries):
        return token
    return {**token, 'lemma': '連れる', 'base_form': '連れる', 'reading': 'つれる',
            'components': token_components(token), 'analysis_rule': 'object_tsureru_motion'}


def _is_kigakawatta_predicate(tokens: list[dict], start: int, end: int) -> bool:
    """Keep the past verb in a clearly delimited 気が変わった predicate."""
    if start < 2 or end - start != 2:
        return False
    prefix = tokens[start - 2:start]
    verb, auxiliary = tokens[start:end]
    if ([(t['surface'], t['lemma']) for t in prefix] != [('気', '気'), ('が', 'が')]
            or verb['surface'] != '変わっ' or verb['lemma'] != '変わる'
            or verb['pos'][0] != '動詞' or auxiliary['surface'] != 'た'
            or auxiliary['lemma'] != 'た' or auxiliary['pos'][0] != '助動詞'):
        return False
    if end == len(tokens):
        return True
    following = tokens[end]
    return (following['pos'][0] in {'補助記号', '記号'}
            or (following['surface'] in {'か', 'ん'} and following['pos'][0] == '助詞'))


def _is_prenominal_entry_in_past_predicate(tokens: list[dict], start: int, end: int, entry) -> bool:
    """Avoid mistaking a delimited past verb for a prenominal-only entry."""
    if end - start != 2 or end >= len(tokens) or not isinstance(entry, dict):
        return False
    verb, past = tokens[start:end]
    following = tokens[end]
    tags = [tag.lower() for sense in entry.get('senses', []) for tag in sense.get('pos', [])]
    if (not tags or any(tag != 'noun or verb acting prenominally' for tag in tags)
            or verb['pos'][0] != '動詞' or not has_kanji(verb['lemma'])
            or past['pos'][0] != '助動詞' or past['lemma'] != 'た' or past['surface'] != 'た'):
        return False
    # Commas can separate two modifiers (優れた、天才的な発想), and a
    # clipped sentence may stop before the noun. Leave both ambiguous.
    return (following['surface'] in {'。', '！', '？', '!', '?', '…'}
            and following['pos'][0] in {'補助記号', '記号'}
            or (following['surface'] in {'か', 'ん'} and following['pos'][0] == '助詞'))


def _repair_komatta_koto_boundary(tokens: list[dict]) -> list[dict]:
    """Expose the problem noun inside Sudachi's merged ことがある token."""
    result = []
    for i, token in enumerate(tokens):
        if (i >= 2 and i + 1 < len(tokens)
                and tokens[i - 2]['surface'] == '困っ' and tokens[i - 2]['lemma'] == '困る'
                and tokens[i - 1]['surface'] == 'た' and tokens[i - 1]['pos'][:1] in (('助動詞',), ['助動詞'])
                and token['surface'] == token['lemma'] == 'ことがある'
                and tokens[i + 1]['surface'] == 'なら' and tokens[i + 1]['lemma'] == 'だ'
                and not (i >= 3 and tokens[i - 3]['surface'] == 'に'
                         and tokens[i - 3]['pos'][:1] in (('助詞',), ['助詞']))):
            evidence = token_components(token)
            result.extend([
                {'surface': 'こと', 'lemma': 'こと', 'base_form': 'こと', 'reading': 'こと',
                 'pos': ('名詞', '普通名詞', '一般', '*', '*', '*'),
                 'components': evidence, 'analysis_rule': 'komatta_koto_boundary'},
                {'surface': 'が', 'lemma': 'が', 'base_form': 'が', 'reading': 'が',
                 'pos': ('助詞', '格助詞', '*', '*', '*', '*'),
                 'components': evidence, 'analysis_rule': 'komatta_koto_boundary'},
                {'surface': 'ある', 'lemma': 'ある', 'base_form': 'ある', 'reading': 'ある',
                 'pos': ('動詞', '非自立可能', '*', '*', '五段-ラ行', '終止形-一般'),
                 'components': evidence, 'analysis_rule': 'komatta_koto_boundary'},
            ])
        else:
            result.append(token)
    return result


def _repair_chigee_boundary(tokens: list[dict]) -> list[dict]:
    """Recover colloquial ちげぇ after それは from a split numeral analysis."""
    result = []
    i = 0
    while i < len(tokens):
        if (i > 0 and i + 2 < len(tokens) and tokens[i - 1]['surface'] == 'それ'
                and [t['surface'] for t in tokens[i:i + 3]] == ['はち', 'げ', 'ぇ']
                and tokens[i]['pos'][:2] in (('名詞', '数詞'), ['名詞', '数詞'])
                and tokens[i + 1]['pos'][:1] in (('接尾辞',), ['接尾辞'])
                and tokens[i + 2]['pos'][:1] in (('補助記号',), ['補助記号'])):
            evidence = tuple(c for t in tokens[i:i + 3] for c in token_components(t))
            result.extend([
                {'surface': 'は', 'lemma': 'は', 'base_form': 'は', 'reading': 'は',
                 'pos': ('助詞', '係助詞', '*', '*', '*', '*'),
                 'components': token_components(tokens[i]), 'analysis_rule': 'chigee_boundary'},
                {'surface': 'ちげぇ', 'lemma': 'ちげぇ', 'base_form': 'ちげぇ', 'reading': 'ちげぇ',
                 'pos': ('形容詞', '一般', '*', '*', '*', '*'),
                 'components': evidence, 'analysis_rule': 'chigee_boundary'},
            ])
            i += 3
        else:
            result.append(tokens[i])
            i += 1
    return result


def _repair_warui_koto_boundary(tokens: list[dict]) -> list[dict]:
    """Recover the adjective and noun before continuative する."""
    result = []
    for index, token in enumerate(tokens):
        following = tokens[index + 1] if index + 1 < len(tokens) else None
        if (token['surface'] == token['lemma'] == '悪いこと'
                and following and following['surface'] == 'し'
                and following['lemma'] == 'する'
                and following['pos'][:1] in (('動詞',), ['動詞'])):
            evidence = token_components(token)
            result.extend([
                {'surface': '悪い', 'lemma': '悪い', 'base_form': '悪い', 'reading': 'わるい',
                 'pos': ('形容詞', '一般', '*', '*', '形容詞', '連体形-一般'),
                 'components': evidence, 'analysis_rule': 'warui_koto_boundary'},
                {'surface': 'こと', 'lemma': 'こと', 'base_form': 'こと', 'reading': 'こと',
                 'pos': ('名詞', '普通名詞', '一般', '*', '*', '*'),
                 'components': evidence, 'analysis_rule': 'warui_koto_boundary'},
            ])
        else:
            result.append(token)
    return result


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
                 "pos": ("名詞", "普通名詞", "一般", "*", "*", "*"), "components": evidence, "analysis_rule": "kotoshi_boundary"},
                {"surface": "し", "lemma": "する", "base_form": "する", "reading": "する",
                 "pos": ("動詞", "非自立可能", "*", "*", "サ行変格", "連用形-一般"), "components": evidence, "analysis_rule": "kotoshi_boundary"},
            ])
        else:
            result.append(token)
    return result


def _repair_kigahen_boundaries(token_list: list[dict], jmdict) -> list[dict]:
    """Recover 変わる when the preceding token has swallowed its first kanji."""
    result = []
    i = 0
    while i < len(token_list):
        parts = token_list[i:i + 3]
        if (len(parts) == 3 and not any(t.get('grammar_reserved') for t in parts[:2])
                and parts[0]['surface'] == '気が変'
                and parts[0]['lemma'] == '気が変' and parts[0]['pos'][0] == '形状詞'
                and parts[1]['surface'] == 'わっ' and parts[1]['lemma'] == 'わる'
                and parts[1]['pos'][0] == '動詞'
                and ((parts[2]['lemma'] == 'た' and parts[2]['pos'][0] == '助動詞')
                     or (parts[2]['surface'] == 'て' and parts[2]['pos'][0] == '助詞'))
                and jmdict.lookup_word('変わる')):
            from src.steps.TokenizeStep import get_tokenizer, TOKENIZER_MODE
            nodes = get_tokenizer().tokenize('変わっ' + parts[2]['surface'], TOKENIZER_MODE)
            if (nodes and nodes[0].surface() == '変わっ'
                    and nodes[0].dictionary_form() == '変わる'
                    and nodes[0].part_of_speech()[0] == '動詞'):
                first, second = parts[:2]
                evidence = token_components(first)
                result.extend([
                    {'surface': '気', 'lemma': '気', 'base_form': '気', 'reading': 'き',
                     'pos': ('名詞', '普通名詞', '一般', '*', '*', '*'), 'components': evidence, 'analysis_rule': 'kigahen_boundary'},
                    {'surface': 'が', 'lemma': 'が', 'base_form': 'が', 'reading': 'が',
                     'pos': ('助詞', '格助詞', '*', '*', '*', '*'), 'components': evidence, 'analysis_rule': 'kigahen_boundary'},
                    {'surface': '変わっ', 'lemma': '変わる', 'base_form': '変わる', 'reading': 'かわる',
                     'pos': tuple(nodes[0].part_of_speech()),
                     'components': evidence + token_components(second), 'reassigned_from': 'わる', 'analysis_rule': 'kigahen_boundary'},
                ])
                i += 2
                continue
        result.append(token_list[i])
        i += 1
    return result


def _match_kamoshirenai(parts: list[dict]) -> int:
    """Return the length of a verified negative construction, or zero."""
    if (len(parts) < 4 or [t['surface'] for t in parts[:3]] != ['か', 'も', 'しれ']
            or [t['pos'][0] for t in parts[:3]] != ['助詞', '助詞', '動詞']
            or parts[2]['lemma'] != 'しれる'):
        return 0
    negative_index = 3
    if (parts[3]['surface'] == 'ませ' and parts[3]['lemma'] == 'ます'
            and len(parts[3]['pos']) > 4 and parts[3]['pos'][4] == '助動詞-マス'):
        negative_index = 4
    if len(parts) <= negative_index:
        return 0
    negative = parts[negative_index]
    if (len(negative['pos']) > 4 and negative['pos'][0] == '助動詞'
            and ((negative['surface'] in {'ねえ', 'ねぇ'} and negative['pos'][4] == '助動詞-ナイ' and negative_index == 3)
                 or (negative['surface'] in ({'ぬ'} if negative_index == 4 else {'ぬ', 'ん'})
                     and negative['pos'][4] == '助動詞-ヌ'))):
        return negative_index + 1
    return 0


def _repair_grammatical_constructions(token_list: list[dict]) -> list[dict]:
    """Recognize evidenced constructions before greedy dictionary merging."""
    result = []
    i = 0
    while i < len(token_list):
        length = _match_kamoshirenai(token_list[i:i + 5])
        if (not length and token_list[i]['surface'] == 'かもしれん'
                and token_list[i]['lemma'] == 'かもしれん'):
            length = 1
        if length:
            parts = token_list[i:i + length]
            result.append({
                'surface': ''.join(t['surface'] for t in parts),
                'lemma': 'かもしれない', 'base_form': 'かもしれない',
                'reading': 'かもしれない', 'pos': ['exp', '*', '*', '*', '*', '*'],
                'analysis_rule': 'kamoshirenai_construction',
                'components': tuple(c for t in parts for c in token_components(t)),
            })
            i += length
        else:
            result.append(token_list[i])
            i += 1
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
            "reassigned_from": lemma, "components": token_components(token), "analysis_rule": "imperative_reassignment"}


def has_kanji(text: str) -> bool:
    return any("一" <= char <= "龯" for char in text)


def _recover_me_ni_mieru(tokens, jmdict):
    """Keep the source-grounded expression before a reserved て-form ending."""
    result = []
    i = 0
    while i < len(tokens):
        parts = tokens[i:i + 4]
        if (len(parts) == 4 and [t['surface'] for t in parts[:3]] == ['目', 'に', '見え']
                and parts[0]['lemma'] == '目' and parts[1]['pos'][0] == '助詞'
                and parts[2]['lemma'] == '見える' and parts[2]['pos'][0] == '動詞'
                and parts[3].get('grammar_reserved')
                and parts[3]['surface'] in ('て', 'てる', 'てた', 'てない')):
            word = '目に見える'
            entries = jmdict.lookup_word(word)
            entry = jmdict.get_best_entry(word) if entries else None
            if entry and isinstance(entry, dict) and any(
                    any('expression' in tag.lower() for tag in sense.get('pos', []))
                    for sense in entry.get('senses', [])):
                result.append({'surface': '目に見え', 'lemma': word, 'base_form': word,
                               'reading': jmdict.get_reading(entry, word),
                               'pos': ['exp', '*', '*', '*', '*', '*'],
                               'dictionary_expression': True,
                               'analysis_rule': 'inflected_expression_me_ni_mieru',
                               'components': tuple(c for t in parts[:3] for c in token_components(t))})
                i += 3
                continue
        result.append(tokens[i])
        i += 1
    return result


def combine_tokens_jmdict(
    token_list: list[dict],
    jmdict: JMDict,
    max_lookahead: int = 10,
    rejected: list | None = None,
) -> list[dict]:
    """Greedily merge adjacent lexical compounds recognized by JMdict."""
    token_list = _recover_me_ni_mieru(token_list, jmdict)
    # The reserved ending remains context for this repair, but is never changed.
    token_list = _repair_kigahen_boundaries(token_list, jmdict)
    # Boundary repairs operate on lexical runs, while the merge guards below
    # can still inspect adjacent reserved tokens as context.
    def repair_run(run):
        return _repair_grammatical_constructions(_repair_kotoshi_boundaries(_repair_warui_koto_boundary(_repair_chigee_boundary(_repair_komatta_koto_boundary(run)))))

    if any(token.get('grammar_reserved') for token in token_list):
        repaired, start = [], 0
        for index, token in enumerate(token_list):
            if token.get('grammar_reserved'):
                repaired.extend(repair_run(token_list[start:index]))
                repaired.append(token)
                start = index + 1
        repaired.extend(repair_run(token_list[start:]))
        token_list = repaired
    else:
        token_list = repair_run(token_list)
    token_list = [_repair_imperative(token_list, i, jmdict) if not token.get('grammar_reserved') else token
                  for i, token in enumerate(token_list)]
    token_list = [_repair_tsurete_motion(token_list, i, jmdict) if not token.get('grammar_reserved') else token
                  for i, token in enumerate(token_list)]
    merged_tokens = []
    n = len(token_list)
    # Each candidate range stops before the next reserved token. Compute that
    # boundary once instead of scanning every overlapping candidate slice.
    next_reserved = [n] * n
    boundary = n
    for index in range(n - 1, -1, -1):
        if token_list[index].get('grammar_reserved'):
            boundary = index
        next_reserved[index] = boundary
    i = 0

    while i < n:
        if token_list[i].get('grammar_reserved'):
            merged_tokens.append(token_list[i])
            i += 1
            continue
        match_found = False
        for j in range(min(n, i + max_lookahead), i + 1, -1):
            if j > next_reserved[i]:
                continue
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

            def reject(reason):
                if rejected is not None:
                    rejected.append({"surface": token_string, "start_token": i, "end_token": j,
                                     "components": [t["surface"] for t in token_list[i:j]], "reason": reason})

            if (
                not has_kanji(token_string)
                and not _has_useful_kana_only_sense(definitions)
                and not _is_prefixed_kana_noun(token_list[i:j], definitions)
                and not _is_split_kana_noun(token_list[i:j], definitions)
            ):
                reject("kana_entry_not_eligible_for_merge")
                continue

            match_word = jmdict.get_best_entry(token_string)
            if not match_word:
                reject("no_best_entry")
                continue
            # In 従っておく, 従って is the て-form of 従う, not the
            # dictionary adverb meaning "therefore". Preserve its verb evidence.
            if (token_string == '従って' and j < n
                    and token_list[i]['lemma'] == '従う'
                    and token_list[j]['lemma'] == 'おく'
                    and tuple(token_list[j]['pos'][:2]) == ('動詞', '非自立可能')):
                reject('te_oku_verb_not_adverb')
                continue
            if (_is_inflection_misread_as_noun(token_list, i, j, match_word)
                    or _is_auxiliary_oru_misread(token_list, i, j)
                    or (any(t.get('analysis_rule') == 'object_tsureru_motion' for t in token_list[i:j])
                        and not (token_list[i].get('analysis_rule') == 'object_tsureru_motion'
                                 and j - i >= 3))):
                reject("inflection_or_motion_boundary")
                continue
            if _is_kigakawatta_predicate(token_list, i, j):
                reject("kigakawatta_predicate")
                continue
            if _is_prenominal_entry_in_past_predicate(token_list, i, j, match_word):
                reject("prenominal_in_past_predicate")
                continue
            if _crosses_incompatible_grammar(token_list[i:j], match_word):
                reject("incompatible_grammar")
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
                "analysis_rule": "jmdict_surface_merge",
                "dictionary_expression": isinstance(match_word, dict) and any(
                    "expression" in tag.lower() for sense in match_word.get("senses", [])
                    for tag in sense.get("pos", [])),
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
