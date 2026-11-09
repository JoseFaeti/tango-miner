"""Protect grammatical boundaries during dictionary merging."""

from .helpers import has_kanji


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

def _is_past_predicate_misread_as_expression(tokens, start, end, entry):
    """Keep verb evidence when a past form only resembles an interjection."""
    if end - start != 2 or not isinstance(entry, dict):
        return False
    verb, past = tokens[start:end]
    if (verb['pos'][0] != '動詞' or past['pos'][0] != '助動詞'
            or past['lemma'] != 'た' or verb['surface'] == verb['lemma']):
        return False
    tags = [tag.lower() for sense in entry.get('senses', []) for tag in sense.get('pos', [])]
    if (not tags or not any(tag.startswith('interjection') for tag in tags)
            or any(not (tag.startswith('interjection') or tag.startswith('expression')
                        or tag.startswith('noun or verb acting prenominally')) for tag in tags)):
        return False
    # A prenominal entry can describe a person, but a case-marked subject,
    # verbal noun, or following grammatical noun supports a past predicate.
    if (any('prenominally' in tag for tag in tags) and end < len(tokens)
            and tokens[end]['pos'][0] == '名詞'):
        following = tokens[end]
        previous = tokens[start - 1] if start else None
        case_subject = (previous and previous['surface'] in {'が', 'を'}
                        and previous['pos'][0] == '助詞')
        verbal_noun = (previous and previous['pos'][0] == '名詞'
                       and len(previous['pos']) > 2 and 'サ変' in previous['pos'][2])
        grammatical_noun = following['lemma'] in {'とき', '時', '気', 'わけ', '訳'}
        # A locative に before できた describes something formed at that
        # location, rather than the fine-character modifier できた人.
        locative_dekiru = (verb['lemma'] in {'できる', '出来る'} and previous
                           and previous['surface'] == 'に' and tuple(previous['pos'][:2]) == ('助詞', '格助詞')
                           and start >= 2 and tokens[start - 2]['pos'][0] in {'名詞', '代名詞'})
        return bool(case_subject or verbal_noun or grammatical_noun or locative_dekiru)
    # An isolated utterance can genuinely be an interjection (来た！).
    at_start = start == 0 or tokens[start - 1]['pos'][0] == '補助記号'
    at_end = end == len(tokens) or tokens[end]['pos'][0] == '補助記号'
    # Utterance-initial interjections can carry a final particle before a pause:
    # 参ったな、... . Do not treat a following relative clause as an utterance.
    if (verb['lemma'] in {'参る', 'まいる'} and at_start and end < len(tokens) and tokens[end]['surface'] in {'な', 'ね', 'よ'}
            and tuple(tokens[end]['pos'][:2]) == ('助詞', '終助詞')
            and (end + 1 == len(tokens) or tokens[end + 1]['pos'][0] == '補助記号')):
        return False
    return not (at_start and at_end)

def _is_particle_boundary_misread(tokens, start, end):
    """Keep source-supported case particles distinct from similar expressions."""
    if end - start != 2:
        return False
    first, second = tokens[start:end]
    if (first['surface'] == 'から' and tuple(first['pos'][:2]) == ('助詞', '格助詞')
            and second['surface'] == 'は' and second['pos'][0] == '助詞'):
        return True
    # A past relative clause before ときより supplies a comparative time anchor.
    return (first['surface'] == 'とき' and second['surface'] == 'より'
            and first['pos'][0] == '名詞' and second['pos'][0] == '助詞'
            and start > 0 and tokens[start - 1]['lemma'] == 'た'
            and tokens[start - 1]['pos'][0] == '助動詞')

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
