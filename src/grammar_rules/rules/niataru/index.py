"""Correspondence expressed by に当たる, with focused lexical controls."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, positive_verb_end

# Keep unknown complements eligible; a semantic allowlist would lose correspondence uses.
LEXICAL_COMPLEMENTS = frozenset({'本当', '頭', '壁', '攻撃', '弾', '球'})


def damaging_conditional(tokens, end):
    """Recognize a local impact consequence without borrowing another sentence's cues."""
    following = tokens[end + 1:end + 2]
    if not following or following[0]['surface'] != 'と' or not adjacent(tokens[end], following[0]):
        return False
    for token in tokens[end + 2:end + 14]:
        if token['pos'][:1] == ['補助記号']:
            break
        if token['lemma'] in ('痛手', '怪我', 'けが'):
            return True
    return False


def match_niataru(tokens):
    for i in range(1, len(tokens) - 1):
        before, ni, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'])
                or before['lemma'] in LEXICAL_COMPLEMENTS
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('当たる', 'あたる')
                or not adjacent(before, ni) or not adjacent(ni, verb)):
            continue
        stem = '当た' if verb['lemma'] == '当たる' else 'あた'
        end = positive_verb_end(tokens, i + 1, lemma=verb['lemma'],
                                base_forms=(stem + 'る',), past_stems=(stem + 'っ',),
                                polite_stems=(stem + 'り',), conditional_stems=(stem + 'れ',))
        if (end is not None
                and not (before['pos'][:1] == ['代名詞']
                         and damaging_conditional(tokens, end))):
            yield ni['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='niataru', expression='〜にあたる', reading='にあたる',
                   meaning='corresponds to; amounts to', match=match_niataru)
RULES = (RULE,)
