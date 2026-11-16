"""Formal occasion: noun or dictionary verb + にあたって / に際して."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

EXPRESSION = '〜にあたって / 〜に際して'

# Common lexical uses of 当たる: contact or collision.
LEXICAL_ATARU_NOUNS = frozenset({'人', '壁'})
LEXICAL_AUXILIARIES = frozenset({'いる', '居る', 'くださる', '下さる', 'もらう', '貰う'})


def match_niatatsute_nisaishite(tokens):
    for i in range(1, len(tokens) - 2):
        preceding, ni, verb, te = tokens[i - 1:i + 3]
        if (preceding['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'])
                or (preceding['pos'][:1] == ['動詞']
                    and preceding['surface'] != preceding['lemma'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (
                    ('あたっ', 'あたる'), ('当たっ', '当たる'), ('際し', '際する'))
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (preceding, ni, verb), (ni, verb, te)))):
            continue
        if verb['lemma'] in ('あたる', '当たる'):
            following = tokens[i + 3] if i + 3 < len(tokens) else None
            if (preceding['lemma'] in LEXICAL_ATARU_NOUNS
                    or (following and adjacent(te, following)
                        and following['lemma'] in LEXICAL_AUXILIARIES)):
                continue
        yield ni['span'][0], te['span'][1]


RULE = GrammarRule(order=11, id='niatatsute_nisaishite', expression=EXPRESSION,
                   reading='にあたって / にさいして', meaning='on the occasion of; when',
                   match=match_niatatsute_nisaishite)
RULES = (RULE,)
