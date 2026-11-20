"""It is preferable that the preceding action or state does not occur."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nikoshitakotohanai.index import match_nikoshitakotohanai

NAINIKOSHITAKOTOHANAI_EXPRESSION = '〜ないに越したことはない'


def match_nainikoshitakotohanai(tokens):
    suffixes = dict(match_nikoshitakotohanai(tokens))
    for negative, marker in zip(tokens, tokens[1:]):
        if (negative['surface'] == 'ない' and negative['lemma'] == 'ない'
                and negative['pos'][:1] in (['助動詞'], ['形容詞'])
                and adjacent(negative, marker) and marker['span'][0] in suffixes):
            yield negative['span'][0], suffixes[marker['span'][0]]


RULE = GrammarRule(id='nainikoshitakotohanai', expression=NAINIKOSHITAKOTOHANAI_EXPRESSION,
                   reading='ないにこしたことはない', meaning='it is best not to; better without',
                   match=match_nainikoshitakotohanai, order=78)
RULES = (RULE,)
