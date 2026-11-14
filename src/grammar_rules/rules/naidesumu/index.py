"""Verbal negative followed by で済む (avoid having to)."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.naide.index import match_naide
from src.grammar_rules.rules.tesumu.index import sumu_end

NAIDESUMU_EXPRESSION = '〜ないですむ'


def match_naidesumu(tokens):
    starts = {start for start, _ in match_naide(tokens)}
    for i in range(len(tokens) - 2):
        negative, connective, sumu = tokens[i:i + 3]
        if (negative['span'][0] not in starts or negative['surface'] != 'ない'
                or connective['surface'] != 'で' or not adjacent(connective, sumu)
                or sumu['lemma'] not in ('済む', 'すむ') or sumu['pos'][:1] != ['動詞']):
            continue
        end = sumu_end(tokens, i + 2)
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='naidesumu', expression=NAIDESUMU_EXPRESSION,
                   reading='ないですむ', meaning='avoid having to do',
                   match=match_naidesumu, order=117)
RULES = (RULE,)
