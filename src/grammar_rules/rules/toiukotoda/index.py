"""Reported information or an explanation with ということだ."""
TOIUKOTODA_EXPRESSION = '〜ということだ'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_toiukotoda(tokens):
    for i in range(1, len(tokens) - 3):
        previous, particle, verb, koto, copula = tokens[i - 1:i + 4]
        pos = previous['pos']
        if (len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith(('終止形', '連体形', '意志推量形'))
                or particle['surface'] not in ('と', 'って') or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う') or verb['lemma'] not in ('いう', '言う')
                or verb['pos'][:1] != ['動詞']
                or koto['surface'] not in ('こと', '事') or koto['pos'][:2] != ['名詞', '普通名詞']
                or copula['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 3
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 3, 'た', '助動詞', 'た')):
            end = i + 4
        else:
            continue
        yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=89, id='toiukotoda', expression=TOIUKOTODA_EXPRESSION,
                   reading='ということだ', meaning='that means; it is said that',
                   match=match_toiukotoda)

RULES = (RULE,)
