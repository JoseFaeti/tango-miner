"""Noun + に関わる: relating to, affecting, or being involved in."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKAKAWARU_EXPRESSION = '〜に関わる'


def match_nikakawaru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('関わる', 'かかわる', '拘わる')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if verb['surface'] in ('関わる', 'かかわる', '拘わる'):
            yield ni['span'][0], verb['span'][1]
        elif (verb['surface'] in ('関わっ', 'かかわっ', '拘わっ')
              and i + 2 < len(tokens)):
            suffix = tokens[i + 2]
            if (adjacent(verb, suffix)
                    and (suffix['surface'], suffix['pos'][0]) in (('た', '助動詞'), ('て', '助詞'))):
                yield ni['span'][0], suffix['span'][1]


RULE = GrammarRule(order=7, id='nikakawaru', expression=NIKAKAWARU_EXPRESSION,
                   reading='にかかわる', meaning='relating to; affecting; involved in',
                   match=match_nikakawaru)
RULES = (RULE,)
