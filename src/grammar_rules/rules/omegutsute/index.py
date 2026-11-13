"""Noun + をめぐって: concerning or surrounding an issue."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_omegutsute(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('めぐっ', '巡っ')
                or verb['lemma'] not in ('巡る', 'めぐる')
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, particle, verb), (particle, verb, te)))):
            continue
        yield particle['span'][0], te['span'][1]


RULE = GrammarRule(order=7, id='omegutsute', expression='〜をめぐって',
                   reading='をめぐって', meaning='concerning; over; surrounding',
                   match=match_omegutsute)

RULES = (RULE,)
