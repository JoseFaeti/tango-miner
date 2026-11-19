"""Noun or predicate followed by とともに."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_totomoni(tokens):
    for i in range(1, len(tokens) - 2):
        preceding, particle, together, ni = tokens[i - 1:i + 3]
        if (preceding['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'], ['形容詞'], ['助動詞'])
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or together['surface'] not in ('とも', '共')
                or together['pos'][:1] != ['名詞']
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (preceding, particle, together), (particle, together, ni)))):
            continue
        yield particle['span'][0], ni['span'][1]


RULE = GrammarRule(id='totomoni', expression='〜とともに', reading='とともに',
                   meaning='together with; as; at the same time as',
                   match=match_totomoni, order=11)
RULES = (RULE,)
