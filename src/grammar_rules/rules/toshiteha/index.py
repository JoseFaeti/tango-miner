"""Noun + としては: from a role or standpoint."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOSHITEHA_EXPRESSION = '〜としては'


def match_toshiteha(tokens):
    for i in range(1, len(tokens) - 3):
        previous, to, shi, te, ha = tokens[i - 1:i + 4]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'])
                and not (previous['surface'] == '初めて' and previous['pos'][:1] == ['副詞'])):
            continue
        if ([(t['surface'], t['pos'][:1]) for t in (to, shi, te, ha)] !=
                [('と', ['助詞']), ('し', ['動詞']), ('て', ['助詞']), ('は', ['助詞'])]
                or shi['lemma'] != 'する'
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield to['span'][0], ha['span'][1]


RULE = GrammarRule(order=16, id='toshiteha', expression=TOSHITEHA_EXPRESSION,
                   reading='としては', meaning='as for; from the standpoint of',
                   match=match_toshiteha)
RULES = (RULE,)
