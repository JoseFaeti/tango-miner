"""Noun + に至っては: singling out an extreme example."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_niitatsuteha(tokens):
    for i in range(1, len(tokens) - 3):
        noun, ni, verb, te, ha = tokens[i - 1:i + 4]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and verb['surface'] in ('至っ', 'いたっ')
                and verb['lemma'] in ('至る', 'いたる') and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and ha['surface'] == 'は' and ha['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((noun, ni, verb, te), (ni, verb, te, ha)))):
            yield ni['span'][0], ha['span'][1]


RULE = GrammarRule(order=1, id='niitatsuteha', expression='〜に至っては',
                   reading='にいたっては', meaning='as for; particularly; when it comes to',
                   match=match_niitatsuteha)
RULES = (RULE,)
