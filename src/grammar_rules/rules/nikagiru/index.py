"""〜に限る: the best choice or a restriction."""
NIKAGIRU_EXPRESSION = '〜に限る'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikagiru(tokens):
    for i in range(1, len(tokens) - 1):
        preceding, ni, verb = tokens[i - 1:i + 2]
        if (preceding['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'], ['形容詞'], ['助動詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] not in ('限る', 'かぎる')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(preceding, ni) or not adjacent(ni, verb)):
            continue
        if verb['surface'] in ('限る', 'かぎる'):
            yield ni['span'][0], verb['span'][1]
        elif verb['surface'] in ('限り', 'かぎり') and i + 2 < len(tokens):
            polite = tokens[i + 2]
            if (polite['surface'] == 'ます' and polite['pos'][:1] == ['助動詞']
                    and adjacent(verb, polite)):
                yield ni['span'][0], polite['span'][1]


RULE = GrammarRule(order=3, id='nikagiru', expression=NIKAGIRU_EXPRESSION,
                   reading='にかぎる', meaning='best to; limited to',
                   match=match_nikagiru)

RULES = (RULE,)
