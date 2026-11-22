"""Not only X but also Y, with an adjacent nominal Yも."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nikagirazu.index import match_nikagirazu

NIKAGIRAZUMO_EXPRESSION = '〜に限らず〜も'


def match_nikagirazumo(tokens):
    for start, end in match_nikagirazu(tokens):
        for i, token in enumerate(tokens):
            if token['span'][0] != end or i + 1 >= len(tokens):
                continue
            particle = tokens[i + 1]
            if (token['pos'][:1] in (['名詞'], ['代名詞'])
                    and particle['surface'] == 'も' and particle['lemma'] == 'も'
                    and particle['pos'][:1] == ['助詞'] and adjacent(token, particle)):
                yield start, end
            break


RULE = GrammarRule(order=1, id='nikagirazumo', expression=NIKAGIRAZUMO_EXPRESSION,
                   reading='にかぎらず も', meaning='not only ... but also',
                   match=match_nikagirazumo)
RULES = (RULE,)
