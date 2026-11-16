"""Literary expression of sufficient merit or suitability."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nitaru(tokens):
    for i in range(1, len(tokens) - 1):
        before, particle, verb = tokens[i - 1:i + 2]
        if (before['pos'][:1] in (['名詞'], ['動詞'])
                and particle['surface'] == 'に' and particle['pos'][:1] == ['助詞']
                and verb['surface'] in ('足る', 'たる')
                and verb['lemma'] in ('足る', 'たる') and verb['pos'][:1] == ['動詞']
                and adjacent(before, particle) and adjacent(particle, verb)):
            yield particle['span'][0], verb['span'][1]


RULE = GrammarRule(id='nitaru', expression='〜にたる / 〜に足る', reading='にたる',
                   meaning='worthy of; sufficient for', match=match_nitaru)
RULES = (RULE,)
