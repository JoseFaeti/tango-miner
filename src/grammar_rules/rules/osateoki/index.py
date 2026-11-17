"""Setting something aside with をさておき."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_osateoki(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, word = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or particle['surface'] != 'を' or particle['lemma'] != 'を'
                or particle['pos'][:1] != ['助詞']
                or word['surface'] not in ('さておき', 'さて置き')
                or word['lemma'] not in ('さておく', 'さて置く')
                or word['pos'][:1] != ['動詞']
                or not adjacent(previous, particle) or not adjacent(particle, word)):
            continue
        yield particle['span'][0], word['span'][1]


RULE = GrammarRule(order=2, id='osateoki', expression='〜をさておき',
                   reading='をさておき', meaning='setting aside; leaving aside',
                   match=match_osateoki)
RULES = (RULE,)
