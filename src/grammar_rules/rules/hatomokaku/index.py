"""Source-aligned はともかく setting-aside construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

HATOMOKAKU_EXPRESSION = '〜はともかく'


def match_hatomokaku(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, word = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or particle['surface'] != 'は' or particle['lemma'] != 'は'
                or particle['pos'][:2] != ['助詞', '係助詞']
                or word['surface'] not in ('ともかく', '兎も角')
                or word['lemma'] not in ('ともかく', '兎も角')
                or word['pos'][:1] != ['副詞']
                or not adjacent(previous, particle) or not adjacent(particle, word)):
            continue
        yield particle['span'][0], word['span'][1]


RULE = GrammarRule(order=2, id='hatomokaku', expression=HATOMOKAKU_EXPRESSION,
                   reading='はともかく', meaning='setting aside; regardless of',
                   match=match_hatomokaku)
RULES = (RULE,)
