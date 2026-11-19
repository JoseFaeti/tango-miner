"""Source-aligned はおろか construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

HAOROKA_EXPRESSION = '〜はおろか'


def match_haoroka(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, word = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or particle['surface'] != 'は' or particle['lemma'] != 'は'
                or particle['pos'][:2] != ['助詞', '係助詞']
                or word['surface'] not in ('おろか', '愚か')
                or word['lemma'] not in ('おろか', '愚か')
                or word['pos'][:1] not in (['名詞'], ['形状詞'])
                or not adjacent(previous, particle) or not adjacent(particle, word)):
            continue
        if (i + 2 < len(tokens) and adjacent(word, tokens[i + 2])
                and tokens[i + 2]['lemma'] in ('だ', 'です')
                and tokens[i + 2]['pos'][:1] == ['助動詞']):
            continue
        yield particle['span'][0], word['span'][1]


RULE = GrammarRule(order=2, id='haoroka', expression=HAOROKA_EXPRESSION,
                   reading='はおろか', meaning='let alone; not to mention',
                   match=match_haoroka)
RULES = (RULE,)
