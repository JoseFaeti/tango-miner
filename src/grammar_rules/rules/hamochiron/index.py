"""Source-aligned はもちろん additive construction."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

HAMOCHIRON_EXPRESSION = '〜はもちろん'


def match_hamochiron(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, word = tokens[i - 1:i + 2]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or particle['surface'] != 'は' or particle['lemma'] != 'は'
                or particle['pos'][:2] != ['助詞', '係助詞']
                or word['surface'] not in ('もちろん', '勿論')
                or word['lemma'] not in ('もちろん', '勿論')
                or word['pos'][:1] != ['副詞']
                or not adjacent(previous, particle) or not adjacent(particle, word)):
            continue
        yield particle['span'][0], word['span'][1]


RULE = GrammarRule(order=2, id='hamochiron', expression=HAMOCHIRON_EXPRESSION,
                   reading='はもちろん', meaning='not to mention; of course ... as well as',
                   match=match_hamochiron)
RULES = (RULE,)
