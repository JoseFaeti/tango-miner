"""Formal purpose expressed by verbal べく."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

BEKU_EXPRESSION = '〜べく'


def match_beku(tokens):
    for i in range(1, len(tokens)):
        verb, beku = tokens[i - 1:i + 1]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or not adjacent(verb, beku) or beku['surface'] != 'べく'
                or beku['lemma'] != 'べし' or beku['pos'][:1] != ['助動詞']):
            continue
        if i + 1 < len(tokens) and adjacent(beku, tokens[i + 1]):
            following = tokens[i + 1]
            if following['surface'] == 'も' and following['pos'][:1] == ['助詞']:
                continue
            if (following['surface'] == 'し' and following['lemma'] == 'する'
                    and i + 2 < len(tokens) and adjacent(following, tokens[i + 2])
                    and tokens[i + 2]['surface'] == 'て'
                    and tokens[i + 2]['pos'][:1] == ['助詞']):
                continue
        yield beku['span']


RULE = GrammarRule(id='beku', expression=BEKU_EXPRESSION, reading='べく',
                   meaning='in order to', match=match_beku, order=156)
RULES = (RULE,)
