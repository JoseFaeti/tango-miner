"""Immediate succession after a dictionary-form verb."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

YAINAYA_EXPRESSION = '〜や否や'


def match_yainaya(tokens):
    for i in range(1, len(tokens) - 2):
        verb, ya, ina, final_ya = tokens[i - 1:i + 3]
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith(('終止形', '連体形'))
                and ya['surface'] == 'や' and ya['pos'][:1] == ['助詞']
                and ina['surface'] in ('否', 'いな') and ina['pos'][:1] == ['名詞']
                and final_ya['surface'] == 'や' and final_ya['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip(
                    (verb, ya, ina), (ya, ina, final_ya)))):
            yield ya['span'][0], final_ya['span'][1]


RULE = GrammarRule(order=51, id='yainaya', expression=YAINAYA_EXPRESSION,
                   reading='やいなや', meaning='as soon as; the moment that',
                   match=match_yainaya)
RULES = (RULE,)
