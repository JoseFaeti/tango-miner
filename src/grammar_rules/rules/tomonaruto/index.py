"""When a particular situation or stage is reached."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOMONARUTO_EXPRESSION = '〜ともなると'


def match_tomonaruto(tokens):
    for i in range(1, len(tokens) - 3):
        previous, to, mo, naru, ending = tokens[i - 1:i + 4]
        if (previous['pos'][:1] in (['補助記号'], ['空白'])
                or to['surface'] != 'と' or to['pos'][:1] != ['助詞']
                or mo['surface'] != 'も' or mo['pos'][:1] != ['助詞']
                or naru['surface'] != 'なる' or naru['lemma'] != 'なる'
                or naru['pos'][:1] != ['動詞']
                or ending['surface'] != 'と' or ending['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield to['span'][0], ending['span'][1]


RULE = GrammarRule(order=13, id='tomonaruto', expression=TOMONARUTO_EXPRESSION,
                   reading='ともなると', meaning='when it comes to; once it reaches the stage of',
                   match=match_tomonaruto)

RULES = (RULE,)
