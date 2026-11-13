"""After doing, or in the course of doing, an action."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


UEDE_EXPRESSION = '〜上で'


def match_uede(tokens):
    for i in range(1, len(tokens) - 1):
        previous, ue, de = tokens[i - 1:i + 2]
        if (ue['surface'] not in ('上', 'うえ') or ue['lemma'] not in ('上', 'うえ')
                or ue['pos'][:1] != ['名詞'] or de['surface'] != 'で'
                or de['lemma'] != 'で' or de['pos'][:1] != ['助詞']
                or not adjacent(previous, ue) or not adjacent(ue, de)):
            continue
        pos = previous['pos']
        verb = (pos[:1] == ['動詞'] and len(pos) >= 6
                and pos[5].startswith('連体形'))
        past = (previous['surface'] in ('た', 'だ') and previous['lemma'] in ('た', 'だ')
                and pos[:1] == ['助動詞'] and i >= 2
                and tokens[i - 2]['pos'][:1] == ['動詞']
                and adjacent(tokens[i - 2], previous))
        nominal = (previous['surface'] == previous['lemma'] == 'の'
                   and pos[:1] == ['助詞'] and i >= 2
                   and tokens[i - 2]['pos'][:1] == ['名詞']
                   and adjacent(tokens[i - 2], previous))
        if verb or past or nominal:
            yield ue['span'][0], de['span'][1]


RULE = GrammarRule(id='uede', expression=UEDE_EXPRESSION, reading='うえで',
                   meaning='after doing; in doing; in the course of',
                   match=match_uede, order=86)
RULES = (RULE,)
