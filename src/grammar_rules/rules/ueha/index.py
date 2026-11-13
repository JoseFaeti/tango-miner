"""Consequential 上は following an attributive verb."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


UEHA_EXPRESSION = '〜うえは'


def match_ueha(tokens):
    for i in range(1, len(tokens) - 1):
        previous, ue, wa = tokens[i - 1:i + 2]
        pos = previous['pos']
        verb = (pos[:1] == ['動詞'] and len(pos) >= 6
                and pos[5].startswith('連体形'))
        past = (previous['surface'] in ('た', 'だ') and previous['lemma'] in ('た', 'だ')
                and pos[:1] == ['助動詞'] and i >= 2
                and tokens[i - 2]['pos'][:1] == ['動詞']
                and adjacent(tokens[i - 2], previous))
        if ((verb or past) and ue['surface'] in ('上', 'うえ')
                and ue['lemma'] in ('上', 'うえ') and ue['pos'][:1] == ['名詞']
                and wa['surface'] == wa['lemma'] == 'は'
                and wa['pos'][:2] == ['助詞', '係助詞']
                and adjacent(previous, ue) and adjacent(ue, wa)):
            yield ue['span'][0], wa['span'][1]


RULE = GrammarRule(id='ueha', expression=UEHA_EXPRESSION, reading='うえは',
                   meaning='now that; since', match=match_ueha, order=162)
RULES = (RULE,)
