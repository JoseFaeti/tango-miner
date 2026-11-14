"""Source-aligned concessive とはいえ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOHAIE_EXPRESSION = '〜とはいえ'


def match_tohaie(tokens):
    for i in range(len(tokens) - 2):
        to, wa, ie = tokens[i:i + 3]
        if (to['surface'] != 'と' or to['pos'][:2] != ['助詞', '格助詞']
                or wa['surface'] != 'は' or wa['pos'][:2] != ['助詞', '係助詞']
                or ie['surface'] not in ('いえ', '言え')
                or ie['pos'][:1] != ['動詞'] or len(ie['pos']) < 6
                or not ie['pos'][4].startswith('文語四段')
                or not ie['pos'][5].startswith('已然形')
                or not adjacent(to, wa) or not adjacent(wa, ie)):
            continue
        # A comma may separate the clauses; a sentence boundary cannot.
        following = tokens[i + 3:]
        for token in following:
            if token['surface'] in ('。', '！', '？', '!', '?'):
                break
            if token['pos'][:1] not in (['補助記号'], ['空白']):
                yield to['span'][0], ie['span'][1]
                break


RULE = GrammarRule(order=2, id='tohaie', expression=TOHAIE_EXPRESSION,
                   reading='とはいえ', meaning='although; even so; nevertheless',
                   match=match_tohaie)
RULES = (RULE,)
