"""Contrast with a noun or nominalized clause + にひきかえ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIHIKIKAE_EXPRESSION = '〜にひきかえ'


def match_nihikikae(tokens):
    for i in range(1, len(tokens) - 1):
        target, ni, verb = tokens[i - 1:i + 2]
        nominal = (target['pos'][:1] in (['名詞'], ['代名詞'])
                   or (target['surface'] == 'の' and target['pos'][:2] == ['助詞', '準体助詞']))
        if (not nominal or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('ひきかえ', '引き換え', '引きかえ', 'ひき換え')
                or verb['lemma'] not in ('ひきかえる', '引き換える', '引きかえる', 'ひき換える')
                or verb['pos'][:1] != ['動詞']
                or not adjacent(target, ni) or not adjacent(ni, verb)):
            continue
        if (i + 2 < len(tokens) and adjacent(verb, tokens[i + 2])
                and tokens[i + 2]['pos'][:1] in (['助動詞'], ['助詞'])
                and tokens[i + 2]['surface'] not in ('は', 'も')):
            continue
        yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=6, id='nihikikae', expression=NIHIKIKAE_EXPRESSION,
                   reading='にひきかえ', meaning='in contrast to', match=match_nihikikae)
RULES = (RULE,)
