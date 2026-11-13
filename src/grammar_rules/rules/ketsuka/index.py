"""A result following an action or process."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KETSUKA_EXPRESSION = '〜結果'


def match_ketsuka(tokens):
    for i in range(2, len(tokens)):
        before, previous, result = tokens[i - 2:i + 1]
        nominal = (before['pos'][:1] in (['名詞'], ['代名詞'])
                   and previous['surface'] == previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'])
        past_verb = (before['pos'][:1] == ['動詞']
                     and previous['surface'] in ('た', 'だ')
                     and previous['lemma'] in ('た', 'だ')
                     and previous['pos'][:1] == ['助動詞'])
        if ((nominal or past_verb) and result['surface'] == result['lemma'] == '結果'
                and result['pos'][:1] == ['名詞']
                and adjacent(before, previous) and adjacent(previous, result)):
            yield result['span'][0], result['span'][1]


RULE = GrammarRule(order=105, id='ketsuka', expression=KETSUKA_EXPRESSION,
                   reading='けっか', meaning='as a result of', match=match_ketsuka)
RULES = (RULE,)
