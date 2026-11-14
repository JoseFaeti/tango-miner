"""Attached くせに with an attributive predicate or noun + の."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KUSENI_EXPRESSION = '〜くせに'


def match_kuseni(tokens):
    for i in range(1, len(tokens) - 1):
        previous, kuse, ni = tokens[i - 1:i + 2]
        if (kuse['surface'] not in ('くせ', 'クセ')
                or kuse['pos'][:2] != ['名詞', '普通名詞']
                or ni['surface'] != 'に' or ni['lemma'] != 'に'
                or ni['pos'][:2] != ['助詞', '格助詞']
                or not adjacent(previous, kuse) or not adjacent(kuse, ni)):
            continue
        # くせになる describes acquiring a habit; reserve neither component.
        if (i + 2 < len(tokens) and adjacent(ni, tokens[i + 2])
                and tokens[i + 2]['lemma'] == 'なる'
                and tokens[i + 2]['pos'][:1] == ['動詞']):
            continue
        predicate = (previous['pos'][:1] in (['動詞'], ['形容詞'], ['助動詞'])
                     and len(previous['pos']) >= 6
                     and previous['pos'][5].startswith('連体形'))
        noun_no = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞']
                   and i >= 2 and tokens[i - 2]['pos'][:1] in (['名詞'], ['代名詞'])
                   and adjacent(tokens[i - 2], previous))
        if predicate or noun_no:
            yield kuse['span'][0], ni['span'][1]


RULE = GrammarRule(id='kuseni', expression=KUSENI_EXPRESSION, reading='くせに',
                   meaning='although; even though (with criticism)', match=match_kuseni)
RULES = (RULE,)
