"""Attached temporal before/after expressions."""
from src.GrammarConstants import MAENI_ATODE_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_maeni_atode(tokens):
    for i in range(1, len(tokens) - 1):
        previous, noun, particle = tokens[i - 1:i + 2]
        nominal = (previous['surface'] == 'の' and previous['lemma'] == 'の'
                   and previous['pos'][:2] == ['助詞', '格助詞'])
        verb_before = (previous['pos'][:1] == ['動詞']
                       and len(previous['pos']) >= 6
                       and previous['pos'][5].startswith('連体形'))
        past_after = (i >= 2 and previous['surface'] in ('た', 'だ')
                      and previous['lemma'] in ('た', 'だ')
                      and previous['pos'][:1] == ['助動詞']
                      and tokens[i - 2]['pos'][:1] in (['動詞'], ['助動詞'], ['形容詞'])
                      and adjacent(tokens[i - 2], previous))
        before = noun['surface'] in ('前', 'まえ') and particle['surface'] == 'に'
        after = noun['surface'] in ('後', 'あと') and particle['surface'] == 'で'
        if not ((before and (nominal or verb_before)) or (after and (nominal or past_after))):
            continue
        if (noun['pos'][:1] != ['名詞'] or particle['lemma'] != particle['surface']
                or particle['pos'][:2] != ['助詞', '格助詞']
                or not adjacent(previous, noun) or not adjacent(noun, particle)):
            continue
        yield noun['span'][0], particle['span'][1]


RULE = GrammarRule(
    id='maeni_atode', expression=MAENI_ATODE_EXPRESSION, reading='まえに / あとで',
    meaning='before; after', match=match_maeni_atode,
)
