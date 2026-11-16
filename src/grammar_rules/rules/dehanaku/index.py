"""Contrastive negative copula in its continuative form."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

DEHANAKU_EXPRESSION = '〜ではなく'


def match_dehanaku(tokens):
    for i in range(1, len(tokens) - 1):
        previous, copula = tokens[i - 1:i + 1]
        if (previous['pos'][:1] in (['空白'], ['補助記号'])
                or not adjacent(previous, copula)):
            continue
        if (copula['surface'] == 'じゃ' and copula['lemma'] == 'だ'
                and copula['pos'][:1] == ['助動詞']):
            end = i
        elif (copula['surface'] == 'で'
              and ((copula['lemma'] == 'だ' and copula['pos'][:1] == ['助動詞'])
                   or (copula['lemma'] == 'で' and copula['pos'][:1] == ['助詞']))
              and next_matches(tokens, i, 'は', '助詞', 'は')):
            end = i + 1
        else:
            continue
        if not next_matches(tokens, end, 'ない', '形容詞', 'なく'):
            continue
        end += 1
        if (end + 1 < len(tokens) and adjacent(tokens[end], tokens[end + 1])
                and tokens[end + 1]['lemma'] in ('なる', 'て')):
            continue
        yield copula['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=168, id='dehanaku', expression=DEHANAKU_EXPRESSION,
                   reading='ではなく', meaning='rather than; not ... but', match=match_dehanaku)
RULES = (RULE,)
