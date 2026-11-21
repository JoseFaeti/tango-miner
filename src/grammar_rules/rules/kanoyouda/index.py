"""Finite かのようだ comparisons with source-aligned boundaries."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

EXPRESSION = '〜かのようだ'


def match_kanoyouda(tokens):
    for i in range(1, len(tokens) - 3):
        previous, ka, no, you, copula = tokens[i - 1:i + 4]
        pos = previous['pos']
        if not (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形'))):
            continue
        if not (ka['surface'] == ka['lemma'] == 'か' and ka['pos'][:1] == ['助詞']
                and no['surface'] == no['lemma'] == 'の' and no['pos'][:1] == ['助詞']
                and you['surface'] == you['lemma'] == 'よう'
                and you['pos'][:2] == ['形状詞', '助動詞語幹']
                and copula['pos'][:1] == ['助動詞']
                and all(adjacent(tokens[j], tokens[j + 1]) for j in range(i - 1, i + 3))):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 3
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 3, 'た', '助動詞', 'た')):
            end = i + 4
        else:
            continue
        yield ka['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=138, id='kanoyouda', expression=EXPRESSION,
                   reading='かのようだ', meaning='as if; as though', match=match_kanoyouda)
RULES = (RULE,)
