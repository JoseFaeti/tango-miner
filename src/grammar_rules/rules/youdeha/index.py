"""Conditional ようでは following a predicate."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

YOUDEHA_EXPRESSION = '〜ようでは'


def match_youdeha(tokens):
    for i in range(1, len(tokens) - 1):
        previous, you, copula = tokens[i - 1:i + 2]
        pos = previous['pos']
        if (pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(pos) < 6 or not pos[5].startswith(('終止形', '連体形'))
                or you['surface'] != 'よう' or you['lemma'] != 'よう'
                or you['pos'][:2] != ['形状詞', '助動詞語幹']
                or not adjacent(previous, you) or not adjacent(you, copula)):
            continue
        if (copula['surface'] == 'で' and copula['lemma'] == 'だ'
                and copula['pos'][:1] == ['助動詞'] and i + 2 < len(tokens)
                and adjacent(copula, tokens[i + 2])
                and tokens[i + 2]['surface'] == tokens[i + 2]['lemma'] == 'は'
                and tokens[i + 2]['pos'][:1] == ['助詞']):
            end = i + 2
        elif (copula['surface'], copula['lemma'], copula['pos'][:1]) == ('じゃ', 'だ', ['助動詞']):
            end = i + 1
        else:
            continue
        tail = tokens[end + 1:]
        if not any(t['pos'][:1] != ['補助記号'] for t in tail):
            continue
        # A directly attached negative is usually the negative of ようだ.
        if tail and adjacent(tokens[end], tail[0]) and tail[0]['lemma'] in ('ない', 'ある'):
            continue
        yield you['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='youdeha', expression=YOUDEHA_EXPRESSION, reading='ようでは',
                   meaning='if things are such that; if this is the situation',
                   match=match_youdeha, order=138)
RULES = (RULE,)
