"""Tentative conjecture or suggestion with ではないだろうか."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_dehanaidarouka(tokens):
    for i in range(1, len(tokens) - 4):
        previous, copula = tokens[i - 1:i + 1]
        pos = previous['pos']
        if not adjacent(previous, copula):
            continue
        if pos[:1] in (['動詞'], ['形容詞'], ['助動詞']):
            if len(pos) < 6 or not pos[5].startswith(('終止形', '連体形')):
                continue
        elif (pos[:1] not in (['名詞'], ['代名詞'], ['形状詞'], ['副詞'])
              and not (previous['surface'] == 'の' and pos[:1] == ['助詞'])):
            continue
        if (copula['surface'] == 'で' and copula['pos'][:1] in (['助詞'], ['助動詞'])
                and next_matches(tokens, i, 'は', '助詞', 'は')
                and next_matches(tokens, i + 1, 'ない', '形容詞', 'ない')
                and next_matches(tokens, i + 2, 'だ', '助動詞', 'だろう')
                and next_matches(tokens, i + 3, 'か', '助詞', 'か')
                and tokens[i + 4]['pos'][:2] in (['助詞', '終助詞'], ['助詞', '副助詞'])):
            yield copula['span'][0], tokens[i + 4]['span'][1]


RULE = GrammarRule(order=30, id='dehanaidarouka', expression='〜ではないだろうか',
                   reading='ではないだろうか', meaning='might it not be; perhaps; I suggest that',
                   match=match_dehanaidarouka)
RULES = (RULE,)
