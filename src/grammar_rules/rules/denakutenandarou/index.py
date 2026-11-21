"""Rhetorical identification with でなくて何だろう."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_denakutenandarou(tokens):
    for i in range(1, len(tokens) - 4):
        previous, copula = tokens[i - 1:i + 1]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'], ['形状詞'])
                or not adjacent(previous, copula)
                or copula['surface'] != 'で' or copula['lemma'] != 'だ'
                or copula['pos'][:1] != ['助動詞']):
            continue
        if (next_matches(tokens, i, 'ない', '形容詞', 'なく')
                and next_matches(tokens, i + 1, 'て', '助詞', 'て')
                and next_matches(tokens, i + 2, '何', '代名詞', '何')
                and next_matches(tokens, i + 3, 'だ', '助動詞', 'だろう')):
            yield copula['span'][0], tokens[i + 4]['span'][1]


RULE = GrammarRule(order=30, id='denakutenandarou', expression='〜でなくて何だろう',
                   reading='でなくてなんだろう', meaning='if this is not ... what else could it be?',
                   match=match_denakutenandarou)
RULES = (RULE,)
