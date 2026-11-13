"""Conditional といい expressing a hoped-for outcome."""
TOII_TOIINA_EXPRESSION = '〜といい / 〜といいな'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toii_toiina(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, adjective = tokens[i - 1:i + 2]
        pos = previous['pos']
        if (len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith(('終止形', '連体形'))
                or particle['surface'] != 'と' or particle['pos'][:2] not in
                (['助詞', '接続助詞'], ['助詞', '格助詞'])
                or adjective['pos'][:1] != ['形容詞']
                or adjective['lemma'] not in ('いい', 'よい', '良い')
                or adjective['surface'] not in ('いい', 'よい', '良い')
                or not adjacent(previous, particle) or not adjacent(particle, adjective)):
            continue
        end = i + 1
        if (end + 1 < len(tokens) and adjacent(adjective, tokens[end + 1])
                and tokens[end + 1]['surface'] == 'な'
                and tokens[end + 1]['pos'][:2] == ['助詞', '終助詞']):
            end += 1
        yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=23, id='toii_toiina', expression=TOII_TOIINA_EXPRESSION,
                   reading='といい / といいな', meaning='hope that; it would be good if',
                   match=match_toii_toiina)

RULES = (RULE,)
