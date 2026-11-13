"""Resemblance expressed by みたいに or みたいな."""
MITAI_NI_EXPRESSION = '〜みたいに / 〜みたいな'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_mitai_ni(tokens):
    for i in range(1, len(tokens) - 1):
        previous, mitai, ending = tokens[i - 1:i + 2]
        if not adjacent(previous, mitai) or not adjacent(mitai, ending):
            continue
        if (mitai['surface'], mitai['lemma'], mitai['pos'][:2]) != (
                'みたい', 'みたい', ['形状詞', '助動詞語幹']):
            continue
        pos = previous['pos']
        attachment = (pos[:1] in (['名詞'], ['代名詞']) or
                      (pos[:1] in (['動詞'], ['形容詞'], ['助動詞'])
                       and len(pos) >= 6 and pos[5].startswith(('終止形', '連体形'))))
        if (attachment and ending['surface'] in ('に', 'な')
                and ending['lemma'] == 'だ' and ending['pos'][:1] == ['助動詞']):
            yield mitai['span'][0], ending['span'][1]


RULE = GrammarRule(order=83, id='mitai_ni', expression=MITAI_NI_EXPRESSION,
                   reading='みたいに / みたいな', meaning='like; resembling', match=match_mitai_ni)

RULES = (RULE,)
