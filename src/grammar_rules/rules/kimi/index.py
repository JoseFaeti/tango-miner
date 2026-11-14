"""Noun or continuative stem + 気味, a slight tendency toward a state."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

KIMI_EXPRESSION = '〜気味'


def match_kimi(tokens):
    for i in range(1, len(tokens)):
        host, suffix = tokens[i - 1:i + 1]
        pos = host['pos']
        valid_host = (pos[:1] == ['名詞'] or
                      (pos[:1] in (['動詞'], ['助動詞']) and len(pos) >= 6
                       and pos[5].startswith('連用形')))
        if (valid_host and adjacent(host, suffix)
                and suffix['surface'] == suffix['lemma'] == '気味'
                and suffix['pos'][:2] == ['名詞', '普通名詞']):
            yield tuple(suffix['span'])


RULE = GrammarRule(order=57, id='kimi', expression=KIMI_EXPRESSION,
                   reading='ぎみ', meaning='slightly; tending toward', match=match_kimi)
RULES = (RULE,)
