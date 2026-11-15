"""Negative volition and conjecture expressed by verbal まい."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

MAI_EXPRESSION = '〜まい'


def match_mai(tokens):
    for i in range(1, len(tokens)):
        previous, mai = tokens[i - 1:i + 1]
        if (mai['surface'] != 'まい' or mai['lemma'] != 'まい'
                or mai['pos'][:1] != ['助動詞'] or not adjacent(previous, mai)
                or len(previous['pos']) < 6):
            continue
        pos, conjugation, form = previous['pos'][0], previous['pos'][4], previous['pos'][5]
        verbal = pos == '動詞' or (pos == '助動詞' and previous['lemma'] in
                                  ('ます', 'れる', 'られる', 'せる', 'させる'))
        stem = form.startswith(('終止形', '連体形')) or (
            form.startswith(('連用形', '未然形')) and
            (conjugation.startswith(('上一段', '下一段')) or previous['lemma'] in ('する', '来る')))
        if verbal and stem:
            yield mai['span']


RULE = GrammarRule(id='mai', expression=MAI_EXPRESSION, reading='まい',
                   meaning='will not; probably will not', match=match_mai, order=156)
RULES = (RULE,)
