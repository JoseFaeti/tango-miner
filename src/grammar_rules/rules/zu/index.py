"""Literary verbal negative continuative ず."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

ZU_EXPRESSION = '〜ず'


def match_zu(tokens):
    for i in range(1, len(tokens)):
        previous, negative = tokens[i - 1:i + 1]
        if (negative['surface'] != 'ず' or negative['pos'][:1] != ['助動詞']
                or not adjacent(previous, negative) or len(previous['pos']) < 6):
            continue
        pos, conjugation, form = previous['pos'][0], previous['pos'][4], previous['pos'][5]
        verbal = pos == '動詞' or (pos == '助動詞' and previous['lemma'] in
                                  ('れる', 'られる', 'せる', 'させる'))
        stem = form.startswith('未然形') or (form.startswith('連用形') and
                (conjugation.startswith(('上一段', '下一段')) or previous['lemma'] == '来る'))
        if verbal and stem:
            yield negative['span']


RULE = GrammarRule(id='zu', expression=ZU_EXPRESSION, reading='ず',
                   meaning='without doing; not doing (literary)', match=match_zu, order=155)
RULES = (RULE,)
