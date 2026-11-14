"""Literary and colloquial verbal negative auxiliaries ぬ and ん."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NU_N_EXPRESSION = '〜ぬ / 〜ん'


def match_nu_n(tokens):
    for i in range(1, len(tokens)):
        previous, negative = tokens[i - 1:i + 1]
        if (negative['surface'] not in ('ぬ', 'ん') or negative['lemma'] != 'ぬ'
                or negative['pos'][:1] != ['助動詞'] or not adjacent(previous, negative)
                or len(previous['pos']) < 6):
            continue
        pos, conjugation, form = previous['pos'][0], previous['pos'][4], previous['pos'][5]
        verbal = pos == '動詞' or (pos == '助動詞' and previous['lemma'] in
                                  ('れる', 'られる', 'せる', 'させる'))
        # Sudachi often labels the identical ichidan/来る irrealis as continuative.
        stem = form.startswith('未然形') or (form.startswith('連用形') and
                (conjugation.startswith(('上一段', '下一段')) or previous['lemma'] == '来る'))
        if verbal and stem:
            yield negative['span']


RULE = GrammarRule(id='nu_n', expression=NU_N_EXPRESSION, reading='ぬ / ん',
                   meaning='not; do not (literary or colloquial)', match=match_nu_n, order=155)
RULES = (RULE,)
