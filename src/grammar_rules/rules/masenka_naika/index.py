"""Verb negative questions used for invitations."""
MASENKA_NAIKA_EXPRESSION = '〜ませんか / 〜ないか'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, polite_suffix_end


def match_masenka_naika(tokens):
    for i in range(1, len(tokens)):
        previous, suffix = tokens[i - 1:i + 1]
        if (not adjacent(previous, suffix)
                or previous['pos'][:1] not in (['動詞'], ['助動詞'])
                or len(previous['pos']) < 6):
            continue
        form = previous['pos'][5]
        if suffix['surface'] in ('ませ', 'ません') and form.startswith('連用形'):
            end = polite_suffix_end(tokens, i, forms=('negative',))
        elif (suffix['surface'] == 'ない' and suffix['lemma'] == 'ない'
              and suffix['pos'][:1] == ['助動詞'] and form.startswith('未然形')
              and (previous['pos'][:1] == ['動詞'] or previous['lemma'] in
                   ('れる', 'られる', 'せる', 'させる'))):
            end = i
        else:
            continue
        if end is None or end + 1 >= len(tokens):
            continue
        question = tokens[end + 1]
        if (not adjacent(tokens[end], question) or question['surface'] != 'か'
                or question['pos'][:2] != ['助詞', '終助詞']):
            continue
        if end + 2 < len(tokens) and tokens[end + 2]['pos'][:1] != ['補助記号']:
            continue
        yield suffix['span'][0], question['span'][1]


RULE = GrammarRule(order=171, id='masenka_naika', expression=MASENKA_NAIKA_EXPRESSION,
                   reading='ませんか / ないか', meaning='would you like to; shall we',
                   match=match_masenka_naika)

RULES = (RULE,)
