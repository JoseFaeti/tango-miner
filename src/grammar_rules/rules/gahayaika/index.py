"""Immediate succession with a finite verb followed by が早いか."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

GAHAYAIKA_EXPRESSION = '〜が早いか'


def match_gahayaika(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ga, hayai, ka = tokens[i - 1:i + 3]
        if not (ga['surface'] == 'が' and ga['pos'][:1] == ['助詞']
                and hayai['surface'] in ('早い', 'はやい')
                and hayai['lemma'] in ('早い', 'はやい')
                and hayai['pos'][:1] == ['形容詞']
                and ka['surface'] == 'か' and ka['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip(
                    (previous, ga, hayai), (ga, hayai, ka)))):
            continue
        finite_verb = (previous['pos'][:1] == ['動詞'] and len(previous['pos']) >= 6
                       and previous['pos'][5].startswith('終止形'))
        past_verb = (previous['surface'] == 'た' and previous['lemma'] == 'た'
                     and previous['pos'][:1] == ['助動詞'] and i >= 2
                     and tokens[i - 2]['pos'][:1] == ['動詞']
                     and adjacent(tokens[i - 2], previous))
        if finite_verb or past_verb:
            yield ga['span'][0], ka['span'][1]


RULE = GrammarRule(order=160, id='gahayaika', expression=GAHAYAIKA_EXPRESSION,
                   reading='がはやいか', meaning='as soon as; the moment that',
                   match=match_gahayaika)
RULES = (RULE,)
