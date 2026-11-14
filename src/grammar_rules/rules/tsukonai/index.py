"""Verb-stem っこない, expressing impossibility."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches

TSUKONAI_EXPRESSION = '〜っこない'


def match_tsukonai(tokens):
    for i in range(1, len(tokens) - 1):
        verb, suffix, negative = tokens[i - 1:i + 2]
        pos = verb['pos']
        if not (pos[:1] == ['動詞'] and len(pos) >= 6
                and pos[5].startswith('連用形')
                and (suffix['surface'], suffix['lemma'], suffix['pos'][:1])
                == ('っこ', 'っこ', ['接尾辞'])
                and adjacent(verb, suffix) and adjacent(suffix, negative)):
            continue
        end = adjective_inflection_end(
            tokens, i + 1, lemma='ない', pos='形容詞', base='ない',
            past_stem='なかっ', continuative_stem='なく', conditional_stem='なけれ',
            forms=('base', 'past', 'te', 'conditional'))
        if end is not None:
            if tokens[end]['surface'] == 'ない' and next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
            yield suffix['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=34, id='tsukonai', expression=TSUKONAI_EXPRESSION,
                   reading='っこない', meaning='cannot possibly; there is no chance of doing',
                   match=match_tsukonai)
RULES = (RULE,)
