"""Verb-stem そうもない, expressing an unlikely event."""
SOU_MO_NAI_EXPRESSION = '〜そうもない'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, adjective_inflection_end


def match_sou_mo_nai(tokens):
    for i in range(1, len(tokens) - 2):
        verb, sou = tokens[i - 1:i + 1]
        pos = verb['pos']
        if (pos[:1] != ['動詞'] or len(pos) < 6 or not pos[5].startswith('連用形')
                or not adjacent(verb, sou) or sou['surface'] != 'そう'
                or sou['lemma'] != 'そう' or sou['pos'][:2] != ['形状詞', '助動詞語幹']
                or not next_matches(tokens, i, 'も', '助詞', 'も')
                or not next_matches(tokens, i + 1, 'ない', '形容詞')):
            continue
        end = adjective_inflection_end(tokens, i + 2, lemma='ない', pos='形容詞',
                                       base='ない', past_stem='なかっ',
                                       continuative_stem='なく', conditional_stem='なけれ',
                                       forms=('base', 'past', 'te', 'conditional'))
        if end is not None:
            yield sou['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=126, id='sou_mo_nai', expression=SOU_MO_NAI_EXPRESSION,
                   reading='そうもない', meaning='unlikely to; shows no sign of',
                   match=match_sou_mo_nai)

RULES = (RULE,)
