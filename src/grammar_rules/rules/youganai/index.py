"""Verb stem plus ようがない: no way to perform an action."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches

YOUGANAI_EXPRESSION = '〜ようがない'


def match_youganai(tokens):
    for i in range(1, len(tokens) - 2):
        verb, you, particle, nai = tokens[i - 1:i + 3]
        if not all(adjacent(a, b) for a, b in ((verb, you), (you, particle), (particle, nai))):
            continue
        if not (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith('連用形')):
            continue
        if (you['surface'], you['lemma'], you['pos'][:2]) != ('よう', 'よう', ['接尾辞', '名詞的']):
            continue
        if particle['surface'] not in ('が', 'も') or particle['lemma'] != particle['surface'] or particle['pos'][:1] != ['助詞']:
            continue
        end = adjective_inflection_end(tokens, i + 2, lemma='ない', pos='形容詞',
                                       base='ない', past_stem='なかっ', continuative_stem='なく',
                                       conditional_stem='なけれ', forms=('base', 'past', 'te', 'conditional'))
        if end is not None:
            if next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
            yield you['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=35, id='youganai', expression=YOUGANAI_EXPRESSION,
                   reading='ようがない', meaning='there is no way to do', match=match_youganai)
RULES = (RULE,)
