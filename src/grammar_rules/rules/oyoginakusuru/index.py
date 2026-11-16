"""Active 余儀なくする following an object."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches, polite_suffix_end


def match_oyoginakusuru(tokens):
    for i in range(1, len(tokens) - 3):
        before, particle, yogi, naku, verb = tokens[i - 1:i + 4]
        if (before['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or yogi['surface'] != '余儀' or yogi['pos'][:1] != ['名詞']
                or naku['surface'] != 'なく' or naku['lemma'] != 'ない'
                or verb['lemma'] != 'する' or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        j = i + 3
        end = None
        if verb['surface'] == 'する':
            end = j
        elif verb['surface'] == 'し':
            if next_matches(tokens, j, 'た', '助動詞') or next_matches(tokens, j, 'て', '助詞', 'て'):
                end = j + 1
            elif next_matches(tokens, j, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, j + 1, forms=('positive', 'negative'))
            elif next_matches(tokens, j, 'ない', '助動詞'):
                end = adjective_inflection_end(tokens, j + 1, lemma='ない', pos='助動詞',
                    base='ない', past_stem='なかっ', continuative_stem='なく', conditional_stem='なけれ')
        elif verb['surface'] == 'すれ' and next_matches(tokens, j, 'ば', '助詞', 'ば'):
            end = j + 1
        if end is not None:
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='oyoginakusuru', expression='〜を余儀なくする',
                   reading='をよぎなくする', meaning='force someone to; necessitate',
                   match=match_oyoginakusuru, order=30)
RULES = (RULE,)
