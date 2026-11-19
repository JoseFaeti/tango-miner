"""Extreme degree following a na-adjectival stem."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, adjective_inflection_end, next_matches

KIWAMARINAI_KIWAMARU_EXPRESSION = '〜極まりない / 〜極まる'


def match_kiwamarinai_kiwamaru(tokens):
    for i in range(1, len(tokens)):
        previous, token = tokens[i - 1:i + 1]
        pos = previous['pos']
        if not (adjacent(previous, token) and
                (pos[:1] == ['形状詞'] or
                 (pos[:1] == ['名詞'] and len(pos) > 2 and '形状詞可能' in pos[2]))):
            continue
        end = None
        if token['lemma'] == '極まりない':
            end = adjective_inflection_end(tokens, i, lemma='極まりない', pos='形容詞',
                base='極まりない', past_stem='極まりなかっ', continuative_stem='極まりなく',
                conditional_stem='極まりなけれ')
        elif token['surface'] == 'きわまり' and token['lemma'] == 'きわまり' and i + 1 < len(tokens) and adjacent(token, tokens[i + 1]):
            end = adjective_inflection_end(tokens, i + 1, lemma='ない', pos='形容詞',
                base='ない', past_stem='なかっ', continuative_stem='なく', conditional_stem='なけれ')
        elif token['lemma'] == '極まる' and token['pos'][:1] == ['動詞']:
            if token['surface'] == '極まる':
                end = i
            elif token['surface'] == '極まっ' and (next_matches(tokens, i, 'た', '助動詞') or next_matches(tokens, i, 'て', '助詞', 'て')):
                end = i + 1
        if end is not None:
            yield token['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=181, id='kiwamarinai_kiwamaru',
                   expression=KIWAMARINAI_KIWAMARU_EXPRESSION, reading='きわまりない / きわまる',
                   meaning='extremely; exceedingly', match=match_kiwamarinai_kiwamaru)
RULES = (RULE,)
