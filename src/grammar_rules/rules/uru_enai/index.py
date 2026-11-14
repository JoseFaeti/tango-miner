"""Possibility expressed by a verb's continuative form plus 得る."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end

URU_ENAI_EXPRESSION = '〜得る / 〜得ない'


def match_uru_enai(tokens):
    for i in range(1, len(tokens)):
        verb, suffix = tokens[i - 1:i + 1]
        if not (adjacent(verb, suffix) and verb['pos'][:1] == ['動詞']
                and len(verb['pos']) >= 6 and verb['pos'][5].startswith('連用形')
                and suffix['pos'][:1] == ['動詞']
                and suffix['lemma'] in ('得る', 'える', 'うる')):
            continue
        surface = suffix['surface']
        end = None
        if surface in ('得る', 'える', 'うる'):
            end = i
        elif surface in ('得れ', 'えれ') and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        elif surface in ('得', 'え'):
            if next_matches(tokens, i, 'ない', '助動詞'):
                end = negative_suffix_end(tokens, i + 1, pos='助動詞',
                                          continuative=True, conditional=True)
            elif next_matches(tokens, i, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, i + 1, forms=('positive', 'negative'))
            elif (next_matches(tokens, i, 'た', '助動詞', 'た')
                  or next_matches(tokens, i, 'て', '助詞', 'て')
                  or next_matches(tokens, i, 'ぬ', '助動詞', 'ぬ')):
                end = i + 1
        if end is not None:
            yield suffix['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(id='uru_enai', expression=URU_ENAI_EXPRESSION, reading='うる / えない',
                   meaning='can happen; be possible / cannot happen; be impossible',
                   match=match_uru_enai, order=181)
RULES = (RULE,)
