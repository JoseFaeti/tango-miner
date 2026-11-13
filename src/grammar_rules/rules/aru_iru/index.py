"""Standalone existence predicates following a subject, topic or location marker."""
ARU_IRU_EXPRESSION = 'ある / いる'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end


def match_aru_iru(tokens):
    for i in range(1, len(tokens)):
        marker, verb = tokens[i - 1:i + 1]
        if (marker['surface'] not in ('が', 'は', 'も', 'に')
                or marker['pos'][:1] != ['助詞'] or not adjacent(marker, verb)
                or verb['pos'][:1] != ['動詞']):
            continue
        lemma, surface = verb['lemma'], verb['surface']
        if lemma in ('ある', '有る', '在る'):
            bases, stems, polite, conditional = ('ある', '有る', '在る'), ('あっ', '有っ', '在っ'), ('あり', '有り', '在り'), ('あれ', '有れ', '在れ')
        elif lemma in ('いる', '居る'):
            bases, stems, polite, conditional = ('いる', '居る'), ('い', '居'), ('い', '居'), ('いれ', '居れ')
        else:
            continue
        end = None
        if surface in bases:
            end = i
        elif surface in stems and (next_matches(tokens, i, 'た', '助動詞', 'た')
                                    or next_matches(tokens, i, 'て', '助詞', 'て')):
            end = i + 1
        elif surface in polite and next_matches(tokens, i, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, i + 1, forms=('positive', 'negative'))
        elif surface in conditional and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        elif lemma in ('いる', '居る') and surface in stems and next_matches(tokens, i, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, i + 1, pos='助動詞', conditional=True)
        if end is not None:
            yield verb['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=168, id='aru_iru', expression=ARU_IRU_EXPRESSION, reading='ある / いる',
                   meaning='exist; be present', match=match_aru_iru)

RULES = (RULE,)
