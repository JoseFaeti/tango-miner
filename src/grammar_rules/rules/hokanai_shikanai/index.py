"""No choice but to perform a verb, with しか or ほか."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


HOKANAI_SHIKANAI_EXPRESSION = '〜ほかない / 〜しかない'


def match_hokanai_shikanai(tokens):
    for i in range(1, len(tokens) - 1):
        verb, marker = tokens[i - 1:i + 1]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or not adjacent(verb, marker)):
            continue
        if not ((marker['surface'] == 'しか' and marker['lemma'] == 'しか'
                 and marker['pos'][:2] == ['助詞', '副助詞'])
                or (marker['surface'] in ('ほか', '他') and marker['lemma'] in ('ほか', '他')
                    and marker['pos'][:1] == ['名詞'])):
            continue
        j = i + 1
        if marker['surface'] in ('ほか', '他') and next_matches(tokens, i, 'は', '助詞', 'は'):
            j += 1
        if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        end = negative_predicate_end(tokens, j, polite_lemma='ある', polite_stems={'あり'})
        if end is not None:
            yield marker['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=44, id='hokanai_shikanai', expression=HOKANAI_SHIKANAI_EXPRESSION,
                   reading='ほかない / しかない', meaning='have no choice but to',
                   match=match_hokanai_shikanai)
RULES = (RULE,)
