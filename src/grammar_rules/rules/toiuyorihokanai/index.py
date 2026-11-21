"""An assessment is the only thing one can say."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


TOIUYORIHOKANAI_EXPRESSION = '〜と言うよりほかない'


def match_toiuyorihokanai(tokens):
    for i in range(len(tokens) - 4):
        to, iu, yori, hoka = tokens[i:i + 4]
        if not (to['surface'] == 'と' and to['pos'][:2] == ['助詞', '格助詞']
                and iu['surface'] in ('言う', 'いう') and iu['lemma'] in ('言う', 'いう')
                and iu['pos'][:1] == ['動詞']
                and yori['surface'] == 'より' and yori['pos'][:1] == ['助詞']
                and hoka['surface'] in ('ほか', '他') and hoka['lemma'] in ('ほか', '他')
                and hoka['pos'][:1] == ['名詞']
                and all(adjacent(a, b) for a, b in zip(tokens[i:i + 3], tokens[i + 1:i + 4]))):
            continue
        j = i + 4
        if next_matches(tokens, i + 3, 'は', '助詞', 'は'):
            j += 1
        if j >= len(tokens) or not adjacent(tokens[j - 1], tokens[j]):
            continue
        end = negative_predicate_end(tokens, j, polite_lemma='ある', polite_stems={'あり'})
        if end is not None:
            yield to['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=43, id='toiuyorihokanai', expression=TOIUYORIHOKANAI_EXPRESSION,
                   reading='というよりほかない', meaning='can only be described as',
                   match=match_toiuyorihokanai)
RULES = (RULE,)
