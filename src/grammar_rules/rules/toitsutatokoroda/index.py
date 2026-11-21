"""Approximate assessment expressed with といったところだ."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches

TOITSUTATOKORODA_EXPRESSION = '〜といったところだ'


def match_toitsutatokoroda(tokens):
    for i in range(1, len(tokens) - 4):
        particle, verb, past, noun, copula = tokens[i:i + 5]
        if (not adjacent(tokens[i - 1], particle)
                or particle['surface'] != 'と' or particle['pos'][:2] != ['助詞', '格助詞']
                or verb['surface'] not in ('いっ', '言っ')
                or verb['lemma'] not in ('いう', '言う') or verb['pos'][:1] != ['動詞']
                or past['surface'] != 'た' or past['lemma'] != 'た' or past['pos'][:1] != ['助動詞']
                or noun['surface'] not in ('ところ', '所') or noun['lemma'] not in ('ところ', '所')
                or noun['pos'][:1] != ['名詞'] or copula['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i:i + 4], tokens[i + 1:i + 5]))):
            continue
        if (copula['lemma'], copula['surface']) in (('だ', 'だ'), ('です', 'です')):
            end = i + 4
        elif ((copula['lemma'], copula['surface']) in (('だ', 'だっ'), ('です', 'でし'))
              and next_matches(tokens, i + 4, 'た', '助動詞', 'た')):
            end = i + 5
        else:
            continue
        yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='toitsutatokoroda', expression=TOITSUTATOKORODA_EXPRESSION,
                   reading='といったところだ', meaning='about; approximately; roughly speaking',
                   match=match_toitsutatokoroda)
RULES = (RULE,)
