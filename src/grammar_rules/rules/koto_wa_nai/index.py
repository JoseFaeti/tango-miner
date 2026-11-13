"""Verb + ことはない, including no need and non-occurrence readings."""
KOTO_WA_NAI = '〜ことはない'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_koto_wa_nai(tokens):
    for i in range(1, len(tokens) - 2):
        verb, koto = tokens[i - 1:i + 1]
        if (verb['pos'][:1] != ['動詞'] or len(verb['pos']) < 6
                or not verb['pos'][5].startswith(('終止形', '連体形'))
                or not adjacent(verb, koto)
                or koto['surface'] != 'こと' or koto['pos'][:1] != ['名詞']
                or not next_matches(tokens, i, 'は', '助詞', 'は')
                or not adjacent(tokens[i + 1], tokens[i + 2])):
            continue
        end = negative_predicate_end(tokens, i + 2, polite_lemma='ある',
                                     polite_stems={'あり'})
        if end is not None:
            yield koto['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=81, id='koto_wa_nai', expression=KOTO_WA_NAI,
                   reading='ことはない', meaning='there is no need to; never happens',
                   match=match_koto_wa_nai)

RULES = (RULE,)
