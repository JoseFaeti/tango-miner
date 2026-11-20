"""Non-necessity expressed by とは限らない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end


def match_tohakagiranai(tokens):
    for i in range(len(tokens) - 3):
        to, wa, verb, suffix = tokens[i:i + 4]
        if (to['surface'] != 'と' or to['pos'][:2] != ['助詞', '格助詞']
                or wa['surface'] != 'は' or wa['pos'][:2] != ['助詞', '係助詞']
                or verb['lemma'] not in ('限る', 'かぎる')
                or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip((to, wa, verb), (wa, verb, suffix)))):
            continue
        end = None
        if verb['surface'] in ('限ら', 'かぎら'):
            end = negative_suffix_end(tokens, i + 3, pos='助動詞', continuative=True,
                                      conditional=True, concessive=True)
        elif verb['surface'] in ('限り', 'かぎり'):
            end = polite_suffix_end(tokens, i + 3, forms=('negative',))
        if end is not None:
            yield to['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='tohakagiranai', expression='〜とは限らない',
                   reading='とはかぎらない', meaning='not necessarily; not always',
                   match=match_tohakagiranai)
RULES = (RULE,)
