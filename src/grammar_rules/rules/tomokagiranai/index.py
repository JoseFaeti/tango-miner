"""Non-necessity expressed by とも限らない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end


def match_tomokagiranai(tokens):
    for i in range(len(tokens) - 3):
        to, mo, verb, suffix = tokens[i:i + 4]
        if (i and adjacent(tokens[i - 1], to)
                and tokens[i - 1]['surface'] == 'ない'
                and tokens[i - 1]['lemma'] == 'ない'
                and tokens[i - 1]['pos'][:1] in (['助動詞'], ['形容詞'])):
            continue
        if (to['surface'] != 'と' or to['pos'][:2] != ['助詞', '格助詞']
                or mo['surface'] != 'も' or mo['pos'][:2] != ['助詞', '係助詞']
                or verb['lemma'] not in ('限る', 'かぎる')
                or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip((to, mo, verb), (mo, verb, suffix)))):
            continue
        end = None
        if verb['surface'] in ('限ら', 'かぎら'):
            end = negative_suffix_end(tokens, i + 3, pos='助動詞', continuative=True,
                                      conditional=True, concessive=True)
        elif verb['surface'] in ('限り', 'かぎり'):
            end = polite_suffix_end(tokens, i + 3, forms=('negative',))
        if end is not None:
            yield to['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='tomokagiranai', expression='〜とも限らない',
                   reading='ともかぎらない', meaning='not necessarily; not always',
                   match=match_tomokagiranai)
RULES = (RULE,)
