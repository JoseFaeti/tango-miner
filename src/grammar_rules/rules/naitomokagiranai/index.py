"""Possibility expressed by negative predicate + とも限らない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end


def match_naitomokagiranai(tokens):
    for i in range(len(tokens) - 4):
        negative, to, mo, verb, suffix = tokens[i:i + 5]
        if (negative['surface'] != 'ない' or negative['lemma'] != 'ない'
                or negative['pos'][:1] not in (['助動詞'], ['形容詞'])
                or to['surface'] != 'と' or to['pos'][:2] != ['助詞', '格助詞']
                or mo['surface'] != 'も' or mo['pos'][:2] != ['助詞', '係助詞']
                or verb['lemma'] not in ('限る', 'かぎる')
                or verb['pos'][:1] != ['動詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (negative, to, mo, verb), (to, mo, verb, suffix)))):
            continue
        end = None
        if verb['surface'] in ('限ら', 'かぎら'):
            end = negative_suffix_end(tokens, i + 4, pos='助動詞', continuative=True,
                                      conditional=True, concessive=True)
        elif verb['surface'] in ('限り', 'かぎり'):
            end = polite_suffix_end(tokens, i + 4, forms=('negative',))
        if end is not None:
            yield negative['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=2, id='naitomokagiranai', expression='〜ないとも限らない',
                   reading='ないともかぎらない', meaning='might; cannot rule out the possibility',
                   match=match_naitomokagiranai)
RULES = (RULE,)
