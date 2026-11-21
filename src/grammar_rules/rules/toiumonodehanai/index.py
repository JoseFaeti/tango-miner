"""Partial negation expressed with というものではない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_toiumonodehanai(tokens):
    for i in range(1, len(tokens) - 4):
        previous, particle, verb, mono, copula = tokens[i - 1:i + 4]
        if (particle['surface'] not in ('と', 'って') or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う') or verb['lemma'] not in ('いう', '言う')
                or verb['pos'][:1] != ['動詞'] or mono['surface'] != 'もの'
                or mono['lemma'] != 'もの' or mono['pos'][:1] != ['名詞']
                or previous['pos'][:1] == ['補助記号']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, particle, verb, mono), (particle, verb, mono, copula)))):
            continue
        if (copula['surface'] == 'で' and copula['pos'][:1] in (['助動詞'], ['助詞'])
                and next_matches(tokens, i + 3, 'は', '助詞', 'は')):
            start = i + 5
        elif (copula['surface'], copula['lemma'], copula['pos'][:1]) == ('じゃ', 'だ', ['助動詞']):
            start = i + 4
        else:
            continue
        if start >= len(tokens) or not adjacent(tokens[start - 1], tokens[start]):
            continue
        end = negative_predicate_end(tokens, start, polite_lemma='ある', polite_stems=('あり',))
        if end is not None:
            if tokens[start]['lemma'] == 'ない' and next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
            yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=71, id='toiumonodehanai', expression='〜というものではない',
                   reading='というものではない', meaning='not necessarily; it is not the case that',
                   match=match_toiumonodehanai)
RULES = (RULE,)
