"""Verb + ものではない: admonition, with literal-thing ambiguity."""
MONODEHANAI_EXPRESSION = '〜ものではない'

from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches
from src.grammar_rules.rules.toiumonodehanai.index import match_toiumonodehanai


def match_monodehanai(tokens):
    longer_spans = list(match_toiumonodehanai(tokens))
    for i in range(1, len(tokens) - 1):
        verb, mono, copula = tokens[i - 1:i + 2]
        pos = verb['pos']
        if (pos[:1] != ['動詞'] or len(pos) < 6
                or not pos[5].startswith(('終止形', '連体形'))
                or mono['surface'] != 'もの' or mono['lemma'] != 'もの'
                or mono['pos'][:1] != ['名詞']
                or not adjacent(verb, mono) or not adjacent(mono, copula)):
            continue
        if (copula['surface'] == 'で' and copula['pos'][:1] in (['助動詞'], ['助詞'])
                and next_matches(tokens, i + 1, 'は', '助詞', 'は')):
            start = i + 3
        elif (copula['surface'], copula['lemma'], copula['pos'][:1]) == ('じゃ', 'だ', ['助動詞']):
            start = i + 2
        else:
            continue
        if start >= len(tokens) or not adjacent(tokens[start - 1], tokens[start]):
            continue
        end = negative_predicate_end(tokens, start, polite_lemma='ある', polite_stems=('あり',))
        if end is not None:
            if tokens[start]['lemma'] == 'ない' and next_matches(tokens, end, 'です', '助動詞', 'です'):
                end += 1
            if not any(start <= mono['span'][0] and tokens[end]['span'][1] <= stop
                       for start, stop in longer_spans):
                yield mono['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=72, id='monodehanai', expression=MONODEHANAI_EXPRESSION,
                   reading='ものではない', meaning='should not; it is not proper to',
                   match=match_monodehanai)

RULES = (RULE,)
