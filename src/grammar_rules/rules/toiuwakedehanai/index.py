"""Partial negation with というわけではない."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_predicate_end, next_matches


def match_toiuwakedehanai(tokens):
    for i in range(1, len(tokens) - 4):
        previous, particle, verb, wake, suffix = tokens[i - 1:i + 4]
        pos = previous['pos']
        if (len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith(('終止形', '連体形'))
                or particle['surface'] not in ('と', 'って') or particle['pos'][:1] != ['助詞']
                or verb['surface'] not in ('いう', '言う') or verb['lemma'] not in ('いう', '言う')
                or verb['pos'][:1] != ['動詞']
                or wake['surface'] not in ('わけ', '訳') or wake['pos'][:2] != ['名詞', '普通名詞']
                or not all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        if (suffix['surface'] == 'で' and suffix['pos'][:1] == ['助詞']
                and next_matches(tokens, i + 3, 'は', '助詞', 'は')):
            start = i + 5
        elif (suffix['surface'], suffix['lemma'], suffix['pos'][:1]) == ('じゃ', 'だ', ['助動詞']):
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


RULE = GrammarRule(order=88, id='toiuwakedehanai', expression='〜というわけではない',
                   reading='というわけではない', meaning='not necessarily; it does not mean that',
                   match=match_toiuwakedehanai)
RULES = (RULE,)
