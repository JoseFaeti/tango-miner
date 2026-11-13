"""Colloquial explanatory ってこと / ってわけ."""
TTEKOTO_TTEWAKE_EXPRESSION = '〜ってこと / 〜ってわけ'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches


def match_ttekoto_ttewake(tokens):
    for i in range(1, len(tokens) - 1):
        previous, particle, noun = tokens[i - 1:i + 2]
        pos = previous['pos']
        if (len(pos) < 6 or pos[:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or not pos[5].startswith(('終止形', '連体形', '意志推量形'))
                or particle['surface'] != 'って' or particle['pos'][:1] != ['助詞']
                or noun['surface'] not in ('こと', '事', 'わけ', '訳')
                or noun['pos'][:2] != ['名詞', '普通名詞']
                or not adjacent(previous, particle) or not adjacent(particle, noun)):
            continue
        end = i + 1
        if end + 1 < len(tokens) and adjacent(noun, tokens[end + 1]):
            suffix = tokens[end + 1]
            if (suffix['lemma'], suffix['surface'], suffix['pos'][:1]) in (
                    ('だ', 'だ', ['助動詞']), ('です', 'です', ['助動詞'])):
                end += 1
            elif ((suffix['lemma'], suffix['surface']) in (('だ', 'だっ'), ('です', 'でし'))
                  and suffix['pos'][:1] == ['助動詞']
                  and next_matches(tokens, end + 1, 'た', '助動詞', 'た')):
                end += 2
            elif suffix['pos'][:1] != ['補助記号']:
                continue
        yield particle['span'][0], tokens[end]['span'][1]


RULE = GrammarRule(order=76, id='ttekoto_ttewake', expression=TTEKOTO_TTEWAKE_EXPRESSION,
                   reading='ってこと / ってわけ', meaning='that means; that is why',
                   match=match_ttekoto_ttewake)

RULES = (RULE,)
