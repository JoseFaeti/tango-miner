"""Entry or intensity expressed by a verb followed by 込む."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end

GOMU_EXPRESSION = '〜込む'


def match_gomu(tokens):
    for i, token in enumerate(tokens):
        lemma, surface = token['lemma'], token['surface']
        if token['pos'][:1] != ['動詞'] or not isinstance(lemma, str):
            continue
        suffix = next((s for s in ('込む', 'こむ') if lemma.endswith(s)), None)
        if suffix is None:
            continue
        if lemma == suffix:
            if not i:
                continue
            previous = tokens[i - 1]
            if not (adjacent(previous, token) and previous['pos'][:1] == ['動詞']
                    and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形')):
                continue
            start = previous['span'][0]
        else:
            # Merged kana strings need not contain this compound suffix.
            if suffix == 'こむ':
                continue
            start = token['span'][0]
        stem = suffix[:-1]
        end = None
        if surface.endswith(suffix):
            end = i
        elif surface.endswith(stem + 'ん'):
            if next_matches(tokens, i, 'だ', '助動詞', 'だ') or next_matches(tokens, i, 'で', '助詞', 'で'):
                end = i + 1
        elif surface.endswith(stem + 'み') and next_matches(tokens, i, 'ます', '助動詞'):
            end = polite_suffix_end(tokens, i + 1)
        elif surface.endswith(stem + 'ま') and next_matches(tokens, i, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, i + 1, pos='助動詞', extended=True)
        elif surface.endswith(stem + 'め') and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=184, id='gomu', expression=GOMU_EXPRESSION,
                   reading='こむ', meaning='go into; do deeply or thoroughly',
                   match=match_gomu)
RULES = (RULE,)
