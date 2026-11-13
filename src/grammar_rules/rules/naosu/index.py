"""Repeated action expressed by a compound verb ending in 直す."""
NAOSU_GRAMMAR_EXPRESSION = '〜直す'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end


def match_naosu(tokens):
    for i, token in enumerate(tokens):
        lemma, surface = token['lemma'], token['surface']
        if token['pos'][:1] != ['動詞'] or not isinstance(lemma, str):
            continue
        suffix = next((s for s in ('直す', 'なおす') if lemma.endswith(s)), None)
        if suffix is None:
            continue
        start = token['span'][0]
        if lemma == suffix:
            if i == 0:
                continue
            previous = tokens[i - 1]
            if not (adjacent(previous, token) and previous['pos'][:1] == ['動詞']
                    and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形')):
                continue
            start = previous['span'][0]
        stem = suffix[:-1]
        end = None
        if surface.endswith(suffix):
            end = i
        elif surface.endswith(stem + 'し'):
            if next_matches(tokens, i, 'た', '助動詞') or next_matches(tokens, i, 'て', '助詞', 'て'):
                end = i + 1
            elif next_matches(tokens, i, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, i + 1)
        elif surface.endswith(stem + 'さ') and next_matches(tokens, i, 'ない', '助動詞'):
            end = negative_suffix_end(tokens, i + 1, pos='助動詞', extended=True)
        elif surface.endswith(stem + 'せ') and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=85, id='naosu', expression=NAOSU_GRAMMAR_EXPRESSION,
                   reading='なおす', meaning='do again; redo', match=match_naosu)

RULES = (RULE,)
