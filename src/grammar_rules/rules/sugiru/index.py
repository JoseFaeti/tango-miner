"""Excess expressed by a verb or adjective followed by すぎる."""
SUGIRU_EXPRESSION = '〜すぎる'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end


def match_sugiru(tokens):
    for i, token in enumerate(tokens):
        lemma, surface = token['lemma'], token['surface']
        if token['pos'][:1] != ['動詞'] or not isinstance(lemma, str):
            continue
        suffix = next((s for s in ('すぎる', '過ぎる') if lemma.endswith(s)), None)
        if suffix is None:
            continue
        if lemma == suffix:
            if i == 0:
                continue
            previous = tokens[i - 1]
            pos = previous['pos']
            verb = pos[:1] == ['動詞'] and len(pos) >= 6 and pos[5].startswith('連用形')
            adjective = pos[:1] == ['形容詞'] and len(pos) >= 6 and pos[5].startswith('語幹')
            if not (adjacent(previous, token) and (verb or adjective or pos[:1] == ['形状詞'])):
                continue
            start = token['span'][0]
        else:
            if token['pos'][:2] != ['動詞', '非自立可能']:
                continue
            start = token['span'][0]
        stem = suffix[:-1]
        end = None
        if surface.endswith(suffix):
            end = i
        elif surface.endswith(stem):
            if next_matches(tokens, i, 'た', '助動詞', 'た') or next_matches(tokens, i, 'て', '助詞', 'て'):
                end = i + 1
            elif next_matches(tokens, i, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, i + 1)
            elif next_matches(tokens, i, 'ない', '助動詞'):
                end = negative_suffix_end(tokens, i + 1, pos='助動詞', extended=True)
        elif surface.endswith(stem + 'れ') and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=175, id='sugiru', expression=SUGIRU_EXPRESSION, reading='すぎる',
                   meaning='too much; excessively', match=match_sugiru)

RULES = (RULE,)
