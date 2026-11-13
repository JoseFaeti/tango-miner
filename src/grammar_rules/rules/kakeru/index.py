"""An action started but unfinished, or on the verge of happening."""
KAKERU_EXPRESSION = '〜かける'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, negative_suffix_end, polite_suffix_end


def match_kakeru(tokens):
    for i, token in enumerate(tokens):
        lemma = token['lemma']
        if token['pos'][:1] != ['動詞'] or not isinstance(lemma, str):
            continue
        suffix = next((s for s in ('かける', '掛ける') if lemma.endswith(s)), None)
        if suffix is None:
            continue
        start = token['span'][0]
        if lemma == suffix:
            if not i:
                continue
            previous = tokens[i - 1]
            if not (adjacent(previous, token) and previous['pos'][:1] == ['動詞']
                    and len(previous['pos']) >= 6 and previous['pos'][5].startswith('連用形')):
                continue
            start = previous['span'][0]
        surface = token['surface']
        end = None
        if surface.endswith(suffix):
            end = i
        elif surface.endswith(suffix[:-1]):
            if next_matches(tokens, i, 'た', '助動詞', 'た') or next_matches(tokens, i, 'て', '助詞', 'て'):
                end = i + 1
            elif next_matches(tokens, i, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, i + 1)
            elif next_matches(tokens, i, 'ない', '助動詞'):
                end = negative_suffix_end(tokens, i + 1, pos='助動詞', extended=True)
        elif surface.endswith(suffix[:-1] + 'れ') and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=180, id='kakeru', expression=KAKERU_EXPRESSION, reading='かける',
                   meaning='start doing but leave unfinished; be on the verge of doing',
                   match=match_kakeru)

RULES = (RULE,)
