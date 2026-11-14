"""Difficulty or unwillingness expressed by a continuative verb plus かねる."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, next_matches, polite_suffix_end

KANERU_EXPRESSION = '〜かねる'


def match_kaneru(tokens):
    for i, token in enumerate(tokens):
        lemma, surface = token['lemma'], token['surface']
        if (token['pos'][:1] != ['動詞'] and token['pos'][:2] != ['接尾辞', '動詞的']) or not isinstance(lemma, str):
            continue
        suffix = next((s for s in ('かねる', '兼ねる') if lemma.endswith(s)), None)
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
        stem = suffix[:-1]
        end = None
        if surface.endswith(suffix):
            end = i
        elif surface.endswith(stem):
            if next_matches(tokens, i, 'た', '助動詞', 'た') or next_matches(tokens, i, 'て', '助詞', 'て'):
                end = i + 1
            elif next_matches(tokens, i, 'ます', '助動詞'):
                end = polite_suffix_end(tokens, i + 1, forms=('positive',))
        elif surface.endswith(stem + 'れ') and next_matches(tokens, i, 'ば', '助詞', 'ば'):
            end = i + 1
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=181, id='kaneru', expression=KANERU_EXPRESSION,
                   reading='かねる', meaning='find it difficult to do; be unable or unwilling to do',
                   match=match_kaneru)

RULES = (RULE,)
