"""A continuative verb plus かねない, expressing an undesirable possibility."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, next_matches

KANENAI_EXPRESSION = '〜かねない'


def match_kanenai(tokens):
    for i, token in enumerate(tokens):
        lemma, surface = token['lemma'], token['surface']
        if (token['pos'][:1] != ['動詞'] and token['pos'][:2] != ['接尾辞', '動詞的']) or not isinstance(lemma, str):
            continue
        suffix = next((s for s in ('かねる', '兼ねる') if lemma.endswith(s)), None)
        if suffix is None or not surface.endswith(suffix[:-1]):
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
        if not next_matches(tokens, i, 'ない', '助動詞'):
            continue
        end = negative_suffix_end(tokens, i + 1, pos='助動詞', continuative=True, conditional=True)
        if end is not None:
            yield start, tokens[end]['span'][1]


RULE = GrammarRule(order=182, id='kanenai', expression=KANENAI_EXPRESSION,
                   reading='かねない', meaning='might; could (an undesirable outcome)',
                   match=match_kanenai)
RULES = (RULE,)
