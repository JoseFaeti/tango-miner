"""Source-aligned verbal negative auxiliary."""
from src.GrammarConstants import NAI_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end


RULE = GrammarRule(
    id='verbal_nai', expression=NAI_GRAMMAR_EXPRESSION, reading='ない',
    meaning='not; do not; does not',
    match=lambda tokens: match_nai(tokens),
)


def match_nai(tokens):
    for i in range(1, len(tokens)):
        previous, negative = tokens[i - 1:i + 1]
        if (not adjacent(previous, negative) or negative['lemma'] != 'ない'
                or negative['pos'][:1] != ['助動詞']
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith('未然形')):
            continue
        if not (previous['pos'][:1] == ['動詞'] or
                (previous['pos'][:1] == ['助動詞'] and
                 previous['lemma'] in ('れる', 'られる', 'せる', 'させる'))):
            continue
        end = negative_suffix_end(tokens, i, pos='助動詞', continuative=True, conditional=True)
        if end is None:
            continue
        yield negative['span'][0], tokens[end]['span'][1]
