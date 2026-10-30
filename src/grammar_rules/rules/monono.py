"""Rule for monono."""
from src.GrammarConstants import MONONO_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


RULE = GrammarRule(
    id='concessive_monono', expression=MONONO_GRAMMAR_EXPRESSION, reading='ものの',
    meaning='although; even though; but',
    match=lambda tokens: match_monono(tokens),
)


def match_monono(tokens):
    # Require Sudachi's conjunction analysis and an adjacent predicate. Split
    # nominal もの + の remains lexical, even after a verb or adjective.
    for i in range(1, len(tokens) - 1):
        previous, token = tokens[i - 1:i + 1]
        if (token['surface'] != 'ものの' or token['lemma'] != 'ものの'
                or token['pos'][:1] != ['接続詞']
                or not adjacent(previous, token)
                or previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith(('終止形', '連体形'))):
            continue
        # A following clause must at least begin; punctuation alone is not one.
        if not any(t['pos'][:1] != ['補助記号'] for t in tokens[i + 1:]):
            continue
        yield tuple(token['span'])
