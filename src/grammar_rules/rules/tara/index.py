"""Source-aligned hypothetical た / だ auxiliary after a predicate."""
from src.GrammarConstants import TARA_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


RULE = GrammarRule(
    id='conditional_tara', expression=TARA_GRAMMAR_EXPRESSION, reading='たら',
    meaning='if; when; after; once',
    match=lambda tokens: match_tara(tokens),
)


def match_tara(tokens):
    for i in range(1, len(tokens) - 1):
        previous, conditional = tokens[i - 1:i + 1]
        if ((conditional['surface'], conditional['lemma']) not in (('たら', 'た'), ('だら', 'だ'))
                or conditional['pos'][:1] != ['助動詞']
                or len(conditional['pos']) < 6
                or not conditional['pos'][5].startswith('仮定形')
                or not adjacent(previous, conditional)
                or previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith('連用形')):
            continue
        # Do not treat a fragment ending at punctuation as a complete example.
        # A comma can separate the condition and the following clause.
        following = tokens[i + 1:]
        if not any(t['pos'][:1] != ['補助記号'] for t in following):
            continue
        yield tuple(conditional['span'])
