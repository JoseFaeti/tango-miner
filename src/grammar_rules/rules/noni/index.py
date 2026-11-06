"""Attached のに, including ambiguous nominal uses."""
from src.GrammarConstants import NONI_GRAMMAR_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_noni(tokens):
    for i in range(1, len(tokens) - 1):
        previous, no, ni = tokens[i - 1:i + 2]
        if no['surface'] != 'の' or ni['surface'] != 'に':
            continue
        if (previous['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(previous['pos']) < 6
                or not previous['pos'][5].startswith('連体形')
                or not adjacent(previous, no) or not adjacent(no, ni)
                or no['lemma'] != 'の'
                or no['pos'][:2] != ['助詞', '準体助詞']
                or ni['lemma'] != 'に'
                or ni['pos'][:2] != ['助詞', '格助詞']):
            continue
        yield no['span'][0], ni['span'][1]


RULE = GrammarRule(
    id='noni', expression=NONI_GRAMMAR_EXPRESSION, reading='のに',
    meaning='although; even though; for (doing)', match=match_noni,
)
