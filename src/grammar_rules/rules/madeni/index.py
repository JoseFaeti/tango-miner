"""Deadline expressions with separately tokenized までに."""
from src.GrammarConstants import MADENI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_madeni(tokens):
    for i in range(1, len(tokens) - 1):
        previous, made, ni = tokens[i - 1:i + 2]
        if (previous['pos'][:1] not in (['名詞'], ['代名詞'], ['動詞'], ['助動詞'])
                or made['surface'] != 'まで' or made['lemma'] != 'まで'
                or made['pos'][:2] != ['助詞', '副助詞']
                or ni['surface'] != 'に' or ni['lemma'] != 'に'
                or ni['pos'][:2] != ['助詞', '格助詞']
                or not adjacent(previous, made) or not adjacent(made, ni)):
            continue
        # Locative demonstratives followed by an adjective can express degree.
        if (previous['lemma'] in ('ここ', 'そこ', 'あそこ', 'どこ')
                and i + 2 < len(tokens) and adjacent(ni, tokens[i + 2])
                and tokens[i + 2]['pos'][:1] == ['形容詞']):
            continue
        yield made['span'][0], ni['span'][1]


RULE = GrammarRule(
    id='madeni', expression=MADENI_EXPRESSION, reading='までに',
    meaning='by; no later than (a deadline)', match=match_madeni,
)
