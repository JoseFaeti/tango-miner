"""Source-aligned recall and confirmation particle っけ."""
TSUKE_EXPRESSION = '〜っけ'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tsuke(tokens):
    for previous, marker in zip(tokens, tokens[1:]):
        if (marker['surface'] == 'っけ' and marker['lemma'] == 'っけ'
                and marker['pos'][:2] == ['助詞', '終助詞']
                and adjacent(previous, marker)
                and previous['pos'][:1] not in (['空白'], ['補助記号'])):
            yield marker['span'][0], marker['span'][1]


RULE = GrammarRule(order=43, id='tsuke', expression=TSUKE_EXPRESSION,
                   reading='っけ', meaning='recalling or confirming', match=match_tsuke)

RULES = (RULE,)
