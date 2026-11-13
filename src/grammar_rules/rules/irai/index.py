"""Since an adjacent nominal time or event."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

IRAI_EXPRESSION = '〜以来'


def match_irai(tokens):
    for i in range(1, len(tokens)):
        previous, irai = tokens[i - 1:i + 1]
        if (previous['pos'][:1] in (['名詞'], ['代名詞'])
                and irai['surface'] == '以来' and irai['lemma'] == '以来'
                and irai['pos'][:1] == ['名詞'] and adjacent(previous, irai)):
            yield irai['span'][0], irai['span'][1]


RULE = GrammarRule(order=86, id='irai', expression=IRAI_EXPRESSION,
                   reading='いらい', meaning='since (a time or event)', match=match_irai)
RULES = (RULE,)
