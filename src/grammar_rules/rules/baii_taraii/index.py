"""Conditional plus いい for suggestions, wishes, and sufficiency."""
BAII_TARAII_EXPRESSION = '〜ばいい / 〜たらいい'
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.ba.index import match_ba
from src.grammar_rules.rules.tara.index import match_tara


def match_baii_taraii(tokens):
    conditionals = set(match_ba(tokens)) | set(match_tara(tokens))
    for i in range(len(tokens) - 1):
        conditional, adjective = tokens[i:i + 2]
        if (tuple(conditional['span']) in conditionals and adjacent(conditional, adjective)
                and adjective['pos'][:1] == ['形容詞']
                and adjective['lemma'] in ('いい', 'よい', '良い')
                and adjective['surface'] in ('いい', 'よい', '良い')):
            yield conditional['span'][0], adjective['span'][1]


RULE = GrammarRule(order=26, id='baii_taraii', expression=BAII_TARAII_EXPRESSION,
                   reading='ばいい / たらいい',
                   meaning='should; it would be good if; all one needs to do',
                   match=match_baii_taraii)

RULES = (RULE,)
