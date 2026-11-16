"""Literary construction describing the duties of a role."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_tarumono(tokens):
    for i in range(1, len(tokens) - 1):
        role, copula, person = tokens[i - 1:i + 2]
        if (role['pos'][:1] == ['名詞']
                and copula['surface'] == 'たる' and copula['lemma'] == 'たり'
                and copula['pos'][:1] == ['助動詞']
                and person['surface'] in ('者', 'もの')
                and person['pos'][:1] == ['名詞']
                and adjacent(role, copula) and adjacent(copula, person)):
            yield copula['span'][0], person['span'][1]


RULE = GrammarRule(id='tarumono', expression='〜たるもの', reading='たるもの',
                   meaning='one who is; a person in that role should (literary)',
                   match=match_tarumono)
RULES = (RULE,)
