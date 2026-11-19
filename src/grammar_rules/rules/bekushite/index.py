"""An event occurring as was inevitable or expected."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

BEKUSHITE_EXPRESSION = '〜べくして'


def match_bekushite(tokens):
    for i in range(1, len(tokens) - 2):
        verb, beku, shi, te = tokens[i - 1:i + 3]
        if (verb['pos'][:1] == ['動詞'] and len(verb['pos']) >= 6
                and verb['pos'][5].startswith(('終止形', '連体形'))
                and beku['surface'] == 'べく' and beku['lemma'] == 'べし'
                and beku['pos'][:1] == ['助動詞']
                and shi['surface'] == 'し' and shi['lemma'] == 'する'
                and shi['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and adjacent(verb, beku) and adjacent(beku, shi)
                and adjacent(shi, te)):
            yield (beku['span'][0], te['span'][1])


RULE = GrammarRule(id='bekushite', expression=BEKUSHITE_EXPRESSION,
                   reading='べくして', meaning='as was inevitable; as expected',
                   match=match_bekushite, order=155)
RULES = (RULE,)
