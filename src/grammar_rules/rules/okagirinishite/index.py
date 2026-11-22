"""A time or event marks the end of an activity, followed by して."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.okagirini.index import match_okagirini


def match_okagirinishite(tokens):
    prefixes = set(match_okagirini(tokens))
    for i in range(len(tokens) - 2):
        ni, shi, te = tokens[i:i + 3]
        if (shi['surface'] == 'し' and shi['lemma'] == 'する'
                and shi['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and adjacent(ni, shi) and adjacent(shi, te)):
            for start, end in prefixes:
                if end == ni['span'][1]:
                    yield start, te['span'][1]


RULE = GrammarRule(order=3, id='okagirinishite', expression='〜を限りにして',
                   reading='をかぎりにして', meaning='ending with; as of (a time or event)',
                   match=match_okagirinishite)
RULES = (RULE,)
