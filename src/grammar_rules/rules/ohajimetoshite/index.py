"""A leading example followed by として."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.ohajime.index import match_ohajime


def match_ohajimetoshite(tokens):
    prefixes = set(match_ohajime(tokens))
    for i in range(len(tokens) - 4):
        wo, hajime, to, shi, te = tokens[i:i + 5]
        if ((wo['span'][0], hajime['span'][1]) in prefixes
                and to['surface'] == 'と' and to['pos'][:1] == ['助詞']
                and shi['surface'] == 'し' and shi['lemma'] == 'する'
                and shi['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip((hajime, to, shi), (to, shi, te)))):
            yield wo['span'][0], te['span'][1]


RULE = GrammarRule(id='ohajimetoshite', expression='〜をはじめとして',
                   reading='をはじめとして', meaning='including; starting with a leading example',
                   match=match_ohajimetoshite)
RULES = (RULE,)
