"""Treat a proposition as an assumption: ものとして."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_monotoshite(tokens):
    for i in range(1, len(tokens) - 3):
        predicate, mono, to, shi, te = tokens[i - 1:i + 4]
        if (predicate['pos'][:1] not in (['動詞'], ['形容詞'], ['助動詞'])
                or len(predicate['pos']) < 6
                or not predicate['pos'][5].startswith('連体形')
                or mono['surface'] != 'もの' or mono['lemma'] != 'もの'
                or mono['pos'][:1] != ['名詞']
                or to['surface'] != 'と' or to['pos'][:1] != ['助詞']
                or shi['surface'] != 'し' or shi['lemma'] != 'する'
                or shi['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip(tokens[i - 1:i + 3], tokens[i:i + 4]))):
            continue
        yield mono['span'][0], te['span'][1]


RULE = GrammarRule(id='monotoshite', expression='〜ものとして', reading='ものとして',
                   meaning='assuming that; treating as', match=match_monotoshite)
RULES = (RULE,)
