"""Noun + を抜きにして: without or leaving out."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_onukinishite(tokens):
    for i in range(1, len(tokens) - 4):
        noun, particle, nuki, ni, shi, te = tokens[i - 1:i + 5]
        if (noun['pos'][:1] in (['名詞'], ['代名詞'])
                and particle['surface'] == 'を' and particle['pos'][:1] == ['助詞']
                and nuki['surface'] in ('抜き', 'ぬき')
                and nuki['lemma'] in ('抜き', 'ぬき') and nuki['pos'][:1] == ['名詞']
                and ni['surface'] == 'に' and ni['pos'][:1] == ['助詞']
                and shi['surface'] == 'し' and shi['lemma'] == 'する' and shi['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:1] == ['助詞']
                and all(adjacent(a, b) for a, b in zip(tokens[i - 1:i + 4], tokens[i:i + 5]))):
            yield particle['span'][0], te['span'][1]


RULE = GrammarRule(order=2, id='onukinishite', expression='〜を抜きにして',
                   reading='をぬきにして', meaning='without; leaving out', match=match_onukinishite)
RULES = (RULE,)
