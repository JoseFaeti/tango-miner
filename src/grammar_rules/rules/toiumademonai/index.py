"""〜と言うまでもない: needless to say."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.mademonai.index import match_mademonai


def match_toiumademonai(tokens):
    endings = dict(match_mademonai(tokens))
    for i in range(len(tokens) - 2):
        particle, verb, made = tokens[i:i + 3]
        if (particle['surface'] == particle['lemma'] == 'と'
                and particle['pos'][:1] == ['助詞']
                and verb['surface'] in ('言う', 'いう')
                and verb['lemma'] in ('言う', 'いう')
                and adjacent(particle, verb) and adjacent(verb, made)
                and made['span'][0] in endings):
            yield particle['span'][0], endings[made['span'][0]]


RULE = GrammarRule(id='toiumademonai', expression='〜と言うまでもない',
                   reading='というまでもない', meaning='needless to say',
                   match=match_toiumademonai, order=79)
RULES = (RULE,)
