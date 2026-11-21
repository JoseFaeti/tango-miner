"""An implied message conveyed by someone's behavior."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nbakarini.index import match_nbakarini


def match_toiwanbakarini(tokens):
    for start, end in match_nbakarini(tokens):
        for i in range(1, len(tokens)):
            verb = tokens[i]
            particle = tokens[i - 1]
            if (verb['span'][1] == start and verb['surface'] in ('言わ', 'いわ')
                    and verb['lemma'] in ('言う', 'いう') and verb['pos'][:1] == ['動詞']
                    and particle['surface'] == 'と' and particle['lemma'] == 'と'
                    and particle['pos'][:1] == ['助詞'] and adjacent(particle, verb)):
                yield particle['span'][0], end


RULE = GrammarRule(id='toiwanbakarini', expression='〜と言わんばかりに',
                   reading='といわんばかりに', meaning='as if to say',
                   match=match_toiwanbakarini, order=1)
RULES = (RULE,)
