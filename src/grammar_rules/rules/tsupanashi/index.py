"""An action continues or its resulting state is left unchanged."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TSUPANASHI_EXPRESSION = '〜っぱなし'


def match_tsupanashi(tokens):
    for i in range(1, len(tokens)):
        verb, suffix = tokens[i - 1:i + 1]
        pos = verb['pos']
        if (adjacent(verb, suffix) and pos[:1] == ['動詞']
                and len(pos) >= 6 and pos[5].startswith('連用形')
                and suffix['surface'] == suffix['lemma'] == 'っぱなし'
                and suffix['pos'][:2] == ['接尾辞', '形状詞的']):
            yield suffix['span'][0], suffix['span'][1]


RULE = GrammarRule(order=84, id='tsupanashi', expression=TSUPANASHI_EXPRESSION,
                   reading='っぱなし', meaning='keep doing; leave in a resulting state',
                   match=match_tsupanashi)

RULES = (RULE,)
