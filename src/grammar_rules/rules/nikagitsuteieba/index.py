"""Limit the scope of a statement with に限って言えば."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.nikagitsute.index import match_nikagitsute


def match_nikagitsuteieba(tokens):
    for start, end in match_nikagitsute(tokens):
        for te, iu, ba in zip(tokens, tokens[1:], tokens[2:]):
            if (te['span'][1] == end and adjacent(te, iu) and adjacent(iu, ba)
                    and iu['surface'] in ('言え', 'いえ')
                    and iu['lemma'] in ('言う', 'いう') and iu['pos'][:1] == ['動詞']
                    and ba['surface'] == ba['lemma'] == 'ば'
                    and ba['pos'][:2] == ['助詞', '接続助詞']):
                yield start, ba['span'][1]


RULE = GrammarRule(order=2, id='nikagitsuteieba', expression='〜に限って言えば',
                   reading='にかぎっていえば', meaning='speaking only of; as far as ... is concerned',
                   match=match_nikagitsuteieba)
RULES = (RULE,)
