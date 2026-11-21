"""Repeated action attributed to a stated reason."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_toitsuteha(tokens):
    for particle, verb, te, wa in zip(tokens, tokens[1:], tokens[2:], tokens[3:]):
        if (particle['surface'] == 'と' and particle['pos'][:2] == ['助詞', '格助詞']
                and verb['surface'] in ('いっ', '言っ')
                and verb['lemma'] in ('いう', '言う') and verb['pos'][:1] == ['動詞']
                and te['surface'] == 'て' and te['pos'][:2] == ['助詞', '接続助詞']
                and wa['surface'] == 'は' and wa['pos'][:2] == ['助詞', '係助詞']
                and all(adjacent(a, b) for a, b in
                        ((particle, verb), (verb, te), (te, wa)))):
            yield particle['span'][0], wa['span'][1]


RULE = GrammarRule(id='toitsuteha', expression='〜といっては', reading='といっては',
                   meaning='whenever saying; repeatedly using as a reason',
                   match=match_toitsuteha, order=2)
RULES = (RULE,)
