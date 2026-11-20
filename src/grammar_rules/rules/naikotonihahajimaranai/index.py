"""A necessary condition followed by negative 始まる."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent, negative_suffix_end, polite_suffix_end
from src.grammar_rules.rules.naikotoniha.index import match_naikotoniha


def match_naikotonihahajimaranai(tokens):
    starts = {start: end for start, end in match_naikotoniha(tokens)}
    for i, token in enumerate(tokens):
        end = starts.get(token['span'][0])
        if end is None or i + 5 >= len(tokens):
            continue
        wa, verb, suffix = tokens[i + 3:i + 6]
        if not (wa['span'][1] == end and adjacent(wa, verb) and adjacent(verb, suffix)
                and verb['lemma'] in ('始まる', 'はじまる') and verb['pos'][:1] == ['動詞']):
            continue
        if verb['surface'] in ('始まら', 'はじまら'):
            last = negative_suffix_end(tokens, i + 5, pos='助動詞', continuative=True, conditional=True)
        elif verb['surface'] in ('始まり', 'はじまり'):
            last = polite_suffix_end(tokens, i + 5, forms=('negative',))
        else:
            continue
        if last is not None:
            yield token['span'][0], tokens[last]['span'][1]


RULE = GrammarRule(id='naikotonihahajimaranai', expression='〜ないことには始まらない',
                   reading='ないことにははじまらない', meaning='nothing can begin unless',
                   match=match_naikotonihahajimaranai, order=1)
RULES = (RULE,)
