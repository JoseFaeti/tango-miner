"""Analyzer-labeled imperatives and dictionary-form verb plus final な."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.te_kudasai.index import match_te_kudasai

RO_NA_EXPRESSION = '〜ろ / 〜な'


def match_ro_na(tokens):
    request_ends = {end for _, end in match_te_kudasai(tokens)}
    for i, token in enumerate(tokens):
        pos = token['pos']
        if (pos[:1] == ['動詞'] and len(pos) >= 6 and pos[5].startswith('命令形')
                and token['span'][1] not in request_ends):
            yield token['span'][0], token['span'][1]
        if i == 0 or token['surface'] != 'な' or token['lemma'] != 'な':
            continue
        verb = tokens[i - 1]
        vpos = verb['pos']
        if (pos[:2] == ['助詞', '終助詞'] and vpos[:1] == ['動詞']
                and len(vpos) >= 6 and vpos[5].startswith('終止形')
                and adjacent(verb, token)):
            yield token['span'][0], token['span'][1]


RULE = GrammarRule(id='ro_na', expression=RO_NA_EXPRESSION, reading='ろ / な',
                   meaning='do (imperative); do not (prohibition)', match=match_ro_na)
RULES = (RULE,)
