"""An event occurring during an ongoing state or action."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent
from src.grammar_rules.rules.te_iru.index import match_te_iru


def match_teiru_aida_ni(tokens):
    continuations = {end: start for start, end in match_te_iru(tokens)}
    for i in range(1, len(tokens) - 1):
        previous, marker, particle = tokens[i - 1:i + 2]
        start = continuations.get(previous['span'][1])
        if (start is not None and previous['surface'] in ('いる', '居る', 'てる', 'でる')
                and marker['surface'] in ('間', 'あいだ') and marker['lemma'] in ('間', 'あいだ')
                and marker['pos'][:1] == ['名詞']
                and particle['surface'] == 'に' and particle['lemma'] == 'に'
                and particle['pos'][:2] == ['助詞', '格助詞']
                and adjacent(previous, marker) and adjacent(marker, particle)):
            yield start, particle['span'][1]


RULE = GrammarRule(order=104, id='teiru_aida_ni', expression='〜ている間に',
                   reading='ているあいだに', meaning='while; during the time that',
                   match=match_teiru_aida_ni)
RULES = (RULE,)
