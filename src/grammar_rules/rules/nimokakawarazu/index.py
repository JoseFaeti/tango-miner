"""Concessive 〜にもかかわらず."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIMOKAKAWARAZU_EXPRESSION = '〜にもかかわらず'


def match_nimokakawarazu(tokens):
    for i in range(1, len(tokens) - 3):
        previous, ni, mo, verb, zu = tokens[i - 1:i + 4]
        if (previous['pos'][:1] == ['補助記号']
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or mo['surface'] != 'も' or mo['pos'][:1] != ['助詞']
                or verb['surface'] not in ('かかわら', '関わら', '拘わら')
                or verb['lemma'] not in ('かかわる', '関わる', '拘わる')
                or verb['pos'][:1] != ['動詞']
                or zu['surface'] != 'ず' or zu['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, ni, mo, verb), (ni, mo, verb, zu)))):
            continue
        # 〜にも関わらずに can be a literal negative verb, not this connective.
        if (i + 4 < len(tokens) and adjacent(zu, tokens[i + 4])
                and tokens[i + 4]['surface'] == 'に'
                and tokens[i + 4]['pos'][:1] == ['助詞']):
            continue
        yield ni['span'][0], zu['span'][1]


RULE = GrammarRule(order=1, id='nimokakawarazu',
                   expression=NIMOKAKAWARAZU_EXPRESSION, reading='にもかかわらず',
                   meaning='despite; although; nevertheless', match=match_nimokakawarazu)
RULES = (RULE,)
