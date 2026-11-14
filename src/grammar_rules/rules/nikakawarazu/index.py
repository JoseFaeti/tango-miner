"""〜にかかわらず: regardless of the preceding condition."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIKAKAWARAZU_EXPRESSION = '〜にかかわらず'


def match_nikakawarazu(tokens):
    for i in range(1, len(tokens) - 2):
        previous, ni, verb, zu = tokens[i - 1:i + 3]
        if (previous['pos'][:1] == ['補助記号']
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['surface'] not in ('かかわら', '関わら', '拘わら')
                or verb['lemma'] not in ('かかわる', '関わる', '拘わる')
                or verb['pos'][:1] != ['動詞']
                or zu['surface'] != 'ず' or zu['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, ni, verb), (ni, verb, zu)))):
            continue
        # ずに can express the literal negative of 関わる.
        if (i + 3 < len(tokens) and adjacent(zu, tokens[i + 3])
                and tokens[i + 3]['surface'] == 'に'
                and tokens[i + 3]['pos'][:1] == ['助詞']):
            continue
        yield ni['span'][0], zu['span'][1]


RULE = GrammarRule(order=1, id='nikakawarazu', expression=NIKAKAWARAZU_EXPRESSION,
                   reading='にかかわらず', meaning='regardless of; irrespective of',
                   match=match_nikakawarazu)
RULES = (RULE,)
