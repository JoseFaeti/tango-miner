"""〜を構わず: without regard for the preceding condition."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_okamawazu(tokens):
    for i in range(1, len(tokens) - 2):
        previous, wo, verb, zu = tokens[i - 1:i + 3]
        if (previous['surface'].isspace()
                or previous['pos'][:1] == ['補助記号']
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or verb['surface'] not in ('かまわ', '構わ')
                or verb['lemma'] not in ('かまう', '構う')
                or verb['pos'][:1] != ['動詞']
                or zu['surface'] != 'ず' or zu['pos'][:1] != ['助動詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (previous, wo, verb), (wo, verb, zu)))):
            continue
        yield wo['span'][0], zu['span'][1]


RULE = GrammarRule(order=1, id='okamawazu', expression='〜を構わず',
                   reading='をかまわず', meaning='regardless of; without regard for',
                   match=match_okamawazu)
RULES = (RULE,)
