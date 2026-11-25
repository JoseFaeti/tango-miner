"""Concessive noun + を押し切って with bounded obstacle contexts."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

# Other objects commonly select literal pushing; keep them lexical for review.
OBSTACLES = frozenset(('反対', '抵抗', '批判', '反発', '異論', '異議', '制止'))


def match_ooshikitsute(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] != ['名詞']
                or not ({noun['surface'], noun['lemma']} & OBSTACLES)
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (
                    ('押し切っ', '押し切る'), ('おしきっ', 'おしきる'))
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, wo, verb), (wo, verb, te)))):
            continue
        yield wo['span'][0], te['span'][1]


RULE = GrammarRule(order=1, id='ooshikitsute', expression='〜を押し切って',
                   reading='をおしきって', meaning='overriding; despite objections or resistance',
                   match=match_ooshikitsute)
RULES = (RULE,)
