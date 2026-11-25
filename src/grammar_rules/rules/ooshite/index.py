"""Concessive noun + を押して with bounded obstacle contexts."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

# Other objects commonly select literal pushing; keep them lexical for review.
OBSTACLES = frozenset(('病', '病気', '風邪', '熱', '高熱', '怪我', 'けが',
                       '反対', '抵抗', '無理', '悪天候', '困難'))


def match_ooshite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, wo, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] != ['名詞']
                or not ({noun['surface'], noun['lemma']} & OBSTACLES)
                or wo['surface'] != 'を' or wo['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (
                    ('押し', '押す'), ('おし', '押す'), ('おし', 'おす'))
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, wo, verb), (wo, verb, te)))):
            continue
        yield wo['span'][0], te['span'][1]


RULE = GrammarRule(order=1, id='ooshite', expression='〜を押して',
                   reading='をおして', meaning='despite; in spite of; braving',
                   match=match_ooshite)
RULES = (RULE,)
