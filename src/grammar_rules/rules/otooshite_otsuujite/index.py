"""Noun + を通して / を通じて: through or throughout."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


OTOOSHITE_OTSUUJITE_EXPRESSION = '〜を通して / 〜を通じて'


def match_otooshite_otsuujite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, verb, te = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (
                    ('通し', '通す'), ('とおし', '通す'),
                    ('通じ', '通じる'), ('通じ', '通ずる'),
                    ('つうじ', '通じる'), ('つうじ', '通ずる'))
                or verb['pos'][:1] != ['動詞']
                or te['surface'] != 'て' or te['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, particle, verb), (particle, verb, te)))):
            continue
        yield particle['span'][0], te['span'][1]


RULE = GrammarRule(order=7, id='otooshite_otsuujite',
                   expression=OTOOSHITE_OTSUUJITE_EXPRESSION,
                   reading='をとおして / をつうじて',
                   meaning='through; by means of; throughout',
                   match=match_otooshite_otsuujite)

RULES = (RULE,)
