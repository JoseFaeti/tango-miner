"""Noun + を通して / を通じて: through or throughout."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OTOOSHITE_OTSUUJITE_EXPRESSION = '〜を通して / 〜を通じて'


def match_otooshite_otsuujite(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or (verb['surface'], verb['lemma']) not in (
                    ('通し', '通す'), ('とおし', '通す'),
                    ('通じ', '通じる'), ('通じ', '通ずる'),
                    ('つうじ', '通じる'), ('つうじ', '通ずる'))
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, particle) or not adjacent(particle, verb)):
            continue
        for suffix in ((('て', '助詞'),), (('まし', '助動詞'), ('て', '助詞'))):
            tail = tokens[i + 2:i + 2 + len(suffix)]
            if (len(tail) == len(suffix)
                    and [(t['surface'], t['pos'][0]) for t in tail] == list(suffix)
                    and all(adjacent(a, b) for a, b in zip([verb] + tail, tail))):
                yield particle['span'][0], tail[-1]['span'][1]
                break


RULE = GrammarRule(order=7, id='otooshite_otsuujite',
                   expression=OTOOSHITE_OTSUUJITE_EXPRESSION,
                   reading='をとおして / をつうじて',
                   meaning='through; by means of; throughout',
                   match=match_otooshite_otsuujite)

RULES = (RULE,)
