"""Noun + と相まって: combined with."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

TOAIMATSUTE_EXPRESSION = '〜と相まって'


def match_toaimatsute(tokens):
    for i in range(1, len(tokens) - 3):
        noun, particle, prefix, verb, suffix = tokens[i - 1:i + 4]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'と' or particle['pos'][:1] != ['助詞']
                or prefix['surface'] not in ('相', 'あい') or prefix['pos'][:1] != ['接頭辞']
                or (verb['surface'], verb['lemma']) not in (('まっ', 'まつ'), ('俟っ', '俟つ'))
                or verb['pos'][:1] != ['動詞']
                or suffix['surface'] != 'て' or suffix['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in zip(
                    (noun, particle, prefix, verb), (particle, prefix, verb, suffix)))):
            continue
        yield particle['span'][0], suffix['span'][1]


RULE = GrammarRule(order=-1, id='toaimatsute', expression=TOAIMATSUTE_EXPRESSION,
                   reading='とあいまって', meaning='combined with; coupled with',
                   match=match_toaimatsute)
RULES = (RULE,)
