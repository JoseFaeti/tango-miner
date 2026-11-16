"""Noun + をもって: by means of or as of, with lexical ambiguity."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

OMOTSUTE_EXPRESSION = '〜をもって'


def match_omotsute(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['surface'] != 'を' or particle['pos'][:1] != ['助詞']
                or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, particle) or not adjacent(particle, verb)):
            continue
        if (verb['surface'], verb['lemma']) in (('もっ', 'もつ'), ('持っ', '持つ'), ('以', '以つ')):
            suffix = (('て', '助詞'),)
        elif (verb['surface'], verb['lemma']) in (('もち', 'もつ'), ('持ち', '持つ')):
            suffix = (('まし', '助動詞'), ('て', '助詞'))
        else:
            continue
        tail = tokens[i + 2:i + 2 + len(suffix)]
        if (len(tail) == len(suffix)
                and [(t['surface'], t['pos'][0]) for t in tail] == list(suffix)
                and all(adjacent(a, b) for a, b in zip([verb] + tail, tail))):
            yield particle['span'][0], tail[-1]['span'][1]


RULE = GrammarRule(order=-1, id='omotsute', expression=OMOTSUTE_EXPRESSION,
                   reading='をもって', meaning='with; by means of; as of',
                   match=match_omotsute)
RULES = (RULE,)
