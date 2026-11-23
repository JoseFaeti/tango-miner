"""Noun + に比する: compare with, in formal language."""
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent

NIHISURU_EXPRESSION = '〜に比する'


def match_nihisuru(tokens):
    for i in range(1, len(tokens) - 1):
        noun, ni, verb = tokens[i - 1:i + 2]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or ni['surface'] != 'に' or ni['pos'][:1] != ['助詞']
                or verb['lemma'] != '比する' or verb['pos'][:1] != ['動詞']
                or not adjacent(noun, ni) or not adjacent(ni, verb)):
            continue
        if verb['surface'] == '比する':
            yield ni['span'][0], verb['span'][1]
        elif verb['surface'] == '比し':
            if i + 2 < len(tokens) and adjacent(verb, tokens[i + 2]):
                tail = tokens[i + 2]
                if (tail['surface'], tail['pos'][:1]) in (('て', ['助詞']), ('た', ['助動詞'])):
                    yield ni['span'][0], tail['span'][1]
                    continue
                if tail['pos'][:1] in (['助詞'], ['助動詞']):
                    continue
            yield ni['span'][0], verb['span'][1]


RULE = GrammarRule(order=6, id='nihisuru', expression=NIHISURU_EXPRESSION,
                   reading='にひする', meaning='compared with; compare to (formal)',
                   match=match_nihisuru)

RULES = (RULE,)
