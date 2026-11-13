"""Noun + に代わって / の代わりに: substitution or representation."""
from src.GrammarConstants import NIKAWATTE_NOKAWARINI_EXPRESSION
from src.grammar_rules.base import GrammarRule
from src.grammar_rules.helpers import adjacent


def match_nikawatte_nokawarini(tokens):
    for i in range(1, len(tokens) - 2):
        noun, particle, head, tail = tokens[i - 1:i + 3]
        if (noun['pos'][:1] not in (['名詞'], ['代名詞'])
                or particle['pos'][:1] != ['助詞']
                or not all(adjacent(a, b) for a, b in
                           zip((noun, particle, head), (particle, head, tail)))):
            continue
        if (particle['surface'] == 'に'
                and head['surface'] in ('代わっ', 'かわっ')
                and head['lemma'] in ('代わる', 'かわる')
                and head['pos'][:1] == ['動詞']
                and tail['surface'] == 'て' and tail['pos'][:1] == ['助詞']):
            yield particle['span'][0], tail['span'][1]
        elif (particle['surface'] == 'の'
              and head['surface'] in ('代わり', 'かわり')
              and head['lemma'] in ('代わり', 'かわり')
              and head['pos'][:1] == ['名詞']
              and tail['surface'] == 'に' and tail['pos'][:1] == ['助詞']):
            yield particle['span'][0], tail['span'][1]


RULE = GrammarRule(id='nikawatte_nokawarini', expression=NIKAWATTE_NOKAWARINI_EXPRESSION,
                   reading='にかわって / のかわりに',
                   meaning='in place of; on behalf of', match=match_nikawatte_nokawarini)
